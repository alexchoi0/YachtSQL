use std::rc::Rc;

use yachtsql_core::error::Result;
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::Schema;

use super::{ExecutionPlan, ProjectionWithExprExec};
use crate::Table;

#[derive(Debug)]
pub struct DistinctExec {
    input: Rc<dyn ExecutionPlan>,

    schema: Schema,
}

impl DistinctExec {
    pub fn new(input: Rc<dyn ExecutionPlan>) -> Self {
        let schema = input.schema().clone();
        Self { input, schema }
    }
}

impl ExecutionPlan for DistinctExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        use std::collections::HashSet;

        let input_batches = self.input.execute()?;

        let merged_batch = if input_batches.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        } else if input_batches.len() == 1 {
            input_batches
                .into_iter()
                .next()
                .expect("checked len() == 1")
        } else {
            Table::concat(&input_batches)?
        };

        if merged_batch.is_empty() {
            return Ok(vec![merged_batch]);
        }

        let mut seen_rows: HashSet<Vec<String>> = HashSet::new();
        let mut unique_indices = Vec::new();

        for row_idx in 0..merged_batch.num_rows() {
            let mut row_key = Vec::new();
            for col in merged_batch.expect_columns() {
                let value = col.get(row_idx)?;

                row_key.push(format!("{:?}", value));
            }

            if seen_rows.insert(row_key) {
                unique_indices.push(row_idx);
            }
        }

        if unique_indices.is_empty() {
            Ok(vec![Table::empty(self.schema.clone())])
        } else {
            let distinct_columns: Vec<_> = merged_batch
                .expect_columns()
                .iter()
                .map(|col| col.gather(&unique_indices))
                .collect::<Result<Vec<_>>>()?;

            let distinct_batch = Table::new(self.schema.clone(), distinct_columns)?;
            Ok(vec![distinct_batch])
        }
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        "Distinct".to_string()
    }
}

#[derive(Debug)]
pub struct DistinctOnExec {
    input: Rc<dyn ExecutionPlan>,

    expressions: Vec<Expr>,

    schema: Schema,
}

impl DistinctOnExec {
    pub fn new(input: Rc<dyn ExecutionPlan>, expressions: Vec<Expr>) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            expressions,
            schema,
        }
    }

    fn compute_group_key(&self, batch: &Table, row_idx: usize) -> Result<Vec<String>> {
        let mut key = Vec::with_capacity(self.expressions.len());

        for expr in &self.expressions {
            let value = ProjectionWithExprExec::evaluate_expr(expr, batch, row_idx)?;
            key.push(format!("{:?}", value));
        }

        Ok(key)
    }
}

impl ExecutionPlan for DistinctOnExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        use std::collections::HashSet;

        let input_batches = self.input.execute()?;

        if input_batches.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let merged_batch = if input_batches.len() == 1 {
            input_batches
                .into_iter()
                .next()
                .expect("checked len() == 1")
        } else {
            Table::concat(&input_batches)?
        };

        if merged_batch.num_rows() == 0 {
            return Ok(vec![merged_batch]);
        }

        let mut selected_rows = Vec::new();
        let mut seen_keys = HashSet::new();

        for row_idx in 0..merged_batch.num_rows() {
            let key = self.compute_group_key(&merged_batch, row_idx)?;

            if seen_keys.insert(key) {
                selected_rows.push(row_idx);
            }
        }

        if selected_rows.is_empty() {
            Ok(vec![Table::empty(self.schema.clone())])
        } else {
            let output_columns: Vec<_> = merged_batch
                .expect_columns()
                .iter()
                .map(|col| col.gather(&selected_rows))
                .collect::<Result<Vec<_>>>()?;

            let output_batch = Table::new(self.schema.clone(), output_columns)?;
            Ok(vec![output_batch])
        }
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!("DistinctOn({:?})", self.expressions)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::BinaryOp;
    use yachtsql_optimizer::expr::{Expr, LiteralValue};
    use yachtsql_storage::Field;

    use super::*;

    #[derive(Debug)]
    struct MockPlan {
        schema: Schema,
        batches: Vec<Table>,
    }

    impl MockPlan {
        fn new(schema: Schema, batches: Vec<Table>) -> Self {
            Self { schema, batches }
        }
    }

    impl ExecutionPlan for MockPlan {
        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn execute(&self) -> Result<Vec<Table>> {
            Ok(self.batches.clone())
        }

        fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
            Vec::new()
        }

        fn describe(&self) -> String {
            "mock-plan".into()
        }
    }

    fn base_schema() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("category", DataType::String),
            Field::nullable("value", DataType::Int64),
        ])
    }

    fn batch_from_rows(schema: &Schema, rows: Vec<Vec<Value>>) -> Table {
        Table::from_values(schema.clone(), rows).expect("batch creation failed")
    }

    fn row(id: i64, category: &str, value: i64) -> Vec<Value> {
        vec![
            Value::int64(id),
            Value::string(category.to_string()),
            Value::int64(value),
        ]
    }

    #[test]
    fn distinct_exec_deduplicates_across_batches() {
        let schema = base_schema();
        let batches = vec![
            batch_from_rows(
                &schema,
                vec![row(1, "A", 10), row(2, "B", 20), row(2, "B", 20)],
            ),
            batch_from_rows(&schema, vec![row(1, "A", 10), row(3, "C", 30)]),
        ];

        let exec = DistinctExec::new(Rc::new(MockPlan::new(schema.clone(), batches)));
        let mut results = exec.execute().expect("distinct exec failed");
        assert_eq!(results.len(), 1);

        let batch = results.pop().unwrap();
        assert_eq!(batch.num_rows(), 3, "expected exactly three unique rows");

        let categories: Vec<String> = (0..batch.num_rows())
            .map(|idx| {
                let val = batch.expect_columns()[1].get(idx).unwrap();
                val.as_str().expect("expected string").to_string()
            })
            .collect();
        assert_eq!(categories, vec!["A", "B", "C"]);
    }

    #[test]
    fn distinct_exec_handles_empty_input() {
        let schema = base_schema();
        let exec = DistinctExec::new(Rc::new(MockPlan::new(schema.clone(), Vec::new())));

        let results = exec.execute().expect("distinct exec failed");
        assert_eq!(results.len(), 1);
        assert!(results[0].is_empty());
        assert_eq!(results[0].schema(), &schema);
    }

    #[test]
    fn distinct_on_exec_keeps_first_row_per_group() {
        let schema = base_schema();
        let batches = vec![batch_from_rows(
            &schema,
            vec![row(1, "A", 10), row(2, "A", 20), row(3, "B", 30)],
        )];

        let exec = DistinctOnExec::new(
            Rc::new(MockPlan::new(schema.clone(), batches)),
            vec![
                Expr::column("category"),
                Expr::literal(LiteralValue::Int64(42)),
            ],
        );

        let mut results = exec.execute().expect("distinct on exec failed");
        assert_eq!(results.len(), 1);

        let batch = results.pop().unwrap();
        assert_eq!(batch.num_rows(), 2, "first row per category expected");

        let ids: Vec<i64> = (0..batch.num_rows())
            .map(|idx| {
                let val = batch.expect_columns()[0].get(idx).unwrap();
                val.as_i64().expect("expected int64")
            })
            .collect();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn distinct_on_exec_supports_binary_expressions() {
        let schema = base_schema();
        let batches = vec![batch_from_rows(
            &schema,
            vec![
                row(1, "A", 10),
                row(2, "A", 20),
                row(3, "B", 30),
                row(4, "B", 40),
            ],
        )];
        let input = Rc::new(MockPlan::new(schema, batches));

        let exec = DistinctOnExec::new(
            input,
            vec![Expr::binary_op(
                Expr::column("category"),
                BinaryOp::Equal,
                Expr::literal(LiteralValue::String("A".into())),
            )],
        );

        let mut results = exec.execute().expect("execution should succeed");
        assert_eq!(results.len(), 1);

        let batch = results.pop().unwrap();

        assert_eq!(batch.num_rows(), 2, "one row per boolean result expected");
    }

    #[test]
    fn distinct_on_exec_supports_function_expressions() {
        use yachtsql_ir::function::FunctionName;

        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);
        let batches = vec![batch_from_rows(
            &schema,
            vec![
                vec![Value::int64(1), Value::string("Alice".to_string())],
                vec![Value::int64(2), Value::string("ALICE".to_string())],
                vec![Value::int64(3), Value::string("Bob".to_string())],
            ],
        )];
        let input = Rc::new(MockPlan::new(schema, batches));

        let exec = DistinctOnExec::new(
            input,
            vec![Expr::Function {
                name: FunctionName::from("LOWER"),
                args: vec![Expr::column("name")],
            }],
        );

        let mut results = exec.execute().expect("execution should succeed");
        assert_eq!(results.len(), 1);

        let batch = results.pop().unwrap();

        assert_eq!(batch.num_rows(), 2, "one row per lowercase name expected");

        let ids: Vec<i64> = (0..batch.num_rows())
            .map(|idx| {
                let val = batch.expect_columns()[0].get(idx).unwrap();
                val.as_i64().expect("expected int64")
            })
            .collect();
        assert_eq!(ids, vec![1, 3]);
    }
}
