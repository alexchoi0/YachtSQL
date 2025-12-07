use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_object(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if !args.len().is_multiple_of(2) {
            return Err(Error::invalid_query(
                "JSON_OBJECT requires an even number of arguments (key-value pairs)".to_string(),
            ));
        }

        let mut pairs = Vec::new();
        for chunk in args.chunks(2) {
            let key_val = Self::evaluate_expr(&chunk[0], batch, row_idx)?;
            let key_str = key_val
                .as_str()
                .ok_or_else(|| Error::invalid_query("JSON_OBJECT keys must be strings"))?
                .to_string();
            let value = Self::evaluate_expr(&chunk[1], batch, row_idx)?;
            pairs.push((key_str, value));
        }
        yachtsql_functions::json::builder::json_object(pairs)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    #[test]
    fn constructs_empty_object() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let args = vec![];
        let result =
            ProjectionWithExprExec::evaluate_json_object(&args, &batch, 0).expect("success");
        if let Some(val) = result.as_json() {
            assert_eq!(*val, json!({}))
        } else {
            panic!("Expected JSON")
        }
    }

    #[test]
    fn constructs_object_with_single_pair() {
        let schema = Schema::from_fields(vec![
            Field::nullable("k", DataType::String),
            Field::nullable("v", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("name".into()), Value::int64(42)]],
        );
        let args = vec![Expr::column("k"), Expr::column("v")];
        let result =
            ProjectionWithExprExec::evaluate_json_object(&args, &batch, 0).expect("success");
        if let Some(val) = result.as_json() {
            assert_eq!(*val, json!({"name": 42}))
        } else {
            panic!("Expected JSON")
        }
    }

    #[test]
    fn constructs_object_with_multiple_pairs() {
        let schema = Schema::from_fields(vec![
            Field::nullable("k1", DataType::String),
            Field::nullable("v1", DataType::String),
            Field::nullable("k2", DataType::String),
            Field::nullable("v2", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("name".into()),
                Value::string("John".into()),
                Value::string("age".into()),
                Value::int64(30),
            ]],
        );
        let args = vec![
            Expr::column("k1"),
            Expr::column("v1"),
            Expr::column("k2"),
            Expr::column("v2"),
        ];
        let result =
            ProjectionWithExprExec::evaluate_json_object(&args, &batch, 0).expect("success");
        if let Some(val) = result.as_json() {
            assert_eq!(*val, json!({"name": "John", "age": 30}))
        } else {
            panic!("Expected JSON")
        }
    }

    #[test]
    fn errors_on_odd_number_of_arguments() {
        let schema = Schema::from_fields(vec![Field::nullable("k", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("key".into())]]);
        let err = ProjectionWithExprExec::evaluate_json_object(&[Expr::column("k")], &batch, 0)
            .expect_err("odd args");
        assert_error_contains(&err, "even number");
    }
}
