use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_array_concat(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let evaluated_args: Vec<Value> = args
            .iter()
            .map(|arg| Self::evaluate_expr(arg, batch, row_idx))
            .collect::<Result<Vec<_>>>()?;
        crate::functions::array::array_concat(&evaluated_args)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;

    fn schema_with_arrays() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("arr1", DataType::Array(Box::new(DataType::Int64))),
            Field::nullable("arr2", DataType::Array(Box::new(DataType::Int64))),
        ])
    }

    fn build_batch(schema: Schema, rows: Vec<Vec<Value>>) -> Table {
        Table::from_values(schema, rows).expect("record batch")
    }

    #[test]
    fn evaluate_array_concat_combines_multiple_arrays() {
        let schema = schema_with_arrays();
        let batch = build_batch(
            schema.clone(),
            vec![vec![
                Value::array(vec![Value::int64(1), Value::int64(2)]),
                Value::array(vec![Value::int64(3), Value::int64(4)]),
            ]],
        );

        let args = vec![Expr::column("arr1"), Expr::column("arr2")];
        let result =
            ProjectionWithExprExec::evaluate_array_concat(&args, &batch, 0).expect("success");

        assert_eq!(
            result,
            Value::array(vec![
                Value::int64(1),
                Value::int64(2),
                Value::int64(3),
                Value::int64(4)
            ])
        );
    }

    #[test]
    fn evaluate_array_concat_propagates_null_argument() {
        let schema = schema_with_arrays();
        let batch = build_batch(
            schema,
            vec![vec![Value::array(vec![Value::int64(1)]), Value::null()]],
        );

        let args = vec![Expr::column("arr1"), Expr::column("arr2")];
        let result =
            ProjectionWithExprExec::evaluate_array_concat(&args, &batch, 0).expect("success");

        assert_eq!(result, Value::null());
    }

    #[test]
    fn evaluate_array_concat_errors_on_non_array() {
        let schema = Schema::from_fields(vec![
            Field::nullable("not_array", DataType::Int64),
            Field::nullable("arr", DataType::Array(Box::new(DataType::Int64))),
        ]);
        let batch = build_batch(
            schema,
            vec![vec![Value::int64(1), Value::array(vec![Value::int64(2)])]],
        );

        let args = vec![Expr::column("not_array"), Expr::column("arr")];
        let err = ProjectionWithExprExec::evaluate_array_concat(&args, &batch, 0)
            .expect_err("type mismatch expected");
        assert!(
            err.to_string().contains("ARRAY"),
            "expected ARRAY mismatch error, got {err}"
        );
    }

    #[test]
    fn evaluate_array_concat_handles_zero_arguments() {
        let schema = Schema::new();
        let batch = Table::empty(schema);
        let result =
            ProjectionWithExprExec::evaluate_array_concat(&[], &batch, 0).expect("success");
        assert_eq!(result, Value::array(Vec::new()));
    }
}
