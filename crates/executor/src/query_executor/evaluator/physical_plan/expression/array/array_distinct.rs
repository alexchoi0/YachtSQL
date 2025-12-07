use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_array_distinct(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("ARRAY_DISTINCT", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        crate::functions::array::array_distinct(val)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;

    fn schema() -> Schema {
        Schema::from_fields(vec![Field::nullable(
            "arr",
            DataType::Array(Box::new(DataType::Int64)),
        )])
    }

    fn batch(schema: Schema, rows: Vec<Vec<Value>>) -> Table {
        Table::from_values(schema, rows).expect("record batch build")
    }

    #[test]
    fn evaluate_array_distinct_removes_duplicates() {
        let schema = schema();
        let batch = batch(
            schema,
            vec![vec![Value::array(vec![
                Value::int64(1),
                Value::int64(2),
                Value::int64(1),
                Value::int64(3),
            ])]],
        );

        let result =
            ProjectionWithExprExec::evaluate_array_distinct(&[Expr::column("arr")], &batch, 0)
                .expect("success");

        assert_eq!(
            result,
            Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)])
        );
    }

    #[test]
    fn evaluate_array_distinct_passes_through_null() {
        let schema = schema();
        let batch = batch(schema, vec![vec![Value::null()]]);

        let result =
            ProjectionWithExprExec::evaluate_array_distinct(&[Expr::column("arr")], &batch, 0)
                .expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn evaluate_array_distinct_errors_on_non_array() {
        let schema = Schema::from_fields(vec![Field::nullable("arr", DataType::Int64)]);
        let batch = batch(schema, vec![vec![Value::int64(1)]]);
        let err =
            ProjectionWithExprExec::evaluate_array_distinct(&[Expr::column("arr")], &batch, 0)
                .expect_err("expected type mismatch");
        assert!(
            err.to_string().contains("ARRAY"),
            "expected ARRAY type error, got {err}"
        );
    }

    #[test]
    fn evaluate_array_distinct_validates_argument_count() {
        let schema = schema();
        let batch = batch(schema, vec![vec![Value::array(vec![Value::int64(1)])]]);

        assert!(
            ProjectionWithExprExec::evaluate_array_distinct(&[], &batch, 0).is_err(),
            "missing argument should fail validation"
        );
    }
}
