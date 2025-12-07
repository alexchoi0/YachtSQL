use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_array_length(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("ARRAY_LENGTH", args, 1)?;
        let arr_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        crate::functions::array::array_length(&arr_val)
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
    fn evaluate_array_length_returns_size() {
        let schema = schema();
        let batch = batch(
            schema,
            vec![vec![Value::array(vec![
                Value::int64(10),
                Value::int64(20),
                Value::int64(30),
            ])]],
        );

        let result =
            ProjectionWithExprExec::evaluate_array_length(&[Expr::column("arr")], &batch, 0)
                .expect("success");
        assert_eq!(result, Value::int64(3));
    }

    #[test]
    fn evaluate_array_length_handles_empty_array() {
        let schema = schema();
        let batch = batch(schema, vec![vec![Value::array(Vec::new())]]);
        let result =
            ProjectionWithExprExec::evaluate_array_length(&[Expr::column("arr")], &batch, 0)
                .expect("success");
        assert_eq!(result, Value::int64(0));
    }

    #[test]
    fn evaluate_array_length_propagates_null() {
        let schema = schema();
        let batch = batch(schema, vec![vec![Value::null()]]);
        let result =
            ProjectionWithExprExec::evaluate_array_length(&[Expr::column("arr")], &batch, 0)
                .expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn evaluate_array_length_errors_on_non_array() {
        let schema = Schema::from_fields(vec![Field::nullable("arr", DataType::Int64)]);
        let batch = batch(schema, vec![vec![Value::int64(1)]]);
        let err = ProjectionWithExprExec::evaluate_array_length(&[Expr::column("arr")], &batch, 0)
            .expect_err("type mismatch expected");
        assert!(
            err.to_string().contains("ARRAY"),
            "expected ARRAY type error, got {err}"
        );
    }

    #[test]
    fn evaluate_array_length_validates_argument_count() {
        let schema = schema();
        let batch = batch(schema, vec![vec![Value::array(vec![Value::int64(1)])]]);
        assert!(
            ProjectionWithExprExec::evaluate_array_length(&[], &batch, 0).is_err(),
            "expected arg-count validation failure"
        );
    }
}
