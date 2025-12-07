use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_array_contains(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("ARRAY_CONTAINS", args, 2)?;
        let arr_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let search_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        crate::functions::array::array_contains(&arr_val, &search_val)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;

    fn array_schema() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("arr", DataType::Array(Box::new(DataType::Int64))),
            Field::nullable("needle", DataType::Int64),
        ])
    }

    fn batch(schema: Schema, rows: Vec<Vec<Value>>) -> Table {
        Table::from_values(schema, rows).expect("record batch")
    }

    #[test]
    fn evaluate_array_contains_returns_true_when_found() {
        let schema = array_schema();
        let batch = batch(
            schema,
            vec![vec![
                Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]),
                Value::int64(2),
            ]],
        );

        let args = vec![Expr::column("arr"), Expr::column("needle")];
        let result =
            ProjectionWithExprExec::evaluate_array_contains(&args, &batch, 0).expect("success");

        assert_eq!(result, Value::bool_val(true));
    }

    #[test]
    fn evaluate_array_contains_returns_false_when_missing() {
        let schema = array_schema();
        let batch = batch(
            schema,
            vec![vec![Value::array(vec![Value::int64(1)]), Value::int64(42)]],
        );

        let args = vec![Expr::column("arr"), Expr::column("needle")];
        let result =
            ProjectionWithExprExec::evaluate_array_contains(&args, &batch, 0).expect("success");

        assert_eq!(result, Value::bool_val(false));
    }

    #[test]
    fn evaluate_array_contains_propagates_null_array() {
        let schema = array_schema();
        let batch = batch(schema, vec![vec![Value::null(), Value::int64(1)]]);
        let args = vec![Expr::column("arr"), Expr::column("needle")];
        let result =
            ProjectionWithExprExec::evaluate_array_contains(&args, &batch, 0).expect("success");

        assert_eq!(result, Value::null());
    }

    #[test]
    fn evaluate_array_contains_errors_on_non_array() {
        let schema = Schema::from_fields(vec![
            Field::nullable("arr", DataType::Int64),
            Field::nullable("needle", DataType::Int64),
        ]);
        let batch = batch(schema, vec![vec![Value::int64(1), Value::int64(1)]]);
        let args = vec![Expr::column("arr"), Expr::column("needle")];

        let err = ProjectionWithExprExec::evaluate_array_contains(&args, &batch, 0)
            .expect_err("should fail");
        assert!(
            err.to_string().contains("ARRAY"),
            "expected ARRAY type error, got {err}"
        );
    }

    #[test]
    fn evaluate_array_contains_validates_argument_count() {
        let schema = array_schema();
        let batch = batch(
            schema,
            vec![vec![Value::array(vec![Value::int64(1)]), Value::int64(1)]],
        );

        assert!(
            ProjectionWithExprExec::evaluate_array_contains(&[Expr::column("arr")], &batch, 0)
                .is_err(),
            "expected argument count validation error"
        );
    }
}
