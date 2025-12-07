use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_array_reverse(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("ARRAY_REVERSE", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        crate::functions::array::array_reverse(val)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    fn schema() -> Schema {
        Schema::from_fields(vec![Field::nullable(
            "arr",
            DataType::Array(Box::new(DataType::Int64)),
        )])
    }

    #[test]
    fn reverses_array_elements() {
        let batch = create_batch(
            schema(),
            vec![vec![Value::array(vec![
                Value::int64(1),
                Value::int64(2),
                Value::int64(3),
            ])]],
        );

        let result =
            ProjectionWithExprExec::evaluate_array_reverse(&[Expr::column("arr")], &batch, 0)
                .expect("success");

        assert_eq!(
            result,
            Value::array(vec![Value::int64(3), Value::int64(2), Value::int64(1)])
        );
    }

    #[test]
    fn propagates_null_array() {
        let batch = create_batch(schema(), vec![vec![Value::null()]]);

        let result =
            ProjectionWithExprExec::evaluate_array_reverse(&[Expr::column("arr")], &batch, 0)
                .expect("success");

        assert_eq!(result, Value::null());
    }

    #[test]
    fn errors_when_argument_not_array() {
        let schema = Schema::from_fields(vec![Field::nullable("arr", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(1)]]);

        let err = ProjectionWithExprExec::evaluate_array_reverse(&[Expr::column("arr")], &batch, 0)
            .expect_err("expected array type mismatch");
        assert_error_contains(&err, "ARRAY");
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(schema(), vec![vec![Value::array(vec![Value::int64(1)])]]);

        let err = ProjectionWithExprExec::evaluate_array_reverse(&[], &batch, 0)
            .expect_err("missing argument should fail");
        assert_error_contains(&err, "ARRAY_REVERSE");
    }
}
