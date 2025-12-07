use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_safe_add(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(crate::error::Error::invalid_query(
                "SAFE_ADD requires exactly 2 arguments".to_string(),
            ));
        }
        let a = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let b = Self::evaluate_expr(&args[1], batch, row_idx)?;
        crate::query_executor::execution::safe_add(&a, &b)
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

    fn schema_with_two_ints() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::Int64),
        ])
    }

    #[test]
    fn adds_ints_without_overflow() {
        let batch = create_batch(
            schema_with_two_ints(),
            vec![vec![Value::int64(100), Value::int64(200)]],
        );
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result = ProjectionWithExprExec::eval_safe_add(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(300));
    }

    #[test]
    fn adds_floats() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Float64),
            Field::nullable("b", DataType::Float64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::float64(1.5), Value::float64(2.5)]]);
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result = ProjectionWithExprExec::eval_safe_add(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 4.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn returns_null_on_int_overflow() {
        let batch = create_batch(
            schema_with_two_ints(),
            vec![vec![Value::int64(i64::MAX), Value::int64(1)]],
        );
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result = ProjectionWithExprExec::eval_safe_add(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_first_arg() {
        let batch = create_batch(
            schema_with_two_ints(),
            vec![vec![Value::null(), Value::int64(10)]],
        );
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result = ProjectionWithExprExec::eval_safe_add(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_second_arg() {
        let batch = create_batch(
            schema_with_two_ints(),
            vec![vec![Value::int64(10), Value::null()]],
        );
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result = ProjectionWithExprExec::eval_safe_add(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(10)]]);
        let err = ProjectionWithExprExec::eval_safe_add(&[Expr::column("val")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "SAFE_ADD");
    }
}
