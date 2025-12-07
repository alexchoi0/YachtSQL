use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_safe_negate(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(crate::error::Error::invalid_query(
                "SAFE_NEGATE requires exactly 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        crate::query_executor::execution::safe_negate(&val)
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

    #[test]
    fn negates_positive_int() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(42)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_safe_negate(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(-42));
    }

    #[test]
    fn negates_negative_int() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(-42)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_safe_negate(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(42));
    }

    #[test]
    fn negates_float() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(3.14)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_safe_negate(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f + 3.14).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn returns_null_on_int_overflow() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(i64::MIN)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_safe_negate(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_safe_negate(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err =
            ProjectionWithExprExec::eval_safe_negate(&[], &batch, 0).expect_err("missing argument");
        assert_error_contains(&err, "SAFE_NEGATE");
    }
}
