use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_sign(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_min_arg_count("SIGN", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }

        if let Some(i) = val.as_i64() {
            return Ok(Value::int64(if i > 0 {
                1
            } else if i < 0 {
                -1
            } else {
                0
            }));
        }

        if let Some(f) = val.as_f64() {
            return Ok(Value::int64(if f > 0.0 {
                1
            } else if f < 0.0 {
                -1
            } else {
                0
            }));
        }

        if let Some(d) = val.as_numeric() {
            return Ok(Value::int64(if d.is_sign_positive() && !d.is_zero() {
                1
            } else if d.is_sign_negative() {
                -1
            } else {
                0
            }));
        }

        Err(crate::error::Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: val.data_type().to_string(),
        })
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
    fn returns_one_for_positive_int() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(42)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_sign(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(1));
    }

    #[test]
    fn returns_negative_one_for_negative_int() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(-42)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_sign(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(-1));
    }

    #[test]
    fn returns_zero_for_zero() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(0)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_sign(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(0));
    }

    #[test]
    fn returns_one_for_positive_float() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(3.14)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_sign(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(1));
    }

    #[test]
    fn returns_negative_one_for_negative_float() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(-3.14)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_sign(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(-1));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_sign(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::eval_sign(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "SIGN");
    }
}
