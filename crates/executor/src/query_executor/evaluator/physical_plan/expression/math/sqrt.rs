use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_sqrt(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("SQRT", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if val.is_null() {
            return Ok(Value::null());
        }

        if let Some(i) = val.as_i64() {
            if i < 0 {
                return Err(crate::error::Error::InvalidOperation(
                    "Cannot take square root of negative number".to_string(),
                ));
            } else {
                return Ok(Value::float64((i as f64).sqrt()));
            }
        }

        if let Some(f) = val.as_f64() {
            if f < 0.0 {
                return Err(crate::error::Error::InvalidOperation(
                    "Cannot take square root of negative number".to_string(),
                ));
            } else {
                return Ok(Value::float64(f.sqrt()));
            }
        }

        if let Some(d) = val.as_numeric() {
            use rust_decimal::prelude::ToPrimitive;
            let f = d
                .to_f64()
                .ok_or_else(|| crate::error::Error::TypeMismatch {
                    expected: "convertible to FLOAT64".to_string(),
                    actual: "NUMERIC".to_string(),
                })?;
            if f < 0.0 {
                return Err(crate::error::Error::InvalidOperation(
                    "Cannot take square root of negative number".to_string(),
                ));
            } else {
                return Ok(Value::float64(f.sqrt()));
            }
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
    fn calculates_sqrt_of_perfect_square() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(16.0)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_sqrt(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 4.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn calculates_sqrt_of_int() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(25)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_sqrt(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 5.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn calculates_sqrt_of_zero() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(0.0)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_sqrt(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!(f.abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn errors_on_negative_int() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(-4)]]);
        let args = vec![Expr::column("val")];
        let err = ProjectionWithExprExec::eval_sqrt(&args, &batch, 0).expect_err("negative");
        assert_error_contains(&err, "negative");
    }

    #[test]
    fn errors_on_negative_float() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(-4.0)]]);
        let args = vec![Expr::column("val")];
        let err = ProjectionWithExprExec::eval_sqrt(&args, &batch, 0).expect_err("negative");
        assert_error_contains(&err, "negative");
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_sqrt(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::eval_sqrt(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "SQRT");
    }
}
