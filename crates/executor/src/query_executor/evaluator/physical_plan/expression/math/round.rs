use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_round(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(crate::error::Error::invalid_query(
                "ROUND requires 1 or 2 arguments (value, [decimals])".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let decimals = if args.len() == 2 {
            let decimals_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
            if decimals_val.is_null() {
                return Ok(Value::null());
            }
            if let Some(d) = decimals_val.as_i64() {
                d as i32
            } else {
                return Err(crate::error::Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: decimals_val.data_type().to_string(),
                });
            }
        } else {
            0
        };

        if val.is_null() {
            return Ok(Value::null());
        }

        if let Some(i) = val.as_i64() {
            if decimals >= 0 {
                Ok(Value::int64(i))
            } else {
                let factor = 10_i64.pow((-decimals) as u32);
                Ok(Value::int64(
                    ((i as f64 / factor as f64).round() as i64) * factor,
                ))
            }
        } else if let Some(f) = val.as_f64() {
            let factor = 10_f64.powi(decimals);
            Ok(Value::float64((f * factor).round() / factor))
        } else if let Some(d) = val.as_numeric() {
            Ok(Value::numeric(d.round_dp(decimals.max(0) as u32)))
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "NUMERIC".to_string(),
                actual: val.data_type().to_string(),
            })
        }
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
    fn rounds_float_to_zero_decimals_by_default() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(3.7)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_round(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 4.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn rounds_float_to_specified_decimals() {
        let schema = Schema::from_fields(vec![
            Field::nullable("val", DataType::Float64),
            Field::nullable("decimals", DataType::Int64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::float64(3.12131), Value::int64(2)]]);
        let args = vec![Expr::column("val"), Expr::column("decimals")];
        let result = ProjectionWithExprExec::eval_round(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 3.12).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn rounds_int_with_negative_decimals() {
        let schema = Schema::from_fields(vec![
            Field::nullable("val", DataType::Int64),
            Field::nullable("decimals", DataType::Int64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::int64(1234), Value::int64(-2)]]);
        let args = vec![Expr::column("val"), Expr::column("decimals")];
        let result = ProjectionWithExprExec::eval_round(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(1200));
    }

    #[test]
    fn propagates_null_value() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_round(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_decimals() {
        let schema = Schema::from_fields(vec![
            Field::nullable("val", DataType::Float64),
            Field::nullable("decimals", DataType::Int64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::float64(3.14), Value::null()]]);
        let args = vec![Expr::column("val"), Expr::column("decimals")];
        let result = ProjectionWithExprExec::eval_round(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::eval_round(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "ROUND");
    }
}
