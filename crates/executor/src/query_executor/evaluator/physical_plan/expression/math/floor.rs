use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_floor(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(crate::error::Error::invalid_query(
                "FLOOR requires 1 or 2 arguments".to_string(),
            ));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if val.is_null() {
            return Ok(Value::null());
        }

        let decimals = if args.len() == 2 {
            let d = Self::evaluate_expr(&args[1], batch, row_idx)?;
            if d.is_null() {
                return Ok(Value::null());
            }
            d.as_i64()
                .ok_or_else(|| crate::error::Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: d.data_type().to_string(),
                })?
        } else {
            0
        };

        if let Some(i) = val.as_i64() {
            if decimals >= 0 {
                return Ok(Value::int64(i));
            }
            let multiplier = 10_i64.pow((-decimals) as u32);
            return Ok(Value::int64((i / multiplier) * multiplier));
        }

        if let Some(f) = val.as_f64() {
            if decimals == 0 {
                return Ok(Value::float64(f.floor()));
            }
            let multiplier = 10_f64.powi(decimals as i32);
            let floored = (f * multiplier).floor() / multiplier;
            return Ok(Value::float64(floored));
        }

        if let Some(d) = val.as_numeric() {
            if decimals == 0 {
                return Ok(Value::numeric(d.floor()));
            }
            let f: f64 = d.to_string().parse().unwrap_or(0.0);
            let multiplier = 10_f64.powi(decimals as i32);
            let floored = (f * multiplier).floor() / multiplier;
            return Ok(Value::numeric(
                rust_decimal::Decimal::from_f64_retain(floored).unwrap_or_default(),
            ));
        }

        Err(crate::error::Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: val.data_type().to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    #[test]
    fn rounds_down_positive_float() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(3.99)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_floor(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::float64(3.0));
    }

    #[test]
    fn rounds_down_negative_float() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(-3.14)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_floor(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::float64(-4.0));
    }

    #[test]
    fn returns_int_unchanged() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(42)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_floor(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(42));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_floor(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::eval_floor(&[], &batch, 0).expect_err("missing argument");
        assert_error_contains(&err, "FLOOR");
    }
}
