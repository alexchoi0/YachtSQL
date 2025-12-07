use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_cos(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("COS", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }

        if let Some(i) = val.as_i64() {
            return Ok(Value::float64((i as f64).cos()));
        }

        if let Some(f) = val.as_f64() {
            return Ok(Value::float64(f.cos()));
        }

        if let Some(d) = val.as_numeric() {
            use rust_decimal::prelude::ToPrimitive;
            let f = d.to_f64().unwrap_or(0.0);
            return Ok(Value::float64(f.cos()));
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
    fn calculates_cos_of_zero() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(0.0)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_cos(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 1.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn calculates_cos_of_int() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(0)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_cos(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 1.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_cos(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::eval_cos(&[], &batch, 0).expect_err("missing argument");
        assert_error_contains(&err, "COS");
    }
}
