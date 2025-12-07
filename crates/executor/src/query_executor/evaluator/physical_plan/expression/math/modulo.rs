use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_mod(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("MOD", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;

        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        if let (Some(a), Some(b)) = (values[0].as_i64(), values[1].as_i64()) {
            if b == 0 {
                return Err(crate::error::Error::ExecutionError(
                    "division by zero".to_string(),
                ));
            }
            return Ok(Value::int64(a % b));
        }

        if let (Some(a), Some(b)) = (values[0].as_f64(), values[1].as_f64()) {
            if b == 0.0 {
                return Err(crate::error::Error::ExecutionError(
                    "division by zero".to_string(),
                ));
            }
            return Ok(Value::float64(a % b));
        }

        if let (Some(a), Some(b)) = (values[0].as_i64(), values[1].as_f64()) {
            if b == 0.0 {
                return Err(crate::error::Error::ExecutionError(
                    "division by zero".to_string(),
                ));
            }
            return Ok(Value::float64((a as f64) % b));
        }

        if let (Some(a), Some(b)) = (values[0].as_f64(), values[1].as_i64()) {
            if b == 0 {
                return Err(crate::error::Error::ExecutionError(
                    "division by zero".to_string(),
                ));
            }
            return Ok(Value::float64(a % (b as f64)));
        }

        if let (Some(a), Some(b)) = (values[0].as_numeric(), values[1].as_numeric()) {
            if b.is_zero() {
                return Err(crate::error::Error::ExecutionError(
                    "division by zero".to_string(),
                ));
            }
            return Ok(Value::numeric(a % b));
        }

        if let (Some(a), Some(b)) = (values[0].as_numeric(), values[1].as_i64()) {
            if b == 0 {
                return Err(crate::error::Error::ExecutionError(
                    "division by zero".to_string(),
                ));
            }
            use rust_decimal::prelude::FromPrimitive;
            let b_dec = rust_decimal::Decimal::from_i64(b).unwrap_or_default();
            return Ok(Value::numeric(a % b_dec));
        }

        if let (Some(a), Some(b)) = (values[0].as_i64(), values[1].as_numeric()) {
            use rust_decimal::prelude::FromPrimitive;
            if b.is_zero() {
                return Err(crate::error::Error::ExecutionError(
                    "division by zero".to_string(),
                ));
            }
            let a_dec = rust_decimal::Decimal::from_i64(a).unwrap_or_default();
            return Ok(Value::numeric(a_dec % b));
        }

        if let (Some(a), Some(b)) = (values[0].as_numeric(), values[1].as_f64()) {
            use rust_decimal::prelude::ToPrimitive;
            if b == 0.0 {
                return Err(crate::error::Error::ExecutionError(
                    "division by zero".to_string(),
                ));
            }
            let a_f = a.to_f64().unwrap_or(0.0);
            return Ok(Value::float64(a_f % b));
        }

        if let (Some(a), Some(b)) = (values[0].as_f64(), values[1].as_numeric()) {
            use rust_decimal::prelude::ToPrimitive;
            if b.is_zero() {
                return Err(crate::error::Error::ExecutionError(
                    "division by zero".to_string(),
                ));
            }
            let b_f = b.to_f64().unwrap_or(0.0);
            return Ok(Value::float64(a % b_f));
        }

        Err(crate::error::Error::TypeMismatch {
            expected: "NUMERIC, NUMERIC".to_string(),
            actual: format!("{}, {}", values[0].data_type(), values[1].data_type()),
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

    fn schema_with_two_ints() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::Int64),
        ])
    }

    #[test]
    fn calculates_modulo_of_ints() {
        let batch = create_batch(
            schema_with_two_ints(),
            vec![vec![Value::int64(10), Value::int64(3)]],
        );
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result = ProjectionWithExprExec::eval_mod(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(1));
    }

    #[test]
    fn calculates_modulo_of_floats() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Float64),
            Field::nullable("b", DataType::Float64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::float64(10.5), Value::float64(3.0)]],
        );
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result = ProjectionWithExprExec::eval_mod(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 1.5).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn errors_on_division_by_zero() {
        let batch = create_batch(
            schema_with_two_ints(),
            vec![vec![Value::int64(10), Value::int64(0)]],
        );
        let args = vec![Expr::column("a"), Expr::column("b")];
        let err = ProjectionWithExprExec::eval_mod(&args, &batch, 0).expect_err("divide by zero");
        assert_error_contains(&err, "division by zero");
    }

    #[test]
    fn propagates_null_dividend() {
        let batch = create_batch(
            schema_with_two_ints(),
            vec![vec![Value::null(), Value::int64(3)]],
        );
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result = ProjectionWithExprExec::eval_mod(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_divisor() {
        let batch = create_batch(
            schema_with_two_ints(),
            vec![vec![Value::int64(10), Value::null()]],
        );
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result = ProjectionWithExprExec::eval_mod(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(10)]]);
        let err = ProjectionWithExprExec::eval_mod(&[Expr::column("val")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "MOD");
    }
}
