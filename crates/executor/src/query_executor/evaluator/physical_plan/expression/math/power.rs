use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_power(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("POWER", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;

        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        if let (Some(base), Some(exp)) = (values[0].as_i64(), values[1].as_i64()) {
            return Ok(Value::float64((base as f64).powf(exp as f64)));
        }

        if let (Some(base), Some(exp)) = (values[0].as_f64(), values[1].as_i64()) {
            return Ok(Value::float64(base.powf(exp as f64)));
        }

        if let (Some(base), Some(exp)) = (values[0].as_i64(), values[1].as_f64()) {
            return Ok(Value::float64((base as f64).powf(exp)));
        }

        if let (Some(base), Some(exp)) = (values[0].as_f64(), values[1].as_f64()) {
            if base < 0.0 && exp.fract() != 0.0 {
                return Err(yachtsql_core::error::Error::InvalidQuery(
                    "POWER: negative base with fractional exponent produces complex result"
                        .to_string(),
                ));
            }
            return Ok(Value::float64(base.powf(exp)));
        }

        if let (Some(base), Some(exp)) = (values[0].as_numeric(), values[1].as_i64()) {
            use rust_decimal::prelude::ToPrimitive;
            let b = base.to_f64().unwrap_or(0.0);
            return Ok(Value::float64(b.powf(exp as f64)));
        }

        if let (Some(base), Some(exp)) = (values[0].as_numeric(), values[1].as_f64()) {
            use rust_decimal::prelude::ToPrimitive;
            let b = base.to_f64().unwrap_or(0.0);
            return Ok(Value::float64(b.powf(exp)));
        }

        if let (Some(base), Some(exp)) = (values[0].as_numeric(), values[1].as_numeric()) {
            use rust_decimal::prelude::ToPrimitive;
            let b = base.to_f64().unwrap_or(0.0);
            let e = exp.to_f64().unwrap_or(0.0);
            return Ok(Value::float64(b.powf(e)));
        }

        if let (Some(base), Some(exp)) = (values[0].as_i64(), values[1].as_numeric()) {
            use rust_decimal::prelude::ToPrimitive;
            let e = exp.to_f64().unwrap_or(0.0);
            return Ok(Value::float64((base as f64).powf(e)));
        }

        if let (Some(base), Some(exp)) = (values[0].as_f64(), values[1].as_numeric()) {
            use rust_decimal::prelude::ToPrimitive;
            let e = exp.to_f64().unwrap_or(0.0);
            return Ok(Value::float64(base.powf(e)));
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

    fn schema_with_two_nums() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("base", DataType::Float64),
            Field::nullable("exp", DataType::Float64),
        ])
    }

    #[test]
    fn calculates_power_of_ints() {
        let schema = Schema::from_fields(vec![
            Field::nullable("base", DataType::Int64),
            Field::nullable("exp", DataType::Int64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::int64(2), Value::int64(3)]]);
        let args = vec![Expr::column("base"), Expr::column("exp")];
        let result = ProjectionWithExprExec::eval_power(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 8.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn calculates_power_of_floats() {
        let batch = create_batch(
            schema_with_two_nums(),
            vec![vec![Value::float64(2.0), Value::float64(3.0)]],
        );
        let args = vec![Expr::column("base"), Expr::column("exp")];
        let result = ProjectionWithExprExec::eval_power(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 8.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn calculates_fractional_exponent() {
        let batch = create_batch(
            schema_with_two_nums(),
            vec![vec![Value::float64(4.0), Value::float64(0.5)]],
        );
        let args = vec![Expr::column("base"), Expr::column("exp")];
        let result = ProjectionWithExprExec::eval_power(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 2.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn propagates_null_base() {
        let batch = create_batch(
            schema_with_two_nums(),
            vec![vec![Value::null(), Value::float64(2.0)]],
        );
        let args = vec![Expr::column("base"), Expr::column("exp")];
        let result = ProjectionWithExprExec::eval_power(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_exponent() {
        let batch = create_batch(
            schema_with_two_nums(),
            vec![vec![Value::float64(2.0), Value::null()]],
        );
        let args = vec![Expr::column("base"), Expr::column("exp")];
        let result = ProjectionWithExprExec::eval_power(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(2.0)]]);
        let err = ProjectionWithExprExec::eval_power(&[Expr::column("val")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "POWER");
    }
}
