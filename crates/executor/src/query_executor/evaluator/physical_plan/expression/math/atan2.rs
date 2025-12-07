use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_atan2(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("ATAN2", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;

        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        if let (Some(y), Some(x)) = (values[0].as_i64(), values[1].as_i64()) {
            return Ok(Value::float64((y as f64).atan2(x as f64)));
        }

        if let (Some(y), Some(x)) = (values[0].as_f64(), values[1].as_i64()) {
            return Ok(Value::float64(y.atan2(x as f64)));
        }

        if let (Some(y), Some(x)) = (values[0].as_i64(), values[1].as_f64()) {
            return Ok(Value::float64((y as f64).atan2(x)));
        }

        if let (Some(y), Some(x)) = (values[0].as_f64(), values[1].as_f64()) {
            return Ok(Value::float64(y.atan2(x)));
        }

        if let (Some(y), Some(x)) = (values[0].as_numeric(), values[1].as_i64()) {
            use rust_decimal::prelude::ToPrimitive;
            let yf = y.to_f64().unwrap_or(0.0);
            return Ok(Value::float64(yf.atan2(x as f64)));
        }

        if let (Some(y), Some(x)) = (values[0].as_numeric(), values[1].as_f64()) {
            use rust_decimal::prelude::ToPrimitive;
            let yf = y.to_f64().unwrap_or(0.0);
            return Ok(Value::float64(yf.atan2(x)));
        }

        if let (Some(y), Some(x)) = (values[0].as_numeric(), values[1].as_numeric()) {
            use rust_decimal::prelude::ToPrimitive;
            let yf = y.to_f64().unwrap_or(0.0);
            let xf = x.to_f64().unwrap_or(0.0);
            return Ok(Value::float64(yf.atan2(xf)));
        }

        if let (Some(y), Some(x)) = (values[0].as_i64(), values[1].as_numeric()) {
            use rust_decimal::prelude::ToPrimitive;
            let xf = x.to_f64().unwrap_or(0.0);
            return Ok(Value::float64((y as f64).atan2(xf)));
        }

        if let (Some(y), Some(x)) = (values[0].as_f64(), values[1].as_numeric()) {
            use rust_decimal::prelude::ToPrimitive;
            let xf = x.to_f64().unwrap_or(0.0);
            return Ok(Value::float64(y.atan2(xf)));
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
            Field::nullable("y", DataType::Float64),
            Field::nullable("x", DataType::Float64),
        ])
    }

    #[test]
    fn calculates_atan2_of_floats() {
        let batch = create_batch(
            schema_with_two_nums(),
            vec![vec![Value::float64(1.0), Value::float64(1.0)]],
        );
        let args = vec![Expr::column("y"), Expr::column("x")];
        let result = ProjectionWithExprExec::eval_atan2(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 0.7854).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn calculates_atan2_with_mixed_types() {
        let schema = Schema::from_fields(vec![
            Field::nullable("y", DataType::Int64),
            Field::nullable("x", DataType::Float64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::int64(1), Value::float64(1.0)]]);
        let args = vec![Expr::column("y"), Expr::column("x")];
        let result = ProjectionWithExprExec::eval_atan2(&args, &batch, 0).expect("success");
        if let Some(_) = result.as_f64() {
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn propagates_null_y() {
        let batch = create_batch(
            schema_with_two_nums(),
            vec![vec![Value::null(), Value::float64(1.0)]],
        );
        let args = vec![Expr::column("y"), Expr::column("x")];
        let result = ProjectionWithExprExec::eval_atan2(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_x() {
        let batch = create_batch(
            schema_with_two_nums(),
            vec![vec![Value::float64(1.0), Value::null()]],
        );
        let args = vec![Expr::column("y"), Expr::column("x")];
        let result = ProjectionWithExprExec::eval_atan2(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(1.0)]]);
        let err = ProjectionWithExprExec::eval_atan2(&[Expr::column("val")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "ATAN2");
    }
}
