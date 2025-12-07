use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_log(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() == 1 {
            let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
            Self::calculate_natural_log(val)
        } else if args.len() == 2 {
            let base_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
            let value_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

            let base = match Self::value_to_f64(&base_val)? {
                Some(b) => b,
                None => return Ok(Value::null()),
            };

            let value = match Self::value_to_f64(&value_val)? {
                Some(v) => v,
                None => return Ok(Value::null()),
            };

            Self::validate_positive(base, "LOG base")?;
            if (base - 1.0).abs() < 1e-10 {
                return Err(crate::error::Error::InvalidOperation(
                    "LOG base cannot be 1".to_string(),
                ));
            }

            Self::validate_positive(value, "LOG value")?;

            let result = value.ln() / base.ln();
            Ok(Value::float64(result))
        } else {
            Err(crate::error::Error::InvalidOperation(format!(
                "LOG function requires 1 or 2 arguments, got {}",
                args.len()
            )))
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
    fn calculates_natural_log_with_one_arg() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(std::f64::consts::E)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_log(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 1.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn calculates_log_with_custom_base() {
        let schema = Schema::from_fields(vec![
            Field::nullable("base", DataType::Float64),
            Field::nullable("value", DataType::Float64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::float64(10.0), Value::float64(100.0)]],
        );
        let args = vec![Expr::column("base"), Expr::column("value")];
        let result = ProjectionWithExprExec::eval_log(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 2.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn errors_on_base_equal_to_one() {
        let schema = Schema::from_fields(vec![
            Field::nullable("base", DataType::Float64),
            Field::nullable("value", DataType::Float64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::float64(1.0), Value::float64(10.0)]],
        );
        let args = vec![Expr::column("base"), Expr::column("value")];
        let err = ProjectionWithExprExec::eval_log(&args, &batch, 0).expect_err("invalid base");
        assert_error_contains(&err, "base cannot be 1");
    }

    #[test]
    fn propagates_null_with_one_arg() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_log(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_base() {
        let schema = Schema::from_fields(vec![
            Field::nullable("base", DataType::Float64),
            Field::nullable("value", DataType::Float64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::null(), Value::float64(10.0)]]);
        let args = vec![Expr::column("base"), Expr::column("value")];
        let result = ProjectionWithExprExec::eval_log(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::eval_log(&[], &batch, 0).expect_err("no arguments");
        assert_error_contains(&err, "LOG");
        assert_error_contains(&err, "1 or 2 arguments");
    }
}
