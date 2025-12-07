use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_abs(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("ABS", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if val.is_null() {
            return Ok(Value::null());
        }

        if let Some(i) = val.as_i64() {
            return Ok(Value::int64(i.abs()));
        }

        if let Some(f) = val.as_f64() {
            return Ok(Value::float64(f.abs()));
        }

        if let Some(d) = val.as_numeric() {
            return Ok(Value::numeric(d.abs()));
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
    fn returns_absolute_value_of_positive_int() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(42)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_abs(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(42));
    }

    #[test]
    fn returns_absolute_value_of_negative_int() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(-42)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_abs(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(42));
    }

    #[test]
    fn returns_absolute_value_of_float() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(-3.14)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_abs(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::float64(3.14));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_abs(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::eval_abs(&[], &batch, 0).expect_err("missing argument");
        assert_error_contains(&err, "ABS");
    }

    #[test]
    fn errors_on_type_mismatch() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("text".into())]]);
        let args = vec![Expr::column("val")];
        let err = ProjectionWithExprExec::eval_abs(&args, &batch, 0).expect_err("type mismatch");
        assert_error_contains(&err, "NUMERIC");
    }
}
