use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_length(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("LENGTH", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        crate::functions::scalar::eval_length(&val)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_octet_length(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("OCTET_LENGTH", args, 1)?;
        Self::evaluate_byte_length_impl(&args[0], batch, row_idx)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_byte_length(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("BYTE_LENGTH", args, 1)?;
        Self::evaluate_byte_length_impl(&args[0], batch, row_idx)
    }

    fn evaluate_byte_length_impl(arg: &Expr, batch: &Table, row_idx: usize) -> Result<Value> {
        let val = Self::evaluate_expr(arg, batch, row_idx)?;

        if val.is_null() {
            return Ok(Value::null());
        }

        if let Some(s) = val.as_str() {
            return Ok(Value::int64(s.len() as i64));
        }

        if let Some(b) = val.as_bytes() {
            return Ok(Value::int64(b.len() as i64));
        }

        Err(crate::error::Error::TypeMismatch {
            expected: "STRING or BYTES".to_string(),
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
    fn returns_length_of_string() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_length(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(5));
    }

    #[test]
    fn returns_zero_for_empty_string() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_length(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(0));
    }

    #[test]
    fn counts_unicode_characters() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("héllo".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_length(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(5));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_length(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::evaluate_length(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "LENGTH");
    }
}
