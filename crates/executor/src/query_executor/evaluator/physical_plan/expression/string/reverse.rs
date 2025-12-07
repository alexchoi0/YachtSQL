use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_reverse(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("REVERSE", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if val.is_null() {
            return Ok(Value::null());
        }

        if let Some(s) = val.as_str() {
            let reversed: String = s.chars().rev().collect();
            return Ok(Value::string(reversed));
        }

        if let Some(b) = val.as_bytes() {
            let mut reversed = b.to_vec();
            reversed.reverse();
            return Ok(Value::bytes(reversed));
        }

        Err(crate::error::Error::TypeMismatch {
            expected: "STRING".to_string(),
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
    fn reverses_string() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_reverse(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("olleh".into()));
    }

    #[test]
    fn handles_palindrome() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("racecar".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_reverse(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("racecar".into()));
    }

    #[test]
    fn handles_empty_string() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_reverse(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("".into()));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_reverse(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::evaluate_reverse(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "REVERSE");
    }
}
