use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_quote_literal(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("QUOTE_LITERAL", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if val.is_null() {
            return Ok(Value::null());
        }

        if let Some(s) = val.as_str() {
            let escaped = s.replace('\'', "''");
            return Ok(Value::string(format!("'{}'", escaped)));
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
    fn quotes_simple_string() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let args = vec![Expr::column("val")];
        let result =
            ProjectionWithExprExec::evaluate_quote_literal(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("'hello'".into()));
    }

    #[test]
    fn escapes_single_quotes() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("it's".into())]]);
        let args = vec![Expr::column("val")];
        let result =
            ProjectionWithExprExec::evaluate_quote_literal(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("'it''s'".into()));
    }

    #[test]
    fn preserves_backslashes() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("path\\file".into())]]);
        let args = vec![Expr::column("val")];
        let result =
            ProjectionWithExprExec::evaluate_quote_literal(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("'path\\file'".into()));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result =
            ProjectionWithExprExec::evaluate_quote_literal(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err =
            ProjectionWithExprExec::evaluate_quote_literal(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "QUOTE_LITERAL");
    }
}
