use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_farm_fingerprint(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "FARM_FINGERPRINT requires exactly 1 argument".to_string(),
            ));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::scalar::eval_farm_fingerprint(&val)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_optimizer::expr::LiteralValue;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn create_test_batch() -> Table {
        create_batch(
            vec![("data", DataType::String)],
            vec![
                vec![Value::string("test".to_string())],
                vec![Value::string("hello".to_string())],
                vec![Value::null()],
            ],
        )
    }

    #[test]
    fn test_farm_fingerprint_string_literal() {
        let batch = create_test_batch();
        let args = vec![Expr::Literal(LiteralValue::String("test".to_string()))];
        let result = ProjectionWithExprExec::eval_farm_fingerprint(&args, &batch, 0);
        assert!(result.is_ok());
        assert!(result.unwrap().is_int64());
    }

    #[test]
    fn test_farm_fingerprint_column() {
        let batch = create_test_batch();
        let args = vec![Expr::Column {
            name: "data".to_string(),
            table: None,
        }];
        let result = ProjectionWithExprExec::eval_farm_fingerprint(&args, &batch, 0);
        assert!(result.is_ok());
        assert!(result.unwrap().is_int64());
    }

    #[test]
    fn test_farm_fingerprint_null() {
        let batch = create_test_batch();
        let args = vec![Expr::Column {
            name: "data".to_string(),
            table: None,
        }];
        let result = ProjectionWithExprExec::eval_farm_fingerprint(&args, &batch, 2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_farm_fingerprint_no_args() {
        let batch = create_test_batch();
        let args = vec![];
        let result = ProjectionWithExprExec::eval_farm_fingerprint(&args, &batch, 0);
        assert!(result.is_err());
        match result {
            Err(Error::InvalidQuery(msg)) => {
                assert!(msg.contains("FARM_FINGERPRINT requires exactly 1 argument"))
            }
            _ => panic!("Expected InvalidQuery error"),
        }
    }
}
