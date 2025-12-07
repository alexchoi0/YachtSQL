use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_hex(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "TO_HEX requires exactly 1 argument".to_string(),
            ));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::scalar::eval_to_hex(&val)
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
            vec![("data", DataType::Bytes)],
            vec![vec![Value::bytes(vec![0xFF])], vec![Value::null()]],
        )
    }

    #[test]
    fn test_to_hex_non_bytes_literal_errors() {
        let batch = create_test_batch();
        let args = vec![Expr::Literal(LiteralValue::Int64(255))];
        let result = ProjectionWithExprExec::eval_to_hex(&args, &batch, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_hex_column() {
        let batch = create_test_batch();
        let args = vec![Expr::Column {
            name: "data".to_string(),
            table: None,
        }];
        let result = ProjectionWithExprExec::eval_to_hex(&args, &batch, 0);
        assert!(result.is_ok());
        assert!(result.unwrap().is_string());
    }

    #[test]
    fn test_to_hex_null() {
        let batch = create_test_batch();
        let args = vec![Expr::Column {
            name: "data".to_string(),
            table: None,
        }];
        let result = ProjectionWithExprExec::eval_to_hex(&args, &batch, 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_to_hex_no_args() {
        let batch = create_test_batch();
        let args = vec![];
        let result = ProjectionWithExprExec::eval_to_hex(&args, &batch, 0);
        assert!(result.is_err());
        match result {
            Err(Error::InvalidQuery(msg)) => {
                assert!(msg.contains("TO_HEX requires exactly 1 argument"))
            }
            _ => panic!("Expected InvalidQuery error"),
        }
    }
}
