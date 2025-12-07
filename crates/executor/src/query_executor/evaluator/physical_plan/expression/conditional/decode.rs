use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_decode(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() == 2 {
            return Self::eval_decode_base64(args, batch, row_idx);
        }

        if args.len() < 3 {
            return Err(Error::invalid_query(
                "DECODE requires at least 3 arguments: DECODE(expr, search1, result1, ..., default)"
                    .to_string(),
            ));
        }

        let expr = Self::evaluate_expr(&args[0], batch, row_idx)?;

        let pairs_count = (args.len() - 1) / 2;

        for i in 0..pairs_count {
            let search_idx = 1 + (i * 2);
            let result_idx = search_idx + 1;

            if result_idx < args.len() {
                let search = Self::evaluate_expr(&args[search_idx], batch, row_idx)?;

                if crate::query_executor::execution::decode_values_match(&expr, &search) {
                    return Self::evaluate_expr(&args[result_idx], batch, row_idx);
                }
            }
        }

        #[allow(clippy::manual_is_multiple_of)]
        if args.len() % 2 == 0 {
            Self::evaluate_expr(&args[args.len() - 1], batch, row_idx)
        } else {
            Ok(Value::null())
        }
    }

    fn eval_decode_base64(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        let data_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let format_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let data_str = data_val
            .as_str()
            .ok_or_else(|| Error::invalid_query("DECODE data must be a string"))?;

        let format = format_val
            .as_str()
            .ok_or_else(|| Error::invalid_query("DECODE format must be a string"))?;

        let result = match format.to_lowercase().as_str() {
            "hex" => hex::decode(data_str)
                .map_err(|e| Error::invalid_query(format!("Invalid hex string: {}", e)))?,
            "base64" => {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data_str)
                    .map_err(|e| Error::invalid_query(format!("Invalid base64 string: {}", e)))?
            }
            "escape" => data_str.as_bytes().to_vec(),
            _ => {
                return Err(Error::invalid_query(format!(
                    "Unsupported decoding format: {}",
                    format
                )));
            }
        };
        Ok(Value::bytes(result))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_optimizer::expr::LiteralValue;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn create_test_batch() -> Table {
        create_single_row_batch(vec![("col1", DataType::Int64)], vec![Value::int64(42)])
    }

    #[test]
    fn test_decode_first_match() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::String("A".to_string())),
            Expr::Literal(LiteralValue::String("A".to_string())),
            Expr::Literal(LiteralValue::String("Match A".to_string())),
            Expr::Literal(LiteralValue::String("B".to_string())),
            Expr::Literal(LiteralValue::String("Match B".to_string())),
        ];
        let result = ProjectionWithExprExec::eval_decode(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("Match A".to_string()));
    }

    #[test]
    fn test_decode_second_match() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Int64(2)),
            Expr::Literal(LiteralValue::Int64(1)),
            Expr::Literal(LiteralValue::String("One".to_string())),
            Expr::Literal(LiteralValue::Int64(2)),
            Expr::Literal(LiteralValue::String("Two".to_string())),
        ];
        let result = ProjectionWithExprExec::eval_decode(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("Two".to_string()));
    }

    #[test]
    fn test_decode_with_default() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Int64(99)),
            Expr::Literal(LiteralValue::Int64(1)),
            Expr::Literal(LiteralValue::String("One".to_string())),
            Expr::Literal(LiteralValue::Int64(2)),
            Expr::Literal(LiteralValue::String("Two".to_string())),
            Expr::Literal(LiteralValue::String("Default".to_string())),
        ];
        let result = ProjectionWithExprExec::eval_decode(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("Default".to_string()));
    }

    #[test]
    fn test_decode_no_match_no_default() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Int64(99)),
            Expr::Literal(LiteralValue::Int64(1)),
            Expr::Literal(LiteralValue::String("One".to_string())),
            Expr::Literal(LiteralValue::Int64(2)),
            Expr::Literal(LiteralValue::String("Two".to_string())),
        ];
        let result = ProjectionWithExprExec::eval_decode(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_decode_too_few_args() {
        let batch = create_test_batch();
        let args = vec![Expr::Literal(LiteralValue::Int64(1))];
        let result = ProjectionWithExprExec::eval_decode(&args, &batch, 0);
        assert!(result.is_err());
        match result {
            Err(Error::InvalidQuery(msg)) => {
                assert!(msg.contains("DECODE requires at least 3 arguments"))
            }
            _ => panic!("Expected InvalidQuery error"),
        }
    }
}
