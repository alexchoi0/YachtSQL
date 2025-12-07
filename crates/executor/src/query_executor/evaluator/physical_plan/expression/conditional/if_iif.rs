use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_if(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 3 {
            return Err(Error::invalid_query(
                "IF requires exactly 3 arguments: IF(condition, true_value, false_value)"
                    .to_string(),
            ));
        }

        let condition = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let condition_is_true =
            crate::query_executor::execution::evaluate_condition_as_bool(&condition)?;

        if condition_is_true {
            Self::evaluate_expr(&args[1], batch, row_idx)
        } else {
            Self::evaluate_expr(&args[2], batch, row_idx)
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_optimizer::expr::LiteralValue;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn create_test_batch() -> Table {
        create_single_row_batch(vec![("col1", DataType::Bool)], vec![Value::bool_val(true)])
    }

    #[test]
    fn test_if_condition_true() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Boolean(true)),
            Expr::Literal(LiteralValue::String("yes".to_string())),
            Expr::Literal(LiteralValue::String("no".to_string())),
        ];
        let result = ProjectionWithExprExec::eval_if(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("yes".to_string()));
    }

    #[test]
    fn test_if_condition_false() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Boolean(false)),
            Expr::Literal(LiteralValue::Int64(1)),
            Expr::Literal(LiteralValue::Int64(2)),
        ];
        let result = ProjectionWithExprExec::eval_if(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(2));
    }

    #[test]
    fn test_if_with_binary_condition() {
        let batch = create_test_batch();
        let args = vec![
            Expr::BinaryOp {
                left: Box::new(Expr::Literal(LiteralValue::Int64(5))),
                op: yachtsql_optimizer::BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Int64(3))),
            },
            Expr::Literal(LiteralValue::String("greater".to_string())),
            Expr::Literal(LiteralValue::String("not_greater".to_string())),
        ];
        let result = ProjectionWithExprExec::eval_if(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("greater".to_string()));
    }

    #[test]
    fn test_if_wrong_arg_count() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Boolean(true)),
            Expr::Literal(LiteralValue::Int64(1)),
        ];
        let result = ProjectionWithExprExec::eval_if(&args, &batch, 0);
        assert!(result.is_err());
        match result {
            Err(Error::InvalidQuery(msg)) => {
                assert!(msg.contains("IF requires exactly 3 arguments"))
            }
            _ => panic!("Expected InvalidQuery error"),
        }
    }
}
