use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_nullif(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("NULLIF", args, 2)?;
        let first = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let second = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if first == second {
            Ok(Value::null())
        } else {
            Ok(first)
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::error::Error;
    use yachtsql_core::types::DataType;
    use yachtsql_optimizer::expr::LiteralValue;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn create_test_batch() -> Table {
        create_single_row_batch(vec![("col1", DataType::Int64)], vec![Value::int64(42)])
    }

    #[test]
    fn test_nullif_values_equal() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Int64(10)),
            Expr::Literal(LiteralValue::Int64(10)),
        ];
        let result = ProjectionWithExprExec::eval_nullif(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_nullif_values_not_equal() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::String("first".to_string())),
            Expr::Literal(LiteralValue::String("second".to_string())),
        ];
        let result = ProjectionWithExprExec::eval_nullif(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("first".to_string()));
    }

    #[test]
    fn test_nullif_both_null() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Null),
            Expr::Literal(LiteralValue::Null),
        ];
        let result = ProjectionWithExprExec::eval_nullif(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_nullif_first_null() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Null),
            Expr::Literal(LiteralValue::Int64(10)),
        ];
        let result = ProjectionWithExprExec::eval_nullif(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_nullif_wrong_arg_count() {
        let batch = create_test_batch();
        let args = vec![Expr::Literal(LiteralValue::Int64(1))];
        let result = ProjectionWithExprExec::eval_nullif(&args, &batch, 0);
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::InvalidQuery(_))));
    }
}
