use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_coalesce(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        for arg in args {
            let val = Self::evaluate_expr(arg, batch, row_idx)?;
            if !val.is_null() {
                return Ok(val);
            }
        }
        Ok(Value::null())
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
    fn test_coalesce_first_non_null() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Int64(1)),
            Expr::Literal(LiteralValue::Int64(2)),
            Expr::Literal(LiteralValue::Int64(3)),
        ];
        let result = ProjectionWithExprExec::eval_coalesce(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(1));
    }

    #[test]
    fn test_coalesce_skip_nulls() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Null),
            Expr::Literal(LiteralValue::Null),
            Expr::Literal(LiteralValue::String("found".to_string())),
            Expr::Literal(LiteralValue::String("not_reached".to_string())),
        ];
        let result = ProjectionWithExprExec::eval_coalesce(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("found".to_string()));
    }

    #[test]
    fn test_coalesce_all_nulls() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Null),
            Expr::Literal(LiteralValue::Null),
            Expr::Literal(LiteralValue::Null),
        ];
        let result = ProjectionWithExprExec::eval_coalesce(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_coalesce_single_value() {
        let batch = create_test_batch();
        let args = vec![Expr::Literal(LiteralValue::Int64(99))];
        let result = ProjectionWithExprExec::eval_coalesce(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(99));
    }

    #[test]
    fn test_coalesce_empty_args() {
        let batch = create_test_batch();
        let args: Vec<Expr> = vec![];
        let result = ProjectionWithExprExec::eval_coalesce(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }
}
