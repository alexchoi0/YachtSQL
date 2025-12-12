use yachtsql_common::error::Result;
use yachtsql_common::types::Value;

use super::super::super::ProjectionWithExprExec;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_grouping() -> Result<Value>
    {
        // TODO: Implement proper GROUPING SETS support
        Ok(Value::int64(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_grouping_returns_zero() {
        let result = ProjectionWithExprExec::evaluate_grouping();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(0));
    }

    #[test]
    fn test_evaluate_grouping_always_zero() {
        for _ in 0..10 {
            let result = ProjectionWithExprExec::evaluate_grouping();
            assert_eq!(result.unwrap(), Value::int64(0));
        }
    }
}
