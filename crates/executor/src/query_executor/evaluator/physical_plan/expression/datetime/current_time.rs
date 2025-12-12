use yachtsql_common::error::Result;
use yachtsql_common::types::Value;

use super::super::super::ProjectionWithExprExec;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_current_time() -> Result<Value>
    {
        Ok(Value::time(chrono::Local::now().naive_local().time()))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::Value;

    use super::*;

    #[test]
    fn returns_time_value() {
        let result = ProjectionWithExprExec::eval_current_time().expect("success");
        assert!(
            result.data_type() == yachtsql_common::types::DataType::Time,
            "expected Time value"
        );
    }
}
