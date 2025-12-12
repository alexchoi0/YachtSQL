use yachtsql_common::error::Result;
use yachtsql_common::types::Value;

use super::super::super::ProjectionWithExprExec;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_current_date() -> Result<Value>
    {
        Ok(Value::date(chrono::Utc::now().naive_utc().date()))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::Value;

    use super::*;

    #[test]
    fn returns_date_value() {
        let result = ProjectionWithExprExec::eval_current_date().expect("success");
        assert!(
            result.data_type() == yachtsql_common::types::DataType::Date,
            "expected Date value"
        );
    }

    #[test]
    fn returns_consistent_date_within_same_call() {
        let date1 = ProjectionWithExprExec::eval_current_date().expect("success");
        let date2 = ProjectionWithExprExec::eval_current_date().expect("success");
        assert_eq!(date1, date2, "dates should be equal within same test");
    }
}
