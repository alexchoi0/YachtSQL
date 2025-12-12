use yachtsql_common::error::Result;
use yachtsql_common::types::Value;

use super::super::super::ProjectionWithExprExec;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_current_timestamp()
    -> Result<Value> {
        Ok(Value::timestamp(chrono::Utc::now()))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::Value;

    use super::*;

    #[test]
    fn returns_timestamp_value() {
        let result = ProjectionWithExprExec::eval_current_timestamp().expect("success");
        assert!(
            result.data_type() == yachtsql_common::types::DataType::Timestamp,
            "expected Timestamp value"
        );
    }

    #[test]
    fn returns_recent_timestamp() {
        let before = chrono::Utc::now();
        let result = ProjectionWithExprExec::eval_current_timestamp().expect("success");
        let after = chrono::Utc::now();

        if let Some(ts) = result.as_timestamp() {
            let before_micros =
                chrono::DateTime::from_timestamp_micros(before.timestamp_micros()).unwrap();
            let after_micros =
                chrono::DateTime::from_timestamp_micros(after.timestamp_micros()).unwrap();

            assert!(
                ts >= before_micros && ts <= after_micros,
                "timestamp should be between before and after"
            );
        } else {
            panic!("expected Timestamp value");
        }
    }
}
