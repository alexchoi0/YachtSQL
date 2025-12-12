use chrono::NaiveTime;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_time_trunc(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "TIME_TRUNC requires exactly 2 arguments (time, part)",
            ));
        }

        let time_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let part_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if time_val.is_null() {
            return Ok(Value::null());
        }

        let time = time_val.as_time().ok_or_else(|| Error::TypeMismatch {
            expected: "TIME".to_string(),
            actual: time_val.data_type().to_string(),
        })?;

        let part = part_val
            .as_str()
            .ok_or_else(|| Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: part_val.data_type().to_string(),
            })?
            .to_uppercase();

        let truncated = truncate_time(time, &part)?;
        Ok(Value::time(truncated))
    }
}

fn truncate_time(time: NaiveTime, part: &str) -> Result<NaiveTime> {
    use chrono::Timelike;

    match part {
        "HOUR" => NaiveTime::from_hms_opt(time.hour(), 0, 0)
            .ok_or_else(|| Error::ExecutionError("Failed to truncate to hour".to_string())),
        "MINUTE" => NaiveTime::from_hms_opt(time.hour(), time.minute(), 0)
            .ok_or_else(|| Error::ExecutionError("Failed to truncate to minute".to_string())),
        "SECOND" => NaiveTime::from_hms_opt(time.hour(), time.minute(), time.second())
            .ok_or_else(|| Error::ExecutionError("Failed to truncate to second".to_string())),
        _ => Err(Error::invalid_query(format!(
            "Unsupported time truncation unit: {}",
            part
        ))),
    }
}
