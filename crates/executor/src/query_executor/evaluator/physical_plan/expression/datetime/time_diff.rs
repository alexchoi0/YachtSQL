use chrono::NaiveTime;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_time_diff(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 3 {
            return Err(Error::invalid_query(
                "TIME_DIFF requires exactly 3 arguments (time1, time2, part)",
            ));
        }

        let time1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let time2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let part_val = Self::evaluate_expr(&args[2], batch, row_idx)?;

        if time1_val.is_null() || time2_val.is_null() {
            return Ok(Value::null());
        }

        let time1 = time1_val.as_time().ok_or_else(|| Error::TypeMismatch {
            expected: "TIME".to_string(),
            actual: time1_val.data_type().to_string(),
        })?;

        let time2 = time2_val.as_time().ok_or_else(|| Error::TypeMismatch {
            expected: "TIME".to_string(),
            actual: time2_val.data_type().to_string(),
        })?;

        let part = part_val
            .as_str()
            .ok_or_else(|| Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: part_val.data_type().to_string(),
            })?
            .to_uppercase();

        let diff = calculate_time_diff(time1, time2, &part)?;
        Ok(Value::int64(diff))
    }
}

fn calculate_time_diff(time1: NaiveTime, time2: NaiveTime, part: &str) -> Result<i64> {
    let duration = time1.signed_duration_since(time2);

    match part {
        "HOUR" => Ok(duration.num_hours()),
        "MINUTE" => Ok(duration.num_minutes()),
        "SECOND" => Ok(duration.num_seconds()),
        "MILLISECOND" => Ok(duration.num_milliseconds()),
        "MICROSECOND" => Ok(duration.num_microseconds().unwrap_or(0)),
        _ => Err(Error::invalid_query(format!(
            "Unsupported time diff unit: {}",
            part
        ))),
    }
}
