use chrono::{DateTime, Duration, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_timestamp_add(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "TIMESTAMP_ADD requires exactly 2 arguments",
            ));
        }

        let ts_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let interval_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if ts_val.is_null() || interval_val.is_null() {
            return Ok(Value::null());
        }

        let ts = ts_val.as_timestamp().ok_or_else(|| Error::TypeMismatch {
            expected: "TIMESTAMP".to_string(),
            actual: ts_val.data_type().to_string(),
        })?;

        let interval = interval_val
            .as_interval()
            .ok_or_else(|| Error::TypeMismatch {
                expected: "INTERVAL".to_string(),
                actual: interval_val.data_type().to_string(),
            })?;

        let new_ts = add_interval_to_timestamp(ts, interval)?;
        Ok(Value::timestamp(new_ts))
    }
}

fn add_interval_to_timestamp(
    ts: DateTime<Utc>,
    interval: &yachtsql_common::types::Interval,
) -> Result<DateTime<Utc>> {
    use chrono::Months;

    let mut result = ts;

    if interval.months != 0 {
        result = if interval.months > 0 {
            result.checked_add_months(Months::new(interval.months as u32))
        } else {
            result.checked_sub_months(Months::new((-interval.months) as u32))
        }
        .ok_or_else(|| {
            Error::ExecutionError(format!(
                "Timestamp overflow: {} + {} months",
                ts, interval.months
            ))
        })?;
    }

    if interval.days != 0 {
        result = result
            .checked_add_signed(Duration::days(interval.days as i64))
            .ok_or_else(|| {
                Error::ExecutionError(format!(
                    "Timestamp overflow: {} + {} days",
                    ts, interval.days
                ))
            })?;
    }

    if interval.micros != 0 {
        result = result
            .checked_add_signed(Duration::microseconds(interval.micros))
            .ok_or_else(|| {
                Error::ExecutionError(format!(
                    "Timestamp overflow: {} + {} microseconds",
                    ts, interval.micros
                ))
            })?;
    }

    Ok(result)
}
