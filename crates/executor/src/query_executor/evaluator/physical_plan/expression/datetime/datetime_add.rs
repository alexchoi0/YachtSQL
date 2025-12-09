use chrono::{DateTime, Duration, Utc};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_datetime_add(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "DATETIME_ADD requires exactly 2 arguments",
            ));
        }

        let dt_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let interval_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if dt_val.is_null() || interval_val.is_null() {
            return Ok(Value::null());
        }

        let dt = dt_val
            .as_datetime()
            .or_else(|| dt_val.as_timestamp())
            .ok_or_else(|| Error::TypeMismatch {
                expected: "DATETIME/TIMESTAMP".to_string(),
                actual: dt_val.data_type().to_string(),
            })?;

        let interval = interval_val
            .as_interval()
            .ok_or_else(|| Error::TypeMismatch {
                expected: "INTERVAL".to_string(),
                actual: interval_val.data_type().to_string(),
            })?;

        let new_dt = add_interval_to_datetime(dt, interval)?;
        if dt_val.as_datetime().is_some() {
            Ok(Value::datetime(new_dt))
        } else {
            Ok(Value::timestamp(new_dt))
        }
    }
}

fn add_interval_to_datetime(
    dt: DateTime<Utc>,
    interval: &yachtsql_core::types::Interval,
) -> Result<DateTime<Utc>> {
    use chrono::Months;

    let mut result = dt;

    if interval.months != 0 {
        result = if interval.months > 0 {
            result.checked_add_months(Months::new(interval.months as u32))
        } else {
            result.checked_sub_months(Months::new((-interval.months) as u32))
        }
        .ok_or_else(|| {
            Error::ExecutionError(format!(
                "Datetime overflow: {} + {} months",
                dt, interval.months
            ))
        })?;
    }

    if interval.days != 0 {
        result = result
            .checked_add_signed(Duration::days(interval.days as i64))
            .ok_or_else(|| {
                Error::ExecutionError(format!(
                    "Datetime overflow: {} + {} days",
                    dt, interval.days
                ))
            })?;
    }

    if interval.micros != 0 {
        result = result
            .checked_add_signed(Duration::microseconds(interval.micros))
            .ok_or_else(|| {
                Error::ExecutionError(format!(
                    "Datetime overflow: {} + {} microseconds",
                    dt, interval.micros
                ))
            })?;
    }

    Ok(result)
}
