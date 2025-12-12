use chrono::NaiveTime;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_time_sub(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "TIME_SUB requires exactly 2 arguments",
            ));
        }

        let time_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let interval_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if time_val.is_null() || interval_val.is_null() {
            return Ok(Value::null());
        }

        let time = time_val.as_time().ok_or_else(|| Error::TypeMismatch {
            expected: "TIME".to_string(),
            actual: time_val.data_type().to_string(),
        })?;

        let interval = interval_val
            .as_interval()
            .ok_or_else(|| Error::TypeMismatch {
                expected: "INTERVAL".to_string(),
                actual: interval_val.data_type().to_string(),
            })?;

        let new_time = sub_interval_from_time(time, interval)?;
        Ok(Value::time(new_time))
    }
}

fn sub_interval_from_time(
    time: NaiveTime,
    interval: &yachtsql_common::types::Interval,
) -> Result<NaiveTime> {
    use chrono::Duration;

    let total_micros = interval.micros
        + (interval.days as i64) * 24 * 3600 * 1_000_000
        + (interval.months as i64) * 30 * 24 * 3600 * 1_000_000;

    let duration = Duration::microseconds(total_micros);

    Ok(time.overflowing_sub_signed(duration).0)
}
