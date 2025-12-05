use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use debug_print::debug_eprintln;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::RecordBatch;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_at_time_zone(
        args: &[Expr],
        batch: &RecordBatch,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("AT_TIME_ZONE", args, 2)?;
        debug_eprintln!("[datetime::at_time_zone] args[0] = {:?}", &args[0]);
        debug_eprintln!("[datetime::at_time_zone] args[1] = {:?}", &args[1]);
        let ts_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let tz_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        debug_eprintln!(
            "[datetime::at_time_zone] ts_val = {:?}, tz_val = {:?}",
            ts_val,
            tz_val
        );

        if ts_val.is_null() || tz_val.is_null() {
            return Ok(Value::null());
        }

        let utc_dt = ts_val.as_timestamp().ok_or_else(|| Error::TypeMismatch {
            expected: "TIMESTAMP".to_string(),
            actual: ts_val.data_type().to_string(),
        })?;

        let tz_str = tz_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: tz_val.data_type().to_string(),
        })?;

        Self::apply_at_time_zone_impl(utc_dt, tz_str)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_at_time_zone_plain(
        args: &[Expr],
        batch: &RecordBatch,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("AT_TIME_ZONE_PLAIN", args, 2)?;
        let ts_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let tz_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if ts_val.is_null() || tz_val.is_null() {
            return Ok(Value::null());
        }

        let utc_dt = ts_val.as_timestamp().ok_or_else(|| Error::TypeMismatch {
            expected: "TIMESTAMP".to_string(),
            actual: ts_val.data_type().to_string(),
        })?;

        let tz_str = tz_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: tz_val.data_type().to_string(),
        })?;

        Self::interpret_as_timezone_impl(utc_dt, tz_str)
    }

    fn apply_at_time_zone_impl(utc_dt: DateTime<Utc>, tz_str: &str) -> Result<Value> {
        let tz: Tz = if let Ok(tz) = tz_str.parse() {
            tz
        } else if let Some(tz) = Self::resolve_utc_offset(tz_str) {
            tz
        } else {
            return Err(Error::invalid_query(format!(
                "Invalid timezone: {}",
                tz_str
            )));
        };

        let local_dt = utc_dt.with_timezone(&tz);
        let naive = local_dt.naive_local();
        let result = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
        Ok(Value::timestamp(result))
    }

    fn interpret_as_timezone_impl(naive_ts: DateTime<Utc>, tz_str: &str) -> Result<Value> {
        let tz: Tz = if let Ok(tz) = tz_str.parse() {
            tz
        } else if let Some(tz) = Self::resolve_utc_offset(tz_str) {
            tz
        } else {
            return Err(Error::invalid_query(format!(
                "Invalid timezone: {}",
                tz_str
            )));
        };

        let naive_local = NaiveDateTime::from_timestamp_opt(
            naive_ts.timestamp(),
            naive_ts.timestamp_subsec_nanos(),
        )
        .unwrap_or_else(|| naive_ts.naive_utc());

        let local_dt = match tz.from_local_datetime(&naive_local) {
            chrono::LocalResult::Single(dt) => dt,
            chrono::LocalResult::Ambiguous(dt, _) => dt,
            chrono::LocalResult::None => {
                return Err(Error::invalid_query(format!(
                    "Invalid local time for timezone: {}",
                    tz_str
                )));
            }
        };

        Ok(Value::timestamp(local_dt.with_timezone(&Utc)))
    }

    fn resolve_utc_offset(offset_str: &str) -> Option<Tz> {
        let offset_str = offset_str.trim();
        if offset_str.eq_ignore_ascii_case("UTC") {
            return Some(chrono_tz::UTC);
        }
        if offset_str.eq_ignore_ascii_case("GMT") {
            return Some(chrono_tz::GMT);
        }
        None
    }
}
