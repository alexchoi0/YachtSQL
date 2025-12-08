use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_ir::expr::LiteralValue;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_at_time_zone(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("AT_TIME_ZONE", args, 2)?;

        let is_plain = Self::is_plain_timestamp_expr(&args[0]);

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

        if is_plain {
            Self::interpret_as_timezone_impl(utc_dt, tz_str)
        } else {
            Self::apply_at_time_zone_impl(utc_dt, tz_str)
        }
    }

    fn is_plain_timestamp_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Literal(LiteralValue::Timestamp(s)) => !Self::has_timezone_info(s),
            Expr::Cast { expr, .. } | Expr::TryCast { expr, .. } => {
                Self::is_plain_timestamp_expr(expr)
            }
            Expr::Function { name, args } => {
                use yachtsql_ir::FunctionName;
                match name {
                    FunctionName::AtTimeZone => false,
                    _ => args.first().is_some_and(Self::is_plain_timestamp_expr),
                }
            }
            _ => false,
        }
    }

    fn has_timezone_info(ts_str: &str) -> bool {
        let s = ts_str.trim();
        if s.ends_with('Z') || s.ends_with("UTC") || s.ends_with("GMT") {
            return true;
        }
        if let Some(pos) = s.rfind(['+', '-']) {
            let suffix = &s[pos + 1..];
            if suffix.chars().all(|c| c.is_ascii_digit() || c == ':') && !suffix.is_empty() {
                return true;
            }
        }
        if let Some(last_space) = s.rfind(' ') {
            let potential_tz = &s[last_space + 1..];
            if potential_tz.parse::<Tz>().is_ok() {
                return true;
            }
        }
        false
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_at_time_zone_plain(
        args: &[Expr],
        batch: &Table,
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

        let naive_local =
            DateTime::from_timestamp(naive_ts.timestamp(), naive_ts.timestamp_subsec_nanos())
                .map(|dt| dt.naive_utc())
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
