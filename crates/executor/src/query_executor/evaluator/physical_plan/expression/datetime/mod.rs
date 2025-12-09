mod age;
mod at_time_zone;
mod current_date;
mod current_time;
mod current_timestamp;
mod date_add;
mod date_constructor;
mod date_diff;
mod date_part;
mod date_sub;
mod date_trunc;
mod extract;
mod format_date;
mod format_timestamp;
mod interval_literal;
mod make_date;
mod make_timestamp;
mod parse_date;
mod parse_timestamp;
mod str_to_date;
mod timestamp_diff;
mod timestamp_trunc;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

mod day;
mod dayofweek;
mod dayofyear;
mod hour;
mod last_day;
mod minute;
mod month;
mod quarter;
mod second;
mod week;
mod year;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_datetime_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "CURRENT_DATE" => Self::eval_current_date(),
            "CURRENT_TIMESTAMP" | "NOW" => Self::eval_current_timestamp(),
            "CURRENT_TIME" => Self::eval_current_time(),
            "DATE_ADD" => Self::eval_date_add(args, batch, row_idx),
            "DATE_SUB" => Self::eval_date_sub(args, batch, row_idx),
            "DATE_DIFF" => Self::eval_date_diff(args, batch, row_idx),
            "EXTRACT" => Self::eval_extract(args, batch, row_idx),
            "DATE_PART" => Self::eval_date_part(args, batch, row_idx),
            "DATE_TRUNC" => Self::eval_date_trunc(args, batch, row_idx),
            "TIMESTAMP_TRUNC" => Self::eval_timestamp_trunc(args, batch, row_idx),
            "FORMAT_DATE" => Self::eval_format_date(args, batch, row_idx),
            "FORMAT_TIMESTAMP" => Self::eval_format_timestamp(args, batch, row_idx),
            "PARSE_DATE" => Self::eval_parse_date(args, batch, row_idx),
            "PARSE_TIMESTAMP" => Self::eval_parse_timestamp(args, batch, row_idx),
            "STR_TO_DATE" => Self::eval_str_to_date(args, batch, row_idx),
            "MAKE_DATE" => Self::eval_make_date(args, batch, row_idx),
            "MAKE_TIMESTAMP" => Self::eval_make_timestamp(args, batch, row_idx),
            "AGE" => Self::eval_age(args, batch, row_idx),
            "DATE" => Self::eval_date_constructor(args, batch, row_idx),
            "TIMESTAMP_DIFF" => Self::eval_timestamp_diff(args, batch, row_idx),
            "INTERVAL_LITERAL" => Self::eval_interval_literal(args, batch, row_idx),
            "INTERVAL_PARSE" => Self::eval_interval_parse(args, batch, row_idx),
            "YEAR" => Self::eval_year(args, batch, row_idx),
            "MONTH" => Self::eval_month(args, batch, row_idx),
            "DAY" | "DAYOFMONTH" => Self::eval_day(args, batch, row_idx),
            "HOUR" => Self::eval_hour(args, batch, row_idx),
            "MINUTE" => Self::eval_minute(args, batch, row_idx),
            "SECOND" => Self::eval_second(args, batch, row_idx),
            "QUARTER" => Self::eval_quarter(args, batch, row_idx),
            "WEEK" | "ISOWEEK" => Self::eval_week(args, batch, row_idx),
            "DAYOFWEEK" | "WEEKDAY" => Self::eval_dayofweek(args, batch, row_idx),
            "DAYOFYEAR" => Self::eval_dayofyear(args, batch, row_idx),
            "LAST_DAY" => Self::eval_last_day(args, batch, row_idx),
            "AT_TIME_ZONE" => Self::eval_at_time_zone(args, batch, row_idx),
            "JUSTIFY_DAYS" => Self::eval_justify_days(args, batch, row_idx),
            "JUSTIFY_HOURS" => Self::eval_justify_hours(args, batch, row_idx),
            "JUSTIFY_INTERVAL" => Self::eval_justify_interval(args, batch, row_idx),
            "TO_DATE" | "TODATE" => Self::eval_ch_to_date(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown datetime function: {}",
                name
            ))),
        }
    }

    fn eval_ch_to_date(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("toDate requires 1 argument"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(d) = val.as_date() {
            return Ok(Value::date(d));
        }
        if let Some(ts) = val.as_timestamp() {
            return Ok(Value::date(ts.date_naive()));
        }
        if let Some(s) = val.as_str() {
            let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| Error::invalid_query(format!("Invalid date format: {}", s)))?;
            return Ok(Value::date(date));
        }
        Err(Error::TypeMismatch {
            expected: "DATE, TIMESTAMP, or STRING".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    fn eval_justify_days(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "JUSTIFY_DAYS requires exactly 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::interval::justify_days(&val)
    }

    fn eval_justify_hours(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "JUSTIFY_HOURS requires exactly 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::interval::justify_hours(&val)
    }

    fn eval_justify_interval(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "JUSTIFY_INTERVAL requires exactly 1 argument",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::interval::justify_interval(&val)
    }
}
