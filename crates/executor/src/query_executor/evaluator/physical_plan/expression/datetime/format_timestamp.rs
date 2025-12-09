use chrono::{Datelike, Timelike};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_format_timestamp(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "FORMAT_TIMESTAMP requires exactly 2 arguments (format_string, timestamp)"
                    .to_string(),
            ));
        }

        let format_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let ts_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if format_val.is_null() || ts_val.is_null() {
            return Ok(Value::null());
        }

        let format_str = format_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: format_val.data_type().to_string(),
        })?;

        let ts = ts_val.as_timestamp().ok_or_else(|| Error::TypeMismatch {
            expected: "TIMESTAMP".to_string(),
            actual: ts_val.data_type().to_string(),
        })?;

        let result = format_timestamp_with_pattern(format_str, &ts);
        Ok(Value::string(result))
    }
}

fn format_timestamp_with_pattern(format: &str, ts: &chrono::DateTime<chrono::Utc>) -> String {
    let mut result = String::new();
    let mut chars = format.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            match chars.next() {
                Some('Y') => result.push_str(&format!("{:04}", ts.year())),
                Some('y') => result.push_str(&format!("{:02}", ts.year() % 100)),
                Some('m') => result.push_str(&format!("{:02}", ts.month())),
                Some('d') => result.push_str(&format!("{:02}", ts.day())),
                Some('e') => result.push_str(&format!("{:2}", ts.day())),
                Some('j') => result.push_str(&format!("{:03}", ts.ordinal())),
                Some('H') => result.push_str(&format!("{:02}", ts.hour())),
                Some('I') => {
                    let hour = ts.hour() % 12;
                    let hour = if hour == 0 { 12 } else { hour };
                    result.push_str(&format!("{:02}", hour));
                }
                Some('k') => result.push_str(&format!("{:2}", ts.hour())),
                Some('l') => {
                    let hour = ts.hour() % 12;
                    let hour = if hour == 0 { 12 } else { hour };
                    result.push_str(&format!("{:2}", hour));
                }
                Some('M') => result.push_str(&format!("{:02}", ts.minute())),
                Some('S') => result.push_str(&format!("{:02}", ts.second())),
                Some('f') => {
                    result.push_str(&format!("{:06}", ts.nanosecond() / 1000));
                }
                Some('p') => {
                    result.push_str(if ts.hour() < 12 { "AM" } else { "PM" });
                }
                Some('P') => {
                    result.push_str(if ts.hour() < 12 { "am" } else { "pm" });
                }
                Some('A') => result.push_str(weekday_name(ts.weekday())),
                Some('a') => result.push_str(weekday_abbr(ts.weekday())),
                Some('B') => result.push_str(month_name(ts.month())),
                Some('b') | Some('h') => result.push_str(month_abbr(ts.month())),
                Some('u') => {
                    result.push_str(&format!("{}", ts.weekday().num_days_from_monday() + 1))
                }
                Some('w') => result.push_str(&format!("{}", ts.weekday().num_days_from_sunday())),
                Some('W') => result.push_str(&format!("{:02}", ts.iso_week().week())),
                Some('Q') => result.push_str(&format!("{}", (ts.month() - 1) / 3 + 1)),
                Some('Z') => result.push_str("UTC"),
                Some('z') => result.push_str("+0000"),
                Some('%') => result.push('%'),
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
                Some('R') => {
                    result.push_str(&format!("{:02}:{:02}", ts.hour(), ts.minute()));
                }
                Some('T') => {
                    result.push_str(&format!(
                        "{:02}:{:02}:{:02}",
                        ts.hour(),
                        ts.minute(),
                        ts.second()
                    ));
                }
                Some('F') => {
                    result.push_str(&format!(
                        "{:04}-{:02}-{:02}",
                        ts.year(),
                        ts.month(),
                        ts.day()
                    ));
                }
                Some(other) => {
                    result.push('%');
                    result.push(other);
                }
                None => result.push('%'),
            }
        } else {
            result.push(c);
        }
    }

    result
}

fn weekday_name(wd: chrono::Weekday) -> &'static str {
    match wd {
        chrono::Weekday::Mon => "Monday",
        chrono::Weekday::Tue => "Tuesday",
        chrono::Weekday::Wed => "Wednesday",
        chrono::Weekday::Thu => "Thursday",
        chrono::Weekday::Fri => "Friday",
        chrono::Weekday::Sat => "Saturday",
        chrono::Weekday::Sun => "Sunday",
    }
}

fn weekday_abbr(wd: chrono::Weekday) -> &'static str {
    match wd {
        chrono::Weekday::Mon => "Mon",
        chrono::Weekday::Tue => "Tue",
        chrono::Weekday::Wed => "Wed",
        chrono::Weekday::Thu => "Thu",
        chrono::Weekday::Fri => "Fri",
        chrono::Weekday::Sat => "Sat",
        chrono::Weekday::Sun => "Sun",
    }
}

fn month_name(month: u32) -> &'static str {
    match month {
        1 => "January",
        2 => "February",
        3 => "March",
        4 => "April",
        5 => "May",
        6 => "June",
        7 => "July",
        8 => "August",
        9 => "September",
        10 => "October",
        11 => "November",
        12 => "December",
        _ => "",
    }
}

fn month_abbr(month: u32) -> &'static str {
    match month {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn schema_with_format_and_timestamp() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("format", DataType::String),
            Field::nullable("ts", DataType::Timestamp),
        ])
    }

    #[test]
    fn formats_timestamp_with_date_and_time() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-06-15T14:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_format_and_timestamp(),
            vec![vec![
                Value::string("%Y/%m/%d %H:%M".into()),
                Value::timestamp(ts),
            ]],
        );
        let args = vec![Expr::column("format"), Expr::column("ts")];
        let result =
            ProjectionWithExprExec::eval_format_timestamp(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("2024/06/15 14:30".into()));
    }

    #[test]
    fn propagates_null_format() {
        let ts = chrono::Utc::now();
        let batch = create_batch(
            schema_with_format_and_timestamp(),
            vec![vec![Value::null(), Value::timestamp(ts)]],
        );
        let args = vec![Expr::column("format"), Expr::column("ts")];
        let result =
            ProjectionWithExprExec::eval_format_timestamp(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_timestamp() {
        let batch = create_batch(
            schema_with_format_and_timestamp(),
            vec![vec![Value::string("%Y-%m-%d".into()), Value::null()]],
        );
        let args = vec![Expr::column("format"), Expr::column("ts")];
        let result =
            ProjectionWithExprExec::eval_format_timestamp(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }
}
