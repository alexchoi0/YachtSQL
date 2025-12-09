use chrono::Datelike;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_format_date(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "FORMAT_DATE requires exactly 2 arguments (format_string, date)".to_string(),
            ));
        }

        let format_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let date_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if format_val.is_null() || date_val.is_null() {
            return Ok(Value::null());
        }

        let format_str = format_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: format_val.data_type().to_string(),
        })?;

        let date = date_val.as_date().ok_or_else(|| Error::TypeMismatch {
            expected: "DATE".to_string(),
            actual: date_val.data_type().to_string(),
        })?;

        let result = format_date_with_pattern(format_str, &date);
        Ok(Value::string(result))
    }
}

fn format_date_with_pattern(format: &str, date: &chrono::NaiveDate) -> String {
    let mut result = String::new();
    let mut chars = format.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            match chars.next() {
                Some('Y') => result.push_str(&format!("{:04}", date.year())),
                Some('y') => result.push_str(&format!("{:02}", date.year() % 100)),
                Some('m') => result.push_str(&format!("{:02}", date.month())),
                Some('d') => result.push_str(&format!("{:02}", date.day())),
                Some('e') => result.push_str(&format!("{:2}", date.day())),
                Some('j') => result.push_str(&format!("{:03}", date.ordinal())),
                Some('A') => result.push_str(weekday_name(date.weekday())),
                Some('a') => result.push_str(weekday_abbr(date.weekday())),
                Some('B') => result.push_str(month_name(date.month())),
                Some('b') | Some('h') => result.push_str(month_abbr(date.month())),
                Some('u') => {
                    result.push_str(&format!("{}", date.weekday().num_days_from_monday() + 1))
                }
                Some('w') => result.push_str(&format!("{}", date.weekday().num_days_from_sunday())),
                Some('W') => result.push_str(&format!("{:02}", date.iso_week().week())),
                Some('U') => result.push_str(&format!("{:02}", week_from_sunday(date))),
                Some('V') => result.push_str(&format!("{:02}", date.iso_week().week())),
                Some('G') => result.push_str(&format!("{:04}", date.iso_week().year())),
                Some('g') => result.push_str(&format!("{:02}", date.iso_week().year() % 100)),
                Some('Q') => result.push_str(&format!("{}", (date.month() - 1) / 3 + 1)),
                Some('%') => result.push('%'),
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
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

fn week_from_sunday(date: &chrono::NaiveDate) -> u32 {
    let first_day = chrono::NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap();
    let first_sunday_offset = (7 - first_day.weekday().num_days_from_sunday()) % 7;
    let days_since_first_sunday = date.ordinal() as i32 - 1 - first_sunday_offset as i32;
    if days_since_first_sunday < 0 {
        0
    } else {
        (days_since_first_sunday / 7 + 1) as u32
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn schema_with_format_and_date() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("format", DataType::String),
            Field::nullable("date_col", DataType::Date),
        ])
    }

    #[test]
    fn formats_date_with_year_month_day() {
        let batch = create_batch(
            schema_with_format_and_date(),
            vec![vec![
                Value::string("%Y/%m/%d".into()),
                Value::date(chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
            ]],
        );
        let args = vec![Expr::column("format"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_format_date(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("2024/06/15".into()));
    }

    #[test]
    fn formats_date_with_literal_text() {
        let batch = create_batch(
            schema_with_format_and_date(),
            vec![vec![
                Value::string("Date: %Y-%m-%d".into()),
                Value::date(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            ]],
        );
        let args = vec![Expr::column("format"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_format_date(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("Date: 2024-01-01".into()));
    }

    #[test]
    fn propagates_null_format() {
        let batch = create_batch(
            schema_with_format_and_date(),
            vec![vec![
                Value::null(),
                Value::date(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            ]],
        );
        let args = vec![Expr::column("format"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_format_date(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_date() {
        let batch = create_batch(
            schema_with_format_and_date(),
            vec![vec![Value::string("%Y-%m-%d".into()), Value::null()]],
        );
        let args = vec![Expr::column("format"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_format_date(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }
}
