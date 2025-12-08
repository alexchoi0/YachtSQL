use chrono::{Datelike, Timelike};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{Interval, Value};
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_age(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "AGE requires exactly 2 arguments (end_date, start_date)".to_string(),
            ));
        }

        let end_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let start_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if end_val.is_null() || start_val.is_null() {
            return Ok(Value::null());
        }

        let (end_year, end_month, end_day, end_micros) = if let Some(d) = end_val.as_date() {
            (d.year(), d.month() as i32, d.day() as i32, 0i64)
        } else if let Some(ts) = end_val.as_timestamp() {
            let time_micros = (ts.hour() as i64 * 3_600_000_000)
                + (ts.minute() as i64 * 60_000_000)
                + (ts.second() as i64 * 1_000_000)
                + ((ts.nanosecond() / 1000) as i64);
            (ts.year(), ts.month() as i32, ts.day() as i32, time_micros)
        } else {
            return Err(Error::TypeMismatch {
                expected: "DATE or TIMESTAMP".to_string(),
                actual: end_val.data_type().to_string(),
            });
        };

        let (start_year, start_month, start_day, start_micros) =
            if let Some(d) = start_val.as_date() {
                (d.year(), d.month() as i32, d.day() as i32, 0i64)
            } else if let Some(ts) = start_val.as_timestamp() {
                let time_micros = (ts.hour() as i64 * 3_600_000_000)
                    + (ts.minute() as i64 * 60_000_000)
                    + (ts.second() as i64 * 1_000_000)
                    + ((ts.nanosecond() / 1000) as i64);
                (ts.year(), ts.month() as i32, ts.day() as i32, time_micros)
            } else {
                return Err(Error::TypeMismatch {
                    expected: "DATE or TIMESTAMP".to_string(),
                    actual: start_val.data_type().to_string(),
                });
            };

        let mut years = end_year - start_year;
        let mut months = end_month - start_month;
        let mut days = end_day - start_day;
        let mut micros = end_micros - start_micros;

        if micros < 0 {
            micros += 24 * 3_600_000_000;
            days -= 1;
        }

        if days < 0 {
            let prev_month = if end_month == 1 { 12 } else { end_month - 1 };
            let prev_year = if end_month == 1 {
                end_year - 1
            } else {
                end_year
            };
            let days_in_prev_month = days_in_month(prev_year, prev_month as u32);
            days += days_in_prev_month as i32;
            months -= 1;
        }

        if months < 0 {
            months += 12;
            years -= 1;
        }

        let total_months = years * 12 + months;

        Ok(Value::interval(Interval::new(total_months, days, micros)))
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    fn schema_with_two_dates() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("end_date", DataType::Date),
            Field::nullable("start_date", DataType::Date),
        ])
    }

    #[test]
    fn calculates_age_between_dates() {
        let end_date = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let start_date = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let batch = create_batch(
            schema_with_two_dates(),
            vec![vec![Value::date(end_date), Value::date(start_date)]],
        );
        let args = vec![Expr::column("end_date"), Expr::column("start_date")];
        let result = ProjectionWithExprExec::eval_age(&args, &batch, 0).expect("success");
        let interval = result.as_interval().expect("should be interval");
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 14);
        assert_eq!(interval.micros, 0);
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(
            Schema::from_fields(vec![Field::nullable("ts", DataType::Date)]),
            vec![vec![Value::date(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            )]],
        );
        let err = ProjectionWithExprExec::eval_age(&[Expr::column("ts")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "AGE");
        assert_error_contains(&err, "2 arguments");
    }

    #[test]
    fn propagates_null() {
        let end_date = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let batch = create_batch(
            schema_with_two_dates(),
            vec![vec![Value::date(end_date), Value::null()]],
        );
        let args = vec![Expr::column("end_date"), Expr::column("start_date")];
        let result = ProjectionWithExprExec::eval_age(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }
}
