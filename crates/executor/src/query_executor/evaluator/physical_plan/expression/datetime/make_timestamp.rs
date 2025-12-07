use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_make_timestamp(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 6 {
            return Err(Error::invalid_query(
                "MAKE_TIMESTAMP requires exactly 6 arguments (year, month, day, hour, minute, second)".to_string(),
            ));
        }

        let year_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let month_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let day_val = Self::evaluate_expr(&args[2], batch, row_idx)?;
        let hour_val = Self::evaluate_expr(&args[3], batch, row_idx)?;
        let minute_val = Self::evaluate_expr(&args[4], batch, row_idx)?;
        let second_val = Self::evaluate_expr(&args[5], batch, row_idx)?;

        if year_val.is_null()
            || month_val.is_null()
            || day_val.is_null()
            || hour_val.is_null()
            || minute_val.is_null()
            || second_val.is_null()
        {
            return Ok(Value::null());
        }

        let year = year_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: year_val.data_type().to_string(),
        })?;

        let month = month_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: month_val.data_type().to_string(),
        })?;

        let day = day_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: day_val.data_type().to_string(),
        })?;

        let hour = hour_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: hour_val.data_type().to_string(),
        })?;

        let minute = minute_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: minute_val.data_type().to_string(),
        })?;

        let second = second_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: second_val.data_type().to_string(),
        })?;

        use chrono::{DateTime, NaiveDate, Utc};
        match NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
            Some(date) => match date.and_hms_opt(hour as u32, minute as u32, second as u32) {
                Some(naive_datetime) => {
                    let timestamp = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
                    Ok(Value::timestamp(timestamp))
                }
                None => Err(Error::invalid_query(format!(
                    "Invalid time: {}:{:02}:{:02}",
                    hour, minute, second
                ))),
            },
            None => Err(Error::invalid_query(format!(
                "Invalid date: year={}, month={}, day={}",
                year, month, day
            ))),
        }
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

    #[test]
    fn test_make_timestamp_success() {
        let schema = Schema::from_fields(vec![
            Field::nullable("year", DataType::Int64),
            Field::nullable("month", DataType::Int64),
            Field::nullable("day", DataType::Int64),
            Field::nullable("hour", DataType::Int64),
            Field::nullable("minute", DataType::Int64),
            Field::nullable("second", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::int64(2024),
                Value::int64(3),
                Value::int64(15),
                Value::int64(10),
                Value::int64(30),
                Value::int64(45),
            ]],
        );
        let args = vec![
            Expr::column("year"),
            Expr::column("month"),
            Expr::column("day"),
            Expr::column("hour"),
            Expr::column("minute"),
            Expr::column("second"),
        ];
        let result =
            ProjectionWithExprExec::eval_make_timestamp(&args, &batch, 0).expect("should succeed");

        assert!(
            result.as_timestamp().is_some(),
            "Expected a timestamp value"
        );
    }
}
