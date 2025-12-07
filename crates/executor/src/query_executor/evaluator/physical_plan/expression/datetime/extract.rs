use chrono::{Datelike, Timelike};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_extract(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "EXTRACT requires exactly 2 arguments (part, date)".to_string(),
            ));
        }

        let part_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let date_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if part_val.is_null() || date_val.is_null() {
            return Ok(Value::null());
        }

        let part = match part_val.as_str() {
            Some(s) => s,
            None => {
                return Err(Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: part_val.data_type().to_string(),
                });
            }
        };

        let part_upper = part.to_uppercase();

        if let Some(d) = date_val.as_date() {
            match part_upper.as_str() {
                "YEAR" => Ok(Value::int64(d.year() as i64)),
                "MONTH" => Ok(Value::int64(d.month() as i64)),
                "DAY" => Ok(Value::int64(d.day() as i64)),
                "DOW" | "DAYOFWEEK" => Ok(Value::int64(
                    (d.weekday().num_days_from_sunday() as i64) + 1,
                )),
                "DOY" | "DAYOFYEAR" => Ok(Value::int64(d.ordinal() as i64)),
                "QUARTER" => {
                    let quarter = ((d.month() - 1) / 3) + 1;
                    Ok(Value::int64(quarter as i64))
                }
                "WEEK" => Ok(Value::int64(d.iso_week().week() as i64)),
                _ => Err(Error::invalid_query(format!(
                    "Unknown date part for EXTRACT: {}. Supported: YEAR, MONTH, DAY, DAYOFWEEK, DAYOFYEAR, QUARTER, WEEK",
                    part
                ))),
            }
        } else if let Some(ts) = date_val.as_timestamp() {
            match part_upper.as_str() {
                "YEAR" => Ok(Value::int64(ts.year() as i64)),
                "MONTH" => Ok(Value::int64(ts.month() as i64)),
                "DAY" => Ok(Value::int64(ts.day() as i64)),
                "HOUR" => Ok(Value::int64(ts.hour() as i64)),
                "MINUTE" => Ok(Value::int64(ts.minute() as i64)),
                "SECOND" => Ok(Value::int64(ts.second() as i64)),
                "DOW" | "DAYOFWEEK" => Ok(Value::int64(ts.weekday().num_days_from_sunday() as i64)),
                "DOY" | "DAYOFYEAR" => Ok(Value::int64(ts.ordinal() as i64)),
                _ => Err(Error::invalid_query(format!(
                    "Unknown date part for EXTRACT: {}. Supported: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DOW, DOY",
                    part
                ))),
            }
        } else {
            Err(Error::TypeMismatch {
                expected: "DATE or TIMESTAMP".to_string(),
                actual: date_val.data_type().to_string(),
            })
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

    fn schema_with_string_and_date() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("part", DataType::String),
            Field::nullable("date_col", DataType::Date),
        ])
    }

    fn schema_with_string_and_timestamp() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("part", DataType::String),
            Field::nullable("ts", DataType::Timestamp),
        ])
    }

    #[test]
    fn extracts_year_from_date() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("YEAR".into()), Value::date(date)]],
        );
        let args = vec![Expr::column("part"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_extract(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(2024));
    }

    #[test]
    fn extracts_month_from_date() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("MONTH".into()), Value::date(date)]],
        );
        let args = vec![Expr::column("part"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_extract(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(3));
    }

    #[test]
    fn extracts_day_from_date() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("DAY".into()), Value::date(date)]],
        );
        let args = vec![Expr::column("part"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_extract(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(15));
    }

    #[test]
    fn extracts_quarter_from_date() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 8, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("QUARTER".into()), Value::date(date)]],
        );
        let args = vec![Expr::column("part"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_extract(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(3));
    }

    #[test]
    fn extracts_hour_from_timestamp() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-03-15T14:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_string_and_timestamp(),
            vec![vec![Value::string("HOUR".into()), Value::timestamp(ts)]],
        );
        let args = vec![Expr::column("part"), Expr::column("ts")];
        let result = ProjectionWithExprExec::eval_extract(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(14));
    }

    #[test]
    fn extracts_minute_from_timestamp() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-03-15T14:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_string_and_timestamp(),
            vec![vec![Value::string("MINUTE".into()), Value::timestamp(ts)]],
        );
        let args = vec![Expr::column("part"), Expr::column("ts")];
        let result = ProjectionWithExprExec::eval_extract(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(30));
    }

    #[test]
    fn propagates_null_part() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::null(), Value::date(date)]],
        );
        let args = vec![Expr::column("part"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_extract(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_date() {
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("YEAR".into()), Value::null()]],
        );
        let args = vec![Expr::column("part"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_extract(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(
            Schema::from_fields(vec![Field::nullable("part", DataType::String)]),
            vec![vec![Value::string("YEAR".into())]],
        );
        let err = ProjectionWithExprExec::eval_extract(&[Expr::column("part")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "EXTRACT");
        assert_error_contains(&err, "2 arguments");
    }

    #[test]
    fn errors_on_unknown_date_part() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![
                Value::string("INVALID_PART".into()),
                Value::date(date),
            ]],
        );
        let args = vec![Expr::column("part"), Expr::column("date_col")];
        let err = ProjectionWithExprExec::eval_extract(&args, &batch, 0).expect_err("unknown part");
        assert_error_contains(&err, "Unknown date part");
        assert_error_contains(&err, "INVALID_PART");
    }

    #[test]
    fn errors_on_type_mismatch() {
        let schema = Schema::from_fields(vec![
            Field::nullable("part", DataType::String),
            Field::nullable("not_date", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("YEAR".into()),
                Value::string("not-a-date".into()),
            ]],
        );
        let args = vec![Expr::column("part"), Expr::column("not_date")];
        let err =
            ProjectionWithExprExec::eval_extract(&args, &batch, 0).expect_err("type mismatch");
        assert_error_contains(&err, "DATE");
    }
}
