use chrono::{Datelike, Timelike};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_date_trunc(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "DATE_TRUNC requires exactly 2 arguments (precision, date)".to_string(),
            ));
        }

        let precision_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let date_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if precision_val.is_null() || date_val.is_null() {
            return Ok(Value::null());
        }

        let precision = match precision_val.as_str() {
            Some(s) => s,
            None => {
                return Err(Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: precision_val.data_type().to_string(),
                });
            }
        };

        if let Some(d) = date_val.as_date() {
            {
                let precision_upper = precision.to_uppercase();
                match precision_upper.as_str() {
                    "YEAR" => match d.with_month(1).and_then(|d| d.with_day(1)) {
                        Some(truncated) => Ok(Value::date(truncated)),
                        None => Err(Error::invalid_query(
                            "Failed to truncate date to YEAR".to_string(),
                        )),
                    },
                    "MONTH" => match d.with_day(1) {
                        Some(truncated) => Ok(Value::date(truncated)),
                        None => Err(Error::invalid_query(
                            "Failed to truncate date to MONTH".to_string(),
                        )),
                    },
                    "DAY" => Ok(Value::date(d)),
                    "WEEK" => {
                        let days_from_monday = d.weekday().num_days_from_monday();
                        let truncated = d - chrono::Duration::days(days_from_monday as i64);
                        Ok(Value::date(truncated))
                    }
                    "QUARTER" => {
                        let quarter_start_month = ((d.month() - 1) / 3) * 3 + 1;
                        match d
                            .with_month(quarter_start_month)
                            .and_then(|d| d.with_day(1))
                        {
                            Some(truncated) => Ok(Value::date(truncated)),
                            None => Err(Error::invalid_query(
                                "Failed to truncate date to QUARTER".to_string(),
                            )),
                        }
                    }
                    _ => Err(Error::invalid_query(format!(
                        "Unknown precision for DATE_TRUNC: {}. Supported: YEAR, QUARTER, MONTH, WEEK, DAY",
                        precision
                    ))),
                }
            }
        } else if let Some(ts) = date_val.as_timestamp() {
            {
                let precision_upper = precision.to_uppercase();
                match precision_upper.as_str() {
                    "YEAR" => {
                        match ts
                            .with_month(1)
                            .and_then(|t| t.with_day(1))
                            .and_then(|t| t.with_hour(0))
                            .and_then(|t| t.with_minute(0))
                            .and_then(|t| t.with_second(0))
                            .and_then(|t| t.with_nanosecond(0))
                        {
                            Some(truncated) => Ok(Value::timestamp(truncated)),
                            None => Err(Error::invalid_query(
                                "Failed to truncate timestamp to YEAR".to_string(),
                            )),
                        }
                    }
                    "QUARTER" => {
                        let quarter_start_month = ((ts.month() - 1) / 3) * 3 + 1;
                        match ts
                            .with_month(quarter_start_month)
                            .and_then(|t| t.with_day(1))
                            .and_then(|t| t.with_hour(0))
                            .and_then(|t| t.with_minute(0))
                            .and_then(|t| t.with_second(0))
                            .and_then(|t| t.with_nanosecond(0))
                        {
                            Some(truncated) => Ok(Value::timestamp(truncated)),
                            None => Err(Error::invalid_query(
                                "Failed to truncate timestamp to QUARTER".to_string(),
                            )),
                        }
                    }
                    "MONTH" => {
                        match ts
                            .with_day(1)
                            .and_then(|t| t.with_hour(0))
                            .and_then(|t| t.with_minute(0))
                            .and_then(|t| t.with_second(0))
                            .and_then(|t| t.with_nanosecond(0))
                        {
                            Some(truncated) => Ok(Value::timestamp(truncated)),
                            None => Err(Error::invalid_query(
                                "Failed to truncate timestamp to MONTH".to_string(),
                            )),
                        }
                    }
                    "WEEK" => {
                        let days_from_monday = ts.weekday().num_days_from_monday();
                        let truncated_date =
                            ts.date_naive() - chrono::Duration::days(days_from_monday as i64);
                        match truncated_date.and_hms_opt(0, 0, 0) {
                            Some(naive_dt) => Ok(Value::timestamp(
                                chrono::DateTime::from_naive_utc_and_offset(naive_dt, chrono::Utc),
                            )),
                            None => Err(Error::invalid_query(
                                "Failed to truncate timestamp to WEEK".to_string(),
                            )),
                        }
                    }
                    "DAY" => {
                        match ts
                            .with_hour(0)
                            .and_then(|t| t.with_minute(0))
                            .and_then(|t| t.with_second(0))
                            .and_then(|t| t.with_nanosecond(0))
                        {
                            Some(truncated) => Ok(Value::timestamp(truncated)),
                            None => Err(Error::invalid_query(
                                "Failed to truncate timestamp to DAY".to_string(),
                            )),
                        }
                    }
                    "HOUR" => {
                        match ts
                            .with_minute(0)
                            .and_then(|t| t.with_second(0))
                            .and_then(|t| t.with_nanosecond(0))
                        {
                            Some(truncated) => Ok(Value::timestamp(truncated)),
                            None => Err(Error::invalid_query(
                                "Failed to truncate timestamp to HOUR".to_string(),
                            )),
                        }
                    }
                    "MINUTE" => match ts.with_second(0).and_then(|t| t.with_nanosecond(0)) {
                        Some(truncated) => Ok(Value::timestamp(truncated)),
                        None => Err(Error::invalid_query(
                            "Failed to truncate timestamp to MINUTE".to_string(),
                        )),
                    },
                    "SECOND" => match ts.with_nanosecond(0) {
                        Some(truncated) => Ok(Value::timestamp(truncated)),
                        None => Err(Error::invalid_query(
                            "Failed to truncate timestamp to SECOND".to_string(),
                        )),
                    },
                    _ => Err(Error::invalid_query(format!(
                        "Unknown precision for DATE_TRUNC with TIMESTAMP: {}. Supported: YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND",
                        precision
                    ))),
                }
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
            Field::nullable("precision", DataType::String),
            Field::nullable("date_col", DataType::Date),
        ])
    }

    fn schema_with_string_and_timestamp() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("precision", DataType::String),
            Field::nullable("ts", DataType::Timestamp),
        ])
    }

    #[test]
    fn truncates_date_to_year() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 8, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("YEAR".into()), Value::date(date)]],
        );
        let args = vec![Expr::column("precision"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_date_trunc(&args, &batch, 0).expect("success");

        let expected = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(result, Value::date(expected));
    }

    #[test]
    fn truncates_date_to_month() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 8, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("MONTH".into()), Value::date(date)]],
        );
        let args = vec![Expr::column("precision"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_date_trunc(&args, &batch, 0).expect("success");

        let expected = chrono::NaiveDate::from_ymd_opt(2024, 8, 1).unwrap();
        assert_eq!(result, Value::date(expected));
    }

    #[test]
    fn truncates_date_to_quarter() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 8, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("QUARTER".into()), Value::date(date)]],
        );
        let args = vec![Expr::column("precision"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_date_trunc(&args, &batch, 0).expect("success");

        let expected = chrono::NaiveDate::from_ymd_opt(2024, 7, 1).unwrap();
        assert_eq!(result, Value::date(expected));
    }

    #[test]
    fn truncates_date_to_day() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 8, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("DAY".into()), Value::date(date)]],
        );
        let args = vec![Expr::column("precision"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_date_trunc(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::date(date));
    }

    #[test]
    fn truncates_timestamp_to_hour() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_string_and_timestamp(),
            vec![vec![Value::string("HOUR".into()), Value::timestamp(ts)]],
        );
        let args = vec![Expr::column("precision"), Expr::column("ts")];
        let result = ProjectionWithExprExec::eval_date_trunc(&args, &batch, 0).expect("success");

        let expected = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(result, Value::timestamp(expected));
    }

    #[test]
    fn truncates_timestamp_to_minute() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_string_and_timestamp(),
            vec![vec![Value::string("MINUTE".into()), Value::timestamp(ts)]],
        );
        let args = vec![Expr::column("precision"), Expr::column("ts")];
        let result = ProjectionWithExprExec::eval_date_trunc(&args, &batch, 0).expect("success");

        let expected = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(result, Value::timestamp(expected));
    }

    #[test]
    fn propagates_null_precision() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 8, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::null(), Value::date(date)]],
        );
        let args = vec![Expr::column("precision"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_date_trunc(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_date() {
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("YEAR".into()), Value::null()]],
        );
        let args = vec![Expr::column("precision"), Expr::column("date_col")];
        let result = ProjectionWithExprExec::eval_date_trunc(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(
            Schema::from_fields(vec![Field::nullable("precision", DataType::String)]),
            vec![vec![Value::string("YEAR".into())]],
        );
        let err = ProjectionWithExprExec::eval_date_trunc(&[Expr::column("precision")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "DATE_TRUNC");
        assert_error_contains(&err, "2 arguments");
    }

    #[test]
    fn errors_on_unknown_precision() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 8, 15).unwrap();
        let batch = create_batch(
            schema_with_string_and_date(),
            vec![vec![Value::string("INVALID".into()), Value::date(date)]],
        );
        let args = vec![Expr::column("precision"), Expr::column("date_col")];
        let err = ProjectionWithExprExec::eval_date_trunc(&args, &batch, 0)
            .expect_err("unknown precision");
        assert_error_contains(&err, "Unknown precision");
        assert_error_contains(&err, "INVALID");
    }

    #[test]
    fn errors_on_type_mismatch() {
        let schema = Schema::from_fields(vec![
            Field::nullable("precision", DataType::String),
            Field::nullable("not_date", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("YEAR".into()),
                Value::string("not-a-date".into()),
            ]],
        );
        let args = vec![Expr::column("precision"), Expr::column("not_date")];
        let err =
            ProjectionWithExprExec::eval_date_trunc(&args, &batch, 0).expect_err("type mismatch");
        assert_error_contains(&err, "DATE");
    }
}
