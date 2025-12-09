use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_datetime_trunc(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "DATETIME_TRUNC requires exactly 2 arguments (datetime, part)",
            ));
        }

        let dt_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let part_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if dt_val.is_null() {
            return Ok(Value::null());
        }

        let dt = dt_val
            .as_datetime()
            .or_else(|| dt_val.as_timestamp())
            .ok_or_else(|| Error::TypeMismatch {
                expected: "DATETIME/TIMESTAMP".to_string(),
                actual: dt_val.data_type().to_string(),
            })?;

        let part = part_val
            .as_str()
            .ok_or_else(|| Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: part_val.data_type().to_string(),
            })?
            .to_uppercase();

        let truncated = truncate_datetime(dt, &part)?;
        if dt_val.as_datetime().is_some() {
            Ok(Value::datetime(truncated))
        } else {
            Ok(Value::timestamp(truncated))
        }
    }
}

fn truncate_datetime(dt: DateTime<Utc>, part: &str) -> Result<DateTime<Utc>> {
    match part {
        "YEAR" => Utc
            .with_ymd_and_hms(dt.year(), 1, 1, 0, 0, 0)
            .single()
            .ok_or_else(|| Error::ExecutionError("Failed to truncate to year".to_string())),
        "MONTH" => Utc
            .with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0)
            .single()
            .ok_or_else(|| Error::ExecutionError("Failed to truncate to month".to_string())),
        "DAY" => Utc
            .with_ymd_and_hms(dt.year(), dt.month(), dt.day(), 0, 0, 0)
            .single()
            .ok_or_else(|| Error::ExecutionError("Failed to truncate to day".to_string())),
        "HOUR" => Utc
            .with_ymd_and_hms(dt.year(), dt.month(), dt.day(), dt.hour(), 0, 0)
            .single()
            .ok_or_else(|| Error::ExecutionError("Failed to truncate to hour".to_string())),
        "MINUTE" => Utc
            .with_ymd_and_hms(dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(), 0)
            .single()
            .ok_or_else(|| Error::ExecutionError("Failed to truncate to minute".to_string())),
        "SECOND" => Utc
            .with_ymd_and_hms(
                dt.year(),
                dt.month(),
                dt.day(),
                dt.hour(),
                dt.minute(),
                dt.second(),
            )
            .single()
            .ok_or_else(|| Error::ExecutionError("Failed to truncate to second".to_string())),
        _ => Err(Error::invalid_query(format!(
            "Unsupported datetime truncation unit: {}",
            part
        ))),
    }
}
