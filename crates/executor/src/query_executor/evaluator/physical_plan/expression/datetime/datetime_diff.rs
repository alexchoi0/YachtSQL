use chrono::Datelike;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_datetime_diff(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 3 {
            return Err(Error::invalid_query(
                "DATETIME_DIFF requires exactly 3 arguments (datetime1, datetime2, part)",
            ));
        }

        let dt1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let dt2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let part_val = Self::evaluate_expr(&args[2], batch, row_idx)?;

        if dt1_val.is_null() || dt2_val.is_null() {
            return Ok(Value::null());
        }

        let dt1 = dt1_val
            .as_datetime()
            .or_else(|| dt1_val.as_timestamp())
            .ok_or_else(|| Error::TypeMismatch {
                expected: "DATETIME/TIMESTAMP".to_string(),
                actual: dt1_val.data_type().to_string(),
            })?;

        let dt2 = dt2_val
            .as_datetime()
            .or_else(|| dt2_val.as_timestamp())
            .ok_or_else(|| Error::TypeMismatch {
                expected: "DATETIME/TIMESTAMP".to_string(),
                actual: dt2_val.data_type().to_string(),
            })?;

        let part = part_val
            .as_str()
            .ok_or_else(|| Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: part_val.data_type().to_string(),
            })?
            .to_uppercase();

        let diff = match part.as_str() {
            "DAY" => {
                let duration = dt1.signed_duration_since(dt2);
                duration.num_days()
            }
            "HOUR" => {
                let duration = dt1.signed_duration_since(dt2);
                duration.num_hours()
            }
            "MINUTE" => {
                let duration = dt1.signed_duration_since(dt2);
                duration.num_minutes()
            }
            "SECOND" => {
                let duration = dt1.signed_duration_since(dt2);
                duration.num_seconds()
            }
            "MONTH" => {
                let months1 = dt1.year() * 12 + dt1.month() as i32;
                let months2 = dt2.year() * 12 + dt2.month() as i32;
                (months1 - months2) as i64
            }
            "YEAR" => (dt1.year() - dt2.year()) as i64,
            _ => {
                return Err(Error::invalid_query(format!(
                    "Unsupported datetime diff unit: {}",
                    part
                )));
            }
        };

        Ok(Value::int64(diff))
    }
}
