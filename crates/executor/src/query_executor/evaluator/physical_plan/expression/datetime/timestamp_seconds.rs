use chrono::{DateTime, TimeZone, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_timestamp_seconds(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "TIMESTAMP_SECONDS requires exactly 1 argument",
            ));
        }

        let secs_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if secs_val.is_null() {
            return Ok(Value::null());
        }

        let secs = secs_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: secs_val.data_type().to_string(),
        })?;

        let ts: DateTime<Utc> = Utc
            .timestamp_opt(secs, 0)
            .single()
            .ok_or_else(|| Error::ExecutionError(format!("Invalid timestamp seconds: {}", secs)))?;

        Ok(Value::timestamp(ts))
    }
}
