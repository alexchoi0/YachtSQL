use chrono::Timelike;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_minute(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "MINUTE requires exactly 1 argument (timestamp)".to_string(),
            ));
        }

        let ts_val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if ts_val.is_null() {
            return Ok(Value::null());
        }

        if let Some(ts) = ts_val.as_timestamp() {
            Ok(Value::int64(ts.minute() as i64))
        } else if let Some(t) = ts_val.as_time() {
            Ok(Value::int64(t.minute() as i64))
        } else {
            Err(Error::TypeMismatch {
                expected: "TIMESTAMP or TIME".to_string(),
                actual: ts_val.data_type().to_string(),
            })
        }
    }
}
