use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_unix_millis(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "UNIX_MILLIS requires exactly 1 argument",
            ));
        }

        let ts_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if ts_val.is_null() {
            return Ok(Value::null());
        }

        let ts = ts_val.as_timestamp().ok_or_else(|| Error::TypeMismatch {
            expected: "TIMESTAMP".to_string(),
            actual: ts_val.data_type().to_string(),
        })?;

        Ok(Value::int64(ts.timestamp_millis()))
    }
}
