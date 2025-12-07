use chrono::Datelike;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_month(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "MONTH requires exactly 1 argument (date)".to_string(),
            ));
        }

        let date_val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if date_val.is_null() {
            return Ok(Value::null());
        }

        if let Some(d) = date_val.as_date() {
            Ok(Value::int64(d.month() as i64))
        } else if let Some(ts) = date_val.as_timestamp() {
            Ok(Value::int64(ts.month() as i64))
        } else {
            Err(Error::TypeMismatch {
                expected: "DATE or TIMESTAMP".to_string(),
                actual: date_val.data_type().to_string(),
            })
        }
    }
}
