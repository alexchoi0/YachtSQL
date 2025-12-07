use chrono::Datelike;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_quarter(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "QUARTER requires exactly 1 argument (date)".to_string(),
            ));
        }

        let date_val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if date_val.is_null() {
            return Ok(Value::null());
        }

        let month = if let Some(d) = date_val.as_date() {
            d.month()
        } else if let Some(ts) = date_val.as_timestamp() {
            ts.month()
        } else {
            return Err(Error::TypeMismatch {
                expected: "DATE or TIMESTAMP".to_string(),
                actual: date_val.data_type().to_string(),
            });
        };

        let quarter = ((month - 1) / 3) + 1;
        Ok(Value::int64(quarter as i64))
    }
}
