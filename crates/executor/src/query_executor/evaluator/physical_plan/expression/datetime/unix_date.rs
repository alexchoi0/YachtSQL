use chrono::NaiveDate;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

const UNIX_EPOCH_DATE: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
    Some(d) => d,
    None => panic!("Invalid epoch date"),
};

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_unix_date(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "UNIX_DATE requires exactly 1 argument",
            ));
        }

        let date_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if date_val.is_null() {
            return Ok(Value::null());
        }

        let date = date_val.as_date().ok_or_else(|| Error::TypeMismatch {
            expected: "DATE".to_string(),
            actual: date_val.data_type().to_string(),
        })?;

        let days = date.signed_duration_since(UNIX_EPOCH_DATE).num_days();
        Ok(Value::int64(days))
    }
}
