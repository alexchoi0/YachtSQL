use chrono::{Days, NaiveDate};
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
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_date_from_unix_date(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "DATE_FROM_UNIX_DATE requires exactly 1 argument",
            ));
        }

        let days_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if days_val.is_null() {
            return Ok(Value::null());
        }

        let days = days_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: days_val.data_type().to_string(),
        })?;

        let date = if days >= 0 {
            UNIX_EPOCH_DATE.checked_add_days(Days::new(days as u64))
        } else {
            UNIX_EPOCH_DATE.checked_sub_days(Days::new((-days) as u64))
        }
        .ok_or_else(|| {
            Error::ExecutionError(format!("Date overflow: {} days from unix epoch", days))
        })?;

        Ok(Value::date(date))
    }
}
