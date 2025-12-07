use chrono::{Datelike, NaiveDate};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_last_day(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "LAST_DAY requires exactly 1 argument (date)".to_string(),
            ));
        }

        let date_val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if date_val.is_null() {
            return Ok(Value::null());
        }

        let (year, month) = if let Some(d) = date_val.as_date() {
            (d.year(), d.month())
        } else if let Some(ts) = date_val.as_timestamp() {
            (ts.year(), ts.month())
        } else {
            return Err(Error::TypeMismatch {
                expected: "DATE or TIMESTAMP".to_string(),
                actual: date_val.data_type().to_string(),
            });
        };

        let (next_year, next_month) = if month == 12 {
            (year + 1, 1)
        } else {
            (year, month + 1)
        };

        let first_of_next_month = NaiveDate::from_ymd_opt(next_year, next_month, 1)
            .ok_or_else(|| Error::invalid_query("Invalid date calculation".to_string()))?;

        let last_day = first_of_next_month
            .pred_opt()
            .ok_or_else(|| Error::invalid_query("Invalid date calculation".to_string()))?;

        Ok(Value::date(last_day))
    }
}
