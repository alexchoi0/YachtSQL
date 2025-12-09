mod coalesce;
mod decode;
mod greatest;
mod if_iif;
mod ifnull;
mod least;
mod nullif;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_conditional_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "COALESCE" => Self::eval_coalesce(args, batch, row_idx),
            "IFNULL" => Self::eval_ifnull(args, batch, row_idx),
            "NULLIF" => Self::eval_nullif(args, batch, row_idx),
            "IF" | "IIF" => Self::eval_if(args, batch, row_idx),
            "DECODE" => Self::eval_decode(args, batch, row_idx),
            "GREATEST" => Self::eval_greatest(args, batch, row_idx),
            "LEAST" => Self::eval_least(args, batch, row_idx),
            "ISNULL" => Self::eval_isnull(args, batch, row_idx),
            "ISNOTNULL" => Self::eval_isnotnull(args, batch, row_idx),
            "ASSUMENOTNULL" => Self::eval_assumenotnull(args, batch, row_idx),
            "TONULLABLE" => Self::eval_tonullable(args, batch, row_idx),
            "ISZEROORNULL" => Self::eval_iszeroornull(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown conditional function: {}",
                name
            ))),
        }
    }

    fn eval_isnull(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "isNull requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        Ok(Value::bool_val(val.is_null()))
    }

    fn eval_isnotnull(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "isNotNull requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        Ok(Value::bool_val(!val.is_null()))
    }

    fn eval_assumenotnull(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "assumeNotNull requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            Err(Error::invalid_query(
                "assumeNotNull: value is NULL".to_string(),
            ))
        } else {
            Ok(val)
        }
    }

    fn eval_tonullable(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "toNullable requires 1 argument".to_string(),
            ));
        }
        Self::evaluate_expr(&args[0], batch, row_idx)
    }

    fn eval_iszeroornull(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "isZeroOrNull requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::bool_val(true));
        }
        let is_zero = val.as_i64().map(|v| v == 0).unwrap_or(false)
            || val.as_f64().map(|v| v == 0.0).unwrap_or(false);
        Ok(Value::bool_val(is_zero))
    }
}
