use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::{ProjectionWithExprExec, SEQUENCE_EXECUTOR_CONTEXT};
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_sequence_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "NEXTVAL" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("NEXTVAL requires 1 argument"));
                }
                let seq_name = Self::extract_sequence_name(&args[0], batch, row_idx)?;
                let value = SEQUENCE_EXECUTOR_CONTEXT.with(|ctx| {
                    let ctx_ref = ctx.borrow();
                    let executor = ctx_ref.as_ref().ok_or_else(|| {
                        Error::InternalError("Sequence executor context not set".to_string())
                    })?;
                    executor.borrow_mut().nextval(&seq_name)
                })?;
                Ok(Value::int64(value))
            }
            "CURRVAL" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("CURRVAL requires 1 argument"));
                }
                let seq_name = Self::extract_sequence_name(&args[0], batch, row_idx)?;
                let value = SEQUENCE_EXECUTOR_CONTEXT.with(|ctx| {
                    let ctx_ref = ctx.borrow();
                    let executor = ctx_ref.as_ref().ok_or_else(|| {
                        Error::InternalError("Sequence executor context not set".to_string())
                    })?;
                    executor.borrow().currval(&seq_name)
                })?;
                Ok(Value::int64(value))
            }
            "SETVAL" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::invalid_query("SETVAL requires 2 or 3 arguments"));
                }
                let seq_name = Self::extract_sequence_name(&args[0], batch, row_idx)?;
                let value = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let int_value = value.as_i64().ok_or_else(|| {
                    Error::InvalidOperation("SETVAL value must be an integer".to_string())
                })?;
                let is_called = if args.len() == 3 {
                    let called_arg = Self::evaluate_expr(&args[2], batch, row_idx)?;
                    called_arg.as_bool().unwrap_or(true)
                } else {
                    true
                };
                let result = SEQUENCE_EXECUTOR_CONTEXT.with(|ctx| {
                    let ctx_ref = ctx.borrow();
                    let executor = ctx_ref.as_ref().ok_or_else(|| {
                        Error::InternalError("Sequence executor context not set".to_string())
                    })?;
                    executor
                        .borrow_mut()
                        .setval(&seq_name, int_value, is_called)
                })?;
                Ok(Value::int64(result))
            }
            "LASTVAL" => {
                if !args.is_empty() {
                    return Err(Error::invalid_query("LASTVAL takes no arguments"));
                }
                let value = SEQUENCE_EXECUTOR_CONTEXT.with(|ctx| {
                    let ctx_ref = ctx.borrow();
                    let executor = ctx_ref.as_ref().ok_or_else(|| {
                        Error::InternalError("Sequence executor context not set".to_string())
                    })?;
                    executor.borrow().lastval()
                })?;
                Ok(Value::int64(value))
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unknown sequence function: {}",
                name
            ))),
        }
    }

    fn extract_sequence_name(expr: &Expr, batch: &Table, row_idx: usize) -> Result<String> {
        let value = Self::evaluate_expr(expr, batch, row_idx)?;
        value
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| Error::InvalidOperation("Sequence name must be a string".to_string()))
    }
}
