use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn eval_crc32(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("CRC32 requires exactly 1 argument"));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }

        let bytes = if let Some(s) = val.as_str() {
            s.as_bytes().to_vec()
        } else if let Some(b) = val.as_bytes() {
            b.to_vec()
        } else {
            return Err(Error::invalid_query(
                "CRC32 argument must be a string or bytes",
            ));
        };

        let checksum = crc32fast::hash(&bytes);
        Ok(Value::int64(checksum as i64))
    }

    pub(super) fn eval_crc32c(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("CRC32C requires exactly 1 argument"));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }

        let bytes = if let Some(s) = val.as_str() {
            s.as_bytes().to_vec()
        } else if let Some(b) = val.as_bytes() {
            b.to_vec()
        } else {
            return Err(Error::invalid_query(
                "CRC32C argument must be a string or bytes",
            ));
        };

        let checksum = crc32c::crc32c(&bytes);
        Ok(Value::int64(checksum as i64))
    }
}
