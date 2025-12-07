mod aead;
mod deterministic;
mod keys;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_encryption_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            name if name.starts_with("AEAD.") => {
                Self::evaluate_aead_function(name, args, batch, row_idx)
            }
            name if name.starts_with("DETERMINISTIC_") => {
                Self::evaluate_deterministic_function(name, args, batch, row_idx)
            }
            name if name.starts_with("KEYS.") => {
                Self::evaluate_keys_function(name, args, batch, row_idx)
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unknown encryption function: {}",
                name
            ))),
        }
    }
}
