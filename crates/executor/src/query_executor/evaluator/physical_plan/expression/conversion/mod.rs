mod to_char;
mod to_number;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_conversion_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "TO_NUMBER" => Self::eval_to_number(args, batch, row_idx),
            "TO_CHAR" => Self::eval_to_char(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown conversion function: {}",
                name
            ))),
        }
    }
}
