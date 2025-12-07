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
            _ => Err(Error::unsupported_feature(format!(
                "Unknown conditional function: {}",
                name
            ))),
        }
    }
}
