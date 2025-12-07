mod is_feature_enabled;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_system_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "YACHTSQL.IS_FEATURE_ENABLED" => Self::eval_is_feature_enabled(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown system function: {}",
                name
            ))),
        }
    }
}
