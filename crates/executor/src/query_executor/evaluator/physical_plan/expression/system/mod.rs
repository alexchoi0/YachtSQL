use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_system_function(
        name: &str,
        _args: &[Expr],
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        Err(Error::unsupported_feature(format!(
            "Unknown system function: {}",
            name
        )))
    }
}
