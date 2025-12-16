use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_sha384_with_dialect(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "SHA384 requires exactly 1 argument".to_string(),
            ));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let return_hex = false;
        yachtsql_functions::scalar::eval_sha384(&val, return_hex)
    }
}
