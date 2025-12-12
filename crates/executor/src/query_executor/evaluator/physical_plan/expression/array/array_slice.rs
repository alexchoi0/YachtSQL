use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_array_slice_func(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 || args.len() > 3 {
            return Err(crate::error::Error::invalid_query(
                "ARRAY_SLICE requires 2 or 3 arguments".to_string(),
            ));
        }

        let array_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let start_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let end_val = if args.len() == 3 {
            Some(Self::evaluate_expr(&args[2], batch, row_idx)?)
        } else {
            None
        };

        crate::functions::array::array_slice(&array_val, Some(&start_val), end_val.as_ref())
    }
}
