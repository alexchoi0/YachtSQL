use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::OrderByExpr;

use super::WindowExec;
use crate::Table;

impl WindowExec {
    pub(super) fn compute_row_number(indices: &[usize], results: &mut [Value]) {
        for (row_number, &original_idx) in indices.iter().enumerate() {
            results[original_idx] = Value::int64((row_number + 1) as i64);
        }
    }

    pub(super) fn compute_rank(
        indices: &[usize],
        order_by: &[OrderByExpr],
        batch: &Table,
        results: &mut [Value],
        dense: bool,
    ) {
        let mut current_rank = 1i64;
        let mut prev_order_by_values: Option<Vec<Value>> = None;

        for (position, &original_idx) in indices.iter().enumerate() {
            let current_order_by_values =
                Self::evaluate_order_by_values(order_by, batch, original_idx);

            if let Some(ref prev_values) = prev_order_by_values
                && Self::values_differ(&current_order_by_values, prev_values)
            {
                current_rank = if dense {
                    current_rank + 1
                } else {
                    (position + 1) as i64
                };
            }

            results[original_idx] = Value::int64(current_rank);
            prev_order_by_values = Some(current_order_by_values);
        }
    }
}
