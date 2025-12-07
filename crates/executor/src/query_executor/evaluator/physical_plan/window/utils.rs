use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::OrderByExpr;

use super::WindowExec;
use crate::Table;

impl WindowExec {
    pub(super) fn fill_results_with_null(indices: &[usize], results: &mut [Value]) {
        for &original_idx in indices {
            results[original_idx] = Value::null();
        }
    }

    pub(super) fn fill_results_with_value(indices: &[usize], results: &mut [Value], value: Value) {
        for &original_idx in indices {
            results[original_idx] = value.clone();
        }
    }

    pub(super) fn evaluate_order_by_values(
        order_by: &[OrderByExpr],
        batch: &Table,
        row_idx: usize,
    ) -> Vec<Value> {
        order_by
            .iter()
            .map(|order_expr| {
                Self::evaluate_expr(&order_expr.expr, batch, row_idx).unwrap_or(Value::null())
            })
            .collect()
    }

    pub(super) fn values_differ(values_a: &[Value], values_b: &[Value]) -> bool {
        values_a
            .iter()
            .zip(values_b.iter())
            .any(|(a, b)| Self::compare_values(a, b) != std::cmp::Ordering::Equal)
    }
}
