use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::{Expr, OrderByExpr};

use super::WindowExec;
use crate::Table;

impl WindowExec {
    pub(super) fn compute_percent_rank(
        indices: &[usize],
        order_by: &[OrderByExpr],
        batch: &Table,
        results: &mut [Value],
    ) {
        if indices.len() <= 1 {
            Self::fill_results_with_value(indices, results, Value::float64(0.0));
            return;
        }

        let total_rows = indices.len();
        let mut current_rank = 0i64;
        let mut prev_order_by_values: Option<Vec<Value>> = None;

        for (position, &original_idx) in indices.iter().enumerate() {
            let current_order_by_values =
                Self::evaluate_order_by_values(order_by, batch, original_idx);

            if let Some(ref prev_values) = prev_order_by_values
                && Self::values_differ(&current_order_by_values, prev_values)
            {
                current_rank = position as i64;
            }

            let percent = current_rank as f64 / (total_rows - 1) as f64;
            results[original_idx] = Value::float64(percent);
            prev_order_by_values = Some(current_order_by_values);
        }
    }

    pub(super) fn find_tie_group_end(
        indices: &[usize],
        start_position: usize,
        order_by: &[OrderByExpr],
        batch: &Table,
    ) -> usize {
        if start_position >= indices.len() {
            return start_position;
        }

        let start_idx = indices[start_position];
        let start_values = Self::evaluate_order_by_values(order_by, batch, start_idx);

        let mut end_position = start_position;
        for (offset, &next_idx) in indices[(start_position + 1)..].iter().enumerate() {
            let next_values = Self::evaluate_order_by_values(order_by, batch, next_idx);
            if Self::values_differ(&start_values, &next_values) {
                break;
            }
            end_position = start_position + 1 + offset;
        }

        end_position
    }

    pub(super) fn compute_cume_dist(
        indices: &[usize],
        order_by: &[OrderByExpr],
        batch: &Table,
        results: &mut [Value],
    ) {
        if indices.is_empty() {
            return;
        }

        let total_rows = indices.len();
        let mut prev_order_by_values: Option<Vec<Value>> = None;
        let mut tie_group_end = 0usize;

        for position in 0..indices.len() {
            let original_idx = indices[position];
            let current_order_by_values =
                Self::evaluate_order_by_values(order_by, batch, original_idx);

            let is_new_tie_group = prev_order_by_values
                .as_ref()
                .is_none_or(|prev| Self::values_differ(&current_order_by_values, prev));

            if is_new_tie_group {
                tie_group_end = Self::find_tie_group_end(indices, position, order_by, batch);
            }

            let cume_dist = (tie_group_end + 1) as f64 / total_rows as f64;
            results[original_idx] = Value::float64(cume_dist);
            prev_order_by_values = Some(current_order_by_values);
        }
    }

    pub(super) fn compute_ntile(indices: &[usize], args: &[Expr], results: &mut [Value]) {
        let num_buckets = Self::extract_ntile_bucket_count(args);

        if num_buckets == 0 || indices.is_empty() {
            Self::fill_results_with_null(indices, results);
            return;
        }

        let total_rows = indices.len();
        let base_size = total_rows / num_buckets;
        let remainder = total_rows % num_buckets;

        let mut current_bucket = 1usize;
        let mut rows_in_current_bucket = 0usize;

        for &original_idx in indices.iter() {
            results[original_idx] = Value::int64(current_bucket as i64);
            rows_in_current_bucket += 1;

            let bucket_size = if current_bucket <= remainder {
                base_size + 1
            } else {
                base_size
            };

            if rows_in_current_bucket >= bucket_size && current_bucket < num_buckets {
                current_bucket += 1;
                rows_in_current_bucket = 0;
            }
        }
    }

    fn extract_ntile_bucket_count(args: &[Expr]) -> usize {
        args.first()
            .and_then(|arg| match arg {
                Expr::Literal(crate::optimizer::expr::LiteralValue::Int64(n)) => Some(*n as usize),
                _ => None,
            })
            .unwrap_or(1)
    }

    pub(super) fn compute_unknown_function(indices: &[usize], results: &mut [Value]) {
        Self::fill_results_with_null(indices, results);
    }
}
