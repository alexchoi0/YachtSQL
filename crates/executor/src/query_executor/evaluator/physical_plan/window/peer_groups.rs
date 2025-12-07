use std::collections::{HashMap, HashSet};

use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::OrderByExpr;

use super::WindowExec;
use crate::Table;

impl WindowExec {
    pub(super) fn build_peer_groups(
        indices: &[usize],
        order_by: &[OrderByExpr],
        batch: &Table,
    ) -> HashMap<usize, Vec<usize>> {
        let mut order_by_groups: HashMap<String, Vec<usize>> = HashMap::new();

        for &row_idx in indices {
            let order_values = Self::evaluate_order_by_values(order_by, batch, row_idx);
            let key = Self::values_to_key(&order_values);
            order_by_groups.entry(key).or_default().push(row_idx);
        }

        let mut peer_groups = HashMap::new();
        for peer_indices in order_by_groups.values() {
            for &row_idx in peer_indices {
                peer_groups.insert(row_idx, peer_indices.clone());
            }
        }

        peer_groups
    }

    pub(super) fn values_to_key(values: &[Value]) -> String {
        values
            .iter()
            .map(|v| format!("{:?}", v))
            .collect::<Vec<_>>()
            .join("|")
    }

    pub(super) fn find_peer_group(
        peer_groups: &HashMap<usize, Vec<usize>>,
        row_idx: usize,
    ) -> HashSet<usize> {
        peer_groups
            .get(&row_idx)
            .map(|indices| indices.iter().copied().collect())
            .unwrap_or_default()
    }

    pub(super) fn detect_peer_groups_ordered(
        indices: &[usize],
        order_by: &[OrderByExpr],
        batch: &Table,
    ) -> Vec<Vec<usize>> {
        if indices.is_empty() {
            return Vec::new();
        }

        let mut peer_groups = Vec::new();
        let mut current_group = vec![indices[0]];
        let mut prev_values = Self::evaluate_order_by_values(order_by, batch, indices[0]);

        for &row_idx in indices.iter().skip(1) {
            let current_values = Self::evaluate_order_by_values(order_by, batch, row_idx);

            if Self::values_equal(&prev_values, &current_values) {
                current_group.push(row_idx);
            } else {
                peer_groups.push(current_group);
                current_group = vec![row_idx];
                prev_values = current_values;
            }
        }

        peer_groups.push(current_group);
        peer_groups
    }

    pub(super) fn find_peer_group_from_ordered(
        peer_groups: &[Vec<usize>],
        row_idx: usize,
    ) -> HashSet<usize> {
        peer_groups
            .iter()
            .find(|group| group.contains(&row_idx))
            .map(|group| group.iter().copied().collect())
            .unwrap_or_default()
    }
}
