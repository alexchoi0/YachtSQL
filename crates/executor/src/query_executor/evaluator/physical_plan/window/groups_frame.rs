use std::collections::HashSet;

use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::{ExcludeMode, Expr, OrderByExpr};

use super::WindowExec;
use crate::Table;

impl WindowExec {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_groups_frame_window(
        name: &str,
        args: &[Expr],
        indices: &[usize],
        order_by: &[OrderByExpr],
        batch: &Table,
        results: &mut [Value],
        frame_start_offset: Option<i64>,
        frame_end_offset: Option<i64>,
        exclude: Option<ExcludeMode>,
        registry: &std::rc::Rc<crate::functions::FunctionRegistry>,
    ) {
        if order_by.is_empty() {
            Self::fill_results_with_null(indices, results);
            return;
        }

        let peer_groups = Self::detect_peer_groups_ordered(indices, order_by, batch);
        let func_name_upper = name.to_uppercase();

        if func_name_upper == "FIRST_VALUE"
            || func_name_upper == "LAST_VALUE"
            || func_name_upper == "NTH_VALUE"
        {
            Self::compute_groups_value_function(
                &func_name_upper,
                args,
                indices,
                &peer_groups,
                batch,
                results,
                frame_start_offset,
                frame_end_offset,
                order_by,
                exclude,
            );
        } else if registry.has_aggregate(&func_name_upper) {
            Self::compute_groups_aggregate_with_registry(
                &func_name_upper,
                args,
                indices,
                &peer_groups,
                batch,
                results,
                frame_start_offset,
                frame_end_offset,
                exclude,
                registry,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_groups_aggregate_with_registry(
        func_name: &str,
        args: &[Expr],
        indices: &[usize],
        peer_groups: &[Vec<usize>],
        batch: &Table,
        results: &mut [Value],
        frame_start_offset: Option<i64>,
        frame_end_offset: Option<i64>,
        exclude: Option<ExcludeMode>,
        registry: &std::rc::Rc<crate::functions::FunctionRegistry>,
    ) {
        let Some(agg_func) = registry.get_aggregate(func_name) else {
            Self::fill_results_with_null(indices, results);
            return;
        };

        for &row_idx in indices.iter() {
            let frame_indices = Self::compute_frame_indices(
                row_idx,
                indices,
                peer_groups,
                frame_start_offset,
                frame_end_offset,
            );

            let result = Self::compute_aggregate_for_frame_with_registry(
                func_name,
                args,
                &frame_indices,
                batch,
                exclude,
                peer_groups,
                row_idx,
                agg_func.as_ref(),
            );

            results[row_idx] = result;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_groups_value_function(
        func_name: &str,
        args: &[Expr],
        indices: &[usize],
        peer_groups: &[Vec<usize>],
        batch: &Table,
        results: &mut [Value],
        frame_start_offset: Option<i64>,
        frame_end_offset: Option<i64>,
        order_by: &[OrderByExpr],
        exclude: Option<ExcludeMode>,
    ) {
        let is_first_value = func_name == "FIRST_VALUE";
        let is_nth_value = func_name == "NTH_VALUE";

        let n = if is_nth_value && args.len() >= 2 {
            match &args[1] {
                Expr::Literal(crate::optimizer::expr::LiteralValue::Int64(n)) => *n,
                _ => 1,
            }
        } else {
            0
        };

        let peer_groups_map = if exclude.is_some() {
            Self::build_peer_groups(indices, order_by, batch)
        } else {
            std::collections::HashMap::new()
        };

        for &row_idx in indices.iter() {
            let mut frame_indices = Self::compute_frame_indices(
                row_idx,
                indices,
                peer_groups,
                frame_start_offset,
                frame_end_offset,
            );

            if let Some(exclude_mode) = exclude {
                let excluded_indices: HashSet<usize> = match exclude_mode {
                    ExcludeMode::NoOthers => HashSet::new(),
                    ExcludeMode::CurrentRow => {
                        let mut set = HashSet::new();
                        set.insert(row_idx);
                        set
                    }
                    ExcludeMode::Group => Self::find_peer_group(&peer_groups_map, row_idx),
                    ExcludeMode::Ties => {
                        let mut peer_group = Self::find_peer_group(&peer_groups_map, row_idx);
                        peer_group.remove(&row_idx);
                        peer_group
                    }
                };

                frame_indices.retain(|idx| !excluded_indices.contains(idx));
            }

            if frame_indices.is_empty() {
                results[row_idx] = Value::null();
                continue;
            }

            let result = if is_first_value {
                Self::compute_first_value_in_frame(&frame_indices, args, batch)
            } else if is_nth_value {
                if n < 1 || n > frame_indices.len() as i64 {
                    Value::null()
                } else {
                    let nth_idx = frame_indices[(n - 1) as usize];
                    if let Some(value_expr) = args.first() {
                        Self::evaluate_expr(value_expr, batch, nth_idx).unwrap_or(Value::null())
                    } else {
                        Value::null()
                    }
                }
            } else {
                Self::compute_last_value_in_frame(&frame_indices, args, batch)
            };

            results[row_idx] = result;
        }
    }

    pub(super) fn compute_frame_indices(
        row_idx: usize,
        indices: &[usize],
        peer_groups: &[Vec<usize>],
        frame_start_offset: Option<i64>,
        frame_end_offset: Option<i64>,
    ) -> Vec<usize> {
        let (start_row, end_row) = Self::compute_groups_frame_for_row(
            row_idx,
            peer_groups,
            frame_start_offset,
            frame_end_offset,
        );

        indices
            .iter()
            .filter(|&&idx| idx >= start_row && idx <= end_row)
            .copied()
            .collect()
    }

    pub(super) fn compute_groups_frame_for_row(
        row_idx: usize,
        peer_groups: &[Vec<usize>],
        start_offset: Option<i64>,
        end_offset: Option<i64>,
    ) -> (usize, usize) {
        let current_group_idx = peer_groups
            .iter()
            .position(|group| group.contains(&row_idx))
            .expect("Row must be in some peer group");

        let start_group_idx = Self::calculate_group_boundary(
            current_group_idx,
            start_offset,
            peer_groups.len(),
            true,
        );

        let end_group_idx =
            Self::calculate_group_boundary(current_group_idx, end_offset, peer_groups.len(), false);

        let start_row = peer_groups[start_group_idx]
            .first()
            .copied()
            .expect("Peer group must have at least one row");
        let end_row = peer_groups[end_group_idx]
            .last()
            .copied()
            .expect("Peer group must have at least one row");

        (start_row, end_row)
    }

    pub(super) fn calculate_group_boundary(
        current_group_idx: usize,
        offset: Option<i64>,
        total_groups: usize,
        is_start: bool,
    ) -> usize {
        match offset {
            None => {
                if is_start {
                    0
                } else {
                    total_groups - 1
                }
            }
            Some(0) => current_group_idx,
            Some(n) if n > 0 => {
                if is_start {
                    current_group_idx.saturating_sub(n as usize)
                } else {
                    (current_group_idx + n as usize).min(total_groups - 1)
                }
            }
            Some(n) if n < 0 => {
                if is_start {
                    (current_group_idx + (-n) as usize).min(total_groups - 1)
                } else {
                    current_group_idx.saturating_sub((-n) as usize)
                }
            }
            _ => current_group_idx,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_aggregate_for_frame_with_registry(
        func_name: &str,
        args: &[Expr],
        frame_indices: &[usize],
        batch: &Table,
        exclude: Option<ExcludeMode>,
        peer_groups: &[Vec<usize>],
        current_row_idx: usize,
        agg_func: &dyn crate::functions::AggregateFunction,
    ) -> Value {
        let excluded_indices: HashSet<usize> = match exclude {
            None | Some(ExcludeMode::NoOthers) => HashSet::new(),
            Some(ExcludeMode::CurrentRow) => {
                let mut set = HashSet::new();
                set.insert(current_row_idx);
                set
            }
            Some(ExcludeMode::Group) => {
                Self::find_peer_group_from_ordered(peer_groups, current_row_idx)
            }
            Some(ExcludeMode::Ties) => {
                let mut group = Self::find_peer_group_from_ordered(peer_groups, current_row_idx);
                group.remove(&current_row_idx);
                group
            }
        };

        let filtered_indices: Vec<usize> = frame_indices
            .iter()
            .filter(|idx| !excluded_indices.contains(idx))
            .copied()
            .collect();

        let is_count_star = func_name == "COUNT"
            && (args.is_empty() || (args.len() == 1 && matches!(args[0], Expr::Wildcard)));

        let mut accumulator = agg_func.create_accumulator();

        if is_count_star {
            for _ in &filtered_indices {
                let _ = accumulator.accumulate(&Value::int64(1));
            }
        } else if args.len() == 1 {
            if let Some(value_expr) = args.first() {
                for &idx in &filtered_indices {
                    if let Ok(value) = Self::evaluate_expr(value_expr, batch, idx) {
                        let _ = accumulator.accumulate(&value);
                    }
                }
            }
        } else if args.len() >= 2 {
            for &idx in &filtered_indices {
                let mut values = Vec::new();
                for arg in args {
                    values.push(Self::evaluate_expr(arg, batch, idx).unwrap_or(Value::null()));
                }
                let _ = accumulator.accumulate(&Value::array(values));
            }
        }

        accumulator.finalize().unwrap_or(Value::null())
    }
}
