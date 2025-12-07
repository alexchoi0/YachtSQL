use std::collections::HashSet;
use std::rc::Rc;

use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::{ExcludeMode, Expr, OrderByExpr};

use super::WindowExec;
use crate::Table;
use crate::functions::FunctionRegistry;

impl WindowExec {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_range_frame_window(
        name: &str,
        args: &[Expr],
        indices: &[usize],
        order_by: &[OrderByExpr],
        batch: &Table,
        results: &mut [Value],
        frame_start_offset: Option<i64>,
        frame_end_offset: Option<i64>,
        exclude: Option<ExcludeMode>,
        registry: &Rc<FunctionRegistry>,
    ) {
        if order_by.is_empty() {
            Self::compute_range_no_order_by(name, args, indices, batch, results, exclude, registry);
            return;
        }

        let peer_groups = Self::detect_peer_groups_ordered(indices, order_by, batch);
        let func_name_upper = name.to_uppercase();

        if func_name_upper == "FIRST_VALUE"
            || func_name_upper == "LAST_VALUE"
            || func_name_upper == "NTH_VALUE"
        {
            Self::compute_range_value_function(
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
            Self::compute_range_aggregate_with_registry(
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

    fn compute_range_no_order_by(
        name: &str,
        args: &[Expr],
        indices: &[usize],
        batch: &Table,
        results: &mut [Value],
        exclude: Option<ExcludeMode>,
        registry: &Rc<FunctionRegistry>,
    ) {
        let func_name_upper = name.to_uppercase();

        if let Some(agg_func) = registry.get_aggregate(&func_name_upper) {
            let mut accumulator = agg_func.create_accumulator();
            let is_count_star = func_name_upper == "COUNT" && args.is_empty();

            for &idx in indices {
                let value = if is_count_star {
                    Value::int64(1)
                } else if let Some(expr) = args.first() {
                    Self::evaluate_expr(expr, batch, idx).unwrap_or(Value::null())
                } else {
                    Value::null()
                };
                let _ = accumulator.accumulate(&value);
            }

            let result = accumulator.finalize().unwrap_or(Value::null());

            for &row_idx in indices {
                if exclude == Some(ExcludeMode::CurrentRow) || exclude == Some(ExcludeMode::Group) {
                    let mut acc = agg_func.create_accumulator();
                    for &idx in indices {
                        if idx == row_idx {
                            continue;
                        }
                        let value = if is_count_star {
                            Value::int64(1)
                        } else if let Some(expr) = args.first() {
                            Self::evaluate_expr(expr, batch, idx).unwrap_or(Value::null())
                        } else {
                            Value::null()
                        };
                        let _ = acc.accumulate(&value);
                    }
                    results[row_idx] = acc.finalize().unwrap_or(Value::null());
                } else {
                    results[row_idx] = result.clone();
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_range_aggregate_with_registry(
        func_name: &str,
        args: &[Expr],
        indices: &[usize],
        peer_groups: &[Vec<usize>],
        batch: &Table,
        results: &mut [Value],
        frame_start_offset: Option<i64>,
        frame_end_offset: Option<i64>,
        exclude: Option<ExcludeMode>,
        registry: &Rc<FunctionRegistry>,
    ) {
        let Some(agg_func) = registry.get_aggregate(func_name) else {
            Self::fill_results_with_null(indices, results);
            return;
        };

        for (group_idx, group) in peer_groups.iter().enumerate() {
            let frame_indices = Self::compute_range_frame_indices(
                group_idx,
                indices,
                peer_groups,
                frame_start_offset,
                frame_end_offset,
            );

            for &row_idx in group {
                let result = Self::compute_range_aggregate_for_row(
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
    }

    fn compute_range_frame_indices(
        current_group_idx: usize,
        indices: &[usize],
        peer_groups: &[Vec<usize>],
        frame_start_offset: Option<i64>,
        frame_end_offset: Option<i64>,
    ) -> Vec<usize> {
        let total_groups = peer_groups.len();

        let start_group_idx = match frame_start_offset {
            None => 0,
            Some(0) => current_group_idx,
            Some(n) if n > 0 => current_group_idx.saturating_sub(n as usize),
            Some(n) if n < 0 => (current_group_idx + (-n) as usize).min(total_groups - 1),
            _ => current_group_idx,
        };

        let end_group_idx = match frame_end_offset {
            None => total_groups - 1,
            Some(0) => current_group_idx,
            Some(n) if n > 0 => (current_group_idx + n as usize).min(total_groups - 1),
            Some(n) if n < 0 => current_group_idx.saturating_sub((-n) as usize),
            _ => current_group_idx,
        };

        let mut frame_indices = Vec::new();
        for group_idx in start_group_idx..=end_group_idx {
            frame_indices.extend(peer_groups[group_idx].iter().copied());
        }

        frame_indices.retain(|idx| indices.contains(idx));
        frame_indices
            .sort_by_key(|&idx| indices.iter().position(|&i| i == idx).unwrap_or(usize::MAX));

        frame_indices
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_range_aggregate_for_row(
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

        let is_count_star = func_name == "COUNT" && args.is_empty();

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

    #[allow(clippy::too_many_arguments)]
    fn compute_range_value_function(
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

        for (group_idx, group) in peer_groups.iter().enumerate() {
            let frame_indices = Self::compute_range_frame_indices(
                group_idx,
                indices,
                peer_groups,
                frame_start_offset,
                frame_end_offset,
            );

            for &row_idx in group {
                let mut row_frame_indices = frame_indices.clone();

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

                    row_frame_indices.retain(|idx| !excluded_indices.contains(idx));
                }

                if row_frame_indices.is_empty() {
                    results[row_idx] = Value::null();
                    continue;
                }

                let result = if is_first_value {
                    Self::compute_first_value_in_frame(&row_frame_indices, args, batch)
                } else if is_nth_value {
                    if n < 1 || n > row_frame_indices.len() as i64 {
                        Value::null()
                    } else {
                        let nth_idx = row_frame_indices[(n - 1) as usize];
                        if let Some(value_expr) = args.first() {
                            Self::evaluate_expr(value_expr, batch, nth_idx).unwrap_or(Value::null())
                        } else {
                            Value::null()
                        }
                    }
                } else {
                    Self::compute_last_value_in_frame(&row_frame_indices, args, batch)
                };

                results[row_idx] = result;
            }
        }
    }
}
