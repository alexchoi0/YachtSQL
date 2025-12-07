use std::collections::HashSet;
use std::rc::Rc;

use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::{ExcludeMode, Expr, OrderByExpr};

use super::WindowExec;
use crate::Table;
use crate::functions::FunctionRegistry;

impl WindowExec {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_rows_frame_window(
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
        let func_name_upper = name.to_uppercase();

        if func_name_upper == "FIRST_VALUE"
            || func_name_upper == "LAST_VALUE"
            || func_name_upper == "NTH_VALUE"
        {
            Self::compute_rows_value_function(
                &func_name_upper,
                args,
                indices,
                batch,
                results,
                frame_start_offset,
                frame_end_offset,
                order_by,
                exclude,
            );
        } else if registry.has_aggregate(&func_name_upper) {
            Self::compute_rows_aggregate_with_registry(
                &func_name_upper,
                args,
                indices,
                order_by,
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
    fn compute_rows_aggregate_with_registry(
        func_name: &str,
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
        let Some(agg_func) = registry.get_aggregate(func_name) else {
            Self::fill_results_with_null(indices, results);
            return;
        };

        let peer_groups = if exclude.is_some() && !order_by.is_empty() {
            Self::build_peer_groups(indices, order_by, batch)
        } else {
            std::collections::HashMap::new()
        };

        let num_rows = indices.len();

        for (position, &row_idx) in indices.iter().enumerate() {
            let frame_indices = Self::compute_rows_frame_indices(
                position,
                indices,
                frame_start_offset,
                frame_end_offset,
            );

            let excluded_indices: HashSet<usize> = match exclude {
                None | Some(ExcludeMode::NoOthers) => HashSet::new(),
                Some(ExcludeMode::CurrentRow) => {
                    let mut set = HashSet::new();
                    set.insert(row_idx);
                    set
                }
                Some(ExcludeMode::Group) => Self::find_peer_group(&peer_groups, row_idx),
                Some(ExcludeMode::Ties) => {
                    let mut peer_group = Self::find_peer_group(&peer_groups, row_idx);
                    peer_group.remove(&row_idx);
                    peer_group
                }
            };

            let filtered_indices: Vec<usize> = frame_indices
                .iter()
                .filter(|idx| !excluded_indices.contains(idx))
                .copied()
                .collect();

            let mut accumulator = agg_func.create_accumulator();
            let is_count_star = func_name == "COUNT" && args.is_empty();

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

            results[row_idx] = accumulator.finalize().unwrap_or(Value::null());
        }
    }

    fn compute_rows_frame_indices(
        current_position: usize,
        indices: &[usize],
        frame_start_offset: Option<i64>,
        frame_end_offset: Option<i64>,
    ) -> Vec<usize> {
        let num_rows = indices.len();

        let start_position = match frame_start_offset {
            None => 0,
            Some(0) => current_position,
            Some(n) if n > 0 => current_position.saturating_sub(n as usize),
            Some(n) if n < 0 => (current_position + (-n) as usize).min(num_rows - 1),
            _ => current_position,
        };

        let end_position = match frame_end_offset {
            None => num_rows - 1,
            Some(0) => current_position,
            Some(n) if n > 0 => (current_position + n as usize).min(num_rows - 1),
            Some(n) if n < 0 => current_position.saturating_sub((-n) as usize),
            _ => current_position,
        };

        if start_position <= end_position {
            indices[start_position..=end_position].to_vec()
        } else {
            Vec::new()
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_rows_value_function(
        func_name: &str,
        args: &[Expr],
        indices: &[usize],
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

        let peer_groups = if exclude.is_some() && !order_by.is_empty() {
            Self::build_peer_groups(indices, order_by, batch)
        } else {
            std::collections::HashMap::new()
        };

        for (position, &row_idx) in indices.iter().enumerate() {
            let mut frame_indices = Self::compute_rows_frame_indices(
                position,
                indices,
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
                    ExcludeMode::Group => Self::find_peer_group(&peer_groups, row_idx),
                    ExcludeMode::Ties => {
                        let mut peer_group = Self::find_peer_group(&peer_groups, row_idx);
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
}
