use std::collections::HashSet;

use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::{ExcludeMode, Expr, OrderByExpr};

use super::WindowExec;
use crate::Table;

impl WindowExec {
    pub(super) fn compute_first_value(
        indices: &[usize],
        args: &[Expr],
        batch: &Table,
        results: &mut [Value],
        order_by: &[OrderByExpr],
        exclude: Option<ExcludeMode>,
        null_treatment: Option<yachtsql_optimizer::expr::NullTreatment>,
    ) {
        Self::compute_frame_edge_value(
            indices,
            args,
            batch,
            results,
            true,
            order_by,
            exclude,
            null_treatment,
        );
    }

    pub(super) fn compute_last_value(
        indices: &[usize],
        args: &[Expr],
        batch: &Table,
        results: &mut [Value],
        order_by: &[OrderByExpr],
        exclude: Option<ExcludeMode>,
        null_treatment: Option<yachtsql_optimizer::expr::NullTreatment>,
    ) {
        Self::compute_frame_edge_value(
            indices,
            args,
            batch,
            results,
            false,
            order_by,
            exclude,
            null_treatment,
        );
    }

    pub(super) fn compute_frame_edge_value(
        indices: &[usize],
        args: &[Expr],
        batch: &Table,
        results: &mut [Value],
        is_first: bool,
        order_by: &[OrderByExpr],
        exclude: Option<ExcludeMode>,
        null_treatment: Option<yachtsql_optimizer::expr::NullTreatment>,
    ) {
        let Some(value_expr) = args.first() else {
            Self::fill_results_with_null(indices, results);
            return;
        };

        if indices.is_empty() {
            return;
        }

        let peer_groups = if exclude.is_some() || !order_by.is_empty() {
            Self::build_peer_groups(indices, order_by, batch)
        } else {
            std::collections::HashMap::new()
        };

        for &row_idx in indices.iter() {
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

            let frame_indices: Vec<usize> = if !order_by.is_empty() && !is_first {
                let current_peer_group = Self::find_peer_group(&peer_groups, row_idx);
                let max_peer_idx = current_peer_group.iter().max().copied().unwrap_or(row_idx);
                indices
                    .iter()
                    .take_while(|&&idx| idx <= max_peer_idx || current_peer_group.contains(&idx))
                    .filter(|&&idx| !excluded_indices.contains(&idx))
                    .copied()
                    .collect()
            } else {
                indices
                    .iter()
                    .filter(|&&idx| !excluded_indices.contains(&idx))
                    .copied()
                    .collect()
            };

            if frame_indices.is_empty() {
                results[row_idx] = Value::null();
                continue;
            }

            let ignore_nulls = matches!(
                null_treatment,
                Some(yachtsql_optimizer::expr::NullTreatment::IgnoreNulls)
            );

            if ignore_nulls {
                let edge_value_opt = if is_first {
                    frame_indices.iter().find_map(|&idx| {
                        let val = Self::evaluate_expr(value_expr, batch, idx).ok()?;
                        if val.is_null() { None } else { Some(val) }
                    })
                } else {
                    frame_indices.iter().rev().find_map(|&idx| {
                        let val = Self::evaluate_expr(value_expr, batch, idx).ok()?;
                        if val.is_null() { None } else { Some(val) }
                    })
                };

                results[row_idx] = edge_value_opt.unwrap_or(Value::null());
            } else {
                let edge_idx = if is_first {
                    frame_indices[0]
                } else {
                    frame_indices[frame_indices.len() - 1]
                };

                let edge_value =
                    Self::evaluate_expr(value_expr, batch, edge_idx).unwrap_or(Value::null());

                results[row_idx] = edge_value;
            }
        }
    }

    pub(super) fn compute_nth_value(
        indices: &[usize],
        args: &[Expr],
        batch: &Table,
        results: &mut [Value],
        order_by: &[OrderByExpr],
        exclude: Option<ExcludeMode>,
        null_treatment: Option<yachtsql_optimizer::expr::NullTreatment>,
    ) {
        if args.len() < 2 {
            Self::fill_results_with_null(indices, results);
            return;
        }

        let Some(value_expr) = args.first() else {
            Self::fill_results_with_null(indices, results);
            return;
        };

        let n = match &args[1] {
            Expr::Literal(crate::optimizer::expr::LiteralValue::Int64(n)) => *n,
            _ => {
                Self::fill_results_with_null(indices, results);
                return;
            }
        };

        if indices.is_empty() {
            return;
        }

        let peer_groups = if exclude.is_some() || !order_by.is_empty() {
            Self::build_peer_groups(indices, order_by, batch)
        } else {
            std::collections::HashMap::new()
        };

        for &row_idx in indices.iter() {
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

            let frame_indices: Vec<usize> = if !order_by.is_empty() {
                let current_peer_group = Self::find_peer_group(&peer_groups, row_idx);
                let max_peer_idx = current_peer_group.iter().max().copied().unwrap_or(row_idx);
                indices
                    .iter()
                    .take_while(|&&idx| idx <= max_peer_idx || current_peer_group.contains(&idx))
                    .filter(|&&idx| !excluded_indices.contains(&idx))
                    .copied()
                    .collect()
            } else {
                indices
                    .iter()
                    .filter(|&&idx| !excluded_indices.contains(&idx))
                    .copied()
                    .collect()
            };

            if frame_indices.is_empty() || n < 1 {
                results[row_idx] = Value::null();
                continue;
            }

            let ignore_nulls = matches!(
                null_treatment,
                Some(yachtsql_optimizer::expr::NullTreatment::IgnoreNulls)
            );

            if ignore_nulls {
                let non_null_values: Vec<(usize, Value)> = frame_indices
                    .iter()
                    .filter_map(|&idx| {
                        let val = Self::evaluate_expr(value_expr, batch, idx).ok()?;
                        if val.is_null() {
                            None
                        } else {
                            Some((idx, val))
                        }
                    })
                    .collect();

                if (n as usize) > non_null_values.len() {
                    results[row_idx] = Value::null();
                } else {
                    results[row_idx] = non_null_values[(n - 1) as usize].1.clone();
                }
            } else {
                if n > frame_indices.len() as i64 {
                    results[row_idx] = Value::null();
                    continue;
                }

                let nth_idx = frame_indices[(n - 1) as usize];
                let nth_value =
                    Self::evaluate_expr(value_expr, batch, nth_idx).unwrap_or(Value::null());

                results[row_idx] = nth_value;
            }
        }
    }

    pub(super) fn compute_first_value_in_frame(
        frame_indices: &[usize],
        args: &[Expr],
        batch: &Table,
    ) -> Value {
        if frame_indices.is_empty() {
            return Value::null();
        }

        if let Some(value_expr) = args.first()
            && let Ok(value) = Self::evaluate_expr(value_expr, batch, frame_indices[0])
        {
            return value;
        }

        Value::null()
    }

    pub(super) fn compute_last_value_in_frame(
        frame_indices: &[usize],
        args: &[Expr],
        batch: &Table,
    ) -> Value {
        if frame_indices.is_empty() {
            return Value::null();
        }

        if let Some(value_expr) = args.first() {
            let last_idx = frame_indices[frame_indices.len() - 1];
            if let Ok(value) = Self::evaluate_expr(value_expr, batch, last_idx) {
                return value;
            }
        }

        Value::null()
    }
}
