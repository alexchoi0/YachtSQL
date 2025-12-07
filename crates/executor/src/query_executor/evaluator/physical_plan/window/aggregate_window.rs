use std::collections::HashSet;
use std::rc::Rc;

use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::{ExcludeMode, Expr, OrderByExpr};

use super::WindowExec;
use crate::Table;
use crate::functions::FunctionRegistry;

impl WindowExec {
    pub(super) fn compute_aggregate_with_registry(
        function_name: &str,
        indices: &[usize],
        args: &[Expr],
        order_by: &[OrderByExpr],
        batch: &Table,
        results: &mut [Value],
        exclude: Option<ExcludeMode>,
        registry: &Rc<FunctionRegistry>,
    ) {
        let func_name_upper = function_name.to_uppercase();

        let Some(agg_func) = registry.get_aggregate(&func_name_upper) else {
            Self::fill_results_with_null(indices, results);
            return;
        };

        let peer_groups = if exclude.is_some() && !order_by.is_empty() {
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

            let mut accumulator = agg_func.create_accumulator();

            for &idx in indices {
                if excluded_indices.contains(&idx) {
                    continue;
                }

                let value = if func_name_upper == "COUNT" && args.is_empty() {
                    Value::int64(1)
                } else if args.len() == 1 {
                    Self::evaluate_expr(&args[0], batch, idx).unwrap_or(Value::null())
                } else if args.len() >= 2 {
                    let mut values = Vec::new();
                    for arg in args {
                        values.push(Self::evaluate_expr(arg, batch, idx).unwrap_or(Value::null()));
                    }
                    Value::array(values)
                } else {
                    Value::null()
                };

                if accumulator.accumulate(&value).is_err() {
                    results[row_idx] = Value::null();
                    break;
                }
            }

            results[row_idx] = accumulator.finalize().unwrap_or(Value::null());
        }
    }

    pub(super) fn compute_aggregate_window_function(
        indices: &[usize],
        args: &[Expr],
        order_by: &[OrderByExpr],
        batch: &Table,
        results: &mut [Value],
        exclude: Option<ExcludeMode>,
        registry: &Rc<FunctionRegistry>,
        function_name: &str,
    ) {
        Self::compute_aggregate_with_registry(
            function_name,
            indices,
            args,
            order_by,
            batch,
            results,
            exclude,
            registry,
        );
    }
}
