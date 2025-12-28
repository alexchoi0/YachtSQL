mod accumulator;

use std::collections::HashMap;

use accumulator::Accumulator;
use ordered_float::OrderedFloat;
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{AggregateFunction, Expr, PlanSchema};
use yachtsql_storage::Table;

use super::{PlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

const PARALLEL_AGG_THRESHOLD: usize = 10000;

pub fn compute_aggregate(
    input_table: &Table,
    group_by: &[Expr],
    aggregates: &[Expr],
    schema: &PlanSchema,
    grouping_sets: Option<&Vec<Vec<usize>>>,
    variables: &HashMap<String, Value>,
    user_function_defs: &HashMap<String, crate::ir_evaluator::UserFunctionDef>,
    parallel: bool,
) -> Result<Table> {
    if can_use_columnar_aggregate(aggregates, group_by, grouping_sets) {
        return execute_columnar_aggregate(input_table, aggregates, schema);
    }

    let input_schema = input_table.schema().clone();
    let evaluator = IrEvaluator::new(&input_schema)
        .with_variables(variables)
        .with_user_functions(user_function_defs);

    let result_schema = plan_schema_to_schema(schema);
    let mut result = Table::empty(result_schema);

    if group_by.is_empty() {
        let mut accumulators: Vec<Accumulator> =
            aggregates.iter().map(Accumulator::from_expr).collect();

        for record in input_table.rows()? {
            for (acc, agg_expr) in accumulators.iter_mut().zip(aggregates.iter()) {
                if matches!(
                    acc,
                    Accumulator::SumIf(_)
                        | Accumulator::AvgIf { .. }
                        | Accumulator::MinIf(_)
                        | Accumulator::MaxIf(_)
                ) {
                    let (value, condition) =
                        extract_conditional_agg_args(&evaluator, agg_expr, &record)?;
                    acc.accumulate_conditional(&value, condition)?;
                } else if matches!(acc, Accumulator::ArrayAgg { .. }) {
                    let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                    let sort_keys = extract_order_by_keys(&evaluator, agg_expr, &record)?;
                    acc.accumulate_array_agg(&arg_val, sort_keys)?;
                } else if matches!(acc, Accumulator::Covariance { .. }) {
                    let (x, y) = extract_bivariate_args(&evaluator, agg_expr, &record)?;
                    acc.accumulate_bivariate(&x, &y)?;
                } else if matches!(acc, Accumulator::ApproxTopSum { .. }) {
                    let (value, weight) = extract_bivariate_args(&evaluator, agg_expr, &record)?;
                    acc.accumulate_approx_top_sum(&value, &weight)?;
                } else {
                    let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                    acc.accumulate(&arg_val)?;
                }
            }
        }

        let row: Vec<Value> = accumulators.iter().map(|a| a.finalize()).collect();
        result.push_row(row)?;
    } else if let Some(sets) = grouping_sets {
        let rows = input_table.rows()?;

        for grouping_set in sets {
            let active_indices: Vec<usize> = grouping_set.clone();
            let mut group_map: HashMap<String, (Vec<Value>, Vec<Accumulator>, Vec<usize>)> =
                HashMap::new();

            for record in &rows {
                let mut group_key_values = Vec::new();
                for (i, group_expr) in group_by.iter().enumerate() {
                    if active_indices.contains(&i) {
                        let val = evaluator.evaluate(group_expr, record)?;
                        group_key_values.push(val);
                    } else {
                        group_key_values.push(Value::Null);
                    }
                }
                let key = format!("{:?}", group_key_values);

                let entry = group_map.entry(key).or_insert_with(|| {
                    let mut accs: Vec<Accumulator> =
                        aggregates.iter().map(Accumulator::from_expr).collect();
                    for (acc, agg_expr) in accs.iter_mut().zip(aggregates.iter()) {
                        match acc {
                            Accumulator::Grouping { .. } => {
                                let col_idx = get_grouping_column_index(agg_expr, group_by);
                                let is_active = col_idx
                                    .map(|idx| active_indices.contains(&idx))
                                    .unwrap_or(true);
                                acc.set_grouping_value(if is_active { 0 } else { 1 });
                            }
                            Accumulator::GroupingId { .. } => {
                                let gid = compute_grouping_id(agg_expr, group_by, &active_indices);
                                acc.set_grouping_value(gid);
                            }
                            _ => {}
                        }
                    }
                    (group_key_values.clone(), accs, active_indices.clone())
                });

                for (acc, agg_expr) in entry.1.iter_mut().zip(aggregates.iter()) {
                    if matches!(
                        acc,
                        Accumulator::Grouping { .. } | Accumulator::GroupingId { .. }
                    ) {
                        continue;
                    }
                    if matches!(
                        acc,
                        Accumulator::SumIf(_)
                            | Accumulator::AvgIf { .. }
                            | Accumulator::MinIf(_)
                            | Accumulator::MaxIf(_)
                    ) {
                        let (value, condition) =
                            extract_conditional_agg_args(&evaluator, agg_expr, record)?;
                        acc.accumulate_conditional(&value, condition)?;
                    } else if matches!(acc, Accumulator::ArrayAgg { .. }) {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, record)?;
                        let sort_keys = extract_order_by_keys(&evaluator, agg_expr, record)?;
                        acc.accumulate_array_agg(&arg_val, sort_keys)?;
                    } else if matches!(acc, Accumulator::Covariance { .. }) {
                        let (x, y) = extract_bivariate_args(&evaluator, agg_expr, record)?;
                        acc.accumulate_bivariate(&x, &y)?;
                    } else if matches!(acc, Accumulator::ApproxTopSum { .. }) {
                        let (value, weight) = extract_bivariate_args(&evaluator, agg_expr, record)?;
                        acc.accumulate_approx_top_sum(&value, &weight)?;
                    } else {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, record)?;
                        acc.accumulate(&arg_val)?;
                    }
                }
            }

            for (group_key, accumulators, _active) in group_map.into_values() {
                let mut row = group_key;
                row.extend(accumulators.iter().map(|a| a.finalize()));
                result.push_row(row)?;
            }
        }
    } else {
        let rows = input_table.rows()?;
        let sample_accs: Vec<Accumulator> = aggregates.iter().map(Accumulator::from_expr).collect();
        let can_merge = sample_accs.iter().all(|a| a.is_mergeable());

        if parallel && rows.len() >= PARALLEL_AGG_THRESHOLD && can_merge {
            let num_threads = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            let chunk_size = rows.len().div_ceil(num_threads);

            let input_schema_ref = &input_schema;
            let local_results: Vec<
                Result<(
                    HashMap<Vec<String>, Vec<Accumulator>>,
                    HashMap<Vec<String>, Vec<Value>>,
                )>,
            > = std::thread::scope(|s| {
                let handles: Vec<_> = rows
                    .chunks(chunk_size)
                    .map(|chunk| {
                        s.spawn(move || {
                            let evaluator = IrEvaluator::new(input_schema_ref)
                                .with_variables(variables)
                                .with_user_functions(user_function_defs);
                            let mut local_groups: HashMap<Vec<String>, Vec<Accumulator>> =
                                HashMap::new();
                            let mut local_keys: HashMap<Vec<String>, Vec<Value>> = HashMap::new();

                            for record in chunk {
                                let group_key_values: Vec<Value> = group_by
                                    .iter()
                                    .map(|e| evaluator.evaluate(e, record))
                                    .collect::<Result<_>>()?;
                                let group_key_strings: Vec<String> = group_key_values
                                    .iter()
                                    .map(|v| format!("{:?}", v))
                                    .collect();

                                let accumulators = local_groups
                                    .entry(group_key_strings.clone())
                                    .or_insert_with(|| {
                                        aggregates.iter().map(Accumulator::from_expr).collect()
                                    });
                                local_keys
                                    .entry(group_key_strings.clone())
                                    .or_insert(group_key_values);

                                for (acc, agg_expr) in
                                    accumulators.iter_mut().zip(aggregates.iter())
                                {
                                    if matches!(
                                        acc,
                                        Accumulator::SumIf(_)
                                            | Accumulator::AvgIf { .. }
                                            | Accumulator::MinIf(_)
                                            | Accumulator::MaxIf(_)
                                    ) {
                                        let (value, condition) = extract_conditional_agg_args(
                                            &evaluator, agg_expr, record,
                                        )?;
                                        acc.accumulate_conditional(&value, condition)?;
                                    } else {
                                        let arg_val =
                                            extract_agg_arg(&evaluator, agg_expr, record)?;
                                        acc.accumulate(&arg_val)?;
                                    }
                                }
                            }
                            Ok((local_groups, local_keys))
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });

            let mut merged_groups: HashMap<Vec<String>, Vec<Accumulator>> = HashMap::new();
            let mut merged_keys: HashMap<Vec<String>, Vec<Value>> = HashMap::new();

            for local_result in local_results {
                let (local_groups, local_keys) = local_result?;
                for (key, local_accs) in local_groups {
                    merged_groups
                        .entry(key.clone())
                        .and_modify(|existing| {
                            for (e, l) in existing.iter_mut().zip(local_accs.iter()) {
                                e.merge(l);
                            }
                        })
                        .or_insert(local_accs);
                }
                for (key, vals) in local_keys {
                    merged_keys.entry(key).or_insert(vals);
                }
            }

            for (key_strings, accumulators) in merged_groups {
                let mut row = merged_keys.get(&key_strings).unwrap().clone();
                row.extend(accumulators.iter().map(|a| a.finalize()));
                result.push_row(row)?;
            }
        } else {
            let mut groups: HashMap<Vec<String>, Vec<Accumulator>> = HashMap::new();
            let mut group_keys: HashMap<Vec<String>, Vec<Value>> = HashMap::new();

            for record in rows {
                let group_key_values: Vec<Value> = group_by
                    .iter()
                    .map(|e| evaluator.evaluate(e, &record))
                    .collect::<Result<_>>()?;
                let group_key_strings: Vec<String> = group_key_values
                    .iter()
                    .map(|v| format!("{:?}", v))
                    .collect();

                let accumulators = groups
                    .entry(group_key_strings.clone())
                    .or_insert_with(|| aggregates.iter().map(Accumulator::from_expr).collect());
                group_keys
                    .entry(group_key_strings.clone())
                    .or_insert(group_key_values);

                for (acc, agg_expr) in accumulators.iter_mut().zip(aggregates.iter()) {
                    if matches!(
                        acc,
                        Accumulator::SumIf(_)
                            | Accumulator::AvgIf { .. }
                            | Accumulator::MinIf(_)
                            | Accumulator::MaxIf(_)
                    ) {
                        let (value, condition) =
                            extract_conditional_agg_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_conditional(&value, condition)?;
                    } else if matches!(acc, Accumulator::ArrayAgg { .. }) {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                        let sort_keys = extract_order_by_keys(&evaluator, agg_expr, &record)?;
                        acc.accumulate_array_agg(&arg_val, sort_keys)?;
                    } else if matches!(acc, Accumulator::Covariance { .. }) {
                        let (x, y) = extract_bivariate_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_bivariate(&x, &y)?;
                    } else if matches!(acc, Accumulator::ApproxTopSum { .. }) {
                        let (value, weight) =
                            extract_bivariate_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_approx_top_sum(&value, &weight)?;
                    } else {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                        acc.accumulate(&arg_val)?;
                    }
                }
            }

            for (key_strings, accumulators) in groups {
                let mut row = group_keys.get(&key_strings).unwrap().clone();
                row.extend(accumulators.iter().map(|a| a.finalize()));
                result.push_row(row)?;
            }
        }
    }

    Ok(result)
}

#[allow(clippy::wildcard_enum_match_arm)]
fn get_simple_column_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Column { index, .. } => *index,
        Expr::Alias { expr, .. } => get_simple_column_index(expr),
        Expr::Aggregate { args, .. } if args.len() == 1 => get_simple_column_index(&args[0]),
        _ => None,
    }
}

#[allow(clippy::wildcard_enum_match_arm)]
fn can_use_columnar_aggregate(
    aggregates: &[Expr],
    group_by: &[Expr],
    grouping_sets: Option<&Vec<Vec<usize>>>,
) -> bool {
    if !group_by.is_empty() || grouping_sets.is_some() {
        return false;
    }

    aggregates.iter().all(|expr| {
        let (func, distinct, filter, args) = match expr {
            Expr::Aggregate {
                func,
                args,
                distinct,
                filter,
                ..
            } => (func, *distinct, filter.is_some(), args),
            Expr::Alias { expr, .. } => match expr.as_ref() {
                Expr::Aggregate {
                    func,
                    args,
                    distinct,
                    filter,
                    ..
                } => (func, *distinct, filter.is_some(), args),
                _ => return false,
            },
            _ => return false,
        };

        if distinct || filter {
            return false;
        }

        let is_simple_column = args.len() == 1 && get_simple_column_index(&args[0]).is_some();
        let is_count_star = args.is_empty() || matches!(args.first(), Some(Expr::Wildcard { .. }));

        match func {
            AggregateFunction::Count => is_count_star || is_simple_column,
            AggregateFunction::Sum
            | AggregateFunction::Avg
            | AggregateFunction::Min
            | AggregateFunction::Max => is_simple_column,
            _ => false,
        }
    })
}

fn execute_columnar_aggregate(
    input_table: &Table,
    aggregates: &[Expr],
    schema: &PlanSchema,
) -> Result<Table> {
    let result_schema = plan_schema_to_schema(schema);
    let mut result = Table::empty(result_schema);
    let mut row: Vec<Value> = Vec::with_capacity(aggregates.len());

    for expr in aggregates {
        let (func, args) = match expr {
            Expr::Aggregate { func, args, .. } => (func, args),
            Expr::Alias { expr, .. } => match expr.as_ref() {
                Expr::Aggregate { func, args, .. } => (func, args),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };

        let value = match func {
            AggregateFunction::Count => {
                if args.is_empty() || matches!(args.first(), Some(Expr::Wildcard { .. })) {
                    Value::Int64(input_table.row_count() as i64)
                } else {
                    let col_idx = get_simple_column_index(&args[0]).unwrap();
                    let column = input_table.column(col_idx).unwrap();
                    Value::Int64(column.count_valid() as i64)
                }
            }
            AggregateFunction::Sum => {
                let col_idx = get_simple_column_index(&args[0]).unwrap();
                let column = input_table.column(col_idx).unwrap();
                column.sum().map(Value::float64).unwrap_or(Value::Null)
            }
            AggregateFunction::Avg => {
                let col_idx = get_simple_column_index(&args[0]).unwrap();
                let column = input_table.column(col_idx).unwrap();
                let count = column.count_valid();
                if count == 0 {
                    Value::Null
                } else {
                    column
                        .sum()
                        .map(|s| Value::float64(s / count as f64))
                        .unwrap_or(Value::Null)
                }
            }
            AggregateFunction::Min => {
                let col_idx = get_simple_column_index(&args[0]).unwrap();
                let column = input_table.column(col_idx).unwrap();
                column.min().unwrap_or(Value::Null)
            }
            AggregateFunction::Max => {
                let col_idx = get_simple_column_index(&args[0]).unwrap();
                let column = input_table.column(col_idx).unwrap();
                column.max().unwrap_or(Value::Null)
            }
            _ => unreachable!(),
        };
        row.push(value);
    }

    result.push_row(row)?;
    Ok(result)
}

impl<'a> PlanExecutor<'a> {
    pub fn execute_aggregate(
        &mut self,
        input: &PhysicalPlan,
        group_by: &[Expr],
        aggregates: &[Expr],
        schema: &PlanSchema,
        grouping_sets: Option<&Vec<Vec<usize>>>,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;

        if can_use_columnar_aggregate(aggregates, group_by, grouping_sets) {
            return execute_columnar_aggregate(&input_table, aggregates, schema);
        }

        let input_schema = input_table.schema().clone();
        let evaluator = IrEvaluator::new(&input_schema);

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        if group_by.is_empty() {
            let mut accumulators: Vec<Accumulator> =
                aggregates.iter().map(Accumulator::from_expr).collect();

            for record in input_table.rows()? {
                for (acc, agg_expr) in accumulators.iter_mut().zip(aggregates.iter()) {
                    if matches!(
                        acc,
                        Accumulator::SumIf(_)
                            | Accumulator::AvgIf { .. }
                            | Accumulator::MinIf(_)
                            | Accumulator::MaxIf(_)
                    ) {
                        let (value, condition) =
                            extract_conditional_agg_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_conditional(&value, condition)?;
                    } else if matches!(acc, Accumulator::ArrayAgg { .. }) {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                        let sort_keys = extract_order_by_keys(&evaluator, agg_expr, &record)?;
                        acc.accumulate_array_agg(&arg_val, sort_keys)?;
                    } else if matches!(acc, Accumulator::Covariance { .. }) {
                        let (x, y) = extract_bivariate_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_bivariate(&x, &y)?;
                    } else if matches!(acc, Accumulator::ApproxTopSum { .. }) {
                        let (value, weight) =
                            extract_bivariate_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_approx_top_sum(&value, &weight)?;
                    } else {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                        acc.accumulate(&arg_val)?;
                    }
                }
            }

            let row: Vec<Value> = accumulators.iter().map(|a| a.finalize()).collect();
            result.push_row(row)?;
        } else if let Some(sets) = grouping_sets {
            let rows = input_table.rows()?;

            for grouping_set in sets {
                let active_indices: Vec<usize> = grouping_set.clone();
                let mut group_map: HashMap<String, (Vec<Value>, Vec<Accumulator>, Vec<usize>)> =
                    HashMap::new();

                for record in &rows {
                    let mut group_key_values = Vec::new();
                    for (i, group_expr) in group_by.iter().enumerate() {
                        if active_indices.contains(&i) {
                            let val = evaluator.evaluate(group_expr, record)?;
                            group_key_values.push(val);
                        } else {
                            group_key_values.push(Value::Null);
                        }
                    }
                    let key = format!("{:?}", group_key_values);

                    let entry = group_map.entry(key).or_insert_with(|| {
                        let mut accs: Vec<Accumulator> =
                            aggregates.iter().map(Accumulator::from_expr).collect();
                        for (acc, agg_expr) in accs.iter_mut().zip(aggregates.iter()) {
                            match acc {
                                Accumulator::Grouping { .. } => {
                                    let col_idx = get_grouping_column_index(agg_expr, group_by);
                                    let is_active = col_idx
                                        .map(|idx| active_indices.contains(&idx))
                                        .unwrap_or(true);
                                    acc.set_grouping_value(if is_active { 0 } else { 1 });
                                }
                                Accumulator::GroupingId { .. } => {
                                    let gid =
                                        compute_grouping_id(agg_expr, group_by, &active_indices);
                                    acc.set_grouping_value(gid);
                                }
                                _ => {}
                            }
                        }
                        (group_key_values.clone(), accs, active_indices.clone())
                    });

                    for (acc, agg_expr) in entry.1.iter_mut().zip(aggregates.iter()) {
                        if matches!(
                            acc,
                            Accumulator::Grouping { .. } | Accumulator::GroupingId { .. }
                        ) {
                            continue;
                        }
                        if matches!(
                            acc,
                            Accumulator::SumIf(_)
                                | Accumulator::AvgIf { .. }
                                | Accumulator::MinIf(_)
                                | Accumulator::MaxIf(_)
                        ) {
                            let (value, condition) =
                                extract_conditional_agg_args(&evaluator, agg_expr, record)?;
                            acc.accumulate_conditional(&value, condition)?;
                        } else if matches!(acc, Accumulator::ArrayAgg { .. }) {
                            let arg_val = extract_agg_arg(&evaluator, agg_expr, record)?;
                            let sort_keys = extract_order_by_keys(&evaluator, agg_expr, record)?;
                            acc.accumulate_array_agg(&arg_val, sort_keys)?;
                        } else if matches!(acc, Accumulator::Covariance { .. }) {
                            let (x, y) = extract_bivariate_args(&evaluator, agg_expr, record)?;
                            acc.accumulate_bivariate(&x, &y)?;
                        } else if matches!(acc, Accumulator::ApproxTopSum { .. }) {
                            let (value, weight) =
                                extract_bivariate_args(&evaluator, agg_expr, record)?;
                            acc.accumulate_approx_top_sum(&value, &weight)?;
                        } else {
                            let arg_val = extract_agg_arg(&evaluator, agg_expr, record)?;
                            acc.accumulate(&arg_val)?;
                        }
                    }
                }

                for (group_key, accumulators, _active) in group_map.into_values() {
                    let mut row = group_key;
                    row.extend(accumulators.iter().map(|a| a.finalize()));
                    result.push_row(row)?;
                }
            }
        } else {
            let mut groups: HashMap<Vec<String>, Vec<Accumulator>> = HashMap::new();
            let mut group_keys: HashMap<Vec<String>, Vec<Value>> = HashMap::new();

            for record in input_table.rows()? {
                let group_key_values: Vec<Value> = group_by
                    .iter()
                    .map(|e| evaluator.evaluate(e, &record))
                    .collect::<Result<_>>()?;
                let group_key_strings: Vec<String> = group_key_values
                    .iter()
                    .map(|v| format!("{:?}", v))
                    .collect();

                let accumulators = groups
                    .entry(group_key_strings.clone())
                    .or_insert_with(|| aggregates.iter().map(Accumulator::from_expr).collect());
                group_keys
                    .entry(group_key_strings.clone())
                    .or_insert(group_key_values);

                for (acc, agg_expr) in accumulators.iter_mut().zip(aggregates.iter()) {
                    if matches!(
                        acc,
                        Accumulator::SumIf(_)
                            | Accumulator::AvgIf { .. }
                            | Accumulator::MinIf(_)
                            | Accumulator::MaxIf(_)
                    ) {
                        let (value, condition) =
                            extract_conditional_agg_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_conditional(&value, condition)?;
                    } else if matches!(acc, Accumulator::ArrayAgg { .. }) {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                        let sort_keys = extract_order_by_keys(&evaluator, agg_expr, &record)?;
                        acc.accumulate_array_agg(&arg_val, sort_keys)?;
                    } else if matches!(acc, Accumulator::Covariance { .. }) {
                        let (x, y) = extract_bivariate_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_bivariate(&x, &y)?;
                    } else if matches!(acc, Accumulator::ApproxTopSum { .. }) {
                        let (value, weight) =
                            extract_bivariate_args(&evaluator, agg_expr, &record)?;
                        acc.accumulate_approx_top_sum(&value, &weight)?;
                    } else {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                        acc.accumulate(&arg_val)?;
                    }
                }
            }

            for (key_strings, accumulators) in groups {
                let mut row = group_keys.get(&key_strings).unwrap().clone();
                row.extend(accumulators.iter().map(|a| a.finalize()));
                result.push_row(row)?;
            }
        }

        Ok(result)
    }
}

fn extract_agg_arg(
    evaluator: &IrEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<Value> {
    match agg_expr {
        Expr::Aggregate { args, .. } => {
            if args.is_empty() {
                Ok(Value::Null)
            } else if matches!(&args[0], Expr::Wildcard { .. }) {
                Ok(Value::Int64(1))
            } else {
                evaluator.evaluate(&args[0], record)
            }
        }
        Expr::UserDefinedAggregate { args, .. } => {
            if args.is_empty() {
                Ok(Value::Null)
            } else {
                evaluator.evaluate(&args[0], record)
            }
        }
        Expr::Alias { expr, .. } => extract_agg_arg(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => evaluator.evaluate(agg_expr, record),
    }
}

fn extract_conditional_agg_args(
    evaluator: &IrEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<(Value, bool)> {
    match agg_expr {
        Expr::Aggregate { args, .. } => {
            if args.len() >= 2 {
                let value = evaluator.evaluate(&args[0], record)?;
                let condition_val = evaluator.evaluate(&args[1], record)?;
                let condition = condition_val.as_bool().unwrap_or(false);
                Ok((value, condition))
            } else if args.len() == 1 {
                let value = evaluator.evaluate(&args[0], record)?;
                Ok((value, true))
            } else {
                Ok((Value::Null, false))
            }
        }
        Expr::UserDefinedAggregate { args, .. } => {
            if args.len() >= 2 {
                let value = evaluator.evaluate(&args[0], record)?;
                let condition_val = evaluator.evaluate(&args[1], record)?;
                let condition = condition_val.as_bool().unwrap_or(false);
                Ok((value, condition))
            } else if args.len() == 1 {
                let value = evaluator.evaluate(&args[0], record)?;
                Ok((value, true))
            } else {
                Ok((Value::Null, false))
            }
        }
        Expr::Alias { expr, .. } => extract_conditional_agg_args(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => Ok((Value::Null, false)),
    }
}

fn extract_bivariate_args(
    evaluator: &IrEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<(Value, Value)> {
    match agg_expr {
        Expr::Aggregate { args, .. } | Expr::UserDefinedAggregate { args, .. } => {
            if args.len() >= 2 {
                let x = evaluator.evaluate(&args[0], record)?;
                let y = evaluator.evaluate(&args[1], record)?;
                Ok((x, y))
            } else {
                Ok((Value::Null, Value::Null))
            }
        }
        Expr::Alias { expr, .. } => extract_bivariate_args(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => Ok((Value::Null, Value::Null)),
    }
}

fn extract_order_by_keys(
    evaluator: &IrEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<Vec<(Value, bool)>> {
    match agg_expr {
        Expr::Aggregate { order_by, .. } => {
            let mut keys = Vec::new();
            for sort_expr in order_by {
                let val = evaluator.evaluate(&sort_expr.expr, record)?;
                keys.push((val, sort_expr.asc));
            }
            Ok(keys)
        }
        Expr::UserDefinedAggregate { .. } => Ok(Vec::new()),
        Expr::Alias { expr, .. } => extract_order_by_keys(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => Ok(Vec::new()),
    }
}

fn has_order_by(agg_expr: &Expr) -> bool {
    match agg_expr {
        Expr::Aggregate { order_by, .. } => !order_by.is_empty(),
        Expr::UserDefinedAggregate { .. } => false,
        Expr::Alias { expr, .. } => has_order_by(expr),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => false,
    }
}

fn get_grouping_column_index(agg_expr: &Expr, group_by: &[Expr]) -> Option<usize> {
    let arg = match agg_expr {
        Expr::Aggregate { args, .. } => args.first(),
        Expr::Alias { expr, .. } => {
            return get_grouping_column_index(expr, group_by);
        }
        _ => None,
    }?;

    for (i, group_expr) in group_by.iter().enumerate() {
        if exprs_match(arg, group_expr) {
            return Some(i);
        }
    }
    None
}

fn exprs_match(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (
            Expr::Column {
                name: n1,
                table: t1,
                ..
            },
            Expr::Column {
                name: n2,
                table: t2,
                ..
            },
        ) => {
            n1.eq_ignore_ascii_case(n2)
                && match (t1, t2) {
                    (Some(t1), Some(t2)) => t1.eq_ignore_ascii_case(t2),
                    (None, None) => true,
                    _ => true,
                }
        }
        (Expr::Alias { expr: e1, .. }, e2) => exprs_match(e1, e2),
        (e1, Expr::Alias { expr: e2, .. }) => exprs_match(e1, e2),
        _ => format!("{:?}", a) == format!("{:?}", b),
    }
}

fn compute_grouping_id(agg_expr: &Expr, group_by: &[Expr], active_indices: &[usize]) -> i64 {
    let args = match agg_expr {
        Expr::Aggregate { args, .. } => args,
        Expr::Alias { expr, .. } => {
            return compute_grouping_id(expr, group_by, active_indices);
        }
        _ => return 0,
    };

    let mut result: i64 = 0;
    let n = args.len();
    for (arg_pos, arg) in args.iter().enumerate() {
        let mut is_active = true;
        for (i, group_expr) in group_by.iter().enumerate() {
            if exprs_match(arg, group_expr) {
                is_active = active_indices.contains(&i);
                break;
            }
        }
        if !is_active {
            result |= 1 << (n - 1 - arg_pos);
        }
    }
    result
}
