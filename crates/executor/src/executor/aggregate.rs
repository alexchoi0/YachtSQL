use std::collections::HashMap;

use ordered_float::OrderedFloat;
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{AggregateFunction, Expr, PlanSchema};
use yachtsql_storage::Table;

use super::{PlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

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
        Expr::Aggregate { args, .. } => {
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

fn get_agg_func(expr: &Expr) -> Option<&AggregateFunction> {
    match expr {
        Expr::Aggregate { func, .. } => Some(func),
        Expr::Alias { expr, .. } => get_agg_func(expr),
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
        | Expr::Default => None,
    }
}

fn is_distinct_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { distinct, .. } => *distinct,
        Expr::Alias { expr, .. } => is_distinct_aggregate(expr),
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

fn extract_agg_limit(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Aggregate { limit, .. } => *limit,
        Expr::Alias { expr, .. } => extract_agg_limit(expr),
        _ => None,
    }
}

fn has_ignore_nulls(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { ignore_nulls, .. } => *ignore_nulls,
        Expr::Alias { expr, .. } => has_ignore_nulls(expr),
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

fn extract_int_arg(expr: &Expr, index: usize) -> Option<i64> {
    match expr {
        Expr::Aggregate { args, .. } => {
            if args.len() > index
                && let Expr::Literal(yachtsql_ir::Literal::Int64(n)) = &args[index]
            {
                return Some(*n);
            }
            None
        }
        Expr::Alias { expr, .. } => extract_int_arg(expr, index),
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
        | Expr::Default => None,
    }
}

fn extract_string_agg_separator(expr: &Expr) -> String {
    match expr {
        Expr::Aggregate { args, .. } => {
            if args.len() >= 2
                && let Expr::Literal(yachtsql_ir::Literal::String(s)) = &args[1]
            {
                return s.clone();
            }
            ",".to_string()
        }
        Expr::Alias { expr, .. } => extract_string_agg_separator(expr),
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
        | Expr::Default => ",".to_string(),
    }
}

#[derive(Clone)]
enum Accumulator {
    Count(i64),
    CountIf(i64),
    Sum(Option<f64>),
    Avg {
        sum: f64,
        count: i64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    ArrayAgg {
        items: Vec<(Value, Vec<(Value, bool)>)>,
        ignore_nulls: bool,
        limit: Option<usize>,
    },
    StringAgg {
        values: Vec<String>,
        separator: String,
    },
    First(Option<Value>),
    Last(Option<Value>),
    CountDistinct(Vec<Value>),
    BitAnd(Option<i64>),
    BitOr(Option<i64>),
    BitXor(Option<i64>),
    LogicalAnd(Option<bool>),
    LogicalOr(Option<bool>),
    Variance {
        count: i64,
        mean: f64,
        m2: f64,
        is_sample: bool,
        is_stddev: bool,
    },
    SumIf(Option<f64>),
    AvgIf {
        sum: f64,
        count: i64,
    },
    MinIf(Option<Value>),
    MaxIf(Option<Value>),
    Covariance {
        count: i64,
        mean_x: f64,
        mean_y: f64,
        c_xy: f64,
        m2_x: f64,
        m2_y: f64,
        is_sample: bool,
        stat_type: CovarianceStatType,
    },
    Grouping {
        value: i64,
    },
    GroupingId {
        value: i64,
    },
    ApproxQuantiles {
        values: Vec<f64>,
        num_quantiles: usize,
    },
    ApproxTopCount {
        counts: std::collections::HashMap<String, i64>,
        top_n: usize,
    },
    ApproxTopSum {
        sums: std::collections::HashMap<String, f64>,
        top_n: usize,
    },
}

#[derive(Clone, Copy)]
enum CovarianceStatType {
    Covariance,
    Correlation,
}

impl Accumulator {
    fn from_expr(expr: &Expr) -> Self {
        let distinct = is_distinct_aggregate(expr);
        match get_agg_func(expr) {
            Some(func) => match func {
                AggregateFunction::Count => {
                    if distinct {
                        Accumulator::CountDistinct(Vec::new())
                    } else {
                        Accumulator::Count(0)
                    }
                }
                AggregateFunction::CountIf => Accumulator::CountIf(0),
                AggregateFunction::SumIf => Accumulator::SumIf(None),
                AggregateFunction::AvgIf => Accumulator::AvgIf { sum: 0.0, count: 0 },
                AggregateFunction::MinIf => Accumulator::MinIf(None),
                AggregateFunction::MaxIf => Accumulator::MaxIf(None),
                AggregateFunction::Sum => Accumulator::Sum(None),
                AggregateFunction::Avg => Accumulator::Avg { sum: 0.0, count: 0 },
                AggregateFunction::Min => Accumulator::Min(None),
                AggregateFunction::Max => Accumulator::Max(None),
                AggregateFunction::ArrayAgg => Accumulator::ArrayAgg {
                    items: Vec::new(),
                    ignore_nulls: has_ignore_nulls(expr),
                    limit: extract_agg_limit(expr),
                },
                AggregateFunction::StringAgg => Accumulator::StringAgg {
                    values: Vec::new(),
                    separator: extract_string_agg_separator(expr),
                },
                AggregateFunction::XmlAgg => Accumulator::StringAgg {
                    values: Vec::new(),
                    separator: String::new(),
                },
                AggregateFunction::AnyValue => Accumulator::First(None),
                AggregateFunction::BitAnd => Accumulator::BitAnd(None),
                AggregateFunction::BitOr => Accumulator::BitOr(None),
                AggregateFunction::BitXor => Accumulator::BitXor(None),
                AggregateFunction::LogicalAnd => Accumulator::LogicalAnd(None),
                AggregateFunction::LogicalOr => Accumulator::LogicalOr(None),
                AggregateFunction::Stddev | AggregateFunction::StddevSamp => {
                    Accumulator::Variance {
                        count: 0,
                        mean: 0.0,
                        m2: 0.0,
                        is_sample: true,
                        is_stddev: true,
                    }
                }
                AggregateFunction::StddevPop => Accumulator::Variance {
                    count: 0,
                    mean: 0.0,
                    m2: 0.0,
                    is_sample: false,
                    is_stddev: true,
                },
                AggregateFunction::Variance | AggregateFunction::VarSamp => Accumulator::Variance {
                    count: 0,
                    mean: 0.0,
                    m2: 0.0,
                    is_sample: true,
                    is_stddev: false,
                },
                AggregateFunction::VarPop => Accumulator::Variance {
                    count: 0,
                    mean: 0.0,
                    m2: 0.0,
                    is_sample: false,
                    is_stddev: false,
                },
                AggregateFunction::Grouping => Accumulator::Grouping { value: 0 },
                AggregateFunction::GroupingId => Accumulator::GroupingId { value: 0 },
                AggregateFunction::ApproxQuantiles => {
                    let num_quantiles = extract_int_arg(expr, 1).unwrap_or(100) as usize;
                    Accumulator::ApproxQuantiles {
                        values: Vec::new(),
                        num_quantiles,
                    }
                }
                AggregateFunction::ApproxTopCount => {
                    let top_n = extract_int_arg(expr, 1).unwrap_or(10) as usize;
                    Accumulator::ApproxTopCount {
                        counts: std::collections::HashMap::new(),
                        top_n,
                    }
                }
                AggregateFunction::ApproxTopSum => {
                    let top_n = extract_int_arg(expr, 2).unwrap_or(10) as usize;
                    Accumulator::ApproxTopSum {
                        sums: std::collections::HashMap::new(),
                        top_n,
                    }
                }
                AggregateFunction::Corr => Accumulator::Covariance {
                    count: 0,
                    mean_x: 0.0,
                    mean_y: 0.0,
                    c_xy: 0.0,
                    m2_x: 0.0,
                    m2_y: 0.0,
                    is_sample: false,
                    stat_type: CovarianceStatType::Correlation,
                },
                AggregateFunction::CovarPop => Accumulator::Covariance {
                    count: 0,
                    mean_x: 0.0,
                    mean_y: 0.0,
                    c_xy: 0.0,
                    m2_x: 0.0,
                    m2_y: 0.0,
                    is_sample: false,
                    stat_type: CovarianceStatType::Covariance,
                },
                AggregateFunction::CovarSamp => Accumulator::Covariance {
                    count: 0,
                    mean_x: 0.0,
                    mean_y: 0.0,
                    c_xy: 0.0,
                    m2_x: 0.0,
                    m2_y: 0.0,
                    is_sample: true,
                    stat_type: CovarianceStatType::Covariance,
                },
                AggregateFunction::ApproxCountDistinct => Accumulator::CountDistinct(Vec::new()),
            },
            None => Accumulator::Count(0),
        }
    }

    fn accumulate(&mut self, value: &Value) -> Result<()> {
        match self {
            Accumulator::Count(n) => {
                if !value.is_null() {
                    *n += 1;
                }
            }
            Accumulator::CountIf(n) => {
                if let Some(true) = value.as_bool() {
                    *n += 1;
                }
            }
            Accumulator::Sum(sum) => {
                if let Some(v) = value.as_f64() {
                    *sum = Some(sum.unwrap_or(0.0) + v);
                } else if let Some(v) = value.as_i64() {
                    *sum = Some(sum.unwrap_or(0.0) + v as f64);
                } else if let Value::Numeric(v) = value {
                    use rust_decimal::prelude::ToPrimitive;
                    if let Some(f) = v.to_f64() {
                        *sum = Some(sum.unwrap_or(0.0) + f);
                    }
                }
            }
            Accumulator::Avg { sum, count } => {
                if let Some(v) = value.as_f64() {
                    *sum += v;
                    *count += 1;
                } else if let Some(v) = value.as_i64() {
                    *sum += v as f64;
                    *count += 1;
                } else if let Value::Numeric(v) = value {
                    use rust_decimal::prelude::ToPrimitive;
                    if let Some(f) = v.to_f64() {
                        *sum += f;
                        *count += 1;
                    }
                }
            }
            Accumulator::Min(min) => {
                if !value.is_null() {
                    *min = Some(match min.take() {
                        Some(m) => {
                            if value < &m {
                                value.clone()
                            } else {
                                m
                            }
                        }
                        None => value.clone(),
                    });
                }
            }
            Accumulator::Max(max) => {
                if !value.is_null() {
                    *max = Some(match max.take() {
                        Some(m) => {
                            if value > &m {
                                value.clone()
                            } else {
                                m
                            }
                        }
                        None => value.clone(),
                    });
                }
            }
            Accumulator::ArrayAgg { .. } => {}
            Accumulator::StringAgg { values, .. } => {
                if let Some(s) = value.as_str() {
                    values.push(s.to_string());
                }
            }
            Accumulator::First(first) => {
                if first.is_none() && !value.is_null() {
                    *first = Some(value.clone());
                }
            }
            Accumulator::Last(last) => {
                if !value.is_null() {
                    *last = Some(value.clone());
                }
            }
            Accumulator::CountDistinct(values) => {
                if !value.is_null() && !values.contains(value) {
                    values.push(value.clone());
                }
            }
            Accumulator::BitAnd(acc) => {
                if let Some(v) = value.as_i64() {
                    *acc = Some(match *acc {
                        Some(a) => a & v,
                        None => v,
                    });
                }
            }
            Accumulator::BitOr(acc) => {
                if let Some(v) = value.as_i64() {
                    *acc = Some(match *acc {
                        Some(a) => a | v,
                        None => v,
                    });
                }
            }
            Accumulator::BitXor(acc) => {
                if let Some(v) = value.as_i64() {
                    *acc = Some(match *acc {
                        Some(a) => a ^ v,
                        None => v,
                    });
                }
            }
            Accumulator::LogicalAnd(acc) => {
                if let Some(b) = value.as_bool() {
                    *acc = Some(acc.unwrap_or(true) && b);
                }
            }
            Accumulator::LogicalOr(acc) => {
                if let Some(b) = value.as_bool() {
                    *acc = Some(acc.unwrap_or(false) || b);
                }
            }
            Accumulator::Variance {
                count, mean, m2, ..
            } => {
                let v = if let Some(f) = value.as_f64() {
                    Some(f)
                } else if let Some(i) = value.as_i64() {
                    Some(i as f64)
                } else if let Value::Numeric(d) = value {
                    use rust_decimal::prelude::ToPrimitive;
                    d.to_f64()
                } else {
                    None
                };
                if let Some(x) = v {
                    *count += 1;
                    let delta = x - *mean;
                    *mean += delta / *count as f64;
                    let delta2 = x - *mean;
                    *m2 += delta * delta2;
                }
            }
            Accumulator::SumIf(_)
            | Accumulator::AvgIf { .. }
            | Accumulator::MinIf(_)
            | Accumulator::MaxIf(_)
            | Accumulator::Covariance { .. }
            | Accumulator::Grouping { .. }
            | Accumulator::GroupingId { .. } => {}
            Accumulator::ApproxQuantiles { values, .. } => {
                if value.is_null() {
                    return Ok(());
                }
                let v = if let Some(f) = value.as_f64() {
                    Some(f)
                } else if let Some(i) = value.as_i64() {
                    Some(i as f64)
                } else if let Value::Numeric(d) = value {
                    use rust_decimal::prelude::ToPrimitive;
                    d.to_f64()
                } else {
                    None
                };
                if let Some(x) = v {
                    values.push(x);
                }
            }
            Accumulator::ApproxTopCount { counts, .. } => {
                if !value.is_null() {
                    let key = format!("{:?}", value);
                    *counts.entry(key).or_insert(0) += 1;
                }
            }
            Accumulator::ApproxTopSum { .. } => {}
        }
        Ok(())
    }

    fn accumulate_array_agg(&mut self, value: &Value, sort_keys: Vec<(Value, bool)>) -> Result<()> {
        match self {
            Accumulator::ArrayAgg {
                items,
                ignore_nulls,
                ..
            } => {
                if *ignore_nulls && value.is_null() {
                    return Ok(());
                }
                items.push((value.clone(), sort_keys));
            }
            Accumulator::Count(_)
            | Accumulator::CountIf(_)
            | Accumulator::Sum(_)
            | Accumulator::Avg { .. }
            | Accumulator::Min(_)
            | Accumulator::Max(_)
            | Accumulator::StringAgg { .. }
            | Accumulator::First(_)
            | Accumulator::Last(_)
            | Accumulator::CountDistinct(_)
            | Accumulator::BitAnd(_)
            | Accumulator::BitOr(_)
            | Accumulator::BitXor(_)
            | Accumulator::LogicalAnd(_)
            | Accumulator::LogicalOr(_)
            | Accumulator::Variance { .. }
            | Accumulator::SumIf(_)
            | Accumulator::AvgIf { .. }
            | Accumulator::MinIf(_)
            | Accumulator::MaxIf(_)
            | Accumulator::Covariance { .. }
            | Accumulator::ApproxQuantiles { .. }
            | Accumulator::ApproxTopCount { .. }
            | Accumulator::ApproxTopSum { .. } => {
                self.accumulate(value)?;
            }
            Accumulator::Grouping { .. } | Accumulator::GroupingId { .. } => {}
        }
        Ok(())
    }

    fn accumulate_conditional(&mut self, value: &Value, condition: bool) -> Result<()> {
        if !condition {
            return Ok(());
        }
        match self {
            Accumulator::SumIf(sum) => {
                if let Some(v) = value.as_f64() {
                    *sum = Some(sum.unwrap_or(0.0) + v);
                } else if let Some(v) = value.as_i64() {
                    *sum = Some(sum.unwrap_or(0.0) + v as f64);
                } else if let Value::Numeric(v) = value {
                    use rust_decimal::prelude::ToPrimitive;
                    if let Some(f) = v.to_f64() {
                        *sum = Some(sum.unwrap_or(0.0) + f);
                    }
                }
            }
            Accumulator::AvgIf { sum, count } => {
                if let Some(v) = value.as_f64() {
                    *sum += v;
                    *count += 1;
                } else if let Some(v) = value.as_i64() {
                    *sum += v as f64;
                    *count += 1;
                } else if let Value::Numeric(v) = value {
                    use rust_decimal::prelude::ToPrimitive;
                    if let Some(f) = v.to_f64() {
                        *sum += f;
                        *count += 1;
                    }
                }
            }
            Accumulator::MinIf(min) => {
                if !value.is_null() {
                    *min = Some(match min.take() {
                        Some(m) => {
                            if value < &m {
                                value.clone()
                            } else {
                                m
                            }
                        }
                        None => value.clone(),
                    });
                }
            }
            Accumulator::MaxIf(max) => {
                if !value.is_null() {
                    *max = Some(match max.take() {
                        Some(m) => {
                            if value > &m {
                                value.clone()
                            } else {
                                m
                            }
                        }
                        None => value.clone(),
                    });
                }
            }
            Accumulator::Count(_)
            | Accumulator::CountIf(_)
            | Accumulator::Sum(_)
            | Accumulator::Avg { .. }
            | Accumulator::Min(_)
            | Accumulator::Max(_)
            | Accumulator::ArrayAgg { .. }
            | Accumulator::StringAgg { .. }
            | Accumulator::First(_)
            | Accumulator::Last(_)
            | Accumulator::CountDistinct(_)
            | Accumulator::BitAnd(_)
            | Accumulator::BitOr(_)
            | Accumulator::BitXor(_)
            | Accumulator::LogicalAnd(_)
            | Accumulator::LogicalOr(_)
            | Accumulator::Variance { .. }
            | Accumulator::Covariance { .. }
            | Accumulator::Grouping { .. }
            | Accumulator::GroupingId { .. }
            | Accumulator::ApproxQuantiles { .. }
            | Accumulator::ApproxTopCount { .. }
            | Accumulator::ApproxTopSum { .. } => {}
        }
        Ok(())
    }

    fn accumulate_bivariate(&mut self, x: &Value, y: &Value) -> Result<()> {
        let x_val = if let Some(f) = x.as_f64() {
            Some(f)
        } else if let Some(i) = x.as_i64() {
            Some(i as f64)
        } else if let Value::Numeric(d) = x {
            use rust_decimal::prelude::ToPrimitive;
            d.to_f64()
        } else {
            None
        };
        let y_val = if let Some(f) = y.as_f64() {
            Some(f)
        } else if let Some(i) = y.as_i64() {
            Some(i as f64)
        } else if let Value::Numeric(d) = y {
            use rust_decimal::prelude::ToPrimitive;
            d.to_f64()
        } else {
            None
        };

        if let (Some(x), Some(y)) = (x_val, y_val)
            && let Accumulator::Covariance {
                count,
                mean_x,
                mean_y,
                c_xy,
                m2_x,
                m2_y,
                ..
            } = self
        {
            *count += 1;
            let n = *count as f64;
            let delta_x = x - *mean_x;
            let delta_y = y - *mean_y;
            *mean_x += delta_x / n;
            *mean_y += delta_y / n;
            let delta_x2 = x - *mean_x;
            let delta_y2 = y - *mean_y;
            *c_xy += delta_x * delta_y2;
            *m2_x += delta_x * delta_x2;
            *m2_y += delta_y * delta_y2;
        }
        Ok(())
    }

    fn accumulate_approx_top_sum(&mut self, value: &Value, weight: &Value) -> Result<()> {
        if let Accumulator::ApproxTopSum { sums, .. } = self {
            if value.is_null() || weight.is_null() {
                return Ok(());
            }
            let key = format!("{:?}", value);
            let w = if let Some(f) = weight.as_f64() {
                f
            } else if let Some(i) = weight.as_i64() {
                i as f64
            } else if let Value::Numeric(d) = weight {
                use rust_decimal::prelude::ToPrimitive;
                d.to_f64().unwrap_or(0.0)
            } else {
                0.0
            };
            *sums.entry(key).or_insert(0.0) += w;
        }
        Ok(())
    }

    fn finalize(&self) -> Value {
        match self {
            Accumulator::Count(n) => Value::Int64(*n),
            Accumulator::CountIf(n) => Value::Int64(*n),
            Accumulator::Sum(sum) => sum
                .map(|s| Value::Float64(OrderedFloat(s)))
                .unwrap_or(Value::Null),
            Accumulator::Avg { sum, count } => {
                if *count > 0 {
                    Value::Float64(OrderedFloat(*sum / *count as f64))
                } else {
                    Value::Null
                }
            }
            Accumulator::Min(min) => min.clone().unwrap_or(Value::Null),
            Accumulator::Max(max) => max.clone().unwrap_or(Value::Null),
            Accumulator::ArrayAgg { items, limit, .. } => {
                let mut sorted_items = items.clone();
                if !sorted_items.is_empty() && !sorted_items[0].1.is_empty() {
                    sorted_items.sort_by(|a, b| {
                        for ((val_a, asc_a), (val_b, _)) in a.1.iter().zip(b.1.iter()) {
                            let cmp = val_a
                                .partial_cmp(val_b)
                                .unwrap_or(std::cmp::Ordering::Equal);
                            let cmp = if *asc_a { cmp } else { cmp.reverse() };
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                let result_items: Vec<Value> = if let Some(lim) = limit {
                    sorted_items
                        .into_iter()
                        .take(*lim)
                        .map(|(v, _)| v)
                        .collect()
                } else {
                    sorted_items.into_iter().map(|(v, _)| v).collect()
                };
                Value::Array(result_items)
            }
            Accumulator::StringAgg { values, separator } => {
                if values.is_empty() {
                    Value::Null
                } else {
                    Value::String(values.join(separator))
                }
            }
            Accumulator::First(first) => first.clone().unwrap_or(Value::Null),
            Accumulator::Last(last) => last.clone().unwrap_or(Value::Null),
            Accumulator::CountDistinct(values) => Value::Int64(values.len() as i64),
            Accumulator::BitAnd(acc) => acc.map(Value::Int64).unwrap_or(Value::Null),
            Accumulator::BitOr(acc) => acc.map(Value::Int64).unwrap_or(Value::Null),
            Accumulator::BitXor(acc) => acc.map(Value::Int64).unwrap_or(Value::Null),
            Accumulator::LogicalAnd(acc) => acc.map(Value::Bool).unwrap_or(Value::Null),
            Accumulator::LogicalOr(acc) => acc.map(Value::Bool).unwrap_or(Value::Bool(false)),
            Accumulator::Variance {
                count,
                m2,
                is_sample,
                is_stddev,
                ..
            } => {
                if *count < 2 && *is_sample {
                    Value::Null
                } else if *count == 0 {
                    Value::Null
                } else {
                    let divisor = if *is_sample { *count - 1 } else { *count };
                    let variance = *m2 / divisor as f64;
                    let result = if *is_stddev {
                        variance.sqrt()
                    } else {
                        variance
                    };
                    Value::Float64(OrderedFloat(result))
                }
            }
            Accumulator::SumIf(sum) => sum
                .map(|s| Value::Float64(OrderedFloat(s)))
                .unwrap_or(Value::Null),
            Accumulator::AvgIf { sum, count } => {
                if *count > 0 {
                    Value::Float64(OrderedFloat(*sum / *count as f64))
                } else {
                    Value::Null
                }
            }
            Accumulator::MinIf(min) => min.clone().unwrap_or(Value::Null),
            Accumulator::MaxIf(max) => max.clone().unwrap_or(Value::Null),
            Accumulator::Covariance {
                count,
                c_xy,
                m2_x,
                m2_y,
                is_sample,
                stat_type,
                ..
            } => {
                if *count < 2 {
                    return Value::Null;
                }
                let divisor = if *is_sample {
                    (*count - 1) as f64
                } else {
                    *count as f64
                };
                match stat_type {
                    CovarianceStatType::Covariance => Value::Float64(OrderedFloat(*c_xy / divisor)),
                    CovarianceStatType::Correlation => {
                        let var_x = *m2_x / *count as f64;
                        let var_y = *m2_y / *count as f64;
                        if var_x <= 0.0 || var_y <= 0.0 {
                            return Value::Null;
                        }
                        let std_x = var_x.sqrt();
                        let std_y = var_y.sqrt();
                        let cov = *c_xy / *count as f64;
                        let corr = cov / (std_x * std_y);
                        Value::Float64(OrderedFloat(corr))
                    }
                }
            }
            Accumulator::Grouping { value } => Value::Int64(*value),
            Accumulator::GroupingId { value } => Value::Int64(*value),
            Accumulator::ApproxQuantiles {
                values,
                num_quantiles,
            } => {
                if values.is_empty() {
                    return Value::Array(vec![]);
                }
                let mut sorted = values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = sorted.len();
                let mut quantiles = Vec::with_capacity(*num_quantiles + 1);
                for i in 0..=*num_quantiles {
                    let pos = (i as f64 / *num_quantiles as f64) * (n - 1) as f64;
                    let idx = pos.floor() as usize;
                    let frac = pos - idx as f64;
                    let val = if idx + 1 < n {
                        sorted[idx] * (1.0 - frac) + sorted[idx + 1] * frac
                    } else {
                        sorted[idx]
                    };
                    quantiles.push(Value::Float64(OrderedFloat(val)));
                }
                Value::Array(quantiles)
            }
            Accumulator::ApproxTopCount { counts, top_n } => {
                let mut entries: Vec<_> = counts.iter().collect();
                entries.sort_by(|a, b| b.1.cmp(a.1));
                entries.truncate(*top_n);
                let result: Vec<Value> = entries
                    .into_iter()
                    .map(|(key, count)| {
                        let parsed_val = if key.starts_with("String(\"") && key.ends_with("\")") {
                            Value::String(key[8..key.len() - 2].to_string())
                        } else if key.starts_with("Int64(") && key.ends_with(")") {
                            key[6..key.len() - 1]
                                .parse::<i64>()
                                .map(Value::Int64)
                                .unwrap_or_else(|_| Value::String(key.clone()))
                        } else {
                            Value::String(key.clone())
                        };
                        Value::Struct(vec![
                            ("value".to_string(), parsed_val),
                            ("count".to_string(), Value::Int64(*count)),
                        ])
                    })
                    .collect();
                Value::Array(result)
            }
            Accumulator::ApproxTopSum { sums, top_n } => {
                let mut entries: Vec<_> = sums.iter().collect();
                entries.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
                entries.truncate(*top_n);
                let result: Vec<Value> = entries
                    .into_iter()
                    .map(|(key, sum)| {
                        let parsed_val = if key.starts_with("String(\"") && key.ends_with("\")") {
                            Value::String(key[8..key.len() - 2].to_string())
                        } else if key.starts_with("Int64(") && key.ends_with(")") {
                            key[6..key.len() - 1]
                                .parse::<i64>()
                                .map(Value::Int64)
                                .unwrap_or_else(|_| Value::String(key.clone()))
                        } else {
                            Value::String(key.clone())
                        };
                        Value::Struct(vec![
                            ("value".to_string(), parsed_val),
                            ("sum".to_string(), Value::Float64(OrderedFloat(*sum))),
                        ])
                    })
                    .collect();
                Value::Array(result)
            }
        }
    }

    fn set_grouping_value(&mut self, value: i64) {
        match self {
            Accumulator::Grouping { value: v } => *v = value,
            Accumulator::GroupingId { value: v } => *v = value,
            _ => {}
        }
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
