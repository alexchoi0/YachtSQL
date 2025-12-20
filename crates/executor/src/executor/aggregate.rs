use std::collections::HashMap;

use ordered_float::OrderedFloat;
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{AggregateFunction, Expr, PlanSchema};
use yachtsql_storage::Table;

use super::{PlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_aggregate(
        &mut self,
        input: &ExecutorPlan,
        group_by: &[Expr],
        aggregates: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
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
                    } else {
                        let arg_val = extract_agg_arg(&evaluator, agg_expr, &record)?;
                        acc.accumulate(&arg_val)?;
                    }
                }
            }

            let row: Vec<Value> = accumulators.iter().map(|a| a.finalize()).collect();
            result.push_row(row)?;
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
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => false,
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
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => false,
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
                AggregateFunction::Grouping
                | AggregateFunction::ApproxQuantiles
                | AggregateFunction::ApproxTopCount
                | AggregateFunction::ApproxTopSum => Accumulator::Count(0),
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
            | Accumulator::Covariance { .. } => {}
        }
        Ok(())
    }

    fn accumulate_array_agg(&mut self, value: &Value, sort_keys: Vec<(Value, bool)>) -> Result<()> {
        match self {
            Accumulator::ArrayAgg {
                items,
                ignore_nulls,
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
            | Accumulator::Covariance { .. } => {
                self.accumulate(value)?;
            }
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
            | Accumulator::Covariance { .. } => {}
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

        if let (Some(x), Some(y)) = (x_val, y_val) {
            if let Accumulator::Covariance {
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
            Accumulator::ArrayAgg { items, .. } => {
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
                Value::Array(sorted_items.into_iter().map(|(v, _)| v).collect())
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
        }
    }
}
