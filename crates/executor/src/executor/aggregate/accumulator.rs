use std::collections::HashMap;

use ordered_float::OrderedFloat;
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{AggregateFunction, Expr};

fn get_agg_func(expr: &Expr) -> Option<&AggregateFunction> {
    match expr {
        Expr::Aggregate { func, .. } => Some(func),
        Expr::UserDefinedAggregate { .. } => None,
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
        Expr::UserDefinedAggregate { distinct, .. } => *distinct,
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
        Expr::UserDefinedAggregate { .. } => false,
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
        Expr::Aggregate { args, .. } | Expr::UserDefinedAggregate { args, .. } => {
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
        | Expr::UserDefinedAggregate { .. }
        | Expr::Default => ",".to_string(),
    }
}

#[derive(Clone)]
pub(crate) enum Accumulator {
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
        counts: HashMap<String, i64>,
        top_n: usize,
    },
    ApproxTopSum {
        sums: HashMap<String, f64>,
        top_n: usize,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum CovarianceStatType {
    Covariance,
    Correlation,
}

impl Accumulator {
    pub(crate) fn from_expr(expr: &Expr) -> Self {
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
                        counts: HashMap::new(),
                        top_n,
                    }
                }
                AggregateFunction::ApproxTopSum => {
                    let top_n = extract_int_arg(expr, 2).unwrap_or(10) as usize;
                    Accumulator::ApproxTopSum {
                        sums: HashMap::new(),
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

    pub(crate) fn accumulate(&mut self, value: &Value) -> Result<()> {
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

    pub(crate) fn accumulate_array_agg(
        &mut self,
        value: &Value,
        sort_keys: Vec<(Value, bool)>,
    ) -> Result<()> {
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

    pub(crate) fn accumulate_conditional(&mut self, value: &Value, condition: bool) -> Result<()> {
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

    pub(crate) fn accumulate_bivariate(&mut self, x: &Value, y: &Value) -> Result<()> {
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

    pub(crate) fn accumulate_approx_top_sum(
        &mut self,
        value: &Value,
        weight: &Value,
    ) -> Result<()> {
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

    pub(crate) fn finalize(&self) -> Value {
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

    pub(crate) fn set_grouping_value(&mut self, value: i64) {
        match self {
            Accumulator::Grouping { value: v } => *v = value,
            Accumulator::GroupingId { value: v } => *v = value,
            _ => {}
        }
    }

    pub(crate) fn is_mergeable(&self) -> bool {
        match self {
            Accumulator::Count(_)
            | Accumulator::CountIf(_)
            | Accumulator::Sum(_)
            | Accumulator::Avg { .. }
            | Accumulator::Min(_)
            | Accumulator::Max(_)
            | Accumulator::CountDistinct(_)
            | Accumulator::BitAnd(_)
            | Accumulator::BitOr(_)
            | Accumulator::BitXor(_)
            | Accumulator::LogicalAnd(_)
            | Accumulator::LogicalOr(_)
            | Accumulator::SumIf(_)
            | Accumulator::AvgIf { .. }
            | Accumulator::MinIf(_)
            | Accumulator::MaxIf(_) => true,
            Accumulator::Variance { .. }
            | Accumulator::Covariance { .. }
            | Accumulator::ArrayAgg { .. }
            | Accumulator::StringAgg { .. }
            | Accumulator::First(_)
            | Accumulator::Last(_)
            | Accumulator::Grouping { .. }
            | Accumulator::GroupingId { .. }
            | Accumulator::ApproxQuantiles { .. }
            | Accumulator::ApproxTopCount { .. }
            | Accumulator::ApproxTopSum { .. } => false,
        }
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        match (self, other) {
            (Accumulator::Count(a), Accumulator::Count(b)) => *a += b,
            (Accumulator::CountIf(a), Accumulator::CountIf(b)) => *a += b,
            (Accumulator::Sum(a), Accumulator::Sum(b)) => {
                *a = match (*a, *b) {
                    (Some(x), Some(y)) => Some(x + y),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                };
            }
            (Accumulator::Avg { sum: s1, count: c1 }, Accumulator::Avg { sum: s2, count: c2 }) => {
                *s1 += s2;
                *c1 += c2;
            }
            (Accumulator::Min(a), Accumulator::Min(b)) => {
                *a = match (a.take(), b) {
                    (Some(x), Some(y)) => Some(if &x < y { x } else { y.clone() }),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y.clone()),
                    (None, None) => None,
                };
            }
            (Accumulator::Max(a), Accumulator::Max(b)) => {
                *a = match (a.take(), b) {
                    (Some(x), Some(y)) => Some(if &x > y { x } else { y.clone() }),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y.clone()),
                    (None, None) => None,
                };
            }
            (Accumulator::CountDistinct(a), Accumulator::CountDistinct(b)) => {
                for v in b {
                    if !a.contains(v) {
                        a.push(v.clone());
                    }
                }
            }
            (Accumulator::BitAnd(a), Accumulator::BitAnd(b)) => {
                *a = match (*a, *b) {
                    (Some(x), Some(y)) => Some(x & y),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                };
            }
            (Accumulator::BitOr(a), Accumulator::BitOr(b)) => {
                *a = match (*a, *b) {
                    (Some(x), Some(y)) => Some(x | y),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                };
            }
            (Accumulator::BitXor(a), Accumulator::BitXor(b)) => {
                *a = match (*a, *b) {
                    (Some(x), Some(y)) => Some(x ^ y),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                };
            }
            (Accumulator::LogicalAnd(a), Accumulator::LogicalAnd(b)) => {
                *a = match (*a, *b) {
                    (Some(x), Some(y)) => Some(x && y),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                };
            }
            (Accumulator::LogicalOr(a), Accumulator::LogicalOr(b)) => {
                *a = match (*a, *b) {
                    (Some(x), Some(y)) => Some(x || y),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                };
            }
            (Accumulator::SumIf(a), Accumulator::SumIf(b)) => {
                *a = match (*a, *b) {
                    (Some(x), Some(y)) => Some(x + y),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                };
            }
            (
                Accumulator::AvgIf { sum: s1, count: c1 },
                Accumulator::AvgIf { sum: s2, count: c2 },
            ) => {
                *s1 += s2;
                *c1 += c2;
            }
            (Accumulator::MinIf(a), Accumulator::MinIf(b)) => {
                *a = match (a.take(), b) {
                    (Some(x), Some(y)) => Some(if &x < y { x } else { y.clone() }),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y.clone()),
                    (None, None) => None,
                };
            }
            (Accumulator::MaxIf(a), Accumulator::MaxIf(b)) => {
                *a = match (a.take(), b) {
                    (Some(x), Some(y)) => Some(if &x > y { x } else { y.clone() }),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y.clone()),
                    (None, None) => None,
                };
            }
            _ => {}
        }
    }
}
