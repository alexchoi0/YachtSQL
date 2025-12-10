use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::rc::Rc;

use sqlparser::ast::{Expr as SqlExpr, Function, FunctionArguments};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_storage::{Row, Schema};

use crate::functions::{Accumulator, FunctionRegistry};
use crate::query_executor::expression_evaluator::ExpressionEvaluator;

fn contains_aggregate(expr: &SqlExpr, registry: &FunctionRegistry) -> bool {
    match expr {
        SqlExpr::Function(func) if func.over.is_none() => {
            let func_name = func.name.to_string().to_uppercase();
            registry.has_aggregate(&func_name)
        }
        SqlExpr::BinaryOp { left, right, op } => {
            contains_aggregate(left, registry) || contains_aggregate(right, registry)
        }
        SqlExpr::UnaryOp { expr, op } => contains_aggregate(expr, registry),
        SqlExpr::Nested(inner) => contains_aggregate(inner, registry),
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .map(|e| contains_aggregate(e, registry))
                .unwrap_or(false)
                || conditions.iter().any(|c| {
                    contains_aggregate(&c.condition, registry)
                        || contains_aggregate(&c.result, registry)
                })
                || else_result
                    .as_ref()
                    .map(|e| contains_aggregate(e, registry))
                    .unwrap_or(false)
        }
        SqlExpr::Cast { expr, .. } => contains_aggregate(expr, registry),
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            contains_aggregate(expr, registry)
                || substring_from
                    .as_ref()
                    .map(|e| contains_aggregate(e, registry))
                    .unwrap_or(false)
                || substring_for
                    .as_ref()
                    .map(|e| contains_aggregate(e, registry))
                    .unwrap_or(false)
        }
        SqlExpr::Trim {
            expr, trim_what, ..
        } => {
            contains_aggregate(expr, registry)
                || trim_what
                    .as_ref()
                    .map(|e| contains_aggregate(e, registry))
                    .unwrap_or(false)
        }
        SqlExpr::Position { expr, r#in } => {
            contains_aggregate(expr, registry) || contains_aggregate(r#in, registry)
        }
        SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::IsFalse(inner)
        | SqlExpr::IsNotFalse(inner)
        | SqlExpr::IsTrue(inner)
        | SqlExpr::IsNotTrue(inner)
        | SqlExpr::IsUnknown(inner)
        | SqlExpr::IsNotUnknown(inner) => contains_aggregate(inner, registry),
        SqlExpr::IsDistinctFrom(left, right) | SqlExpr::IsNotDistinctFrom(left, right) => {
            contains_aggregate(left, registry) || contains_aggregate(right, registry)
        }
        SqlExpr::Like { expr, pattern, .. }
        | SqlExpr::ILike { expr, pattern, .. }
        | SqlExpr::SimilarTo { expr, pattern, .. }
        | SqlExpr::RLike { expr, pattern, .. } => {
            contains_aggregate(expr, registry) || contains_aggregate(pattern, registry)
        }
        SqlExpr::InList { expr, list, .. } => {
            contains_aggregate(expr, registry)
                || list.iter().any(|e| contains_aggregate(e, registry))
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            contains_aggregate(expr, registry)
                || contains_aggregate(low, registry)
                || contains_aggregate(high, registry)
        }
        SqlExpr::Array(arr) => arr.elem.iter().any(|e| contains_aggregate(e, registry)),
        SqlExpr::Tuple(exprs) => exprs.iter().any(|e| contains_aggregate(e, registry)),
        SqlExpr::Extract { expr, .. } => contains_aggregate(expr, registry),
        SqlExpr::Ceil { expr, .. } => contains_aggregate(expr, registry),
        SqlExpr::Floor { expr, .. } => contains_aggregate(expr, registry),
        SqlExpr::Collate { expr, .. } => contains_aggregate(expr, registry),
        SqlExpr::AtTimeZone { timestamp, .. } => contains_aggregate(timestamp, registry),
        SqlExpr::Interval(interval) => contains_aggregate(&interval.value, registry),
        SqlExpr::Convert { expr, .. } => contains_aggregate(expr, registry),
        SqlExpr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
            ..
        } => {
            contains_aggregate(expr, registry)
                || contains_aggregate(overlay_what, registry)
                || contains_aggregate(overlay_from, registry)
                || overlay_for
                    .as_ref()
                    .map(|e| contains_aggregate(e, registry))
                    .unwrap_or(false)
        }
        SqlExpr::CompoundFieldAccess { root, access_chain } => {
            contains_aggregate(root, registry)
                || access_chain.iter().any(|access| match access {
                    sqlparser::ast::AccessExpr::Subscript(subscript) => match subscript {
                        sqlparser::ast::Subscript::Index { index } => {
                            contains_aggregate(index, registry)
                        }
                        sqlparser::ast::Subscript::Slice {
                            lower_bound,
                            upper_bound,
                            stride,
                        } => {
                            lower_bound
                                .as_ref()
                                .map(|e| contains_aggregate(e, registry))
                                .unwrap_or(false)
                                || upper_bound
                                    .as_ref()
                                    .map(|e| contains_aggregate(e, registry))
                                    .unwrap_or(false)
                                || stride
                                    .as_ref()
                                    .map(|e| contains_aggregate(e, registry))
                                    .unwrap_or(false)
                        }
                    },
                    sqlparser::ast::AccessExpr::Dot(_) => false,
                })
        }
        SqlExpr::Prefixed { value, .. } => contains_aggregate(value, registry),
        SqlExpr::Named { expr, .. } => contains_aggregate(expr, registry),
        SqlExpr::Struct { values, .. } => values.iter().any(|e| contains_aggregate(e, registry)),
        SqlExpr::Dictionary(fields) => fields
            .iter()
            .any(|f| contains_aggregate(&f.value, registry)),
        SqlExpr::Map(map) => map.entries.iter().any(|e| {
            contains_aggregate(&e.key, registry) || contains_aggregate(&e.value, registry)
        }),

        SqlExpr::Identifier(_)
        | SqlExpr::CompoundIdentifier(_)
        | SqlExpr::Value(_)
        | SqlExpr::TypedString { .. }
        | SqlExpr::Function(_)
        | SqlExpr::Wildcard(_)
        | SqlExpr::QualifiedWildcard(_, _)
        | SqlExpr::JsonAccess { .. }
        | SqlExpr::InSubquery { .. }
        | SqlExpr::InUnnest { .. }
        | SqlExpr::AnyOp { .. }
        | SqlExpr::AllOp { .. }
        | SqlExpr::Exists { .. }
        | SqlExpr::Subquery(_)
        | SqlExpr::GroupingSets(_)
        | SqlExpr::Cube(_)
        | SqlExpr::Rollup(_)
        | SqlExpr::MatchAgainst { .. }
        | SqlExpr::OuterJoin(_)
        | SqlExpr::Prior(_)
        | SqlExpr::Lambda(_)
        | SqlExpr::IsNormalized { .. }
        | SqlExpr::MemberOf(_) => false,
    }
}

type GroupState = (
    Vec<Value>,
    Vec<Box<dyn Accumulator>>,
    Vec<std::collections::HashSet<String>>,
    Vec<Vec<(Value, Vec<Value>)>>,
    Vec<usize>,
);

#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub function_name: String,
    pub argument_exprs: Vec<SqlExpr>,
    pub is_count_star: bool,
    pub distinct: bool,
    pub filter: Option<SqlExpr>,
    pub order_by: Vec<(SqlExpr, bool)>,
    pub limit: Option<usize>,

    pub within_group: Vec<(SqlExpr, bool)>,
}

impl AggregateSpec {
    pub fn from_expr(expr: &SqlExpr, registry: &FunctionRegistry) -> Result<Option<Self>> {
        match expr {
            SqlExpr::Function(func) => Self::from_function(func, registry),
            _ => Ok(None),
        }
    }

    fn from_function(func: &Function, registry: &FunctionRegistry) -> Result<Option<Self>> {
        let func_name = func.name.to_string().to_uppercase();

        if !registry.has_aggregate(&func_name) {
            return Ok(None);
        }

        let FunctionArguments::List(args) = &func.args else {
            return Err(Error::InvalidQuery(format!(
                "Invalid arguments for {} function",
                func_name
            )));
        };

        let distinct = matches!(
            args.duplicate_treatment.as_ref(),
            Some(sqlparser::ast::DuplicateTreatment::Distinct)
        );

        if let Some(filter_expr) = &func.filter {
            if contains_aggregate(filter_expr, registry) {
                return Err(Error::InvalidQuery(
                    "FILTER clause cannot contain aggregate functions".to_string(),
                ));
            }
        }

        if func_name == "COUNT"
            && args.args.len() == 1
            && let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) =
                &args.args[0]
        {
            return Ok(Some(AggregateSpec {
                function_name: func_name,
                argument_exprs: Vec::new(),
                is_count_star: true,
                distinct: false,
                filter: func.filter.as_ref().map(|f| (**f).clone()),
                order_by: Vec::new(),
                limit: None,
                within_group: Vec::new(),
            }));
        }

        let argument_exprs: Vec<SqlExpr> = args
            .args
            .iter()
            .filter_map(|arg| match arg {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    expr,
                )) => Some(expr.clone()),
                _ => None,
            })
            .collect();

        let order_by =
            if let Some(sqlparser::ast::FunctionArgumentClause::OrderBy(order_by_exprs)) =
                args.clauses.first()
            {
                order_by_exprs
                    .iter()
                    .map(|order_expr| {
                        let asc = order_expr.options.asc.unwrap_or(true);
                        (order_expr.expr.clone(), asc)
                    })
                    .collect()
            } else {
                Vec::new()
            };

        let within_group: Vec<(SqlExpr, bool)> = func
            .within_group
            .iter()
            .map(|order_expr| {
                let asc = order_expr.options.asc.unwrap_or(true);
                (order_expr.expr.clone(), asc)
            })
            .collect();

        let limit = None;

        Ok(Some(AggregateSpec {
            function_name: func_name,
            argument_exprs,
            is_count_star: false,
            distinct,
            filter: func.filter.as_ref().map(|f| (**f).clone()),
            order_by,
            limit,
            within_group,
        }))
    }

    pub fn create_accumulator(&self, registry: &FunctionRegistry) -> Result<Box<dyn Accumulator>> {
        use yachtsql_functions::aggregate::approximate::{
            ApproxQuantilesAccumulator, ApproxTopCountAccumulator, ApproxTopSumAccumulator,
        };

        if self.is_count_star {
            registry
                .get_aggregate("COUNT")
                .ok_or_else(|| Error::InternalError("COUNT function not found".to_string()))
                .map(|func| func.create_accumulator())
        } else if self.function_name == "APPROX_QUANTILES" {
            if self.argument_exprs.len() != 2 {
                return Err(Error::InvalidQuery(
                    "APPROX_QUANTILES requires exactly 2 arguments: (expr, num_quantiles)"
                        .to_string(),
                ));
            }

            let num_quantiles = match &self.argument_exprs[1] {
                SqlExpr::Value(val_with_span) => match &val_with_span.value {
                    sqlparser::ast::Value::Number(n, _) => n.parse::<i64>().map_err(|_| {
                        Error::InvalidQuery(format!(
                            "APPROX_QUANTILES num_quantiles must be an integer, got: {}",
                            n
                        ))
                    })?,
                    _ => {
                        return Err(Error::InvalidQuery(
                            "APPROX_QUANTILES num_quantiles must be a literal integer".to_string(),
                        ));
                    }
                },
                _ => {
                    return Err(Error::InvalidQuery(
                        "APPROX_QUANTILES num_quantiles must be a literal integer".to_string(),
                    ));
                }
            };

            Ok(Box::new(ApproxQuantilesAccumulator::new(num_quantiles)))
        } else if self.function_name == "APPROX_TOP_COUNT" {
            if self.argument_exprs.len() != 2 {
                return Err(Error::InvalidQuery(
                    "APPROX_TOP_COUNT requires exactly 2 arguments: (expr, number)".to_string(),
                ));
            }

            let number = match &self.argument_exprs[1] {
                SqlExpr::Value(val_with_span) => match &val_with_span.value {
                    sqlparser::ast::Value::Number(n, _) => n.parse::<i64>().map_err(|_| {
                        Error::InvalidQuery(format!(
                            "APPROX_TOP_COUNT number must be an integer, got: {}",
                            n
                        ))
                    })?,
                    _ => {
                        return Err(Error::InvalidQuery(
                            "APPROX_TOP_COUNT number must be a literal integer".to_string(),
                        ));
                    }
                },
                _ => {
                    return Err(Error::InvalidQuery(
                        "APPROX_TOP_COUNT number must be a literal integer".to_string(),
                    ));
                }
            };

            Ok(Box::new(ApproxTopCountAccumulator::new(number)))
        } else if self.function_name == "APPROX_TOP_SUM" {
            if self.argument_exprs.len() != 3 {
                return Err(Error::InvalidQuery(
                    "APPROX_TOP_SUM requires exactly 3 arguments: (expr, weight, number)"
                        .to_string(),
                ));
            }

            let number = match &self.argument_exprs[2] {
                SqlExpr::Value(val_with_span) => match &val_with_span.value {
                    sqlparser::ast::Value::Number(n, _) => n.parse::<i64>().map_err(|_| {
                        Error::InvalidQuery(format!(
                            "APPROX_TOP_SUM number must be an integer, got: {}",
                            n
                        ))
                    })?,
                    _ => {
                        return Err(Error::InvalidQuery(
                            "APPROX_TOP_SUM number must be a literal integer".to_string(),
                        ));
                    }
                },
                _ => {
                    return Err(Error::InvalidQuery(
                        "APPROX_TOP_SUM number must be a literal integer".to_string(),
                    ));
                }
            };

            Ok(Box::new(ApproxTopSumAccumulator::new(number)))
        } else if self.function_name == "STRING_AGG" || self.function_name == "LISTAGG" {
            use yachtsql_functions::aggregate::string_agg::ListAggAccumulator;

            let delimiter = if self.argument_exprs.len() >= 2 {
                match &self.argument_exprs[1] {
                    SqlExpr::Value(val_with_span) => match &val_with_span.value {
                        sqlparser::ast::Value::SingleQuotedString(s)
                        | sqlparser::ast::Value::DoubleQuotedString(s) => s.clone(),
                        _ => ",".to_string(),
                    },
                    _ => ",".to_string(),
                }
            } else {
                if self.function_name == "STRING_AGG" {
                    ",".to_string()
                } else {
                    String::new()
                }
            };

            Ok(Box::new(ListAggAccumulator::with_delimiter(delimiter)))
        } else if self.function_name == "PERCENTILE_CONT" || self.function_name == "PERCENTILE_DISC"
        {
            use yachtsql_functions::aggregate::statistical::{
                PercentileContAccumulator, PercentileDiscAccumulator,
            };

            if self.argument_exprs.is_empty() {
                return Err(Error::InvalidQuery(format!(
                    "{} requires a percentile argument",
                    self.function_name
                )));
            }

            let extract_percentile = |expr: &SqlExpr| -> Result<f64> {
                match expr {
                    SqlExpr::Value(val_with_span) => match &val_with_span.value {
                        sqlparser::ast::Value::Number(n, _) => n.parse::<f64>().map_err(|_| {
                            Error::InvalidQuery(format!(
                                "{} percentile must be a number, got: {}",
                                self.function_name, n
                            ))
                        }),
                        _ => Err(Error::InvalidQuery(format!(
                            "{} percentile must be a literal number",
                            self.function_name
                        ))),
                    },
                    _ => Err(Error::InvalidQuery(format!(
                        "{} percentile must be a literal number",
                        self.function_name
                    ))),
                }
            };

            let percentile = if self.argument_exprs.len() == 2 {
                extract_percentile(&self.argument_exprs[1])?
            } else if self.argument_exprs.len() == 1 && !self.within_group.is_empty() {
                extract_percentile(&self.argument_exprs[0])?
            } else if self.argument_exprs.len() == 1 {
                extract_percentile(&self.argument_exprs[0])?
            } else {
                return Err(Error::InvalidQuery(format!(
                    "{} requires either (percentile) WITHIN GROUP (ORDER BY expr) or (expr, percentile) syntax",
                    self.function_name
                )));
            };

            if !(0.0..=1.0).contains(&percentile) {
                return Err(Error::InvalidQuery(format!(
                    "{} percentile must be between 0 and 1, got: {}",
                    self.function_name, percentile
                )));
            }

            if self.function_name == "PERCENTILE_CONT" {
                Ok(Box::new(PercentileContAccumulator::new(percentile)))
            } else {
                Ok(Box::new(PercentileDiscAccumulator::new(percentile)))
            }
        } else {
            let single_arg_aggregates = [
                "BIT_AND", "BIT_OR", "BIT_XOR", "BOOL_AND", "BOOL_OR", "EVERY",
            ];
            if single_arg_aggregates.contains(&self.function_name.as_str()) {
                if self.argument_exprs.is_empty() {
                    return Err(Error::InvalidQuery(format!(
                        "{} requires exactly 1 argument, got 0",
                        self.function_name
                    )));
                }
            }

            if self.function_name == "JSON_OBJECT_AGG" || self.function_name == "JSONB_OBJECT_AGG" {
                if self.argument_exprs.len() != 2 {
                    return Err(Error::InvalidQuery(format!(
                        "{} requires exactly 2 arguments: (key, value), got {}",
                        self.function_name,
                        self.argument_exprs.len()
                    )));
                }
            }

            registry
                .get_aggregate(&self.function_name)
                .ok_or_else(|| {
                    Error::InternalError(format!(
                        "Aggregate function {} not found in registry",
                        self.function_name
                    ))
                })
                .map(|func| func.create_accumulator())
        }
    }

    pub fn extract_values(&self, row: &Row, schema: &Schema) -> Result<Vec<Value>> {
        if self.is_count_star {
            return Ok(vec![Value::int64(1)]);
        }

        let mut values = Vec::new();
        let evaluator = ExpressionEvaluator::new(schema);

        if self.function_name == "PERCENTILE_CONT" || self.function_name == "PERCENTILE_DISC" {
            let col_expr = if !self.within_group.is_empty() {
                &self.within_group[0].0
            } else if self.argument_exprs.len() == 2 {
                &self.argument_exprs[0]
            } else {
                &self.argument_exprs[0]
            };

            let evaluator = ExpressionEvaluator::new(schema);
            let value = match col_expr {
                SqlExpr::Identifier(ident) => {
                    let col_idx = schema
                        .fields()
                        .iter()
                        .position(|f| &f.name == &ident.value)
                        .ok_or_else(|| Error::ColumnNotFound(ident.value.clone()))?;
                    row.values()[col_idx].clone()
                }
                SqlExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
                    let col_name = &parts[1].value;
                    let col_idx = schema
                        .fields()
                        .iter()
                        .position(|f| &f.name == col_name)
                        .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                    row.values()[col_idx].clone()
                }
                SqlExpr::CompoundIdentifier(parts) if parts.len() == 1 => {
                    let col_name = &parts[0].value;
                    let col_idx = schema
                        .fields()
                        .iter()
                        .position(|f| &f.name == col_name)
                        .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                    row.values()[col_idx].clone()
                }
                _ => evaluator.evaluate_expr(col_expr, row)?,
            };
            return Ok(vec![value]);
        }

        let exprs_to_extract = if self.function_name == "APPROX_QUANTILES" {
            &self.argument_exprs[0..1]
        } else if self.function_name == "APPROX_TOP_COUNT" {
            &self.argument_exprs[0..1]
        } else if self.function_name == "APPROX_TOP_SUM" {
            &self.argument_exprs[0..2]
        } else if self.function_name == "STRING_AGG" || self.function_name == "LISTAGG" {
            if self.argument_exprs.is_empty() {
                &self.argument_exprs[..]
            } else {
                &self.argument_exprs[0..1]
            }
        } else {
            &self.argument_exprs[..]
        };

        for col_expr in exprs_to_extract {
            let value = match col_expr {
                SqlExpr::Identifier(ident) => {
                    let col_idx = schema
                        .fields()
                        .iter()
                        .position(|f| &f.name == &ident.value)
                        .ok_or_else(|| Error::ColumnNotFound(ident.value.clone()))?;
                    row.values()[col_idx].clone()
                }
                SqlExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
                    let col_name = &parts[1].value;
                    let col_idx = schema
                        .fields()
                        .iter()
                        .position(|f| &f.name == col_name)
                        .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                    row.values()[col_idx].clone()
                }
                SqlExpr::CompoundIdentifier(parts) if parts.len() == 1 => {
                    let col_name = &parts[0].value;
                    let col_idx = schema
                        .fields()
                        .iter()
                        .position(|f| &f.name == col_name)
                        .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                    row.values()[col_idx].clone()
                }

                _ => evaluator.evaluate_expr(col_expr, row)?,
            };

            values.push(value);
        }

        if values.is_empty() {
            values.push(Value::null());
        }

        Ok(values)
    }

    pub fn extract_order_values(&self, row: &Row, schema: &Schema) -> Result<Vec<Value>> {
        let mut values = Vec::new();
        for (order_expr, _asc) in &self.order_by {
            let col_name = match order_expr {
                SqlExpr::Identifier(ident) => &ident.value,
                SqlExpr::CompoundIdentifier(parts) if parts.len() == 2 => &parts[1].value,
                SqlExpr::CompoundIdentifier(parts) if parts.len() == 1 => &parts[0].value,
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Complex expressions in ORDER BY not yet supported".to_string(),
                    ));
                }
            };

            let col_idx = schema
                .fields()
                .iter()
                .position(|f| &f.name == col_name)
                .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;

            values.push(row.values()[col_idx].clone());
        }
        Ok(values)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey(Vec<String>);

impl GroupKey {
    pub fn from_values(values: Vec<Value>) -> Self {
        let key_strings = values.into_iter().map(|v| format!("{:?}", v)).collect();
        GroupKey(key_strings)
    }
}

pub struct Aggregator {
    group_by_exprs: Vec<SqlExpr>,
    aggregate_specs: Vec<AggregateSpec>,
    groups: HashMap<GroupKey, GroupState>,
    registry: Rc<FunctionRegistry>,
}

impl Aggregator {
    pub fn new(
        group_by_exprs: Vec<SqlExpr>,
        aggregate_specs: Vec<AggregateSpec>,
        registry: Rc<FunctionRegistry>,
    ) -> Self {
        Self {
            group_by_exprs,
            aggregate_specs,
            groups: HashMap::new(),
            registry,
        }
    }

    pub fn process_row(&mut self, row: &Row, schema: &Schema) -> Result<()> {
        let evaluator = ExpressionEvaluator::new(schema);
        let key_values: Vec<Value> = self
            .group_by_exprs
            .iter()
            .map(|expr| evaluator.evaluate_expr(expr, row))
            .collect::<Result<Vec<_>>>()?;
        let key = GroupKey::from_values(key_values.clone());

        let filter_results: Vec<bool> = self
            .aggregate_specs
            .iter()
            .map(|spec| {
                if let Some(ref filter_expr) = spec.filter {
                    Self::evaluate_filter_static(filter_expr, row, schema)
                } else {
                    Ok(true)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let (_stored_values, agg_states, distinct_sets, ordered_buffers, filter_counts) =
            match self.groups.entry(key) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let agg_states: Vec<Box<dyn Accumulator>> = self
                        .aggregate_specs
                        .iter()
                        .map(|spec| spec.create_accumulator(&self.registry))
                        .collect::<Result<Vec<_>>>()?;

                    let distinct_sets =
                        vec![std::collections::HashSet::new(); self.aggregate_specs.len()];
                    let ordered_buffers = vec![Vec::new(); self.aggregate_specs.len()];
                    let filter_counts = vec![0; self.aggregate_specs.len()];

                    entry.insert((
                        key_values.clone(),
                        agg_states,
                        distinct_sets,
                        ordered_buffers,
                        filter_counts,
                    ))
                }
            };

        for (i, spec) in self.aggregate_specs.iter().enumerate() {
            if !filter_results[i] {
                continue;
            }

            filter_counts[i] += 1;

            let values = spec.extract_values(row, schema)?;

            let aggregation_value = if values.len() > 1 {
                Value::array(values.clone())
            } else if values.len() == 1 {
                values[0].clone()
            } else {
                Value::null()
            };

            if !spec.order_by.is_empty() {
                let order_values = spec.extract_order_values(row, schema)?;
                ordered_buffers[i].push((aggregation_value.clone(), order_values));
            } else if spec.distinct {
                let value_key = format!("{:?}", aggregation_value);
                if distinct_sets[i].insert(value_key) {
                    agg_states[i].accumulate(&aggregation_value)?;
                }
            } else {
                agg_states[i].accumulate(&aggregation_value)?;
            }
        }

        Ok(())
    }

    fn evaluate_filter_static(filter_expr: &SqlExpr, row: &Row, schema: &Schema) -> Result<bool> {
        let evaluator = ExpressionEvaluator::new(schema);

        evaluator.evaluate_where(filter_expr, row)
    }

    pub fn finalize(self) -> Result<Vec<(Vec<Value>, Vec<Value>)>> {
        let mut results = Vec::new();

        if self.group_by_exprs.is_empty() && self.groups.is_empty() {
            let agg_states: Vec<Box<dyn Accumulator>> = self
                .aggregate_specs
                .iter()
                .map(|spec| spec.create_accumulator(&self.registry))
                .collect::<Result<Vec<_>>>()?;

            let agg_values: Vec<Value> = agg_states
                .iter()
                .zip(self.aggregate_specs.iter())
                .map(|(acc, spec)| {
                    if spec.filter.is_some() {
                        if spec.function_name == "COUNT" {
                            Value::int64(0)
                        } else {
                            Value::null()
                        }
                    } else {
                        acc.finalize().unwrap_or(Value::null())
                    }
                })
                .collect();
            results.push((Vec::new(), agg_values));
            return Ok(results);
        }

        for (
            _key,
            (group_values, mut agg_states, _distinct_sets, ordered_buffers, filter_counts),
        ) in self.groups.into_iter()
        {
            for (i, spec) in self.aggregate_specs.iter().enumerate() {
                if !spec.order_by.is_empty() && !ordered_buffers[i].is_empty() {
                    let mut sorted_buffer = ordered_buffers[i].clone();
                    Self::sort_by_order_keys(&mut sorted_buffer, &spec.order_by);

                    let limit = spec.limit.unwrap_or(sorted_buffer.len());
                    let values_to_process = sorted_buffer.iter().take(limit);

                    for (value, _order_values) in values_to_process {
                        agg_states[i].accumulate(value)?;
                    }
                }
            }

            let agg_values: Vec<Value> = agg_states
                .iter()
                .zip(self.aggregate_specs.iter())
                .zip(filter_counts.iter())
                .map(|((acc, spec), &count)| {
                    if spec.filter.is_some() && count == 0 {
                        if spec.function_name == "COUNT" {
                            Value::int64(0)
                        } else {
                            Value::null()
                        }
                    } else {
                        acc.finalize().unwrap_or(Value::null())
                    }
                })
                .collect();
            results.push((group_values, agg_values));
        }
        Ok(results)
    }

    fn sort_by_order_keys(buffer: &mut [(Value, Vec<Value>)], order_by: &[(SqlExpr, bool)]) {
        buffer.sort_by(|a, b| {
            for (idx, (_expr, asc)) in order_by.iter().enumerate() {
                let cmp = Self::compare_values(&a.1[idx], &b.1[idx]);
                let cmp = if *asc { cmp } else { cmp.reverse() };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        if a.is_null() && b.is_null() {
            return Ordering::Equal;
        }
        if a.is_null() {
            return Ordering::Less;
        }
        if b.is_null() {
            return Ordering::Greater;
        }

        if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
            return x.cmp(&y);
        }

        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
        }

        if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
            return x.cmp(y);
        }

        if let (Some(fs_a), Some(fs_b)) = (a.as_fixed_string(), b.as_fixed_string()) {
            return fs_a.data.cmp(&fs_b.data);
        }

        if let (Some(fs), Some(s)) = (a.as_fixed_string(), b.as_str()) {
            return fs.to_string_lossy().cmp(&s.to_string());
        }

        if let (Some(s), Some(fs)) = (a.as_str(), b.as_fixed_string()) {
            return s.to_string().cmp(&fs.to_string_lossy());
        }

        if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
            return x.cmp(&y);
        }

        Ordering::Equal
    }
}
