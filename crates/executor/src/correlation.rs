use std::collections::HashMap;

use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;
use yachtsql_optimizer::plan::PlanNode;

#[derive(Debug, Clone)]
pub struct CorrelationContext {
    bindings: HashMap<String, Value>,
}

impl CorrelationContext {
    pub fn new() -> Self {
        Self {
            bindings: HashMap::new(),
        }
    }

    pub fn bind(&mut self, column_name: String, value: Value) {
        self.bindings.insert(column_name, value);
    }

    pub fn get(&self, column_name: &str) -> Option<&Value> {
        self.bindings.get(column_name)
    }

    pub fn contains(&self, column_name: &str) -> bool {
        self.bindings.contains_key(column_name)
    }

    pub fn bindings(&self) -> &HashMap<String, Value> {
        &self.bindings
    }
}

impl Default for CorrelationContext {
    fn default() -> Self {
        Self::new()
    }
}

pub fn has_correlation(expr: &Expr) -> bool {
    match expr {
        Expr::Column { .. } => true,

        Expr::BinaryOp { left, right, .. } => has_correlation(left) || has_correlation(right),
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } | Expr::TryCast { expr, .. } => {
            has_correlation(expr)
        }
        Expr::Function { args, .. } | Expr::Aggregate { args, .. } => {
            args.iter().any(has_correlation)
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            check_optional_correlation(operand)
                || when_then
                    .iter()
                    .any(|(w, t)| has_correlation(w) || has_correlation(t))
                || check_optional_correlation(else_expr)
        }
        Expr::InList { expr, list, .. } => {
            has_correlation(expr) || list.iter().any(has_correlation)
        }
        Expr::Between {
            expr, low, high, ..
        } => has_correlation(expr) || has_correlation(low) || has_correlation(high),

        _ => false,
    }
}

#[inline]
fn check_optional_correlation(expr: &Option<Box<Expr>>) -> bool {
    expr.as_ref().is_some_and(|e| has_correlation(e))
}

pub fn plan_has_correlation(plan: &PlanNode) -> bool {
    match plan {
        PlanNode::Filter { predicate, input } => {
            has_correlation(predicate) || plan_has_correlation(input)
        }
        PlanNode::Projection { expressions, input } => {
            expressions.iter().any(|(expr, _)| has_correlation(expr)) || plan_has_correlation(input)
        }
        PlanNode::Aggregate {
            group_by,
            aggregates,
            input,
            ..
        } => {
            group_by.iter().any(has_correlation)
                || aggregates.iter().any(has_correlation)
                || plan_has_correlation(input)
        }
        PlanNode::Join {
            left, right, on, ..
        } => has_correlation(on) || plan_has_correlation(left) || plan_has_correlation(right),
        PlanNode::Limit { input, .. } => plan_has_correlation(input),
        PlanNode::LimitPercent { input, .. } => plan_has_correlation(input),
        PlanNode::Sort { input, .. } => plan_has_correlation(input),
        PlanNode::Distinct { input, .. } => plan_has_correlation(input),

        PlanNode::Scan { .. } => false,
        PlanNode::EmptyRelation => false,

        PlanNode::Union { left, right, .. } => {
            plan_has_correlation(left) || plan_has_correlation(right)
        }

        PlanNode::Cte {
            cte_plan, input, ..
        } => plan_has_correlation(cte_plan) || plan_has_correlation(input),

        _ => false,
    }
}

pub fn bind_correlation(plan: PlanNode, context: &CorrelationContext) -> Result<PlanNode> {
    let local_tables = collect_table_names(&plan);
    bind_correlation_internal(plan, context, &local_tables)
}

fn collect_table_names(plan: &PlanNode) -> std::collections::HashSet<String> {
    let mut tables = std::collections::HashSet::new();
    collect_table_names_recursive(plan, &mut tables);
    tables
}

fn collect_table_names_recursive(plan: &PlanNode, tables: &mut std::collections::HashSet<String>) {
    match plan {
        PlanNode::Scan {
            table_name, alias, ..
        } => {
            tables.insert(table_name.clone());
            if let Some(a) = alias {
                tables.insert(a.clone());
            }
        }
        PlanNode::Filter { input, .. } => collect_table_names_recursive(input, tables),
        PlanNode::Projection { input, .. } => collect_table_names_recursive(input, tables),
        PlanNode::Aggregate { input, .. } => collect_table_names_recursive(input, tables),
        PlanNode::Sort { input, .. } => collect_table_names_recursive(input, tables),
        PlanNode::Limit { input, .. } => collect_table_names_recursive(input, tables),
        PlanNode::LimitPercent { input, .. } => collect_table_names_recursive(input, tables),
        PlanNode::Distinct { input, .. } => collect_table_names_recursive(input, tables),
        PlanNode::Join { left, right, .. } => {
            collect_table_names_recursive(left, tables);
            collect_table_names_recursive(right, tables);
        }
        PlanNode::Union { left, right, .. } => {
            collect_table_names_recursive(left, tables);
            collect_table_names_recursive(right, tables);
        }
        PlanNode::Cte {
            cte_plan, input, ..
        } => {
            collect_table_names_recursive(cte_plan, tables);
            collect_table_names_recursive(input, tables);
        }
        _ => {}
    }
}

fn bind_correlation_internal(
    plan: PlanNode,
    context: &CorrelationContext,
    local_tables: &std::collections::HashSet<String>,
) -> Result<PlanNode> {
    match plan {
        PlanNode::Filter { predicate, input } => Ok(PlanNode::Filter {
            predicate: bind_correlation_expr(predicate, context, local_tables)?,
            input: Box::new(bind_correlation_internal(
                clone_box(&input),
                context,
                local_tables,
            )?),
        }),
        PlanNode::Projection { expressions, input } => {
            let bound_exprs = expressions
                .into_iter()
                .map(|(expr, alias)| {
                    bind_correlation_expr(expr, context, local_tables).map(|bound| (bound, alias))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(PlanNode::Projection {
                expressions: bound_exprs,
                input: Box::new(bind_correlation_internal(
                    clone_box(&input),
                    context,
                    local_tables,
                )?),
            })
        }
        PlanNode::Join {
            left,
            right,
            on,
            join_type,
            using_columns,
        } => Ok(PlanNode::Join {
            left: Box::new(bind_correlation_internal(
                clone_box(&left),
                context,
                local_tables,
            )?),
            right: Box::new(bind_correlation_internal(
                clone_box(&right),
                context,
                local_tables,
            )?),
            on: bind_correlation_expr(on, context, local_tables)?,
            join_type,
            using_columns,
        }),
        PlanNode::Aggregate {
            group_by,
            aggregates,
            input,
            grouping_metadata,
        } => {
            let bound_group_by = group_by
                .into_iter()
                .map(|expr| bind_correlation_expr(expr, context, local_tables))
                .collect::<Result<Vec<_>>>()?;
            let bound_aggregates = aggregates
                .into_iter()
                .map(|expr| bind_correlation_expr(expr, context, local_tables))
                .collect::<Result<Vec<_>>>()?;
            Ok(PlanNode::Aggregate {
                group_by: bound_group_by,
                aggregates: bound_aggregates,
                input: Box::new(bind_correlation_internal(
                    clone_box(&input),
                    context,
                    local_tables,
                )?),
                grouping_metadata,
            })
        }
        PlanNode::Limit {
            limit,
            offset,
            input,
        } => Ok(PlanNode::Limit {
            limit,
            offset,
            input: Box::new(bind_correlation_internal(
                clone_box(&input),
                context,
                local_tables,
            )?),
        }),
        PlanNode::LimitPercent {
            percent,
            offset,
            with_ties,
            input,
        } => Ok(PlanNode::LimitPercent {
            percent,
            offset,
            with_ties,
            input: Box::new(bind_correlation_internal(
                clone_box(&input),
                context,
                local_tables,
            )?),
        }),

        other => Ok(other),
    }
}

fn bind_correlation_expr(
    expr: Expr,
    context: &CorrelationContext,
    local_tables: &std::collections::HashSet<String>,
) -> Result<Expr> {
    match expr {
        Expr::Column { name, table } => bind_column_reference(name, table, context, local_tables),
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(bind_correlation_expr(
                clone_box(&left),
                context,
                local_tables,
            )?),
            op,
            right: Box::new(bind_correlation_expr(
                clone_box(&right),
                context,
                local_tables,
            )?),
        }),
        Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op,
            expr: Box::new(bind_correlation_expr(
                clone_box(&expr),
                context,
                local_tables,
            )?),
        }),
        Expr::Function { name, args } => {
            let bound_args = args
                .into_iter()
                .map(|arg| bind_correlation_expr(arg, context, local_tables))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::Function {
                name,
                args: bound_args,
            })
        }

        other => Ok(other),
    }
}

fn bind_column_reference(
    name: String,
    table: Option<String>,
    context: &CorrelationContext,
    local_tables: &std::collections::HashSet<String>,
) -> Result<Expr> {
    if let Some(ref t) = table {
        if local_tables.contains(t) {
            return Ok(Expr::Column { name, table });
        }
        let qualified = format!("{}.{}", t, name);
        match context.get(&qualified) {
            Some(v) => Ok(Expr::Literal(value_to_literal(v))),
            None => Ok(Expr::Column { name, table }),
        }
    } else if local_tables.is_empty() {
        match context.get(&name) {
            Some(v) => Ok(Expr::Literal(value_to_literal(v))),
            None => Ok(Expr::Column { name, table }),
        }
    } else {
        Ok(Expr::Column { name, table })
    }
}

#[inline]
fn clone_box<T: Clone>(boxed: &T) -> T {
    boxed.clone()
}

fn value_to_literal(value: &Value) -> yachtsql_optimizer::expr::LiteralValue {
    use yachtsql_optimizer::expr::LiteralValue;

    if value.is_null() {
        return LiteralValue::Null;
    }
    if let Some(b) = value.as_bool() {
        return LiteralValue::Boolean(b);
    }
    if let Some(i) = value.as_i64() {
        return LiteralValue::Int64(i);
    }
    if let Some(f) = value.as_f64() {
        return LiteralValue::Float64(f);
    }
    if let Some(d) = value.as_numeric() {
        return LiteralValue::Numeric(d);
    }
    if let Some(s) = value.as_str() {
        return LiteralValue::String(s.to_string());
    }
    if let Some(d) = value.as_date() {
        return LiteralValue::Date(d.format("%Y-%m-%d").to_string());
    }
    if let Some(ts) = value.as_timestamp() {
        return LiteralValue::Timestamp(ts.to_rfc3339());
    }

    LiteralValue::String(format!("{:?}", value))
}

#[cfg(test)]
mod tests {
    use yachtsql_optimizer::BinaryOp;

    use super::*;

    #[test]
    fn test_correlation_context() {
        let mut ctx = CorrelationContext::new();
        ctx.bind("dept_id".to_string(), Value::int64(10));
        ctx.bind("e.name".to_string(), Value::string("Alice".to_string()));

        assert!(ctx.contains("dept_id"));
        assert!(ctx.contains("e.name"));
        assert!(!ctx.contains("nonexistent"));

        assert_eq!(ctx.get("dept_id"), Some(&Value::int64(10)));
    }

    #[test]
    fn test_has_correlation_simple() {
        let expr = Expr::Column {
            name: "dept_id".to_string(),
            table: None,
        };
        assert!(has_correlation(&expr));

        let literal = Expr::Literal(yachtsql_optimizer::expr::LiteralValue::Int64(42));
        assert!(!has_correlation(&literal));
    }

    #[test]
    fn test_has_correlation_binary_op() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "salary".to_string(),
                table: None,
            }),
            op: BinaryOp::GreaterThan,
            right: Box::new(Expr::Literal(
                yachtsql_optimizer::expr::LiteralValue::Int64(50000),
            )),
        };
        assert!(has_correlation(&expr));
    }

    #[test]
    fn test_bind_correlation_column() {
        let mut ctx = CorrelationContext::new();
        ctx.bind("dept_id".to_string(), Value::int64(10));

        let expr = Expr::Column {
            name: "dept_id".to_string(),
            table: None,
        };

        let local_tables = std::collections::HashSet::new();
        let bound = bind_correlation_expr(expr, &ctx, &local_tables).unwrap();
        assert!(matches!(
            bound,
            Expr::Literal(yachtsql_optimizer::expr::LiteralValue::Int64(10))
        ));
    }

    #[test]
    fn test_value_to_literal() {
        assert!(matches!(
            value_to_literal(&Value::int64(42)),
            yachtsql_optimizer::expr::LiteralValue::Int64(42)
        ));
        assert!(matches!(
            value_to_literal(&Value::null()),
            yachtsql_optimizer::expr::LiteralValue::Null
        ));
    }
}
