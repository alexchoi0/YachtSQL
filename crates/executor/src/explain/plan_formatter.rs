use std::collections::HashMap;

use yachtsql_core::error::Result;
use yachtsql_core::types::{DataType, Value};
use yachtsql_optimizer::plan::PlanNode;
use yachtsql_storage::Schema;

use super::profiler::{OperatorId, OperatorMetrics};
use crate::Table;

#[derive(Debug, Clone)]
pub struct ExplainOptions {
    pub analyze: bool,
    pub verbose: bool,
    pub profiler_metrics: Option<HashMap<OperatorId, OperatorMetrics>>,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeStats {
    pub actual_rows: Option<usize>,
    pub execution_time_ms: Option<f64>,
    pub peak_memory_bytes: Option<usize>,
}

impl Default for ExplainOptions {
    fn default() -> Self {
        Self {
            analyze: false,
            verbose: false,
            profiler_metrics: None,
        }
    }
}

pub fn format_plan(plan: &PlanNode, options: ExplainOptions) -> Result<Table> {
    let mut output = String::new();

    if options.analyze && options.profiler_metrics.is_none() {
        output.push_str("EXPLAIN ANALYZE:\n");
        output.push_str("  Note: Detailed per-operator statistics not yet implemented\n\n");
    }

    format_node_with_metrics(plan, &mut output, 0, &options, 0);

    let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
        "QUERY PLAN".to_string(),
        DataType::String,
    )]);

    let lines: Vec<Vec<Value>> = output
        .lines()
        .map(|line| vec![Value::string(line.to_string())])
        .collect();

    Table::from_values(schema, lines)
}

fn format_node_with_metrics(
    node: &PlanNode,
    output: &mut String,
    indent: usize,
    options: &ExplainOptions,
    operator_id: OperatorId,
) {
    let indent_str = "  ".repeat(indent);

    let metrics = options
        .profiler_metrics
        .as_ref()
        .and_then(|m| m.get(&operator_id));

    match node {
        PlanNode::Scan {
            table_name,
            projection,
            ..
        } => {
            output.push_str(&format!("{}-> TableScan: {}", indent_str, table_name));
            if let Some(m) = metrics {
                output.push_str(&format!(
                    " (rows={}, time={:.2}ms",
                    m.rows_produced,
                    m.execution_time.as_secs_f64() * 1000.0
                ));
                if let Some(mem) = m.peak_memory_bytes {
                    output.push_str(&format!(", mem={}", format_bytes(mem)));
                }
                output.push(')');
            }
            output.push('\n');

            if options.verbose {
                if let Some(cols) = projection {
                    output.push_str(&format!("{}   Columns: {}\n", indent_str, cols.join(", ")));
                }
            }
        }

        PlanNode::IndexScan {
            table_name,
            index_name,
            predicate,
            ..
        } => {
            output.push_str(&format!(
                "{}-> IndexScan: {} (index: {})",
                indent_str, table_name, index_name
            ));
            if let Some(m) = metrics {
                output.push_str(&format!(
                    " (rows={}, time={:.2}ms)",
                    m.rows_produced,
                    m.execution_time.as_secs_f64() * 1000.0
                ));
            }
            output.push('\n');

            if options.verbose {
                output.push_str(&format!(
                    "{}   Predicate: {}\n",
                    indent_str,
                    format_expr(predicate)
                ));
            }
        }

        PlanNode::Filter { input, predicate } => {
            output.push_str(&format!(
                "{}-> Filter: {}",
                indent_str,
                format_expr(predicate)
            ));
            if let Some(m) = metrics {
                output.push_str(&format!(
                    " (rows={}, time={:.2}ms",
                    m.rows_produced,
                    m.execution_time.as_secs_f64() * 1000.0
                ));

                if let Some(selectivity) = m.custom_metrics.get("selectivity") {
                    output.push_str(&format!(", selectivity={}", selectivity));
                }
                output.push(')');
            }
            output.push('\n');
            format_node_with_metrics(input, output, indent + 1, options, operator_id + 1);
        }

        PlanNode::Projection { input, expressions } => {
            let expr_strs: Vec<String> = expressions
                .iter()
                .map(|(expr, alias)| {
                    if let Some(a) = alias {
                        format!("{} AS {}", format_expr(expr), a)
                    } else {
                        format_expr(expr)
                    }
                })
                .collect();
            output.push_str(&format!(
                "{}-> Projection: {}",
                indent_str,
                expr_strs.join(", ")
            ));
            if let Some(m) = metrics {
                output.push_str(&format!(
                    " (rows={}, time={:.2}ms)",
                    m.rows_produced,
                    m.execution_time.as_secs_f64() * 1000.0
                ));
            }
            output.push('\n');
            format_node_with_metrics(input, output, indent + 1, options, operator_id + 1);
        }

        PlanNode::Join {
            left,
            right,
            join_type,
            on,
        } => {
            let join_type_str = format!("{:?}", join_type);
            output.push_str(&format!(
                "{}-> {} Join: {}",
                indent_str,
                join_type_str,
                format_expr(on)
            ));
            if let Some(m) = metrics {
                output.push_str(&format!(
                    " (rows={}, time={:.2}ms",
                    m.rows_produced,
                    m.execution_time.as_secs_f64() * 1000.0
                ));

                if let Some(hash_size) = m.custom_metrics.get("hash_table_size") {
                    output.push_str(&format!(", hash_table={}", hash_size));
                }
                output.push(')');
            }
            output.push('\n');
            let left_id = operator_id + 1;
            format_node_with_metrics(left, output, indent + 1, options, left_id);
            let right_id = left_id + count_nodes(left);
            format_node_with_metrics(right, output, indent + 1, options, right_id);
        }

        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            output.push_str(&format!("{}-> Aggregate", indent_str));
            if let Some(m) = metrics {
                output.push_str(&format!(
                    " (rows={}, time={:.2}ms",
                    m.rows_produced,
                    m.execution_time.as_secs_f64() * 1000.0
                ));
                if let Some(groups) = m.custom_metrics.get("num_groups") {
                    output.push_str(&format!(", groups={}", groups));
                }
                output.push(')');
            }
            output.push('\n');

            if !group_by.is_empty() {
                let group_strs: Vec<String> =
                    group_by.iter().map(|expr| format_expr(expr)).collect();
                output.push_str(&format!(
                    "{}   GROUP BY: {}\n",
                    indent_str,
                    group_strs.join(", ")
                ));
            }
            if !aggregates.is_empty() {
                let agg_strs: Vec<String> =
                    aggregates.iter().map(|expr| format_expr(expr)).collect();
                output.push_str(&format!(
                    "{}   Aggregates: {}\n",
                    indent_str,
                    agg_strs.join(", ")
                ));
            }
            format_node_with_metrics(input, output, indent + 1, options, operator_id + 1);
        }

        PlanNode::Sort { input, order_by } => {
            let order_strs: Vec<String> = order_by
                .iter()
                .map(|order_expr| {
                    let dir = match order_expr.asc {
                        Some(true) | None => "ASC",
                        Some(false) => "DESC",
                    };
                    format!("{} {}", format_expr(&order_expr.expr), dir)
                })
                .collect();
            output.push_str(&format!("{}-> Sort: {}", indent_str, order_strs.join(", ")));
            if let Some(m) = metrics {
                output.push_str(&format!(
                    " (rows={}, time={:.2}ms",
                    m.rows_produced,
                    m.execution_time.as_secs_f64() * 1000.0
                ));
                if let Some(mem) = m.peak_memory_bytes {
                    output.push_str(&format!(", mem={}", format_bytes(mem)));
                }
                output.push(')');
            }
            output.push('\n');
            format_node_with_metrics(input, output, indent + 1, options, operator_id + 1);
        }

        PlanNode::Limit {
            input,
            limit,
            offset,
        } => {
            output.push_str(&format!("{}-> Limit: {}", indent_str, limit));
            if *offset > 0 {
                output.push_str(&format!(" OFFSET: {}", offset));
            }
            if let Some(m) = metrics {
                output.push_str(&format!(
                    " (rows={}, time={:.2}ms)",
                    m.rows_produced,
                    m.execution_time.as_secs_f64() * 1000.0
                ));
            }
            output.push('\n');
            format_node_with_metrics(input, output, indent + 1, options, operator_id + 1);
        }

        PlanNode::Union { left, right, all } => {
            let union_type = if *all { "UNION ALL" } else { "UNION" };
            output.push_str(&format!("{}-> {}", indent_str, union_type));
            if let Some(m) = metrics {
                output.push_str(&format!(
                    " (rows={}, time={:.2}ms)",
                    m.rows_produced,
                    m.execution_time.as_secs_f64() * 1000.0
                ));
            }
            output.push('\n');
            let left_id = operator_id + 1;
            format_node_with_metrics(left, output, indent + 1, options, left_id);
            let right_id = left_id + count_nodes(left);
            format_node_with_metrics(right, output, indent + 1, options, right_id);
        }

        PlanNode::Distinct { input } => {
            output.push_str(&format!("{}-> Distinct", indent_str));
            if let Some(m) = metrics {
                output.push_str(&format!(
                    " (rows={}, time={:.2}ms)",
                    m.rows_produced,
                    m.execution_time.as_secs_f64() * 1000.0
                ));
            }
            output.push('\n');
            format_node_with_metrics(input, output, indent + 1, options, operator_id + 1);
        }

        PlanNode::SubqueryScan { subquery, alias } => {
            output.push_str(&format!("{}-> SubqueryScan ({})", indent_str, alias));
            if let Some(m) = metrics {
                output.push_str(&format!(
                    " (rows={}, time={:.2}ms)",
                    m.rows_produced,
                    m.execution_time.as_secs_f64() * 1000.0
                ));
            }
            output.push('\n');
            format_node_with_metrics(subquery, output, indent + 1, options, operator_id + 1);
        }

        _ => {
            output.push_str(&format!("{}-> {:?}\n", indent_str, node));
        }
    }
}

fn count_nodes(node: &PlanNode) -> usize {
    match node {
        PlanNode::Scan { .. } | PlanNode::IndexScan { .. } => 1,
        PlanNode::Filter { input, .. }
        | PlanNode::Projection { input, .. }
        | PlanNode::Aggregate { input, .. }
        | PlanNode::Sort { input, .. }
        | PlanNode::Limit { input, .. }
        | PlanNode::Distinct { input }
        | PlanNode::SubqueryScan {
            subquery: input, ..
        } => 1 + count_nodes(input),
        PlanNode::Join { left, right, .. } | PlanNode::Union { left, right, .. } => {
            1 + count_nodes(left) + count_nodes(right)
        }
        _ => 1,
    }
}

fn format_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn format_node(node: &PlanNode, output: &mut String, indent: usize, verbose: bool) {
    let indent_str = "  ".repeat(indent);

    match node {
        PlanNode::Scan {
            table_name,
            projection,
            ..
        } => {
            output.push_str(&format!("{}-> TableScan: {}\n", indent_str, table_name));
            if verbose {
                if let Some(cols) = projection {
                    output.push_str(&format!("{}   Columns: {}\n", indent_str, cols.join(", ")));
                }
            }
        }

        PlanNode::IndexScan {
            table_name,
            index_name,
            predicate,
            ..
        } => {
            output.push_str(&format!(
                "{}-> IndexScan: {} (index: {})\n",
                indent_str, table_name, index_name
            ));
            if verbose {
                output.push_str(&format!(
                    "{}   Predicate: {}\n",
                    indent_str,
                    format_expr(predicate)
                ));
            }
        }

        PlanNode::Filter { input, predicate } => {
            output.push_str(&format!(
                "{}-> Filter: {}\n",
                indent_str,
                format_expr(predicate)
            ));
            format_node(input, output, indent + 1, verbose);
        }

        PlanNode::Projection { input, expressions } => {
            let expr_strs: Vec<String> = expressions
                .iter()
                .map(|(expr, alias)| {
                    if let Some(a) = alias {
                        format!("{} AS {}", format_expr(expr), a)
                    } else {
                        format_expr(expr)
                    }
                })
                .collect();
            output.push_str(&format!(
                "{}-> Projection: {}\n",
                indent_str,
                expr_strs.join(", ")
            ));
            format_node(input, output, indent + 1, verbose);
        }

        PlanNode::Join {
            left,
            right,
            join_type,
            on,
        } => {
            let join_type_str = format!("{:?}", join_type);
            output.push_str(&format!(
                "{}-> {} Join: {}\n",
                indent_str,
                join_type_str,
                format_expr(on)
            ));
            format_node(left, output, indent + 1, verbose);
            format_node(right, output, indent + 1, verbose);
        }

        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            output.push_str(&format!("{}-> Aggregate\n", indent_str));
            if !group_by.is_empty() {
                let group_strs: Vec<String> =
                    group_by.iter().map(|expr| format_expr(expr)).collect();
                output.push_str(&format!(
                    "{}   GROUP BY: {}\n",
                    indent_str,
                    group_strs.join(", ")
                ));
            }
            if !aggregates.is_empty() {
                let agg_strs: Vec<String> =
                    aggregates.iter().map(|expr| format_expr(expr)).collect();
                output.push_str(&format!(
                    "{}   Aggregates: {}\n",
                    indent_str,
                    agg_strs.join(", ")
                ));
            }
            format_node(input, output, indent + 1, verbose);
        }

        PlanNode::Sort { input, order_by } => {
            let order_strs: Vec<String> = order_by
                .iter()
                .map(|order_expr| {
                    let dir = match order_expr.asc {
                        Some(true) | None => "ASC",
                        Some(false) => "DESC",
                    };
                    format!("{} {}", format_expr(&order_expr.expr), dir)
                })
                .collect();
            output.push_str(&format!(
                "{}-> Sort: {}\n",
                indent_str,
                order_strs.join(", ")
            ));
            format_node(input, output, indent + 1, verbose);
        }

        PlanNode::Limit {
            input,
            limit,
            offset,
        } => {
            output.push_str(&format!("{}-> Limit: {}", indent_str, limit));
            if *offset > 0 {
                output.push_str(&format!(" OFFSET: {}", offset));
            }
            output.push('\n');
            format_node(input, output, indent + 1, verbose);
        }

        PlanNode::Union { left, right, all } => {
            let union_type = if *all { "UNION ALL" } else { "UNION" };
            output.push_str(&format!("{}-> {}\n", indent_str, union_type));
            format_node(left, output, indent + 1, verbose);
            format_node(right, output, indent + 1, verbose);
        }

        PlanNode::Distinct { input } => {
            output.push_str(&format!("{}-> Distinct\n", indent_str));
            format_node(input, output, indent + 1, verbose);
        }

        PlanNode::SubqueryScan { subquery, alias } => {
            output.push_str(&format!("{}-> SubqueryScan ({})\n", indent_str, alias));
            format_node(subquery, output, indent + 1, verbose);
        }

        _ => {
            output.push_str(&format!("{}-> {:?}\n", indent_str, node));
        }
    }
}

fn format_expr(expr: &yachtsql_optimizer::expr::Expr) -> String {
    use yachtsql_optimizer::expr::{BinaryOp, Expr};

    match expr {
        Expr::Column { name, .. } => name.clone(),
        Expr::Literal(lit) => format!("{:?}", lit),
        Expr::BinaryOp { left, op, right } => {
            let op_str = match op {
                BinaryOp::Add => "+",
                BinaryOp::Subtract => "-",
                BinaryOp::Multiply => "*",
                BinaryOp::Divide => "/",
                BinaryOp::Modulo => "%",
                BinaryOp::Equal => "=",
                BinaryOp::NotEqual => "!=",
                BinaryOp::LessThan => "<",
                BinaryOp::LessThanOrEqual => "<=",
                BinaryOp::GreaterThan => ">",
                BinaryOp::GreaterThanOrEqual => ">=",
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
                BinaryOp::Like => "LIKE",
                BinaryOp::NotLike => "NOT LIKE",
                _ => "?",
            };
            format!("({} {} {})", format_expr(left), op_str, format_expr(right))
        }
        Expr::Function { name, args } => {
            let arg_strs: Vec<String> = args.iter().map(|a| format_expr(a)).collect();
            format!("{}({})", name, arg_strs.join(", "))
        }
        Expr::Aggregate { name, args, .. } => {
            let arg_strs: Vec<String> = args.iter().map(|a| format_expr(a)).collect();
            format!("{}({})", name, arg_strs.join(", "))
        }
        Expr::Cast { expr, data_type } => {
            format!("CAST({} AS {:?})", format_expr(expr), data_type)
        }
        Expr::UnaryOp { op, expr } => {
            use yachtsql_optimizer::expr::UnaryOp;
            let op_str = match op {
                UnaryOp::Not => "NOT",
                UnaryOp::Negate => "-",
                UnaryOp::Plus => "+",
                UnaryOp::IsNull => "IS NULL",
                UnaryOp::IsNotNull => "IS NOT NULL",
                UnaryOp::BitwiseNot => "~",
            };
            if matches!(op, UnaryOp::IsNull | UnaryOp::IsNotNull) {
                format!("{} {}", format_expr(expr), op_str)
            } else {
                format!("{}{}", op_str, format_expr(expr))
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let list_strs: Vec<String> = list.iter().map(|e| format_expr(e)).collect();
            let not = if *negated { "NOT " } else { "" };
            format!("{} {}IN ({})", format_expr(expr), not, list_strs.join(", "))
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut s = String::from("CASE");
            if let Some(op) = operand {
                s.push_str(&format!(" {}", format_expr(op)));
            }
            for (cond, res) in when_then {
                s.push_str(&format!(
                    " WHEN {} THEN {}",
                    format_expr(cond),
                    format_expr(res)
                ));
            }
            if let Some(els) = else_expr {
                s.push_str(&format!(" ELSE {}", format_expr(els)));
            }
            s.push_str(" END");
            s
        }
        _ => format!("{:?}", expr),
    }
}
