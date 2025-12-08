use std::collections::HashSet;

use yachtsql_ir::expr::Expr;
use yachtsql_ir::plan::PlanNode;

pub fn infer_output_columns(node: &PlanNode) -> Option<HashSet<String>> {
    match node {
        PlanNode::Scan { projection, .. } | PlanNode::IndexScan { projection, .. } => {
            projection.clone().map(|cols| cols.into_iter().collect())
        }
        PlanNode::Projection { expressions, .. } => {
            let mut cols = HashSet::new();
            for (expr, alias) in expressions {
                if let Some(name) = alias.as_ref().cloned().or_else(|| find_simple_column(expr)) {
                    cols.insert(name);
                } else {
                    return None;
                }
            }
            Some(cols)
        }
        PlanNode::Filter { input, .. }
        | PlanNode::Sort { input, .. }
        | PlanNode::Limit { input, .. }
        | PlanNode::Distinct { input }
        | PlanNode::DistinctOn { input, .. }
        | PlanNode::Window { input, .. } => infer_output_columns(input),
        PlanNode::Aggregate { .. } => None,
        PlanNode::Join { left, right, .. }
        | PlanNode::LateralJoin { left, right, .. }
        | PlanNode::Union { left, right, .. }
        | PlanNode::Intersect { left, right, .. }
        | PlanNode::Except { left, right, .. } => {
            let left_cols = infer_output_columns(left)?;
            let right_cols = infer_output_columns(right)?;
            Some(merge_sets(left_cols, right_cols))
        }
        PlanNode::Cte { input, .. } => infer_output_columns(input),
        PlanNode::SubqueryScan { subquery, .. } => infer_output_columns(subquery),
        PlanNode::TableSample { input, .. } => infer_output_columns(input),
        PlanNode::Pivot { .. } => None,
        PlanNode::Unpivot { .. } => None,
        PlanNode::Update { .. }
        | PlanNode::Delete { .. }
        | PlanNode::Truncate { .. }
        | PlanNode::AlterTable { .. }
        | PlanNode::InsertOnConflict { .. }
        | PlanNode::Insert { .. }
        | PlanNode::Merge { .. }
        | PlanNode::Unnest { .. }
        | PlanNode::TableValuedFunction { .. }
        | PlanNode::ArrayJoin { .. }
        | PlanNode::EmptyRelation
        | PlanNode::Values { .. } => None,
    }
}

fn find_simple_column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column { name, .. } => Some(name.clone()),
        Expr::Grouping { column } => Some(column.clone()),
        _ => None,
    }
}

fn merge_sets(mut left: HashSet<String>, right: HashSet<String>) -> HashSet<String> {
    left.extend(right);
    left
}
