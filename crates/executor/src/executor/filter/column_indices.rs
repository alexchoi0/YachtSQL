use yachtsql_ir::{Expr, LogicalPlan};
use yachtsql_storage::Schema;

use super::super::PlanExecutor;

impl PlanExecutor<'_> {
    pub(super) fn collect_outer_column_indices_from_expr(expr: &Expr, schema: &Schema) -> Vec<usize> {
        let mut indices = Vec::new();
        Self::collect_column_indices_recursive(expr, schema, &mut indices);
        indices.sort_unstable();
        indices.dedup();
        indices
    }

    fn collect_column_indices_recursive(expr: &Expr, schema: &Schema, indices: &mut Vec<usize>) {
        match expr {
            Expr::Column { name, index, .. } => {
                if let Some(idx) = index {
                    indices.push(*idx);
                } else if let Some(idx) = schema.field_index(name) {
                    indices.push(idx);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_column_indices_recursive(left, schema, indices);
                Self::collect_column_indices_recursive(right, schema, indices);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_column_indices_recursive(expr, schema, indices);
            }
            Expr::ScalarFunction { args, .. } => {
                for arg in args {
                    Self::collect_column_indices_recursive(arg, schema, indices);
                }
            }
            Expr::Exists { subquery, .. } => {
                Self::collect_column_indices_from_plan(subquery, schema, indices);
            }
            Expr::InSubquery { expr, subquery, .. } => {
                Self::collect_column_indices_recursive(expr, schema, indices);
                Self::collect_column_indices_from_plan(subquery, schema, indices);
            }
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) | Expr::ArraySubquery(plan) => {
                Self::collect_column_indices_from_plan(plan, schema, indices);
            }
            _ => {}
        }
    }

    fn collect_column_indices_from_plan(
        plan: &LogicalPlan,
        outer_schema: &Schema,
        indices: &mut Vec<usize>,
    ) {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                Self::collect_outer_refs_from_expr(predicate, outer_schema, indices);
                Self::collect_column_indices_from_plan(input, outer_schema, indices);
            }
            LogicalPlan::Project {
                input, expressions, ..
            } => {
                for expr in expressions {
                    Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
                }
                Self::collect_column_indices_from_plan(input, outer_schema, indices);
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } => {
                for expr in group_by {
                    Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
                }
                for expr in aggregates {
                    Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
                }
                Self::collect_column_indices_from_plan(input, outer_schema, indices);
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            } => {
                if let Some(cond) = condition {
                    Self::collect_outer_refs_from_expr(cond, outer_schema, indices);
                }
                Self::collect_column_indices_from_plan(left, outer_schema, indices);
                Self::collect_column_indices_from_plan(right, outer_schema, indices);
            }
            LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input, .. } => {
                Self::collect_column_indices_from_plan(input, outer_schema, indices);
            }
            _ => {}
        }
    }

    fn collect_outer_refs_from_expr(expr: &Expr, outer_schema: &Schema, indices: &mut Vec<usize>) {
        match expr {
            Expr::Column { name, .. } => {
                if let Some(idx) = outer_schema.field_index(name) {
                    indices.push(idx);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_outer_refs_from_expr(left, outer_schema, indices);
                Self::collect_outer_refs_from_expr(right, outer_schema, indices);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
            }
            Expr::ScalarFunction { args, .. } => {
                for arg in args {
                    Self::collect_outer_refs_from_expr(arg, outer_schema, indices);
                }
            }
            Expr::Aggregate { args, filter, .. } => {
                for arg in args {
                    Self::collect_outer_refs_from_expr(arg, outer_schema, indices);
                }
                if let Some(f) = filter {
                    Self::collect_outer_refs_from_expr(f, outer_schema, indices);
                }
            }
            Expr::Cast { expr, .. } => {
                Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
            }
            Expr::IsNull { expr, .. } => {
                Self::collect_outer_refs_from_expr(expr, outer_schema, indices);
            }
            _ => {}
        }
    }
}
