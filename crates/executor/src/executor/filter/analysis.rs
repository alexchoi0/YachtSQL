use std::collections::HashSet;

use yachtsql_ir::{Expr, LogicalPlan};
use yachtsql_storage::Schema;

use super::super::PlanExecutor;

impl PlanExecutor<'_> {
    pub(super) fn plan_contains_outer_refs(plan: &LogicalPlan, outer_schema: &Schema) -> bool {
        let mut inner_tables = HashSet::new();
        Self::collect_plan_tables(plan, &mut inner_tables);
        Self::plan_has_outer_refs(plan, outer_schema, &inner_tables)
    }

    pub(super) fn collect_plan_tables(plan: &LogicalPlan, tables: &mut HashSet<String>) {
        match plan {
            LogicalPlan::Scan {
                table_name, schema, ..
            } => {
                tables.insert(table_name.to_lowercase());
                for field in &schema.fields {
                    if let Some(ref tbl) = field.table {
                        tables.insert(tbl.to_lowercase());
                    }
                }
            }
            LogicalPlan::Filter { input, .. } => Self::collect_plan_tables(input, tables),
            LogicalPlan::Project { input, schema, .. } => {
                Self::collect_plan_tables(input, tables);
                for field in &schema.fields {
                    if let Some(ref tbl) = field.table {
                        tables.insert(tbl.to_lowercase());
                    }
                }
            }
            LogicalPlan::Aggregate { input, schema, .. } => {
                Self::collect_plan_tables(input, tables);
                for field in &schema.fields {
                    if let Some(ref tbl) = field.table {
                        tables.insert(tbl.to_lowercase());
                    }
                }
            }
            LogicalPlan::Join {
                left,
                right,
                schema,
                ..
            } => {
                Self::collect_plan_tables(left, tables);
                Self::collect_plan_tables(right, tables);
                for field in &schema.fields {
                    if let Some(ref tbl) = field.table {
                        tables.insert(tbl.to_lowercase());
                    }
                }
            }
            LogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => {
                Self::collect_plan_tables(input, tables);
                for col in columns {
                    if let Some(alias) = &col.alias {
                        tables.insert(alias.to_lowercase());
                    }
                }
                for field in &schema.fields {
                    if let Some(ref tbl) = field.table {
                        tables.insert(tbl.to_lowercase());
                    }
                }
            }
            LogicalPlan::Sort { input, .. } => Self::collect_plan_tables(input, tables),
            LogicalPlan::Limit { input, .. } => Self::collect_plan_tables(input, tables),
            LogicalPlan::Distinct { input, .. } => Self::collect_plan_tables(input, tables),
            _ => {}
        }
    }

    fn plan_has_outer_refs(
        plan: &LogicalPlan,
        outer_schema: &Schema,
        inner_tables: &HashSet<String>,
    ) -> bool {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                Self::expr_has_outer_refs(predicate, outer_schema, inner_tables)
                    || Self::plan_has_outer_refs(input, outer_schema, inner_tables)
            }
            LogicalPlan::Project {
                input, expressions, ..
            } => {
                expressions
                    .iter()
                    .any(|e| Self::expr_has_outer_refs(e, outer_schema, inner_tables))
                    || Self::plan_has_outer_refs(input, outer_schema, inner_tables)
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            } => {
                condition
                    .as_ref()
                    .is_some_and(|c| Self::expr_has_outer_refs(c, outer_schema, inner_tables))
                    || Self::plan_has_outer_refs(left, outer_schema, inner_tables)
                    || Self::plan_has_outer_refs(right, outer_schema, inner_tables)
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } => {
                group_by
                    .iter()
                    .any(|e| Self::expr_has_outer_refs(e, outer_schema, inner_tables))
                    || aggregates
                        .iter()
                        .any(|e| Self::expr_has_outer_refs(e, outer_schema, inner_tables))
                    || Self::plan_has_outer_refs(input, outer_schema, inner_tables)
            }
            _ => false,
        }
    }

    fn expr_has_outer_refs(
        expr: &Expr,
        outer_schema: &Schema,
        inner_tables: &HashSet<String>,
    ) -> bool {
        match expr {
            Expr::Column {
                table: Some(tbl),
                name,
                ..
            } => {
                if inner_tables.contains(&tbl.to_lowercase()) {
                    return false;
                }
                outer_schema.fields().iter().any(|f| {
                    f.source_table
                        .as_ref()
                        .is_some_and(|src| src.eq_ignore_ascii_case(tbl))
                        || f.name.eq_ignore_ascii_case(name) && f.source_table.is_none()
                })
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_outer_refs(left, outer_schema, inner_tables)
                    || Self::expr_has_outer_refs(right, outer_schema, inner_tables)
            }
            Expr::UnaryOp { expr, .. } => {
                Self::expr_has_outer_refs(expr, outer_schema, inner_tables)
            }
            Expr::ScalarFunction { args, .. } => args
                .iter()
                .any(|a| Self::expr_has_outer_refs(a, outer_schema, inner_tables)),
            Expr::Aggregate { args, filter, .. } => {
                args.iter()
                    .any(|a| Self::expr_has_outer_refs(a, outer_schema, inner_tables))
                    || filter
                        .as_ref()
                        .is_some_and(|f| Self::expr_has_outer_refs(f, outer_schema, inner_tables))
            }
            _ => false,
        }
    }
}
