use std::collections::HashSet;

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, LogicalPlan, SortExpr, UnnestColumn};
use yachtsql_storage::{Record, Schema};

use super::super::PlanExecutor;

impl PlanExecutor<'_> {
    pub fn substitute_outer_refs_in_plan(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<LogicalPlan> {
        let mut inner_tables = HashSet::new();
        Self::collect_plan_tables(plan, &mut inner_tables);
        self.substitute_outer_refs_in_plan_with_inner_tables(
            plan,
            outer_schema,
            outer_record,
            &inner_tables,
        )
    }

    pub(super) fn substitute_outer_refs_in_expr(
        &self,
        expr: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Expr> {
        match expr {
            Expr::Column { table, name, index } => {
                let should_substitute = if let Some(tbl) = table {
                    outer_schema.fields().iter().any(|f| {
                        f.source_table
                            .as_ref()
                            .is_some_and(|src| src.eq_ignore_ascii_case(tbl))
                    }) || outer_schema
                        .fields()
                        .iter()
                        .any(|f| f.name.eq_ignore_ascii_case(name) && f.source_table.is_none())
                } else {
                    outer_schema.field_index(name).is_some()
                };

                if should_substitute && let Some(idx) = outer_schema.field_index(name) {
                    let value = outer_record
                        .values()
                        .get(idx)
                        .cloned()
                        .unwrap_or(Value::Null);
                    return Ok(Expr::Literal(Self::value_to_literal(value)));
                }
                Ok(Expr::Column {
                    table: table.clone(),
                    name: name.clone(),
                    index: *index,
                })
            }
            Expr::BinaryOp { left, op, right } => {
                let new_left =
                    self.substitute_outer_refs_in_expr(left, outer_schema, outer_record)?;
                let new_right =
                    self.substitute_outer_refs_in_expr(right, outer_schema, outer_record)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(new_left),
                    op: *op,
                    right: Box::new(new_right),
                })
            }
            Expr::IsNull {
                expr: inner,
                negated,
            } => {
                let new_inner =
                    self.substitute_outer_refs_in_expr(inner, outer_schema, outer_record)?;
                Ok(Expr::IsNull {
                    expr: Box::new(new_inner),
                    negated: *negated,
                })
            }
            Expr::ScalarFunction { name, args } => {
                let new_args = args
                    .iter()
                    .map(|a| self.substitute_outer_refs_in_expr(a, outer_schema, outer_record))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::ScalarFunction {
                    name: name.clone(),
                    args: new_args,
                })
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let new_inner =
                    self.substitute_outer_refs_in_expr(inner, outer_schema, outer_record)?;
                Ok(Expr::Cast {
                    expr: Box::new(new_inner),
                    data_type: data_type.clone(),
                    safe: *safe,
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    pub(super) fn substitute_outer_refs_in_plan_with_inner_tables(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
        inner_tables: &HashSet<String>,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            } => Ok(LogicalPlan::Scan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: projection.clone(),
            }),
            LogicalPlan::Filter { input, predicate } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_predicate = self.substitute_outer_refs_in_expr_with_inner_tables(
                    predicate,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(LogicalPlan::Filter {
                    input: Box::new(new_input),
                    predicate: new_predicate,
                })
            }
            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_expressions = expressions
                    .iter()
                    .map(|e| {
                        self.substitute_outer_refs_in_expr_with_inner_tables(
                            e,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Project {
                    input: Box::new(new_input),
                    expressions: new_expressions,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_columns = columns
                    .iter()
                    .map(|c| {
                        let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                            &c.expr,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )?;
                        Ok(UnnestColumn {
                            expr: new_expr,
                            alias: c.alias.clone(),
                            with_offset: c.with_offset,
                            offset_alias: c.offset_alias.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Unnest {
                    input: Box::new(new_input),
                    columns: new_columns,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::SetOperation {
                left,
                right,
                op,
                all,
                schema,
            } => {
                let new_left = self.substitute_outer_refs_in_plan_with_inner_tables(
                    left,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_right = self.substitute_outer_refs_in_plan_with_inner_tables(
                    right,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(LogicalPlan::SetOperation {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    op: *op,
                    all: *all,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let new_left = self.substitute_outer_refs_in_plan_with_inner_tables(
                    left,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_right = self.substitute_outer_refs_in_plan_with_inner_tables(
                    right,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_condition = condition
                    .as_ref()
                    .map(|c| {
                        self.substitute_outer_refs_in_expr_with_inner_tables(
                            c,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )
                    })
                    .transpose()?;
                Ok(LogicalPlan::Join {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    join_type: *join_type,
                    condition: new_condition,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(LogicalPlan::Limit {
                    input: Box::new(new_input),
                    limit: *limit,
                    offset: *offset,
                })
            }
            LogicalPlan::Sort { input, sort_exprs } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_sort_exprs = sort_exprs
                    .iter()
                    .map(|se| {
                        let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                            &se.expr,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )?;
                        Ok(SortExpr {
                            expr: new_expr,
                            asc: se.asc,
                            nulls_first: se.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Sort {
                    input: Box::new(new_input),
                    sort_exprs: new_sort_exprs,
                })
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => {
                let new_input = self.substitute_outer_refs_in_plan_with_inner_tables(
                    input,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_group_by = group_by
                    .iter()
                    .map(|e| {
                        self.substitute_outer_refs_in_expr_with_inner_tables(
                            e,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let new_aggregates = aggregates
                    .iter()
                    .map(|e| {
                        self.substitute_outer_refs_in_expr_with_inner_tables(
                            e,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Aggregate {
                    input: Box::new(new_input),
                    group_by: new_group_by,
                    aggregates: new_aggregates,
                    schema: schema.clone(),
                    grouping_sets: grouping_sets.clone(),
                })
            }
            other => Ok(other.clone()),
        }
    }

    pub(super) fn substitute_outer_refs_in_expr_with_inner_tables(
        &self,
        expr: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
        inner_tables: &HashSet<String>,
    ) -> Result<Expr> {
        match expr {
            Expr::Column { table, name, index } => {
                let should_substitute = if let Some(tbl) = table {
                    if inner_tables.contains(&tbl.to_lowercase()) {
                        false
                    } else {
                        outer_schema.fields().iter().any(|f| {
                            f.source_table
                                .as_ref()
                                .is_some_and(|src| src.eq_ignore_ascii_case(tbl))
                        }) || outer_schema
                            .fields()
                            .iter()
                            .any(|f| f.name.eq_ignore_ascii_case(name) && f.source_table.is_none())
                    }
                } else {
                    let inner_table_is_only_alias = inner_tables
                        .iter()
                        .all(|t| self.catalog.get_table(t).is_none());
                    inner_table_is_only_alias
                        && !inner_tables.contains(&name.to_lowercase())
                        && outer_schema.field_index(name).is_some()
                };

                if should_substitute && let Some(idx) = outer_schema.field_index(name) {
                    let value = outer_record
                        .values()
                        .get(idx)
                        .cloned()
                        .unwrap_or(Value::Null);
                    return Ok(Expr::Literal(Self::value_to_literal(value)));
                }
                Ok(Expr::Column {
                    table: table.clone(),
                    name: name.clone(),
                    index: *index,
                })
            }
            Expr::BinaryOp { left, op, right } => {
                let new_left = self.substitute_outer_refs_in_expr_with_inner_tables(
                    left,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_right = self.substitute_outer_refs_in_expr_with_inner_tables(
                    right,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(Expr::BinaryOp {
                    left: Box::new(new_left),
                    op: *op,
                    right: Box::new(new_right),
                })
            }
            Expr::ScalarFunction { name, args } => {
                let new_args = args
                    .iter()
                    .map(|a| {
                        self.substitute_outer_refs_in_expr_with_inner_tables(
                            a,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::ScalarFunction {
                    name: name.clone(),
                    args: new_args,
                })
            }
            Expr::Struct { fields } => {
                let new_fields = fields
                    .iter()
                    .map(|(name, e)| {
                        let new_e = self.substitute_outer_refs_in_expr_with_inner_tables(
                            e,
                            outer_schema,
                            outer_record,
                            inner_tables,
                        )?;
                        Ok((name.clone(), new_e))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::Struct { fields: new_fields })
            }
            Expr::StructAccess { expr, field } => {
                let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                    expr,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(Expr::StructAccess {
                    expr: Box::new(new_expr),
                    field: field.clone(),
                })
            }
            Expr::IsNull { expr, negated } => {
                let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                    expr,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(Expr::IsNull {
                    expr: Box::new(new_expr),
                    negated: *negated,
                })
            }
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => {
                let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                    expr,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                let new_pattern = self.substitute_outer_refs_in_expr_with_inner_tables(
                    pattern,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(Expr::Like {
                    expr: Box::new(new_expr),
                    pattern: Box::new(new_pattern),
                    negated: *negated,
                    case_insensitive: *case_insensitive,
                })
            }
            Expr::UnaryOp { op, expr } => {
                let new_expr = self.substitute_outer_refs_in_expr_with_inner_tables(
                    expr,
                    outer_schema,
                    outer_record,
                    inner_tables,
                )?;
                Ok(Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(new_expr),
                })
            }
            _ => Ok(expr.clone()),
        }
    }
}
