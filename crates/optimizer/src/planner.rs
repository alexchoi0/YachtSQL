use yachtsql_common::error::Result;
use yachtsql_ir::{JoinType, LogicalPlan, SetOperationType};

use crate::physical_plan::PhysicalPlan;

pub struct PhysicalPlanner;

impl PhysicalPlanner {
    pub fn new() -> Self {
        Self
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn plan(&self, logical: &LogicalPlan) -> Result<PhysicalPlan> {
        match logical {
            LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            } => Ok(PhysicalPlan::TableScan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: projection.clone(),
            }),

            LogicalPlan::Filter { input, predicate } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Filter {
                    input: Box::new(input),
                    predicate: predicate.clone(),
                })
            }

            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Project {
                    input: Box::new(input),
                    expressions: expressions.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::HashAggregate {
                    input: Box::new(input),
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    schema: schema.clone(),
                    grouping_sets: grouping_sets.clone(),
                })
            }

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let left = self.plan(left)?;
                let right = self.plan(right)?;
                match join_type {
                    JoinType::Cross => Ok(PhysicalPlan::CrossJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        schema: schema.clone(),
                    }),
                    _ => Ok(PhysicalPlan::NestedLoopJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        join_type: *join_type,
                        condition: condition.clone(),
                        schema: schema.clone(),
                    }),
                }
            }

            LogicalPlan::Sort { input, sort_exprs } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Sort {
                    input: Box::new(input),
                    sort_exprs: sort_exprs.clone(),
                })
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Limit {
                    input: Box::new(input),
                    limit: *limit,
                    offset: *offset,
                })
            }

            LogicalPlan::Distinct { input } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Distinct {
                    input: Box::new(input),
                })
            }

            LogicalPlan::Values { values, schema } => Ok(PhysicalPlan::Values {
                values: values.clone(),
                schema: schema.clone(),
            }),

            LogicalPlan::Empty { schema } => Ok(PhysicalPlan::Empty {
                schema: schema.clone(),
            }),

            LogicalPlan::SetOperation {
                left,
                right,
                op,
                all,
                schema,
            } => {
                let left = self.plan(left)?;
                let right = self.plan(right)?;
                match op {
                    SetOperationType::Union => Ok(PhysicalPlan::Union {
                        inputs: vec![left, right],
                        all: *all,
                        schema: schema.clone(),
                    }),
                    SetOperationType::Intersect => Ok(PhysicalPlan::Intersect {
                        left: Box::new(left),
                        right: Box::new(right),
                        all: *all,
                        schema: schema.clone(),
                    }),
                    SetOperationType::Except => Ok(PhysicalPlan::Except {
                        left: Box::new(left),
                        right: Box::new(right),
                        all: *all,
                        schema: schema.clone(),
                    }),
                }
            }

            LogicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Window {
                    input: Box::new(input),
                    window_exprs: window_exprs.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::WithCte { ctes, body } => {
                let body = self.plan(body)?;
                let ctes = ctes
                    .iter()
                    .map(|cte| {
                        let query = self.plan(&cte.query)?;
                        Ok(yachtsql_ir::CteDefinition {
                            name: cte.name.clone(),
                            columns: cte.columns.clone(),
                            query: Box::new(query.into_logical()),
                            recursive: cte.recursive,
                            materialized: cte.materialized,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(PhysicalPlan::WithCte {
                    ctes,
                    body: Box::new(body),
                })
            }

            LogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Unnest {
                    input: Box::new(input),
                    columns: columns.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::Qualify { input, predicate } => {
                let input = self.plan(input)?;
                Ok(PhysicalPlan::Qualify {
                    input: Box::new(input),
                    predicate: predicate.clone(),
                })
            }

            LogicalPlan::Insert {
                table_name,
                columns,
                source,
            } => {
                let source = self.plan(source)?;
                Ok(PhysicalPlan::Insert {
                    table_name: table_name.clone(),
                    columns: columns.clone(),
                    source: Box::new(source),
                })
            }

            LogicalPlan::Update {
                table_name,
                assignments,
                filter,
            } => Ok(PhysicalPlan::Update {
                table_name: table_name.clone(),
                assignments: assignments.clone(),
                filter: filter.clone(),
            }),

            LogicalPlan::Delete { table_name, filter } => Ok(PhysicalPlan::Delete {
                table_name: table_name.clone(),
                filter: filter.clone(),
            }),

            LogicalPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => {
                let source = self.plan(source)?;
                Ok(PhysicalPlan::Merge {
                    target_table: target_table.clone(),
                    source: Box::new(source),
                    on: on.clone(),
                    clauses: clauses.clone(),
                })
            }

            LogicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
            } => Ok(PhysicalPlan::CreateTable {
                table_name: table_name.clone(),
                columns: columns.clone(),
                if_not_exists: *if_not_exists,
                or_replace: *or_replace,
            }),

            LogicalPlan::DropTable {
                table_names,
                if_exists,
            } => Ok(PhysicalPlan::DropTable {
                table_names: table_names.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::AlterTable {
                table_name,
                operation,
            } => Ok(PhysicalPlan::AlterTable {
                table_name: table_name.clone(),
                operation: operation.clone(),
            }),

            LogicalPlan::Truncate { table_name } => Ok(PhysicalPlan::Truncate {
                table_name: table_name.clone(),
            }),

            LogicalPlan::CreateView {
                name,
                query,
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
            } => {
                let query = self.plan(query)?;
                Ok(PhysicalPlan::CreateView {
                    name: name.clone(),
                    query: Box::new(query),
                    query_sql: query_sql.clone(),
                    column_aliases: column_aliases.clone(),
                    or_replace: *or_replace,
                    if_not_exists: *if_not_exists,
                })
            }

            LogicalPlan::DropView { name, if_exists } => Ok(PhysicalPlan::DropView {
                name: name.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::CreateSchema {
                name,
                if_not_exists,
            } => Ok(PhysicalPlan::CreateSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
            }),

            LogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => Ok(PhysicalPlan::DropSchema {
                name: name.clone(),
                if_exists: *if_exists,
                cascade: *cascade,
            }),

            LogicalPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_temp,
            } => Ok(PhysicalPlan::CreateFunction {
                name: name.clone(),
                args: args.clone(),
                return_type: return_type.clone(),
                body: body.clone(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
                is_temp: *is_temp,
            }),

            LogicalPlan::DropFunction { name, if_exists } => Ok(PhysicalPlan::DropFunction {
                name: name.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
            } => {
                let body = body
                    .iter()
                    .map(|stmt| self.plan(stmt))
                    .collect::<Result<Vec<_>>>()?;
                Ok(PhysicalPlan::CreateProcedure {
                    name: name.clone(),
                    args: args.clone(),
                    body,
                    or_replace: *or_replace,
                })
            }

            LogicalPlan::DropProcedure { name, if_exists } => Ok(PhysicalPlan::DropProcedure {
                name: name.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::Call {
                procedure_name,
                args,
            } => Ok(PhysicalPlan::Call {
                procedure_name: procedure_name.clone(),
                args: args.clone(),
            }),

            LogicalPlan::ExportData { options, query } => {
                let query = self.plan(query)?;
                Ok(PhysicalPlan::ExportData {
                    options: options.clone(),
                    query: Box::new(query),
                })
            }

            LogicalPlan::Declare {
                name,
                data_type,
                default,
            } => Ok(PhysicalPlan::Declare {
                name: name.clone(),
                data_type: data_type.clone(),
                default: default.clone(),
            }),

            LogicalPlan::SetVariable { name, value } => Ok(PhysicalPlan::SetVariable {
                name: name.clone(),
                value: value.clone(),
            }),

            LogicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => {
                let then_branch = then_branch
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                let else_branch = else_branch
                    .as_ref()
                    .map(|branch| {
                        branch
                            .iter()
                            .map(|p| self.plan(p))
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                Ok(PhysicalPlan::If {
                    condition: condition.clone(),
                    then_branch,
                    else_branch,
                })
            }

            LogicalPlan::While { condition, body } => {
                let body = body
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(PhysicalPlan::While {
                    condition: condition.clone(),
                    body,
                })
            }

            LogicalPlan::Loop { body, label } => {
                let body = body
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(PhysicalPlan::Loop {
                    body,
                    label: label.clone(),
                })
            }

            LogicalPlan::Repeat {
                body,
                until_condition,
            } => {
                let body = body
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(PhysicalPlan::Repeat {
                    body,
                    until_condition: until_condition.clone(),
                })
            }

            LogicalPlan::For {
                variable,
                query,
                body,
            } => {
                let query = self.plan(query)?;
                let body = body
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(PhysicalPlan::For {
                    variable: variable.clone(),
                    query: Box::new(query),
                    body,
                })
            }

            LogicalPlan::Return { value } => Ok(PhysicalPlan::Return {
                value: value.clone(),
            }),

            LogicalPlan::Raise { message, level } => Ok(PhysicalPlan::Raise {
                message: message.clone(),
                level: *level,
            }),

            LogicalPlan::Break => Ok(PhysicalPlan::Break),

            LogicalPlan::Continue => Ok(PhysicalPlan::Continue),

            LogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => Ok(PhysicalPlan::CreateSnapshot {
                snapshot_name: snapshot_name.clone(),
                source_name: source_name.clone(),
                if_not_exists: *if_not_exists,
            }),

            LogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => Ok(PhysicalPlan::DropSnapshot {
                snapshot_name: snapshot_name.clone(),
                if_exists: *if_exists,
            }),
        }
    }
}

impl Default for PhysicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalPlan {
    pub fn into_logical(self) -> LogicalPlan {
        match self {
            PhysicalPlan::TableScan {
                table_name,
                schema,
                projection,
            } => LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            },
            PhysicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(input.into_logical()),
                predicate,
            },
            PhysicalPlan::Project {
                input,
                expressions,
                schema,
            } => LogicalPlan::Project {
                input: Box::new(input.into_logical()),
                expressions,
                schema,
            },
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
            } => LogicalPlan::Join {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                join_type,
                condition,
                schema,
            },
            PhysicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => LogicalPlan::Join {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                join_type: JoinType::Cross,
                condition: None,
                schema,
            },
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => LogicalPlan::Aggregate {
                input: Box::new(input.into_logical()),
                group_by,
                aggregates,
                schema,
                grouping_sets,
            },
            PhysicalPlan::Sort { input, sort_exprs } => LogicalPlan::Sort {
                input: Box::new(input.into_logical()),
                sort_exprs,
            },
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(input.into_logical()),
                limit,
                offset,
            },
            PhysicalPlan::TopN {
                input,
                sort_exprs,
                limit,
            } => LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Sort {
                    input: Box::new(input.into_logical()),
                    sort_exprs,
                }),
                limit: Some(limit),
                offset: None,
            },
            PhysicalPlan::Distinct { input } => LogicalPlan::Distinct {
                input: Box::new(input.into_logical()),
            },
            PhysicalPlan::Union {
                inputs,
                all,
                schema,
            } => {
                let mut iter = inputs.into_iter();
                let first = iter.next().unwrap().into_logical();
                iter.fold(first, |acc, plan| LogicalPlan::SetOperation {
                    left: Box::new(acc),
                    right: Box::new(plan.into_logical()),
                    op: SetOperationType::Union,
                    all,
                    schema: schema.clone(),
                })
            }
            PhysicalPlan::Intersect {
                left,
                right,
                all,
                schema,
            } => LogicalPlan::SetOperation {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                op: SetOperationType::Intersect,
                all,
                schema,
            },
            PhysicalPlan::Except {
                left,
                right,
                all,
                schema,
            } => LogicalPlan::SetOperation {
                left: Box::new(left.into_logical()),
                right: Box::new(right.into_logical()),
                op: SetOperationType::Except,
                all,
                schema,
            },
            PhysicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => LogicalPlan::Window {
                input: Box::new(input.into_logical()),
                window_exprs,
                schema,
            },
            PhysicalPlan::Unnest {
                input,
                columns,
                schema,
            } => LogicalPlan::Unnest {
                input: Box::new(input.into_logical()),
                columns,
                schema,
            },
            PhysicalPlan::Qualify { input, predicate } => LogicalPlan::Qualify {
                input: Box::new(input.into_logical()),
                predicate,
            },
            PhysicalPlan::WithCte { ctes, body } => LogicalPlan::WithCte {
                ctes,
                body: Box::new(body.into_logical()),
            },
            PhysicalPlan::Values { values, schema } => LogicalPlan::Values { values, schema },
            PhysicalPlan::Empty { schema } => LogicalPlan::Empty { schema },
            PhysicalPlan::Insert {
                table_name,
                columns,
                source,
            } => LogicalPlan::Insert {
                table_name,
                columns,
                source: Box::new(source.into_logical()),
            },
            PhysicalPlan::Update {
                table_name,
                assignments,
                filter,
            } => LogicalPlan::Update {
                table_name,
                assignments,
                filter,
            },
            PhysicalPlan::Delete { table_name, filter } => {
                LogicalPlan::Delete { table_name, filter }
            }
            PhysicalPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => LogicalPlan::Merge {
                target_table,
                source: Box::new(source.into_logical()),
                on,
                clauses,
            },
            PhysicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
            } => LogicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
            },
            PhysicalPlan::DropTable {
                table_names,
                if_exists,
            } => LogicalPlan::DropTable {
                table_names,
                if_exists,
            },
            PhysicalPlan::AlterTable {
                table_name,
                operation,
            } => LogicalPlan::AlterTable {
                table_name,
                operation,
            },
            PhysicalPlan::Truncate { table_name } => LogicalPlan::Truncate { table_name },
            PhysicalPlan::CreateView {
                name,
                query,
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
            } => LogicalPlan::CreateView {
                name,
                query: Box::new(query.into_logical()),
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
            },
            PhysicalPlan::DropView { name, if_exists } => LogicalPlan::DropView { name, if_exists },
            PhysicalPlan::CreateSchema {
                name,
                if_not_exists,
            } => LogicalPlan::CreateSchema {
                name,
                if_not_exists,
            },
            PhysicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => LogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            },
            PhysicalPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_temp,
            } => LogicalPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_temp,
            },
            PhysicalPlan::DropFunction { name, if_exists } => {
                LogicalPlan::DropFunction { name, if_exists }
            }
            PhysicalPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
            } => LogicalPlan::CreateProcedure {
                name,
                args,
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                or_replace,
            },
            PhysicalPlan::DropProcedure { name, if_exists } => {
                LogicalPlan::DropProcedure { name, if_exists }
            }
            PhysicalPlan::Call {
                procedure_name,
                args,
            } => LogicalPlan::Call {
                procedure_name,
                args,
            },
            PhysicalPlan::ExportData { options, query } => LogicalPlan::ExportData {
                options,
                query: Box::new(query.into_logical()),
            },
            PhysicalPlan::Declare {
                name,
                data_type,
                default,
            } => LogicalPlan::Declare {
                name,
                data_type,
                default,
            },
            PhysicalPlan::SetVariable { name, value } => LogicalPlan::SetVariable { name, value },
            PhysicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => LogicalPlan::If {
                condition,
                then_branch: then_branch.into_iter().map(|p| p.into_logical()).collect(),
                else_branch: else_branch.map(|b| b.into_iter().map(|p| p.into_logical()).collect()),
            },
            PhysicalPlan::While { condition, body } => LogicalPlan::While {
                condition,
                body: body.into_iter().map(|p| p.into_logical()).collect(),
            },
            PhysicalPlan::Loop { body, label } => LogicalPlan::Loop {
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                label,
            },
            PhysicalPlan::Repeat {
                body,
                until_condition,
            } => LogicalPlan::Repeat {
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                until_condition,
            },
            PhysicalPlan::For {
                variable,
                query,
                body,
            } => LogicalPlan::For {
                variable,
                query: Box::new(query.into_logical()),
                body: body.into_iter().map(|p| p.into_logical()).collect(),
            },
            PhysicalPlan::Return { value } => LogicalPlan::Return { value },
            PhysicalPlan::Raise { message, level } => LogicalPlan::Raise { message, level },
            PhysicalPlan::Break => LogicalPlan::Break,
            PhysicalPlan::Continue => LogicalPlan::Continue,
            PhysicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => LogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            },
            PhysicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => LogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            },
        }
    }
}
