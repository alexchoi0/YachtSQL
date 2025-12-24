use yachtsql_common::error::Result;
use yachtsql_ir::{JoinType, LogicalPlan, SetOperationType};

use crate::optimized_logical_plan::{OptimizedLogicalPlan, SampleType};

pub struct PhysicalPlanner;

impl PhysicalPlanner {
    pub fn new() -> Self {
        Self
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn plan(&self, logical: &LogicalPlan) -> Result<OptimizedLogicalPlan> {
        match logical {
            LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            } => Ok(OptimizedLogicalPlan::TableScan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: projection.clone(),
            }),

            LogicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => {
                let input = self.plan(input)?;
                let phys_sample_type = match sample_type {
                    yachtsql_ir::SampleType::Rows => SampleType::Rows,
                    yachtsql_ir::SampleType::Percent => SampleType::Percent,
                };
                Ok(OptimizedLogicalPlan::Sample {
                    input: Box::new(input),
                    sample_type: phys_sample_type,
                    sample_value: *sample_value,
                })
            }

            LogicalPlan::Filter { input, predicate } => {
                let input = self.plan(input)?;
                Ok(OptimizedLogicalPlan::Filter {
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
                Ok(OptimizedLogicalPlan::Project {
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
                Ok(OptimizedLogicalPlan::HashAggregate {
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
                    JoinType::Cross => Ok(OptimizedLogicalPlan::CrossJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        schema: schema.clone(),
                    }),
                    _ => Ok(OptimizedLogicalPlan::NestedLoopJoin {
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
                Ok(OptimizedLogicalPlan::Sort {
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
                Ok(OptimizedLogicalPlan::Limit {
                    input: Box::new(input),
                    limit: *limit,
                    offset: *offset,
                })
            }

            LogicalPlan::Distinct { input } => {
                let input = self.plan(input)?;
                Ok(OptimizedLogicalPlan::Distinct {
                    input: Box::new(input),
                })
            }

            LogicalPlan::Values { values, schema } => Ok(OptimizedLogicalPlan::Values {
                values: values.clone(),
                schema: schema.clone(),
            }),

            LogicalPlan::Empty { schema } => Ok(OptimizedLogicalPlan::Empty {
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
                    SetOperationType::Union => Ok(OptimizedLogicalPlan::Union {
                        inputs: vec![left, right],
                        all: *all,
                        schema: schema.clone(),
                    }),
                    SetOperationType::Intersect => Ok(OptimizedLogicalPlan::Intersect {
                        left: Box::new(left),
                        right: Box::new(right),
                        all: *all,
                        schema: schema.clone(),
                    }),
                    SetOperationType::Except => Ok(OptimizedLogicalPlan::Except {
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
                Ok(OptimizedLogicalPlan::Window {
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
                Ok(OptimizedLogicalPlan::WithCte {
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
                Ok(OptimizedLogicalPlan::Unnest {
                    input: Box::new(input),
                    columns: columns.clone(),
                    schema: schema.clone(),
                })
            }

            LogicalPlan::Qualify { input, predicate } => {
                let input = self.plan(input)?;
                Ok(OptimizedLogicalPlan::Qualify {
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
                Ok(OptimizedLogicalPlan::Insert {
                    table_name: table_name.clone(),
                    columns: columns.clone(),
                    source: Box::new(source),
                })
            }

            LogicalPlan::Update {
                table_name,
                alias,
                assignments,
                from,
                filter,
            } => {
                let from_plan = match from {
                    Some(plan) => Some(Box::new(self.plan(plan)?)),
                    None => None,
                };
                Ok(OptimizedLogicalPlan::Update {
                    table_name: table_name.clone(),
                    alias: alias.clone(),
                    assignments: assignments.clone(),
                    from: from_plan,
                    filter: filter.clone(),
                })
            }

            LogicalPlan::Delete {
                table_name, filter, ..
            } => Ok(OptimizedLogicalPlan::Delete {
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
                Ok(OptimizedLogicalPlan::Merge {
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
                query,
            } => {
                let optimized_query = if let Some(q) = query {
                    Some(Box::new(self.plan(q)?))
                } else {
                    None
                };
                Ok(OptimizedLogicalPlan::CreateTable {
                    table_name: table_name.clone(),
                    columns: columns.clone(),
                    if_not_exists: *if_not_exists,
                    or_replace: *or_replace,
                    query: optimized_query,
                })
            }

            LogicalPlan::DropTable {
                table_names,
                if_exists,
            } => Ok(OptimizedLogicalPlan::DropTable {
                table_names: table_names.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            } => Ok(OptimizedLogicalPlan::AlterTable {
                table_name: table_name.clone(),
                operation: operation.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::Truncate { table_name } => Ok(OptimizedLogicalPlan::Truncate {
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
                Ok(OptimizedLogicalPlan::CreateView {
                    name: name.clone(),
                    query: Box::new(query),
                    query_sql: query_sql.clone(),
                    column_aliases: column_aliases.clone(),
                    or_replace: *or_replace,
                    if_not_exists: *if_not_exists,
                })
            }

            LogicalPlan::DropView { name, if_exists } => Ok(OptimizedLogicalPlan::DropView {
                name: name.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::CreateSchema {
                name,
                if_not_exists,
            } => Ok(OptimizedLogicalPlan::CreateSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
            }),

            LogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => Ok(OptimizedLogicalPlan::DropSchema {
                name: name.clone(),
                if_exists: *if_exists,
                cascade: *cascade,
            }),

            LogicalPlan::AlterSchema { name, options } => Ok(OptimizedLogicalPlan::AlterSchema {
                name: name.clone(),
                options: options.clone(),
            }),

            LogicalPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_temp,
            } => Ok(OptimizedLogicalPlan::CreateFunction {
                name: name.clone(),
                args: args.clone(),
                return_type: return_type.clone(),
                body: body.clone(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
                is_temp: *is_temp,
            }),

            LogicalPlan::DropFunction { name, if_exists } => {
                Ok(OptimizedLogicalPlan::DropFunction {
                    name: name.clone(),
                    if_exists: *if_exists,
                })
            }

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
                Ok(OptimizedLogicalPlan::CreateProcedure {
                    name: name.clone(),
                    args: args.clone(),
                    body,
                    or_replace: *or_replace,
                })
            }

            LogicalPlan::DropProcedure { name, if_exists } => {
                Ok(OptimizedLogicalPlan::DropProcedure {
                    name: name.clone(),
                    if_exists: *if_exists,
                })
            }

            LogicalPlan::Call {
                procedure_name,
                args,
            } => Ok(OptimizedLogicalPlan::Call {
                procedure_name: procedure_name.clone(),
                args: args.clone(),
            }),

            LogicalPlan::ExportData { options, query } => {
                let query = self.plan(query)?;
                Ok(OptimizedLogicalPlan::ExportData {
                    options: options.clone(),
                    query: Box::new(query),
                })
            }

            LogicalPlan::LoadData {
                table_name,
                options,
                temp_table,
                temp_schema,
            } => Ok(OptimizedLogicalPlan::LoadData {
                table_name: table_name.clone(),
                options: options.clone(),
                temp_table: *temp_table,
                temp_schema: temp_schema.clone(),
            }),

            LogicalPlan::Declare {
                name,
                data_type,
                default,
            } => Ok(OptimizedLogicalPlan::Declare {
                name: name.clone(),
                data_type: data_type.clone(),
                default: default.clone(),
            }),

            LogicalPlan::SetVariable { name, value } => Ok(OptimizedLogicalPlan::SetVariable {
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
                Ok(OptimizedLogicalPlan::If {
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
                Ok(OptimizedLogicalPlan::While {
                    condition: condition.clone(),
                    body,
                })
            }

            LogicalPlan::Loop { body, label } => {
                let body = body
                    .iter()
                    .map(|p| self.plan(p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(OptimizedLogicalPlan::Loop {
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
                Ok(OptimizedLogicalPlan::Repeat {
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
                Ok(OptimizedLogicalPlan::For {
                    variable: variable.clone(),
                    query: Box::new(query),
                    body,
                })
            }

            LogicalPlan::Return { value } => Ok(OptimizedLogicalPlan::Return {
                value: value.clone(),
            }),

            LogicalPlan::Raise { message, level } => Ok(OptimizedLogicalPlan::Raise {
                message: message.clone(),
                level: *level,
            }),

            LogicalPlan::Break => Ok(OptimizedLogicalPlan::Break),

            LogicalPlan::Continue => Ok(OptimizedLogicalPlan::Continue),

            LogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => Ok(OptimizedLogicalPlan::CreateSnapshot {
                snapshot_name: snapshot_name.clone(),
                source_name: source_name.clone(),
                if_not_exists: *if_not_exists,
            }),

            LogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => Ok(OptimizedLogicalPlan::DropSnapshot {
                snapshot_name: snapshot_name.clone(),
                if_exists: *if_exists,
            }),

            LogicalPlan::Assert { condition, message } => Ok(OptimizedLogicalPlan::Assert {
                condition: condition.clone(),
                message: message.clone(),
            }),

            LogicalPlan::Grant {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => Ok(OptimizedLogicalPlan::Grant {
                roles: roles.clone(),
                resource_type: resource_type.clone(),
                resource_name: resource_name.clone(),
                grantees: grantees.clone(),
            }),

            LogicalPlan::Revoke {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => Ok(OptimizedLogicalPlan::Revoke {
                roles: roles.clone(),
                resource_type: resource_type.clone(),
                resource_name: resource_name.clone(),
                grantees: grantees.clone(),
            }),
        }
    }
}

impl Default for PhysicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizedLogicalPlan {
    pub fn into_logical(self) -> LogicalPlan {
        match self {
            OptimizedLogicalPlan::TableScan {
                table_name,
                schema,
                projection,
            } => LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            },
            OptimizedLogicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => {
                let logical_sample_type = match sample_type {
                    SampleType::Rows => yachtsql_ir::SampleType::Rows,
                    SampleType::Percent => yachtsql_ir::SampleType::Percent,
                };
                LogicalPlan::Sample {
                    input: Box::new(input.into_logical()),
                    sample_type: logical_sample_type,
                    sample_value,
                }
            }
            OptimizedLogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(input.into_logical()),
                predicate,
            },
            OptimizedLogicalPlan::Project {
                input,
                expressions,
                schema,
            } => LogicalPlan::Project {
                input: Box::new(input.into_logical()),
                expressions,
                schema,
            },
            OptimizedLogicalPlan::NestedLoopJoin {
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
            OptimizedLogicalPlan::CrossJoin {
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
            OptimizedLogicalPlan::HashAggregate {
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
            OptimizedLogicalPlan::Sort { input, sort_exprs } => LogicalPlan::Sort {
                input: Box::new(input.into_logical()),
                sort_exprs,
            },
            OptimizedLogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(input.into_logical()),
                limit,
                offset,
            },
            OptimizedLogicalPlan::TopN {
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
            OptimizedLogicalPlan::Distinct { input } => LogicalPlan::Distinct {
                input: Box::new(input.into_logical()),
            },
            OptimizedLogicalPlan::Union {
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
            OptimizedLogicalPlan::Intersect {
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
            OptimizedLogicalPlan::Except {
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
            OptimizedLogicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => LogicalPlan::Window {
                input: Box::new(input.into_logical()),
                window_exprs,
                schema,
            },
            OptimizedLogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => LogicalPlan::Unnest {
                input: Box::new(input.into_logical()),
                columns,
                schema,
            },
            OptimizedLogicalPlan::Qualify { input, predicate } => LogicalPlan::Qualify {
                input: Box::new(input.into_logical()),
                predicate,
            },
            OptimizedLogicalPlan::WithCte { ctes, body } => LogicalPlan::WithCte {
                ctes,
                body: Box::new(body.into_logical()),
            },
            OptimizedLogicalPlan::Values { values, schema } => {
                LogicalPlan::Values { values, schema }
            }
            OptimizedLogicalPlan::Empty { schema } => LogicalPlan::Empty { schema },
            OptimizedLogicalPlan::Insert {
                table_name,
                columns,
                source,
            } => LogicalPlan::Insert {
                table_name,
                columns,
                source: Box::new(source.into_logical()),
            },
            OptimizedLogicalPlan::Update {
                table_name,
                alias,
                assignments,
                from,
                filter,
            } => LogicalPlan::Update {
                table_name,
                alias,
                assignments,
                from: from.map(|p| Box::new(p.into_logical())),
                filter,
            },
            OptimizedLogicalPlan::Delete { table_name, filter } => LogicalPlan::Delete {
                table_name,
                alias: None,
                filter,
            },
            OptimizedLogicalPlan::Merge {
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
            OptimizedLogicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
                query,
            } => LogicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
                query: query.map(|q| Box::new(q.into_logical())),
            },
            OptimizedLogicalPlan::DropTable {
                table_names,
                if_exists,
            } => LogicalPlan::DropTable {
                table_names,
                if_exists,
            },
            OptimizedLogicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            } => LogicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            },
            OptimizedLogicalPlan::Truncate { table_name } => LogicalPlan::Truncate { table_name },
            OptimizedLogicalPlan::CreateView {
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
            OptimizedLogicalPlan::DropView { name, if_exists } => {
                LogicalPlan::DropView { name, if_exists }
            }
            OptimizedLogicalPlan::CreateSchema {
                name,
                if_not_exists,
            } => LogicalPlan::CreateSchema {
                name,
                if_not_exists,
            },
            OptimizedLogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => LogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            },
            OptimizedLogicalPlan::AlterSchema { name, options } => {
                LogicalPlan::AlterSchema { name, options }
            }
            OptimizedLogicalPlan::CreateFunction {
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
            OptimizedLogicalPlan::DropFunction { name, if_exists } => {
                LogicalPlan::DropFunction { name, if_exists }
            }
            OptimizedLogicalPlan::CreateProcedure {
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
            OptimizedLogicalPlan::DropProcedure { name, if_exists } => {
                LogicalPlan::DropProcedure { name, if_exists }
            }
            OptimizedLogicalPlan::Call {
                procedure_name,
                args,
            } => LogicalPlan::Call {
                procedure_name,
                args,
            },
            OptimizedLogicalPlan::ExportData { options, query } => LogicalPlan::ExportData {
                options,
                query: Box::new(query.into_logical()),
            },
            OptimizedLogicalPlan::LoadData {
                table_name,
                options,
                temp_table,
                temp_schema,
            } => LogicalPlan::LoadData {
                table_name,
                options,
                temp_table,
                temp_schema,
            },
            OptimizedLogicalPlan::Declare {
                name,
                data_type,
                default,
            } => LogicalPlan::Declare {
                name,
                data_type,
                default,
            },
            OptimizedLogicalPlan::SetVariable { name, value } => {
                LogicalPlan::SetVariable { name, value }
            }
            OptimizedLogicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => LogicalPlan::If {
                condition,
                then_branch: then_branch.into_iter().map(|p| p.into_logical()).collect(),
                else_branch: else_branch.map(|b| b.into_iter().map(|p| p.into_logical()).collect()),
            },
            OptimizedLogicalPlan::While { condition, body } => LogicalPlan::While {
                condition,
                body: body.into_iter().map(|p| p.into_logical()).collect(),
            },
            OptimizedLogicalPlan::Loop { body, label } => LogicalPlan::Loop {
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                label,
            },
            OptimizedLogicalPlan::Repeat {
                body,
                until_condition,
            } => LogicalPlan::Repeat {
                body: body.into_iter().map(|p| p.into_logical()).collect(),
                until_condition,
            },
            OptimizedLogicalPlan::For {
                variable,
                query,
                body,
            } => LogicalPlan::For {
                variable,
                query: Box::new(query.into_logical()),
                body: body.into_iter().map(|p| p.into_logical()).collect(),
            },
            OptimizedLogicalPlan::Return { value } => LogicalPlan::Return { value },
            OptimizedLogicalPlan::Raise { message, level } => LogicalPlan::Raise { message, level },
            OptimizedLogicalPlan::Break => LogicalPlan::Break,
            OptimizedLogicalPlan::Continue => LogicalPlan::Continue,
            OptimizedLogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => LogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            },
            OptimizedLogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => LogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            },
            OptimizedLogicalPlan::Assert { condition, message } => {
                LogicalPlan::Assert { condition, message }
            }
            OptimizedLogicalPlan::Grant {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => LogicalPlan::Grant {
                roles,
                resource_type,
                resource_name,
                grantees,
            },
            OptimizedLogicalPlan::Revoke {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => LogicalPlan::Revoke {
                roles,
                resource_type,
                resource_name,
                grantees,
            },
        }
    }
}
