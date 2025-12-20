mod aggregate;
mod cte;
mod ddl;
mod distinct;
mod dml;
mod filter;
mod join;
mod limit;
mod project;
mod qualify;
mod scan;
mod scripting;
mod set_ops;
mod sort;
mod unnest;
mod values;
mod window;

use std::collections::HashMap;

pub use aggregate::*;
pub use cte::*;
pub use ddl::*;
pub use distinct::*;
pub use dml::*;
pub use filter::*;
pub use join::*;
pub use limit::*;
pub use project::*;
pub use scan::*;
pub use scripting::*;
pub use set_ops::*;
pub use sort::*;
pub use unnest::*;
pub use values::*;
pub use window::*;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::PhysicalPlan;
use yachtsql_storage::{Field, FieldMode, Schema, Table};

use crate::catalog::Catalog;
use crate::ir_evaluator::UserFunctionDef;
use crate::plan::ExecutorPlan;
use crate::session::Session;

pub struct PlanExecutor<'a> {
    catalog: &'a mut Catalog,
    session: &'a mut Session,
    variables: HashMap<String, Value>,
    cte_results: HashMap<String, Table>,
    user_function_defs: HashMap<String, UserFunctionDef>,
}

impl<'a> PlanExecutor<'a> {
    pub fn new(catalog: &'a mut Catalog, session: &'a mut Session) -> Self {
        let user_function_defs = catalog
            .get_functions()
            .iter()
            .map(|(name, func)| {
                (
                    name.clone(),
                    UserFunctionDef {
                        parameters: func.parameters.clone(),
                        body: func.body.clone(),
                    },
                )
            })
            .collect();

        Self {
            catalog,
            session,
            variables: HashMap::new(),
            cte_results: HashMap::new(),
            user_function_defs,
        }
    }

    fn refresh_user_functions(&mut self) {
        self.user_function_defs = self
            .catalog
            .get_functions()
            .iter()
            .map(|(name, func)| {
                (
                    name.clone(),
                    UserFunctionDef {
                        parameters: func.parameters.clone(),
                        body: func.body.clone(),
                    },
                )
            })
            .collect();
    }

    pub fn execute(&mut self, plan: &PhysicalPlan) -> Result<Table> {
        let executor_plan = ExecutorPlan::from_physical(plan);
        self.execute_plan(&executor_plan)
    }

    pub fn execute_plan(&mut self, plan: &ExecutorPlan) -> Result<Table> {
        match plan {
            ExecutorPlan::TableScan {
                table_name, schema, ..
            } => self.execute_scan(table_name, schema),
            ExecutorPlan::Filter { input, predicate } => self.execute_filter(input, predicate),
            ExecutorPlan::Project {
                input,
                expressions,
                schema,
            } => self.execute_project(input, expressions, schema),
            ExecutorPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
            } => self.execute_nested_loop_join(left, right, join_type, condition.as_ref(), schema),
            ExecutorPlan::CrossJoin {
                left,
                right,
                schema,
            } => self.execute_cross_join(left, right, schema),
            ExecutorPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => {
                self.execute_aggregate(input, group_by, aggregates, schema, grouping_sets.as_ref())
            }
            ExecutorPlan::Sort { input, sort_exprs } => self.execute_sort(input, sort_exprs),
            ExecutorPlan::Limit {
                input,
                limit,
                offset,
            } => self.execute_limit(input, *limit, *offset),
            ExecutorPlan::TopN {
                input,
                sort_exprs,
                limit,
            } => self.execute_topn(input, sort_exprs, *limit),
            ExecutorPlan::Distinct { input } => self.execute_distinct(input),
            ExecutorPlan::Union {
                inputs,
                all,
                schema,
            } => self.execute_union(inputs, *all, schema),
            ExecutorPlan::Intersect {
                left,
                right,
                all,
                schema,
            } => self.execute_intersect(left, right, *all, schema),
            ExecutorPlan::Except {
                left,
                right,
                all,
                schema,
            } => self.execute_except(left, right, *all, schema),
            ExecutorPlan::Window {
                input,
                window_exprs,
                schema,
            } => self.execute_window(input, window_exprs, schema),
            ExecutorPlan::WithCte { ctes, body } => self.execute_cte(ctes, body),
            ExecutorPlan::Unnest {
                input,
                columns,
                schema,
            } => self.execute_unnest(input, columns, schema),
            ExecutorPlan::Qualify { input, predicate } => self.execute_qualify(input, predicate),
            ExecutorPlan::Values { values, schema } => self.execute_values(values, schema),
            ExecutorPlan::Empty { schema } => {
                let result_schema = plan_schema_to_schema(schema);
                let mut table = Table::empty(result_schema.clone());
                if result_schema.field_count() == 0 {
                    table.push_row(vec![])?;
                }
                Ok(table)
            }
            ExecutorPlan::Insert {
                table_name,
                columns,
                source,
            } => self.execute_insert(table_name, columns, source),
            ExecutorPlan::Update {
                table_name,
                assignments,
                filter,
            } => self.execute_update(table_name, assignments, filter.as_ref()),
            ExecutorPlan::Delete { table_name, filter } => {
                self.execute_delete(table_name, filter.as_ref())
            }
            ExecutorPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => self.execute_merge(target_table, source, on, clauses),
            ExecutorPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
            } => self.execute_create_table(table_name, columns, *if_not_exists, *or_replace),
            ExecutorPlan::DropTable {
                table_names,
                if_exists,
            } => self.execute_drop_tables(table_names, *if_exists),
            ExecutorPlan::AlterTable {
                table_name,
                operation,
            } => self.execute_alter_table(table_name, operation),
            ExecutorPlan::Truncate { table_name } => self.execute_truncate(table_name),
            ExecutorPlan::CreateView {
                name,
                query: _,
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
            } => self.execute_create_view(
                name,
                query_sql,
                column_aliases,
                *or_replace,
                *if_not_exists,
            ),
            ExecutorPlan::DropView { name, if_exists } => self.execute_drop_view(name, *if_exists),
            ExecutorPlan::CreateSchema {
                name,
                if_not_exists,
            } => self.execute_create_schema(name, *if_not_exists),
            ExecutorPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => self.execute_drop_schema(name, *if_exists, *cascade),
            ExecutorPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_temp,
            } => self.execute_create_function(
                name,
                args,
                return_type,
                body,
                *or_replace,
                *if_not_exists,
                *is_temp,
            ),
            ExecutorPlan::DropFunction { name, if_exists } => {
                self.execute_drop_function(name, *if_exists)
            }
            ExecutorPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
            } => self.execute_create_procedure(name, args, body, *or_replace),
            ExecutorPlan::DropProcedure { name, if_exists } => {
                self.execute_drop_procedure(name, *if_exists)
            }
            ExecutorPlan::Call {
                procedure_name,
                args,
            } => self.execute_call(procedure_name, args),
            ExecutorPlan::ExportData { options, query } => self.execute_export(options, query),
            ExecutorPlan::Declare {
                name,
                data_type,
                default,
            } => self.execute_declare(name, data_type, default.as_ref()),
            ExecutorPlan::SetVariable { name, value } => self.execute_set_variable(name, value),
            ExecutorPlan::If {
                condition,
                then_branch,
                else_branch,
            } => self.execute_if(condition, then_branch, else_branch.as_deref()),
            ExecutorPlan::While { condition, body } => self.execute_while(condition, body),
            ExecutorPlan::Loop { body, label } => self.execute_loop(body, label.as_deref()),
            ExecutorPlan::Repeat {
                body,
                until_condition,
            } => self.execute_repeat(body, until_condition),
            ExecutorPlan::For {
                variable,
                query,
                body,
            } => self.execute_for(variable, query, body),
            ExecutorPlan::Return { value } => {
                Err(Error::InvalidQuery("RETURN outside of function".into()))
            }
            ExecutorPlan::Raise { message, level } => self.execute_raise(message.as_ref(), *level),
            ExecutorPlan::Break => Err(Error::InvalidQuery("BREAK outside of loop".into())),
            ExecutorPlan::Continue => Err(Error::InvalidQuery("CONTINUE outside of loop".into())),
            ExecutorPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => self.execute_create_snapshot(snapshot_name, source_name, *if_not_exists),
            ExecutorPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => self.execute_drop_snapshot(snapshot_name, *if_exists),
        }
    }
}

pub fn plan_schema_to_schema(plan_schema: &yachtsql_ir::PlanSchema) -> Schema {
    let mut schema = Schema::new();
    let mut name_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for field in &plan_schema.fields {
        let mode = if field.nullable {
            FieldMode::Nullable
        } else {
            FieldMode::Required
        };

        let base_name = &field.name;
        let count = name_counts.entry(base_name.clone()).or_insert(0);
        let storage_name = if *count > 0 {
            format!("{}_{}", base_name, count)
        } else {
            base_name.clone()
        };
        *count += 1;

        let mut storage_field = Field::new(&storage_name, field.data_type.clone(), mode);
        if let Some(ref table) = field.table {
            storage_field = storage_field.with_source_table(table.clone());
        }
        schema.add_field(storage_field);
    }
    schema
}
