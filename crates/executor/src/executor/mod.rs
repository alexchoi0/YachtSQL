mod aggregate;
#[cfg(feature = "concurrent")]
pub mod concurrent;
mod cte;
mod ddl;
mod distinct;
mod dml;
mod filter;
mod join;
mod limit;
mod project;
mod qualify;
mod sample;
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
use yachtsql_ir::Expr;
use yachtsql_optimizer::OptimizedLogicalPlan;
use yachtsql_storage::{Field, FieldMode, Schema, Table};

use crate::catalog::Catalog;
use crate::ir_evaluator::{IrEvaluator, UserFunctionDef};
use crate::plan::PhysicalPlan;
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

        let variables = session.variables().clone();

        Self {
            catalog,
            session,
            variables,
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

    pub fn execute(&mut self, plan: &OptimizedLogicalPlan) -> Result<Table> {
        let executor_plan = PhysicalPlan::from_physical(plan);
        self.execute_plan(&executor_plan)
    }

    pub fn execute_plan(&mut self, plan: &PhysicalPlan) -> Result<Table> {
        match plan {
            PhysicalPlan::TableScan {
                table_name, schema, ..
            } => self.execute_scan(table_name, schema),
            PhysicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => self.execute_sample(input, sample_type, *sample_value),
            PhysicalPlan::Filter { input, predicate } => self.execute_filter(input, predicate),
            PhysicalPlan::Project {
                input,
                expressions,
                schema,
            } => self.execute_project(input, expressions, schema),
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
            } => self.execute_nested_loop_join(left, right, join_type, condition.as_ref(), schema),
            PhysicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => self.execute_cross_join(left, right, schema),
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => {
                self.execute_aggregate(input, group_by, aggregates, schema, grouping_sets.as_ref())
            }
            PhysicalPlan::Sort { input, sort_exprs } => self.execute_sort(input, sort_exprs),
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => self.execute_limit(input, *limit, *offset),
            PhysicalPlan::TopN {
                input,
                sort_exprs,
                limit,
            } => self.execute_topn(input, sort_exprs, *limit),
            PhysicalPlan::Distinct { input } => self.execute_distinct(input),
            PhysicalPlan::Union {
                inputs,
                all,
                schema,
            } => self.execute_union(inputs, *all, schema),
            PhysicalPlan::Intersect {
                left,
                right,
                all,
                schema,
            } => self.execute_intersect(left, right, *all, schema),
            PhysicalPlan::Except {
                left,
                right,
                all,
                schema,
            } => self.execute_except(left, right, *all, schema),
            PhysicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => self.execute_window(input, window_exprs, schema),
            PhysicalPlan::WithCte { ctes, body } => self.execute_cte(ctes, body),
            PhysicalPlan::Unnest {
                input,
                columns,
                schema,
            } => self.execute_unnest(input, columns, schema),
            PhysicalPlan::Qualify { input, predicate } => self.execute_qualify(input, predicate),
            PhysicalPlan::Values { values, schema } => self.execute_values(values, schema),
            PhysicalPlan::Empty { schema } => {
                let result_schema = plan_schema_to_schema(schema);
                let mut table = Table::empty(result_schema.clone());
                if result_schema.field_count() == 0 {
                    table.push_row(vec![])?;
                }
                Ok(table)
            }
            PhysicalPlan::Insert {
                table_name,
                columns,
                source,
            } => self.execute_insert(table_name, columns, source),
            PhysicalPlan::Update {
                table_name,
                assignments,
                from,
                filter,
                ..
            } => self.execute_update(table_name, assignments, from.as_deref(), filter.as_ref()),
            PhysicalPlan::Delete { table_name, filter } => {
                self.execute_delete(table_name, filter.as_ref())
            }
            PhysicalPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => self.execute_merge(target_table, source, on, clauses),
            PhysicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
                query,
            } => self.execute_create_table(
                table_name,
                columns,
                *if_not_exists,
                *or_replace,
                query.as_deref(),
            ),
            PhysicalPlan::DropTable {
                table_names,
                if_exists,
            } => self.execute_drop_tables(table_names, *if_exists),
            PhysicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            } => self.execute_alter_table(table_name, operation, *if_exists),
            PhysicalPlan::Truncate { table_name } => self.execute_truncate(table_name),
            PhysicalPlan::CreateView {
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
            PhysicalPlan::DropView { name, if_exists } => self.execute_drop_view(name, *if_exists),
            PhysicalPlan::CreateSchema {
                name,
                if_not_exists,
            } => self.execute_create_schema(name, *if_not_exists),
            PhysicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => self.execute_drop_schema(name, *if_exists, *cascade),
            PhysicalPlan::AlterSchema { name, options } => self.execute_alter_schema(name, options),
            PhysicalPlan::CreateFunction {
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
            PhysicalPlan::DropFunction { name, if_exists } => {
                self.execute_drop_function(name, *if_exists)
            }
            PhysicalPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
            } => self.execute_create_procedure(name, args, body, *or_replace),
            PhysicalPlan::DropProcedure { name, if_exists } => {
                self.execute_drop_procedure(name, *if_exists)
            }
            PhysicalPlan::Call {
                procedure_name,
                args,
            } => self.execute_call(procedure_name, args),
            PhysicalPlan::ExportData { options, query } => self.execute_export(options, query),
            PhysicalPlan::LoadData {
                table_name,
                options,
                temp_table,
                temp_schema,
            } => self.execute_load(table_name, options, *temp_table, temp_schema.as_ref()),
            PhysicalPlan::Declare {
                name,
                data_type,
                default,
            } => self.execute_declare(name, data_type, default.as_ref()),
            PhysicalPlan::SetVariable { name, value } => self.execute_set_variable(name, value),
            PhysicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => self.execute_if(condition, then_branch, else_branch.as_deref()),
            PhysicalPlan::While { condition, body } => self.execute_while(condition, body),
            PhysicalPlan::Loop { body, label } => self.execute_loop(body, label.as_deref()),
            PhysicalPlan::Repeat {
                body,
                until_condition,
            } => self.execute_repeat(body, until_condition),
            PhysicalPlan::For {
                variable,
                query,
                body,
            } => self.execute_for(variable, query, body),
            PhysicalPlan::Return { value } => {
                Err(Error::InvalidQuery("RETURN outside of function".into()))
            }
            PhysicalPlan::Raise { message, level } => self.execute_raise(message.as_ref(), *level),
            PhysicalPlan::Break => Err(Error::InvalidQuery("BREAK outside of loop".into())),
            PhysicalPlan::Continue => Err(Error::InvalidQuery("CONTINUE outside of loop".into())),
            PhysicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => self.execute_create_snapshot(snapshot_name, source_name, *if_not_exists),
            PhysicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => self.execute_drop_snapshot(snapshot_name, *if_exists),
            PhysicalPlan::Assert { condition, message } => {
                self.execute_assert(condition, message.as_ref())
            }
            PhysicalPlan::Grant {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => self.execute_grant(roles, resource_type, resource_name, grantees),
            PhysicalPlan::Revoke {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => self.execute_revoke(roles, resource_type, resource_name, grantees),
        }
    }

    fn execute_assert(&mut self, condition: &Expr, message: Option<&Expr>) -> Result<Table> {
        use yachtsql_storage::Record;

        let empty_schema = Schema::new();
        let evaluator = IrEvaluator::new(&empty_schema)
            .with_variables(&self.variables)
            .with_user_functions(&self.user_function_defs);
        let empty_record = Record::new();
        let result = evaluator.evaluate(condition, &empty_record)?;
        match result {
            Value::Bool(true) => Ok(Table::empty(Schema::new())),
            Value::Bool(false) => {
                let msg = if let Some(msg_expr) = message {
                    let msg_val = evaluator.evaluate(msg_expr, &empty_record)?;
                    match msg_val {
                        Value::String(s) => s,
                        _ => format!("{:?}", msg_val),
                    }
                } else {
                    "Assertion failed".to_string()
                };
                Err(Error::InvalidQuery(format!("ASSERT failed: {}", msg)))
            }
            _ => Err(Error::InvalidQuery(
                "ASSERT condition must evaluate to a boolean".into(),
            )),
        }
    }

    fn execute_grant(
        &mut self,
        _roles: &[String],
        _resource_type: &yachtsql_ir::DclResourceType,
        _resource_name: &str,
        _grantees: &[String],
    ) -> Result<Table> {
        Ok(Table::empty(Schema::new()))
    }

    fn execute_revoke(
        &mut self,
        _roles: &[String],
        _resource_type: &yachtsql_ir::DclResourceType,
        _resource_name: &str,
        _grantees: &[String],
    ) -> Result<Table> {
        Ok(Table::empty(Schema::new()))
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
