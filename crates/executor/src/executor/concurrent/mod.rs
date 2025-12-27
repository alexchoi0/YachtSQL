mod cte;
mod ddl;
mod dml;
mod dql;
mod gap_fill;
mod io;
mod join;
mod scripting;
mod set_ops;
mod unnest;
mod utils;

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use arrow::array::Array;
use async_recursion::async_recursion;
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc};
use futures::future::{join, join_all};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{
    AlterTableOp, Assignment, BinaryOp, ColumnDef, CteDefinition, ExportFormat, ExportOptions,
    Expr, FunctionArg, FunctionBody, GapFillColumn, GapFillStrategy, JoinType, LoadFormat,
    LoadOptions, LogicalPlan, MergeClause, PlanSchema, ProcedureArg, RaiseLevel, SetOperationType,
    SortExpr, UnnestColumn, WindowFrame,
};
use yachtsql_optimizer::{OptimizedLogicalPlan, SampleType, optimize};
use yachtsql_storage::{Field, FieldMode, Record, Schema, Table};

use super::window::{WindowFuncType, compute_window_function, partition_rows, sort_partition};
use crate::catalog::{ColumnDefault, UserFunction, UserProcedure};
use crate::concurrent_catalog::{ConcurrentCatalog, TableLockSet};
use crate::concurrent_session::ConcurrentSession;
use crate::executor::plan_schema_to_schema;
use crate::ir_evaluator::{IrEvaluator, UserFunctionDef};
use crate::plan::PhysicalPlan;

fn coerce_value(value: Value, target_type: &DataType) -> Result<Value> {
    match (&value, target_type) {
        (Value::String(s), DataType::Date) => {
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| Error::InvalidQuery(format!("Invalid date string: {}", e)))?;
            Ok(Value::Date(date))
        }
        (Value::String(s), DataType::Time) => {
            let time = NaiveTime::parse_from_str(s, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid time string: {}", e)))?;
            Ok(Value::Time(time))
        }
        (Value::String(s), DataType::Timestamp) => {
            let dt = DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&Utc))
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                        })
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                        })
                        .map(|ndt| ndt.and_utc())
                })
                .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp string: {}", e)))?;
            Ok(Value::Timestamp(dt))
        }
        (Value::String(s), DataType::DateTime) => {
            let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid datetime string: {}", e)))?;
            Ok(Value::DateTime(dt))
        }
        (Value::Int64(n), DataType::Float64) => {
            Ok(Value::Float64(ordered_float::OrderedFloat(*n as f64)))
        }
        (Value::Float64(f), DataType::Int64) => Ok(Value::Int64(f.0 as i64)),
        (Value::Struct(fields), DataType::Struct(target_fields)) => {
            let mut coerced_fields = Vec::with_capacity(fields.len());
            for (i, (_, val)) in fields.iter().enumerate() {
                let (new_name, new_type) = if i < target_fields.len() {
                    (target_fields[i].name.clone(), &target_fields[i].data_type)
                } else {
                    (format!("_field{}", i), &DataType::Unknown)
                };
                let coerced_val = coerce_value(val.clone(), new_type)?;
                coerced_fields.push((new_name, coerced_val));
            }
            Ok(Value::Struct(coerced_fields))
        }
        (Value::Array(elements), DataType::Array(element_type)) => {
            let coerced_elements: Result<Vec<_>> = elements
                .iter()
                .map(|elem| coerce_value(elem.clone(), element_type))
                .collect();
            Ok(Value::Array(coerced_elements?))
        }
        _ => Ok(value),
    }
}

fn compare_values_for_sort(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater,
        (_, Value::Null) => std::cmp::Ordering::Less,
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Int64(a), Value::Float64(b)) => (*a as f64)
            .partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float64(a), Value::Int64(b)) => {
            a.0.partial_cmp(&(*b as f64))
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
        (Value::Time(a), Value::Time(b)) => a.cmp(b),
        (Value::Numeric(a), Value::Numeric(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

pub struct ConcurrentPlanExecutor<'a> {
    pub(crate) catalog: &'a ConcurrentCatalog,
    pub(crate) session: &'a ConcurrentSession,
    pub(crate) tables: TableLockSet,
    pub(crate) variables: RwLock<HashMap<String, Value>>,
    pub(crate) system_variables: RwLock<HashMap<String, Value>>,
    pub(crate) cte_results: RwLock<HashMap<String, Table>>,
    pub(crate) user_function_defs: RwLock<HashMap<String, UserFunctionDef>>,
}

impl<'a> ConcurrentPlanExecutor<'a> {
    pub fn new(
        catalog: &'a ConcurrentCatalog,
        session: &'a ConcurrentSession,
        tables: TableLockSet,
    ) -> Self {
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

        let variables: HashMap<String, Value> = session
            .variables()
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();

        let system_variables = session.system_variables().clone();

        Self {
            catalog,
            session,
            tables,
            variables: RwLock::new(variables),
            system_variables: RwLock::new(system_variables),
            cte_results: RwLock::new(HashMap::new()),
            user_function_defs: RwLock::new(user_function_defs),
        }
    }

    pub(crate) fn get_system_variables(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<String, Value>> {
        self.system_variables.read().unwrap()
    }

    fn refresh_user_functions(&self) {
        let new_defs: HashMap<String, UserFunctionDef> = self
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
        *self.user_function_defs.write().unwrap() = new_defs;
    }

    pub(crate) fn get_variables(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, Value>> {
        self.variables.read().unwrap()
    }

    pub(crate) fn get_user_functions(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<String, UserFunctionDef>> {
        self.user_function_defs.read().unwrap()
    }

    pub(crate) fn is_parallel_execution_enabled(&self) -> bool {
        if let Some(val) = self.variables.read().unwrap().get("PARALLEL_EXECUTION") {
            return val.as_bool().unwrap_or(true);
        }

        if let Some(val) = self
            .system_variables
            .read()
            .unwrap()
            .get("PARALLEL_EXECUTION")
        {
            return val.as_bool().unwrap_or(true);
        }

        match std::env::var("YACHTSQL_PARALLEL_EXECUTION") {
            Ok(val) => !val.eq_ignore_ascii_case("false") && val != "0",
            Err(_) => true,
        }
    }

    pub async fn execute(&self, plan: &OptimizedLogicalPlan) -> Result<Table> {
        let executor_plan = PhysicalPlan::from_physical(plan);
        self.execute_plan(&executor_plan).await
    }

    #[async_recursion(?Send)]
    pub async fn execute_plan(&self, plan: &PhysicalPlan) -> Result<Table> {
        match plan {
            PhysicalPlan::TableScan {
                table_name, schema, ..
            } => self.execute_scan(table_name, schema).await,
            PhysicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => self.execute_sample(input, sample_type, *sample_value).await,
            PhysicalPlan::Filter { input, predicate } => {
                self.execute_filter(input, predicate).await
            }
            PhysicalPlan::Project {
                input,
                expressions,
                schema,
            } => self.execute_project(input, expressions, schema).await,
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
                parallel,
            } => {
                self.execute_nested_loop_join(
                    left,
                    right,
                    join_type,
                    condition.as_ref(),
                    schema,
                    *parallel,
                )
                .await
            }
            PhysicalPlan::CrossJoin {
                left,
                right,
                schema,
                parallel,
            } => {
                self.execute_cross_join(left, right, schema, *parallel)
                    .await
            }
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
                parallel,
            } => {
                self.execute_hash_join(
                    left, right, join_type, left_keys, right_keys, schema, *parallel,
                )
                .await
            }
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => {
                self.execute_aggregate(input, group_by, aggregates, schema, grouping_sets.as_ref())
                    .await
            }
            PhysicalPlan::Sort { input, sort_exprs } => self.execute_sort(input, sort_exprs).await,
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => self.execute_limit(input, *limit, *offset).await,
            PhysicalPlan::TopN {
                input,
                sort_exprs,
                limit,
            } => self.execute_topn(input, sort_exprs, *limit).await,
            PhysicalPlan::Distinct { input } => self.execute_distinct(input).await,
            PhysicalPlan::Union {
                inputs,
                all,
                schema,
                parallel,
            } => self.execute_union(inputs, *all, schema, *parallel).await,
            PhysicalPlan::Intersect {
                left,
                right,
                all,
                schema,
                parallel,
            } => {
                self.execute_intersect(left, right, *all, schema, *parallel)
                    .await
            }
            PhysicalPlan::Except {
                left,
                right,
                all,
                schema,
                parallel,
            } => {
                self.execute_except(left, right, *all, schema, *parallel)
                    .await
            }
            PhysicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => self.execute_window(input, window_exprs, schema).await,
            PhysicalPlan::WithCte {
                ctes,
                body,
                parallel_ctes,
            } => self.execute_cte(ctes, body, parallel_ctes).await,
            PhysicalPlan::Unnest {
                input,
                columns,
                schema,
            } => self.execute_unnest(input, columns, schema).await,
            PhysicalPlan::Qualify { input, predicate } => {
                self.execute_qualify(input, predicate).await
            }
            PhysicalPlan::Values { values, schema } => self.execute_values(values, schema).await,
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
            } => self.execute_insert(table_name, columns, source).await,
            PhysicalPlan::Update {
                table_name,
                alias,
                assignments,
                from,
                filter,
            } => {
                self.execute_update(
                    table_name,
                    alias.as_deref(),
                    assignments,
                    from.as_deref(),
                    filter.as_ref(),
                )
                .await
            }
            PhysicalPlan::Delete {
                table_name,
                alias,
                filter,
            } => {
                self.execute_delete(table_name, alias.as_deref(), filter.as_ref())
                    .await
            }
            PhysicalPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => self.execute_merge(target_table, source, on, clauses).await,
            PhysicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
                query,
            } => {
                self.execute_create_table(
                    table_name,
                    columns,
                    *if_not_exists,
                    *or_replace,
                    query.as_deref(),
                )
                .await
            }
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
                or_replace,
            } => self.execute_create_schema(name, *if_not_exists, *or_replace),
            PhysicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => self.execute_drop_schema(name, *if_exists, *cascade),
            PhysicalPlan::UndropSchema {
                name,
                if_not_exists,
            } => self.execute_undrop_schema(name, *if_not_exists),
            PhysicalPlan::AlterSchema { name, options } => self.execute_alter_schema(name, options),
            PhysicalPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_temp,
                is_aggregate,
            } => self.execute_create_function(
                name,
                args,
                return_type,
                body,
                *or_replace,
                *if_not_exists,
                *is_temp,
                *is_aggregate,
            ),
            PhysicalPlan::DropFunction { name, if_exists } => {
                self.execute_drop_function(name, *if_exists)
            }
            PhysicalPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
                if_not_exists,
            } => self.execute_create_procedure(name, args, body, *or_replace, *if_not_exists),
            PhysicalPlan::DropProcedure { name, if_exists } => {
                self.execute_drop_procedure(name, *if_exists)
            }
            PhysicalPlan::Call {
                procedure_name,
                args,
            } => self.execute_call(procedure_name, args).await,
            PhysicalPlan::ExportData { options, query } => {
                self.execute_export(options, query).await
            }
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
            PhysicalPlan::SetVariable { name, value } => {
                self.execute_set_variable(name, value).await
            }
            PhysicalPlan::SetMultipleVariables { names, value } => {
                self.execute_set_multiple_variables(names, value).await
            }
            PhysicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => {
                self.execute_if(condition, then_branch, else_branch.as_deref())
                    .await
            }
            PhysicalPlan::While {
                condition,
                body,
                label,
            } => self.execute_while(condition, body, label.as_deref()).await,
            PhysicalPlan::Loop { body, label } => self.execute_loop(body, label.as_deref()).await,
            PhysicalPlan::Block { body, label } => self.execute_block(body, label.as_deref()).await,
            PhysicalPlan::Repeat {
                body,
                until_condition,
            } => self.execute_repeat(body, until_condition).await,
            PhysicalPlan::For {
                variable,
                query,
                body,
            } => self.execute_for(variable, query, body).await,
            PhysicalPlan::Return { value: _ } => {
                Err(Error::InvalidQuery("RETURN outside of function".into()))
            }
            PhysicalPlan::Raise { message, level } => self.execute_raise(message.as_ref(), *level),
            PhysicalPlan::Break { label } => {
                let msg = match label {
                    Some(lbl) => format!("BREAK:{}", lbl),
                    None => "BREAK outside of loop".to_string(),
                };
                Err(Error::InvalidQuery(msg))
            }
            PhysicalPlan::Continue { label } => {
                let msg = match label {
                    Some(lbl) => format!("CONTINUE:{}", lbl),
                    None => "CONTINUE outside of loop".to_string(),
                };
                Err(Error::InvalidQuery(msg))
            }
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
                self.execute_assert(condition, message.as_ref()).await
            }
            PhysicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            } => {
                self.execute_execute_immediate(sql_expr, into_variables, using_params)
                    .await
            }
            PhysicalPlan::Grant { .. } => Ok(Table::empty(Schema::new())),
            PhysicalPlan::Revoke { .. } => Ok(Table::empty(Schema::new())),
            PhysicalPlan::BeginTransaction => {
                self.catalog.begin_transaction();
                let locked_snapshots = self.tables.snapshot_write_locked_tables();
                for (name, table_data) in locked_snapshots {
                    self.catalog.snapshot_table(&name, table_data);
                }
                Ok(Table::empty(Schema::new()))
            }
            PhysicalPlan::Commit => {
                self.catalog.commit();
                Ok(Table::empty(Schema::new()))
            }
            PhysicalPlan::Rollback => {
                self.rollback_transaction();
                Ok(Table::empty(Schema::new()))
            }
            PhysicalPlan::TryCatch {
                try_block,
                catch_block,
            } => self.execute_try_catch(try_block, catch_block).await,
            PhysicalPlan::GapFill {
                input,
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin,
                input_schema,
                schema,
            } => {
                self.execute_gap_fill(
                    input,
                    ts_column,
                    bucket_width,
                    value_columns,
                    partitioning_columns,
                    origin.as_ref(),
                    input_schema,
                    schema,
                )
                .await
            }
        }
    }

    async fn execute_assert(&self, condition: &Expr, message: Option<&Expr>) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::new();

        let result = if Self::expr_contains_subquery(condition) {
            self.eval_expr_with_subqueries(condition, &empty_schema, &empty_record)
                .await?
        } else {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);
            evaluator.evaluate(condition, &empty_record)?
        };

        match result {
            Value::Bool(true) => Ok(Table::empty(Schema::new())),
            Value::Bool(false) => {
                let msg = if let Some(msg_expr) = message {
                    let vars = self.get_variables();
                    let sys_vars = self.get_system_variables();
                    let udf = self.get_user_functions();
                    let evaluator = IrEvaluator::new(&empty_schema)
                        .with_variables(&vars)
                        .with_system_variables(&sys_vars)
                        .with_user_functions(&udf);
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

    #[async_recursion(?Send)]
    async fn eval_expr_with_subqueries(
        &self,
        expr: &Expr,
        schema: &Schema,
        record: &Record,
    ) -> Result<Value> {
        match expr {
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) => {
                self.eval_scalar_subquery(plan, schema, record).await
            }
            Expr::Exists { subquery, negated } => {
                let has_rows = self.eval_exists_subquery(subquery, schema, record).await?;
                Ok(Value::Bool(if *negated { !has_rows } else { has_rows }))
            }
            Expr::ArraySubquery(plan) => self.eval_array_subquery(plan, schema, record).await,
            Expr::InSubquery {
                expr: inner_expr,
                subquery,
                negated,
            } => {
                let val = self
                    .eval_expr_with_subqueries(inner_expr, schema, record)
                    .await?;
                let in_result = self
                    .eval_value_in_subquery(&val, subquery, schema, record)
                    .await?;
                Ok(Value::Bool(if *negated { !in_result } else { in_result }))
            }
            Expr::InUnnest {
                expr: inner_expr,
                array_expr,
                negated,
            } => {
                let val = self
                    .eval_expr_with_subqueries(inner_expr, schema, record)
                    .await?;
                let array_val = self
                    .eval_expr_with_subqueries(array_expr, schema, record)
                    .await?;
                let in_result = if let Value::Array(arr) = array_val {
                    arr.contains(&val)
                } else {
                    false
                };
                Ok(Value::Bool(if *negated { !in_result } else { in_result }))
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_expr_with_subqueries(left, schema, record).await?;
                let right_val = self
                    .eval_expr_with_subqueries(right, schema, record)
                    .await?;
                self.eval_binary_op_values(left_val, *op, right_val)
            }
            Expr::UnaryOp { op, expr: inner } => {
                let val = self
                    .eval_expr_with_subqueries(inner, schema, record)
                    .await?;
                self.eval_unary_op_value(*op, val)
            }
            Expr::ScalarFunction { name, args } => {
                let mut arg_vals: Vec<Value> = Vec::with_capacity(args.len());
                for a in args {
                    arg_vals.push(self.eval_expr_with_subqueries(a, schema, record).await?);
                }
                let vars = self.get_variables();
                let sys_vars = self.get_system_variables();
                let udf = self.get_user_functions();
                let evaluator = IrEvaluator::new(schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);
                evaluator.eval_scalar_function_with_values(name, &arg_vals)
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let val = self
                    .eval_expr_with_subqueries(inner, schema, record)
                    .await?;
                IrEvaluator::cast_value(val, data_type, *safe)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                let operand_val = match operand.as_ref() {
                    Some(e) => Some(self.eval_expr_with_subqueries(e, schema, record).await?),
                    None => None,
                };

                for clause in when_clauses {
                    let condition_val = if let Some(op_val) = &operand_val {
                        let cond_val = self
                            .eval_expr_with_subqueries(&clause.condition, schema, record)
                            .await?;
                        Value::Bool(op_val == &cond_val)
                    } else {
                        self.eval_expr_with_subqueries(&clause.condition, schema, record)
                            .await?
                    };

                    if matches!(condition_val, Value::Bool(true)) {
                        return self
                            .eval_expr_with_subqueries(&clause.result, schema, record)
                            .await;
                    }
                }

                if let Some(else_expr) = else_result {
                    self.eval_expr_with_subqueries(else_expr, schema, record)
                        .await
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::Alias { expr: inner, .. } => {
                self.eval_expr_with_subqueries(inner, schema, record).await
            }
            _ => {
                let vars = self.get_variables();
                let sys_vars = self.get_system_variables();
                let udf = self.get_user_functions();
                let evaluator = IrEvaluator::new(schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);
                evaluator.evaluate(expr, record)
            }
        }
    }

    async fn eval_scalar_subquery(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let substituted = self.substitute_outer_refs_in_plan(plan, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan).await?;

        if result_table.is_empty() {
            return Ok(Value::Null);
        }

        let rows: Vec<_> = result_table.rows()?.into_iter().collect();
        if rows.is_empty() {
            return Ok(Value::Null);
        }

        let first_row = &rows[0];
        let values = first_row.values();
        if values.is_empty() {
            return Ok(Value::Null);
        }

        Ok(values[0].clone())
    }

    async fn eval_scalar_subquery_as_row(&self, plan: &LogicalPlan) -> Result<Value> {
        let physical = optimize(plan)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan).await?;

        if result_table.is_empty() {
            return Ok(Value::Struct(vec![]));
        }

        let rows: Vec<_> = result_table.rows()?.into_iter().collect();
        if rows.is_empty() {
            return Ok(Value::Struct(vec![]));
        }

        let first_row = &rows[0];
        let values = first_row.values();

        let schema = result_table.schema();
        let fields = schema.fields();

        let result: Vec<(String, Value)> = fields
            .iter()
            .zip(values.iter())
            .map(|(f, v)| (f.name.clone(), v.clone()))
            .collect();

        Ok(Value::Struct(result))
    }

    async fn eval_exists_subquery(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<bool> {
        let substituted = self.substitute_outer_refs_in_plan(plan, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan).await?;
        Ok(!result_table.is_empty())
    }

    async fn eval_array_subquery(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let substituted = self.substitute_outer_refs_in_plan(plan, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan).await?;

        let result_schema = result_table.schema();
        let num_fields = result_schema.field_count();

        let mut array_values = Vec::new();
        for record in result_table.rows()? {
            let values = record.values();
            if num_fields == 1 {
                array_values.push(values[0].clone());
            } else {
                let fields: Vec<(String, Value)> = result_schema
                    .fields()
                    .iter()
                    .zip(values.iter())
                    .map(|(f, v)| (f.name.clone(), v.clone()))
                    .collect();
                array_values.push(Value::Struct(fields));
            }
        }

        Ok(Value::Array(array_values))
    }

    async fn eval_value_in_subquery(
        &self,
        value: &Value,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<bool> {
        if matches!(value, Value::Null) {
            return Ok(false);
        }

        let substituted = self.substitute_outer_refs_in_plan(plan, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan).await?;

        for record in result_table.rows()? {
            let values = record.values();
            if !values.is_empty() && &values[0] == value {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn substitute_outer_refs_in_plan(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
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
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_predicate =
                    self.substitute_outer_refs_in_expr(predicate, outer_schema, outer_record)?;
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
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_expressions = expressions
                    .iter()
                    .map(|e| self.substitute_outer_refs_in_expr(e, outer_schema, outer_record))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Project {
                    input: Box::new(new_input),
                    expressions: new_expressions,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                Ok(LogicalPlan::Limit {
                    input: Box::new(new_input),
                    limit: *limit,
                    offset: *offset,
                })
            }
            LogicalPlan::Sort { input, sort_exprs } => {
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_sort_exprs = sort_exprs
                    .iter()
                    .map(|se| {
                        let new_expr = self.substitute_outer_refs_in_expr(
                            &se.expr,
                            outer_schema,
                            outer_record,
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
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_group_by = group_by
                    .iter()
                    .map(|e| self.substitute_outer_refs_in_expr(e, outer_schema, outer_record))
                    .collect::<Result<Vec<_>>>()?;
                let new_aggregates = aggregates
                    .iter()
                    .map(|e| self.substitute_outer_refs_in_expr(e, outer_schema, outer_record))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Aggregate {
                    input: Box::new(new_input),
                    group_by: new_group_by,
                    aggregates: new_aggregates,
                    schema: schema.clone(),
                    grouping_sets: grouping_sets.clone(),
                })
            }
            LogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => {
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_columns = columns
                    .iter()
                    .map(|uc| {
                        let new_expr = self.substitute_outer_refs_in_unnest_expr(
                            &uc.expr,
                            outer_schema,
                            outer_record,
                        )?;
                        Ok(UnnestColumn {
                            expr: new_expr,
                            alias: uc.alias.clone(),
                            with_offset: uc.with_offset,
                            offset_alias: uc.offset_alias.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Unnest {
                    input: Box::new(new_input),
                    columns: new_columns,
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
                let new_left =
                    self.substitute_outer_refs_in_plan(left, outer_schema, outer_record)?;
                let new_right =
                    self.substitute_outer_refs_in_plan(right, outer_schema, outer_record)?;
                let new_condition = condition
                    .as_ref()
                    .map(|c| self.substitute_outer_refs_in_expr(c, outer_schema, outer_record))
                    .transpose()?;
                Ok(LogicalPlan::Join {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    join_type: *join_type,
                    condition: new_condition,
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
                let new_left =
                    self.substitute_outer_refs_in_plan(left, outer_schema, outer_record)?;
                let new_right =
                    self.substitute_outer_refs_in_plan(right, outer_schema, outer_record)?;
                Ok(LogicalPlan::SetOperation {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    op: *op,
                    all: *all,
                    schema: schema.clone(),
                })
            }
            other => Ok(other.clone()),
        }
    }

    fn substitute_outer_refs_in_expr(
        &self,
        expr: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Expr> {
        match expr {
            Expr::Column { table, name, index } => {
                if index.is_some() {
                    return Ok(Expr::Column {
                        table: table.clone(),
                        name: name.clone(),
                        index: *index,
                    });
                }
                let idx = if let Some(tbl) = table {
                    outer_schema.fields().iter().position(|f| {
                        (f.source_table
                            .as_ref()
                            .is_some_and(|src| src.eq_ignore_ascii_case(tbl))
                            || f.source_table.is_none())
                            && f.name.eq_ignore_ascii_case(name)
                    })
                } else {
                    outer_schema
                        .fields()
                        .iter()
                        .position(|f| f.name.eq_ignore_ascii_case(name))
                };

                if let Some(idx) = idx {
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

    fn substitute_outer_refs_in_unnest_expr(
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
                            && f.name.eq_ignore_ascii_case(name)
                    })
                } else {
                    outer_schema.field_index(name).is_some()
                };

                if should_substitute
                    && let Some(idx) = outer_schema.field_index_qualified(name, table.as_deref())
                {
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
            Expr::ScalarFunction { name, args } => {
                let new_args = args
                    .iter()
                    .map(|a| {
                        self.substitute_outer_refs_in_unnest_expr(a, outer_schema, outer_record)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::ScalarFunction {
                    name: name.clone(),
                    args: new_args,
                })
            }
            Expr::StructAccess { expr: base, field } => {
                let new_base =
                    self.substitute_outer_refs_in_unnest_expr(base, outer_schema, outer_record)?;
                Ok(Expr::StructAccess {
                    expr: Box::new(new_base),
                    field: field.clone(),
                })
            }
            Expr::ArrayAccess { array, index } => {
                let new_array =
                    self.substitute_outer_refs_in_unnest_expr(array, outer_schema, outer_record)?;
                let new_index =
                    self.substitute_outer_refs_in_unnest_expr(index, outer_schema, outer_record)?;
                Ok(Expr::ArrayAccess {
                    array: Box::new(new_array),
                    index: Box::new(new_index),
                })
            }
            _ => self.substitute_outer_refs_in_expr(expr, outer_schema, outer_record),
        }
    }

    fn value_to_literal(value: Value) -> yachtsql_ir::Literal {
        use yachtsql_ir::Literal;
        match value {
            Value::Null => Literal::Null,
            Value::Bool(b) => Literal::Bool(b),
            Value::Int64(n) => Literal::Int64(n),
            Value::Float64(f) => Literal::Float64(f),
            Value::String(s) => Literal::String(s),
            Value::Date(d) => Literal::Date(d.num_days_from_ce() - 719163),
            Value::Time(t) => Literal::Time(
                (t.hour() as i64 * 3600 + t.minute() as i64 * 60 + t.second() as i64)
                    * 1_000_000_000
                    + t.nanosecond() as i64,
            ),
            Value::DateTime(dt) => Literal::Datetime(dt.and_utc().timestamp_micros()),
            Value::Timestamp(ts) => Literal::Timestamp(ts.timestamp_micros()),
            Value::Numeric(n) => Literal::Numeric(n),
            Value::Bytes(b) => Literal::Bytes(b),
            Value::Interval(i) => Literal::Interval {
                months: i.months,
                days: i.days,
                nanos: i.nanos,
            },
            Value::Array(arr) => {
                let literal_elements: Vec<Literal> =
                    arr.into_iter().map(Self::value_to_literal).collect();
                Literal::Array(literal_elements)
            }
            Value::Struct(fields) => {
                let literal_fields: Vec<(String, Literal)> = fields
                    .into_iter()
                    .map(|(name, val)| (name, Self::value_to_literal(val)))
                    .collect();
                Literal::Struct(literal_fields)
            }
            _ => Literal::Null,
        }
    }

    fn eval_binary_op_values(&self, left: Value, op: BinaryOp, right: Value) -> Result<Value> {
        match op {
            BinaryOp::Add => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l + r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l + *r)),
                (Value::Int64(l), Value::Float64(r)) => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) + *r))
                }
                (Value::Float64(l), Value::Int64(r)) => {
                    Ok(Value::Float64(*l + ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            BinaryOp::Sub => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l - r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l - *r)),
                (Value::Int64(l), Value::Float64(r)) => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) - *r))
                }
                (Value::Float64(l), Value::Int64(r)) => {
                    Ok(Value::Float64(*l - ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            BinaryOp::Mul => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l * r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l * *r)),
                (Value::Int64(l), Value::Float64(r)) => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) * *r))
                }
                (Value::Float64(l), Value::Int64(r)) => {
                    Ok(Value::Float64(*l * ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            BinaryOp::Div => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) if *r != 0 => Ok(Value::Float64(
                    ordered_float::OrderedFloat(*l as f64 / *r as f64),
                )),
                (Value::Float64(l), Value::Float64(r)) if r.0 != 0.0 => Ok(Value::Float64(*l / *r)),
                (Value::Int64(l), Value::Float64(r)) if r.0 != 0.0 => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) / *r))
                }
                (Value::Float64(l), Value::Int64(r)) if *r != 0 => {
                    Ok(Value::Float64(*l / ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            BinaryOp::And => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::Bool(l && r))
            }
            BinaryOp::Or => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::Bool(l || r))
            }
            BinaryOp::Eq => Ok(Value::Bool(left == right)),
            BinaryOp::NotEq => Ok(Value::Bool(left != right)),
            BinaryOp::Lt => Ok(Value::Bool(left < right)),
            BinaryOp::LtEq => Ok(Value::Bool(left <= right)),
            BinaryOp::Gt => Ok(Value::Bool(left > right)),
            BinaryOp::GtEq => Ok(Value::Bool(left >= right)),
            _ => Ok(Value::Null),
        }
    }

    fn eval_unary_op_value(&self, op: yachtsql_ir::UnaryOp, val: Value) -> Result<Value> {
        match op {
            yachtsql_ir::UnaryOp::Not => {
                let b = val.as_bool().unwrap_or(false);
                Ok(Value::Bool(!b))
            }
            yachtsql_ir::UnaryOp::Minus => match val {
                Value::Int64(n) => Ok(Value::Int64(-n)),
                Value::Float64(f) => Ok(Value::Float64(-f)),
                _ => Ok(Value::Null),
            },
            yachtsql_ir::UnaryOp::Plus => Ok(val),
            yachtsql_ir::UnaryOp::BitwiseNot => match val {
                Value::Int64(n) => Ok(Value::Int64(!n)),
                _ => Ok(Value::Null),
            },
        }
    }

    #[async_recursion(?Send)]
    async fn resolve_subqueries_in_expr(&self, expr: &Expr) -> Result<Expr> {
        match expr {
            Expr::InSubquery {
                expr: inner_expr,
                subquery,
                negated,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner_expr).await?;
                let empty_schema = Schema::new();
                let empty_record = Record::new();
                let substituted =
                    self.substitute_outer_refs_in_plan(subquery, &empty_schema, &empty_record)?;
                let physical = optimize(&substituted)?;
                let executor_plan = PhysicalPlan::from_physical(&physical);
                let result_table = self.execute_plan(&executor_plan).await?;

                let mut list_exprs = Vec::new();
                for record in result_table.rows()? {
                    let values = record.values();
                    if !values.is_empty() {
                        let literal = Self::value_to_literal(values[0].clone());
                        list_exprs.push(Expr::Literal(literal));
                    }
                }

                Ok(Expr::InList {
                    expr: Box::new(resolved_inner),
                    list: list_exprs,
                    negated: *negated,
                })
            }
            Expr::Exists { subquery, negated } => {
                let empty_schema = Schema::new();
                let empty_record = Record::new();
                let substituted =
                    self.substitute_outer_refs_in_plan(subquery, &empty_schema, &empty_record)?;
                let physical = optimize(&substituted)?;
                let executor_plan = PhysicalPlan::from_physical(&physical);
                let result_table = self.execute_plan(&executor_plan).await?;
                let has_rows = !result_table.is_empty();
                let result = if *negated { !has_rows } else { has_rows };
                Ok(Expr::Literal(yachtsql_ir::Literal::Bool(result)))
            }
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) => {
                let empty_schema = Schema::new();
                let empty_record = Record::new();
                let substituted =
                    self.substitute_outer_refs_in_plan(plan, &empty_schema, &empty_record)?;
                let physical = optimize(&substituted)?;
                let executor_plan = PhysicalPlan::from_physical(&physical);
                let result_table = self.execute_plan(&executor_plan).await?;

                if result_table.is_empty() {
                    return Ok(Expr::Literal(yachtsql_ir::Literal::Null));
                }

                let rows: Vec<_> = result_table.rows()?.into_iter().collect();
                if rows.is_empty() {
                    return Ok(Expr::Literal(yachtsql_ir::Literal::Null));
                }

                let values = rows[0].values();
                if values.is_empty() {
                    return Ok(Expr::Literal(yachtsql_ir::Literal::Null));
                }

                let literal = Self::value_to_literal(values[0].clone());
                Ok(Expr::Literal(literal))
            }
            Expr::ArraySubquery(plan) => {
                let empty_schema = Schema::new();
                let empty_record = Record::new();
                let substituted =
                    self.substitute_outer_refs_in_plan(plan, &empty_schema, &empty_record)?;
                let physical = optimize(&substituted)?;
                let executor_plan = PhysicalPlan::from_physical(&physical);
                let result_table = self.execute_plan(&executor_plan).await?;

                let mut array_elements = Vec::new();
                for record in result_table.rows()? {
                    let values = record.values();
                    if !values.is_empty() {
                        array_elements.push(Self::value_to_literal(values[0].clone()));
                    }
                }
                Ok(Expr::Literal(yachtsql_ir::Literal::Array(array_elements)))
            }
            Expr::BinaryOp { left, op, right } => {
                let resolved_left = self.resolve_subqueries_in_expr(left).await?;
                let resolved_right = self.resolve_subqueries_in_expr(right).await?;
                Ok(Expr::BinaryOp {
                    left: Box::new(resolved_left),
                    op: *op,
                    right: Box::new(resolved_right),
                })
            }
            Expr::UnaryOp { op, expr: inner } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                Ok(Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(resolved_inner),
                })
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                Ok(Expr::Cast {
                    expr: Box::new(resolved_inner),
                    data_type: data_type.clone(),
                    safe: *safe,
                })
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                let resolved_operand = match operand.as_ref() {
                    Some(e) => Some(Box::new(self.resolve_subqueries_in_expr(e).await?)),
                    None => None,
                };

                let mut resolved_clauses = Vec::new();
                for clause in when_clauses {
                    resolved_clauses.push(yachtsql_ir::WhenClause {
                        condition: self.resolve_subqueries_in_expr(&clause.condition).await?,
                        result: self.resolve_subqueries_in_expr(&clause.result).await?,
                    });
                }

                let resolved_else = match else_result.as_ref() {
                    Some(e) => Some(Box::new(self.resolve_subqueries_in_expr(e).await?)),
                    None => None,
                };

                Ok(Expr::Case {
                    operand: resolved_operand,
                    when_clauses: resolved_clauses,
                    else_result: resolved_else,
                })
            }
            Expr::ScalarFunction { name, args } => {
                let mut resolved_args = Vec::with_capacity(args.len());
                for a in args {
                    resolved_args.push(self.resolve_subqueries_in_expr(a).await?);
                }
                Ok(Expr::ScalarFunction {
                    name: name.clone(),
                    args: resolved_args,
                })
            }
            Expr::Alias { expr: inner, name } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                Ok(Expr::Alias {
                    expr: Box::new(resolved_inner),
                    name: name.clone(),
                })
            }
            Expr::IsNull {
                expr: inner,
                negated,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                Ok(Expr::IsNull {
                    expr: Box::new(resolved_inner),
                    negated: *negated,
                })
            }
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                let mut resolved_list = Vec::with_capacity(list.len());
                for e in list {
                    resolved_list.push(self.resolve_subqueries_in_expr(e).await?);
                }
                Ok(Expr::InList {
                    expr: Box::new(resolved_inner),
                    list: resolved_list,
                    negated: *negated,
                })
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner).await?;
                let resolved_low = self.resolve_subqueries_in_expr(low).await?;
                let resolved_high = self.resolve_subqueries_in_expr(high).await?;
                Ok(Expr::Between {
                    expr: Box::new(resolved_inner),
                    low: Box::new(resolved_low),
                    high: Box::new(resolved_high),
                    negated: *negated,
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    fn expr_contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::Subquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::ArraySubquery(_) => true,
            Expr::InUnnest {
                expr: inner_expr,
                array_expr,
                ..
            } => {
                Self::expr_contains_subquery(inner_expr) || Self::expr_contains_subquery(array_expr)
            }
            Expr::InList { expr, list, .. } => {
                Self::expr_contains_subquery(expr) || list.iter().any(Self::expr_contains_subquery)
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery(left) || Self::expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::ScalarFunction { args, .. } => args.iter().any(Self::expr_contains_subquery),
            Expr::Cast { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::Alias { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|e| Self::expr_contains_subquery(e))
                    || when_clauses.iter().any(|wc| {
                        Self::expr_contains_subquery(&wc.condition)
                            || Self::expr_contains_subquery(&wc.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::expr_contains_subquery(e))
            }
            _ => false,
        }
    }
}

pub(super) fn default_value_for_type(data_type: &DataType) -> Value {
    match data_type {
        DataType::Int64 => Value::Int64(0),
        DataType::Float64 => Value::Float64(ordered_float::OrderedFloat(0.0)),
        DataType::Bool => Value::Bool(false),
        DataType::String => Value::String(String::new()),
        _ => Value::Null,
    }
}
