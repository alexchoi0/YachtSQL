mod ddl;
mod dml;
mod gap_fill;
mod io;
mod join;
mod scripting;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::Array;
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc};
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
    pub(crate) tables: TableLockSet<'a>,
    pub(crate) variables: HashMap<String, Value>,
    pub(crate) system_variables: HashMap<String, Value>,
    pub(crate) cte_results: HashMap<String, Table>,
    pub(crate) user_function_defs: HashMap<String, UserFunctionDef>,
}

impl<'a> ConcurrentPlanExecutor<'a> {
    pub fn new(
        catalog: &'a ConcurrentCatalog,
        session: &'a ConcurrentSession,
        tables: TableLockSet<'a>,
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
            variables,
            system_variables,
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
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
            } => self.execute_hash_join(left, right, join_type, left_keys, right_keys, schema),
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
                alias,
                assignments,
                from,
                filter,
            } => self.execute_update(
                table_name,
                alias.as_deref(),
                assignments,
                from.as_deref(),
                filter.as_ref(),
            ),
            PhysicalPlan::Delete {
                table_name,
                alias,
                filter,
            } => self.execute_delete(table_name, alias.as_deref(), filter.as_ref()),
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
            PhysicalPlan::SetMultipleVariables { names, value } => {
                self.execute_set_multiple_variables(names, value)
            }
            PhysicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => self.execute_if(condition, then_branch, else_branch.as_deref()),
            PhysicalPlan::While {
                condition,
                body,
                label,
            } => self.execute_while(condition, body, label.as_deref()),
            PhysicalPlan::Loop { body, label } => self.execute_loop(body, label.as_deref()),
            PhysicalPlan::Block { body, label } => self.execute_block(body, label.as_deref()),
            PhysicalPlan::Repeat {
                body,
                until_condition,
            } => self.execute_repeat(body, until_condition),
            PhysicalPlan::For {
                variable,
                query,
                body,
            } => self.execute_for(variable, query, body),
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
                self.execute_assert(condition, message.as_ref())
            }
            PhysicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            } => self.execute_execute_immediate(sql_expr, into_variables, using_params),
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
            } => self.execute_try_catch(try_block, catch_block),
            PhysicalPlan::GapFill {
                input,
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin,
                input_schema,
                schema,
            } => self.execute_gap_fill(
                input,
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin.as_ref(),
                input_schema,
                schema,
            ),
        }
    }

    pub(crate) fn execute_scan(
        &self,
        table_name: &str,
        planned_schema: &PlanSchema,
    ) -> Result<Table> {
        if let Some(cte_table) = self.cte_results.get(table_name) {
            return Ok(self.apply_planned_schema(cte_table, planned_schema));
        }
        let table_name_upper = table_name.to_uppercase();
        if let Some(cte_table) = self.cte_results.get(&table_name_upper) {
            return Ok(self.apply_planned_schema(cte_table, planned_schema));
        }
        let table_name_lower = table_name.to_lowercase();
        if let Some(cte_table) = self.cte_results.get(&table_name_lower) {
            return Ok(self.apply_planned_schema(cte_table, planned_schema));
        }

        if let Some(table) = self.tables.get_table(table_name) {
            return Ok(self.apply_planned_schema(table, planned_schema));
        }

        if let Some(handle) = self.catalog.get_table_handle(table_name)
            && let Ok(guard) = handle.read()
        {
            return Ok(self.apply_planned_schema(&guard, planned_schema));
        }

        Err(Error::TableNotFound(table_name.to_string()))
    }

    pub(crate) fn apply_planned_schema(
        &self,
        source_table: &Table,
        planned_schema: &PlanSchema,
    ) -> Table {
        if planned_schema.fields.is_empty() {
            return source_table.clone();
        }

        let mut new_schema = Schema::new();
        let mut column_indices = Vec::new();
        for plan_field in &planned_schema.fields {
            let mode = if plan_field.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            let mut field = Field::new(&plan_field.name, plan_field.data_type.clone(), mode);
            if let Some(ref table) = plan_field.table {
                field = field.with_source_table(table.clone());
            }
            let source_field_idx = source_table
                .schema()
                .fields()
                .iter()
                .position(|f| f.name.eq_ignore_ascii_case(&plan_field.name));
            if let Some(idx) = source_field_idx {
                if let Some(ref collation) = source_table.schema().fields()[idx].collation {
                    field.collation = Some(collation.clone());
                }
                column_indices.push(idx);
            }
            new_schema.add_field(field);
        }
        source_table.with_reordered_schema(new_schema, &column_indices)
    }

    fn execute_assert(&mut self, condition: &Expr, message: Option<&Expr>) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::new();

        let result = if Self::expr_contains_subquery(condition) {
            self.eval_expr_with_subqueries(condition, &empty_schema, &empty_record)?
        } else {
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_system_variables(&self.system_variables)
                .with_user_functions(&self.user_function_defs);
            evaluator.evaluate(condition, &empty_record)?
        };

        match result {
            Value::Bool(true) => Ok(Table::empty(Schema::new())),
            Value::Bool(false) => {
                let msg = if let Some(msg_expr) = message {
                    let evaluator = IrEvaluator::new(&empty_schema)
                        .with_variables(&self.variables)
                        .with_system_variables(&self.system_variables)
                        .with_user_functions(&self.user_function_defs);
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

    pub(crate) fn execute_filter(
        &mut self,
        input: &PhysicalPlan,
        predicate: &Expr,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let has_subquery = Self::expr_contains_subquery(predicate);
        let mut result = Table::empty(schema.clone());

        if has_subquery {
            for record in input_table.rows()? {
                let val = self.eval_expr_with_subqueries(predicate, &schema, &record)?;
                if val.as_bool().unwrap_or(false) {
                    result.push_row(record.values().to_vec())?;
                }
            }
        } else {
            let evaluator = IrEvaluator::new(&schema)
                .with_variables(&self.variables)
                .with_system_variables(&self.system_variables)
                .with_user_functions(&self.user_function_defs);

            for record in input_table.rows()? {
                let val = evaluator.evaluate(predicate, &record)?;
                if val.as_bool().unwrap_or(false) {
                    result.push_row(record.values().to_vec())?;
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_project(
        &mut self,
        input: &PhysicalPlan,
        expressions: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let input_schema = input_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);
        let has_subqueries = expressions.iter().any(Self::expr_contains_subquery);

        let mut result = Table::empty(result_schema);

        if has_subqueries {
            for record in input_table.rows()? {
                let mut new_row = Vec::with_capacity(expressions.len());
                for expr in expressions {
                    let val = self.eval_expr_with_subqueries(expr, &input_schema, &record)?;
                    new_row.push(val);
                }
                result.push_row(new_row)?;
            }
        } else {
            let evaluator = IrEvaluator::new(&input_schema)
                .with_variables(&self.variables)
                .with_system_variables(&self.system_variables)
                .with_user_functions(&self.user_function_defs);

            for record in input_table.rows()? {
                let mut new_row = Vec::with_capacity(expressions.len());
                for expr in expressions {
                    let val = evaluator.evaluate(expr, &record)?;
                    new_row.push(val);
                }
                result.push_row(new_row)?;
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_sample(
        &mut self,
        input: &PhysicalPlan,
        sample_type: &SampleType,
        sample_value: i64,
    ) -> Result<Table> {
        use rand::Rng;
        use rand::seq::SliceRandom;

        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let rows = input_table.rows()?;
        let mut result = Table::empty(schema);

        match sample_type {
            SampleType::Rows => {
                let n = sample_value as usize;
                let mut rng = rand::thread_rng();
                let sampled: Vec<_> = rows.choose_multiple(&mut rng, n.min(rows.len())).collect();
                for record in sampled {
                    result.push_row(record.values().to_vec())?;
                }
            }
            SampleType::Percent => {
                let pct = sample_value as f64 / 100.0;
                let mut rng = rand::thread_rng();
                for record in rows {
                    if rng.r#gen::<f64>() < pct {
                        result.push_row(record.values().to_vec())?;
                    }
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_sort(
        &mut self,
        input: &PhysicalPlan,
        sort_exprs: &[SortExpr],
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let evaluator = IrEvaluator::new(&schema)
            .with_variables(&self.variables)
            .with_system_variables(&self.system_variables)
            .with_user_functions(&self.user_function_defs);

        let mut rows: Vec<Record> = input_table.rows()?;

        rows.sort_by(|a, b| {
            for sort_expr in sort_exprs {
                let val_a = evaluator
                    .evaluate(&sort_expr.expr, a)
                    .unwrap_or(Value::Null);
                let val_b = evaluator
                    .evaluate(&sort_expr.expr, b)
                    .unwrap_or(Value::Null);

                let ordering = compare_values_for_sort(&val_a, &val_b);
                let ordering = if !sort_expr.asc {
                    ordering.reverse()
                } else {
                    ordering
                };

                match (val_a.is_null(), val_b.is_null()) {
                    (true, true) => {}
                    (true, false) => {
                        return if sort_expr.nulls_first {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Greater
                        };
                    }
                    (false, true) => {
                        return if sort_expr.nulls_first {
                            std::cmp::Ordering::Greater
                        } else {
                            std::cmp::Ordering::Less
                        };
                    }
                    (false, false) => {}
                }

                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        let mut result = Table::empty(schema);
        for record in rows {
            result.push_row(record.values().to_vec())?;
        }

        Ok(result)
    }

    pub(crate) fn execute_limit(
        &mut self,
        input: &PhysicalPlan,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let mut result = Table::empty(schema);

        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(usize::MAX);

        for (i, record) in input_table.rows()?.into_iter().enumerate() {
            if i >= offset && i < offset + limit {
                result.push_row(record.values().to_vec())?;
            }
            if i >= offset + limit {
                break;
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_topn(
        &mut self,
        input: &PhysicalPlan,
        sort_exprs: &[SortExpr],
        limit: usize,
    ) -> Result<Table> {
        let sorted = self.execute_sort(input, sort_exprs)?;
        let schema = sorted.schema().clone();
        let mut result = Table::empty(schema);

        for (i, record) in sorted.rows()?.into_iter().enumerate() {
            if i >= limit {
                break;
            }
            result.push_row(record.values().to_vec())?;
        }

        Ok(result)
    }

    pub(crate) fn execute_distinct(&mut self, input: &PhysicalPlan) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let mut result = Table::empty(schema);
        let mut seen: HashSet<Vec<Value>> = HashSet::new();

        for record in input_table.rows()? {
            let values = record.values().to_vec();
            if seen.insert(values.clone()) {
                result.push_row(values)?;
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_union(
        &mut self,
        inputs: &[PhysicalPlan],
        all: bool,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);
        let mut seen: HashSet<Vec<Value>> = HashSet::new();

        for input in inputs {
            let table = self.execute_plan(input)?;
            for record in table.rows()? {
                let values = record.values().to_vec();
                if all || seen.insert(values.clone()) {
                    result.push_row(values)?;
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_intersect(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        all: bool,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        let mut right_set: HashMap<Vec<Value>, usize> = HashMap::new();
        for record in right_table.rows()? {
            *right_set.entry(record.values().to_vec()).or_insert(0) += 1;
        }

        let mut seen: HashSet<Vec<Value>> = HashSet::new();
        for record in left_table.rows()? {
            let values = record.values().to_vec();
            if let Some(count) = right_set.get_mut(&values)
                && *count > 0
                && (all || seen.insert(values.clone()))
            {
                result.push_row(values)?;
                if all {
                    *count -= 1;
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_except(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        all: bool,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        let mut right_set: HashMap<Vec<Value>, usize> = HashMap::new();
        for record in right_table.rows()? {
            *right_set.entry(record.values().to_vec()).or_insert(0) += 1;
        }

        let mut seen: HashSet<Vec<Value>> = HashSet::new();
        for record in left_table.rows()? {
            let values = record.values().to_vec();
            let in_right = right_set.get_mut(&values).map(|c| {
                if *c > 0 {
                    *c -= 1;
                    true
                } else {
                    false
                }
            });

            if !in_right.unwrap_or(false) && (all || seen.insert(values.clone())) {
                result.push_row(values)?;
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_aggregate(
        &mut self,
        input: &PhysicalPlan,
        group_by: &[Expr],
        aggregates: &[Expr],
        schema: &PlanSchema,
        grouping_sets: Option<&Vec<Vec<usize>>>,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        crate::executor::compute_aggregate(
            &input_table,
            group_by,
            aggregates,
            schema,
            grouping_sets,
            &self.variables,
            &self.user_function_defs,
        )
    }

    pub(crate) fn execute_window(
        &mut self,
        input: &PhysicalPlan,
        window_exprs: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        crate::executor::compute_window(
            &input_table,
            window_exprs,
            schema,
            &self.variables,
            &self.user_function_defs,
        )
    }

    pub(crate) fn execute_cte(
        &mut self,
        ctes: &[CteDefinition],
        body: &PhysicalPlan,
    ) -> Result<Table> {
        for cte in ctes {
            if cte.recursive {
                self.execute_recursive_cte(cte)?;
            } else {
                let physical_cte = yachtsql_optimizer::optimize(&cte.query)?;
                let cte_plan = PhysicalPlan::from_physical(&physical_cte);
                let mut cte_result = self.execute_plan(&cte_plan)?;

                if let Some(ref columns) = cte.columns {
                    cte_result = self.apply_cte_column_aliases(&cte_result, columns)?;
                }

                self.cte_results.insert(cte.name.to_uppercase(), cte_result);
            }
        }
        self.execute_plan(body)
    }

    fn apply_cte_column_aliases(&self, table: &Table, columns: &[String]) -> Result<Table> {
        let mut new_schema = Schema::new();
        for (i, alias) in columns.iter().enumerate() {
            if let Some(old_field) = table.schema().fields().get(i) {
                let mut new_field = Field::new(alias, old_field.data_type.clone(), old_field.mode);
                if let Some(ref src) = old_field.source_table {
                    new_field = new_field.with_source_table(src.clone());
                }
                new_schema.add_field(new_field);
            }
        }
        let rows: Vec<Vec<Value>> = table
            .rows()?
            .into_iter()
            .map(|r| r.values().to_vec())
            .collect();
        let mut result = Table::empty(new_schema);
        for row in rows {
            result.push_row(row)?;
        }
        Ok(result)
    }

    fn execute_recursive_cte(&mut self, cte: &CteDefinition) -> Result<()> {
        const MAX_RECURSION_DEPTH: usize = 500;

        let (anchor_terms, recursive_terms) = Self::split_recursive_cte(&cte.query, &cte.name);

        let mut all_results = Vec::new();
        for anchor in &anchor_terms {
            let physical = yachtsql_optimizer::optimize(anchor)?;
            let anchor_plan = PhysicalPlan::from_physical(&physical);
            let result = self.execute_plan(&anchor_plan)?;
            for row in result.rows()? {
                all_results.push(row.values().to_vec());
            }
        }

        let schema = plan_schema_to_schema(cte.query.schema());
        let mut accumulated = Table::from_values(schema.clone(), all_results.clone())?;

        if let Some(ref columns) = cte.columns {
            accumulated = self.apply_cte_column_aliases(&accumulated, columns)?;
        }

        self.cte_results
            .insert(cte.name.to_uppercase(), accumulated.clone());

        let mut working_set = accumulated.clone();
        let mut iteration = 0;

        while !working_set.is_empty() && iteration < MAX_RECURSION_DEPTH {
            iteration += 1;

            self.cte_results
                .insert(cte.name.to_uppercase(), working_set);

            let mut new_rows = Vec::new();
            for recursive_term in &recursive_terms {
                let physical = yachtsql_optimizer::optimize(recursive_term)?;
                let rec_plan = PhysicalPlan::from_physical(&physical);
                let result = self.execute_plan(&rec_plan)?;
                for row in result.rows()? {
                    new_rows.push(row.values().to_vec());
                }
            }

            if new_rows.is_empty() {
                break;
            }

            for row in &new_rows {
                all_results.push(row.clone());
            }

            working_set = Table::from_values(schema.clone(), new_rows)?;
            if let Some(ref columns) = cte.columns {
                working_set = self.apply_cte_column_aliases(&working_set, columns)?;
            }
            accumulated = Table::from_values(schema.clone(), all_results.clone())?;
            if let Some(ref columns) = cte.columns {
                accumulated = self.apply_cte_column_aliases(&accumulated, columns)?;
            }
        }

        self.cte_results
            .insert(cte.name.to_uppercase(), accumulated);
        Ok(())
    }

    fn split_recursive_cte(
        query: &LogicalPlan,
        cte_name: &str,
    ) -> (Vec<LogicalPlan>, Vec<LogicalPlan>) {
        let mut anchors = Vec::new();
        let mut recursives = Vec::new();

        Self::collect_union_terms(query, cte_name, &mut anchors, &mut recursives);

        if anchors.is_empty() {
            anchors.push(query.clone());
        }

        (anchors, recursives)
    }

    fn collect_union_terms(
        plan: &LogicalPlan,
        cte_name: &str,
        anchors: &mut Vec<LogicalPlan>,
        recursives: &mut Vec<LogicalPlan>,
    ) {
        match plan {
            LogicalPlan::SetOperation {
                left,
                right,
                op: SetOperationType::Union,
                all: true,
                ..
            } => {
                Self::collect_union_terms(left, cte_name, anchors, recursives);
                Self::collect_union_terms(right, cte_name, anchors, recursives);
            }
            _ => {
                if Self::references_table(plan, cte_name) {
                    recursives.push(plan.clone());
                } else {
                    anchors.push(plan.clone());
                }
            }
        }
    }

    fn references_table(plan: &LogicalPlan, table_name: &str) -> bool {
        match plan {
            LogicalPlan::Scan {
                table_name: name, ..
            } => name.eq_ignore_ascii_case(table_name),
            LogicalPlan::Filter { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Project { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Aggregate { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Join { left, right, .. } => {
                Self::references_table(left, table_name)
                    || Self::references_table(right, table_name)
            }
            LogicalPlan::Sort { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Limit { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Distinct { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::SetOperation { left, right, .. } => {
                Self::references_table(left, table_name)
                    || Self::references_table(right, table_name)
            }
            LogicalPlan::Window { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::WithCte { body, .. } => Self::references_table(body, table_name),
            LogicalPlan::Unnest { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Qualify { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Sample { input, .. } => Self::references_table(input, table_name),
            _ => false,
        }
    }

    pub(crate) fn execute_unnest(
        &mut self,
        input: &PhysicalPlan,
        columns: &[UnnestColumn],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let input_schema = input_table.schema().clone();
        let evaluator = IrEvaluator::new(&input_schema)
            .with_variables(&self.variables)
            .with_system_variables(&self.system_variables)
            .with_user_functions(&self.user_function_defs);

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        let input_rows = input_table.rows()?;

        if input_rows.is_empty() && !columns.is_empty() {
            let empty_record = Record::new();
            let first_col = &columns[0];
            let array_val = evaluator.evaluate(&first_col.expr, &empty_record)?;
            Self::unnest_array(&array_val, first_col, &[], &mut result)?;
        } else {
            for record in input_rows {
                let base_values = record.values().to_vec();

                if columns.is_empty() {
                    result.push_row(base_values)?;
                    continue;
                }

                let first_col = &columns[0];
                let array_val = evaluator.evaluate(&first_col.expr, &record)?;
                Self::unnest_array(&array_val, first_col, &base_values, &mut result)?;
            }
        }

        Ok(result)
    }

    fn unnest_array(
        array_val: &Value,
        unnest_col: &UnnestColumn,
        base_values: &[Value],
        result: &mut Table,
    ) -> Result<()> {
        match array_val {
            Value::Array(elements) => {
                for (idx, elem) in elements.iter().enumerate() {
                    let mut row = base_values.to_vec();
                    match elem {
                        Value::Struct(struct_fields) => {
                            for (_, value) in struct_fields {
                                row.push(value.clone());
                            }
                        }
                        _ => {
                            row.push(elem.clone());
                        }
                    }
                    if unnest_col.with_offset {
                        row.push(Value::Int64(idx as i64));
                    }
                    result.push_row(row)?;
                }
            }
            Value::Null => {}
            _ => {
                return Err(Error::InvalidQuery("UNNEST requires array argument".into()));
            }
        }
        Ok(())
    }

    pub(crate) fn execute_qualify(
        &mut self,
        input: &PhysicalPlan,
        predicate: &Expr,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();

        if Self::expr_has_window_function(predicate) {
            self.execute_qualify_with_window(&input_table, predicate)
        } else {
            let evaluator = IrEvaluator::new(&schema)
                .with_variables(&self.variables)
                .with_system_variables(&self.system_variables)
                .with_user_functions(&self.user_function_defs);
            let mut result = Table::empty(schema.clone());

            for record in input_table.rows()? {
                let val = evaluator.evaluate(predicate, &record)?;
                if val.as_bool().unwrap_or(false) {
                    result.push_row(record.values().to_vec())?;
                }
            }

            Ok(result)
        }
    }

    fn execute_qualify_with_window(&mut self, input: &Table, predicate: &Expr) -> Result<Table> {
        let schema = input.schema().clone();
        let rows: Vec<Record> = input.rows()?;
        let evaluator = IrEvaluator::new(&schema)
            .with_variables(&self.variables)
            .with_system_variables(&self.system_variables)
            .with_user_functions(&self.user_function_defs);

        let window_exprs = Self::collect_window_exprs(predicate);
        let mut window_results: HashMap<String, Vec<Value>> = HashMap::new();

        for window_expr in &window_exprs {
            let key = format!("{:?}", window_expr);
            if window_results.contains_key(&key) {
                continue;
            }

            let (partition_by, order_by, frame, func_type) =
                Self::extract_qualify_window_spec(window_expr)?;

            let partitions = partition_rows(&rows, &partition_by, &evaluator)?;
            let mut results = vec![Value::Null; rows.len()];

            for (_key, mut indices) in partitions {
                sort_partition(&rows, &mut indices, &order_by, &evaluator)?;

                let partition_results = compute_window_function(
                    &rows,
                    &indices,
                    window_expr,
                    &func_type,
                    &order_by,
                    &frame,
                    &evaluator,
                )?;

                for (local_idx, row_idx) in indices.iter().enumerate() {
                    results[*row_idx] = partition_results[local_idx].clone();
                }
            }

            window_results.insert(key, results);
        }

        let mut result = Table::empty(schema.clone());

        for (row_idx, record) in rows.iter().enumerate() {
            let val = Self::evaluate_qualify_predicate(
                predicate,
                &schema,
                record,
                row_idx,
                &window_results,
            )?;
            if val.as_bool().unwrap_or(false) {
                result.push_row(record.values().to_vec())?;
            }
        }

        Ok(result)
    }

    fn evaluate_qualify_predicate(
        expr: &Expr,
        schema: &Schema,
        record: &Record,
        row_idx: usize,
        window_results: &HashMap<String, Vec<Value>>,
    ) -> Result<Value> {
        match expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => {
                let key = format!("{:?}", expr);
                Ok(window_results
                    .get(&key)
                    .and_then(|r| r.get(row_idx))
                    .cloned()
                    .unwrap_or(Value::Null))
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_qualify_predicate(
                    left,
                    schema,
                    record,
                    row_idx,
                    window_results,
                )?;
                let right_val = Self::evaluate_qualify_predicate(
                    right,
                    schema,
                    record,
                    row_idx,
                    window_results,
                )?;

                if left_val.is_null() || right_val.is_null() {
                    match op {
                        BinaryOp::And | BinaryOp::Or => {}
                        _ => return Ok(Value::Bool(false)),
                    }
                }

                match op {
                    BinaryOp::Eq => Ok(Value::Bool(left_val == right_val)),
                    BinaryOp::NotEq => Ok(Value::Bool(left_val != right_val)),
                    BinaryOp::Lt => Ok(Value::Bool(left_val < right_val)),
                    BinaryOp::LtEq => Ok(Value::Bool(left_val <= right_val)),
                    BinaryOp::Gt => Ok(Value::Bool(left_val > right_val)),
                    BinaryOp::GtEq => Ok(Value::Bool(left_val >= right_val)),
                    BinaryOp::And => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l && r))
                    }
                    BinaryOp::Or => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l || r))
                    }
                    _ => {
                        let evaluator = IrEvaluator::new(schema);
                        evaluator.evaluate(expr, record)
                    }
                }
            }
            Expr::UnaryOp {
                op: yachtsql_ir::UnaryOp::Not,
                expr: inner,
            } => {
                let val = Self::evaluate_qualify_predicate(
                    inner,
                    schema,
                    record,
                    row_idx,
                    window_results,
                )?;
                Ok(Value::Bool(!val.as_bool().unwrap_or(false)))
            }
            _ => {
                let evaluator = IrEvaluator::new(schema);
                evaluator.evaluate(expr, record)
            }
        }
    }

    fn collect_window_exprs(expr: &Expr) -> Vec<Expr> {
        let mut exprs = Vec::new();
        Self::collect_window_exprs_inner(expr, &mut exprs);
        exprs
    }

    fn collect_window_exprs_inner(expr: &Expr, exprs: &mut Vec<Expr>) {
        match expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => {
                exprs.push(expr.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_window_exprs_inner(left, exprs);
                Self::collect_window_exprs_inner(right, exprs);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_window_exprs_inner(expr, exprs);
            }
            _ => {}
        }
    }

    fn expr_has_window_function(expr: &Expr) -> bool {
        match expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_window_function(left) || Self::expr_has_window_function(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_has_window_function(expr),
            _ => false,
        }
    }

    fn extract_qualify_window_spec(
        expr: &Expr,
    ) -> Result<(
        Vec<Expr>,
        Vec<SortExpr>,
        Option<WindowFrame>,
        WindowFuncType,
    )> {
        match expr {
            Expr::Window {
                func,
                partition_by,
                order_by,
                frame,
                ..
            } => Ok((
                partition_by.clone(),
                order_by.clone(),
                frame.clone(),
                WindowFuncType::Window(*func),
            )),
            Expr::AggregateWindow {
                func,
                partition_by,
                order_by,
                frame,
                ..
            } => Ok((
                partition_by.clone(),
                order_by.clone(),
                frame.clone(),
                WindowFuncType::Aggregate(*func),
            )),
            _ => panic!("Expected window expression in qualify"),
        }
    }

    pub(crate) fn execute_values(
        &mut self,
        values: &[Vec<Expr>],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let result_schema = plan_schema_to_schema(schema);
        let empty_schema = Schema::new();
        let evaluator = IrEvaluator::new(&empty_schema)
            .with_variables(&self.variables)
            .with_system_variables(&self.system_variables)
            .with_user_functions(&self.user_function_defs);
        let empty_record = Record::new();
        let mut result = Table::empty(result_schema);

        for row_exprs in values {
            let mut row = Vec::new();
            for expr in row_exprs {
                let val = evaluator.evaluate(expr, &empty_record)?;
                row.push(val);
            }
            result.push_row(row)?;
        }

        Ok(result)
    }

    fn eval_expr_with_subqueries(
        &mut self,
        expr: &Expr,
        schema: &Schema,
        record: &Record,
    ) -> Result<Value> {
        match expr {
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) => {
                self.eval_scalar_subquery(plan, schema, record)
            }
            Expr::Exists { subquery, negated } => {
                let has_rows = self.eval_exists_subquery(subquery, schema, record)?;
                Ok(Value::Bool(if *negated { !has_rows } else { has_rows }))
            }
            Expr::ArraySubquery(plan) => self.eval_array_subquery(plan, schema, record),
            Expr::InSubquery {
                expr: inner_expr,
                subquery,
                negated,
            } => {
                let val = self.eval_expr_with_subqueries(inner_expr, schema, record)?;
                let in_result = self.eval_value_in_subquery(&val, subquery, schema, record)?;
                Ok(Value::Bool(if *negated { !in_result } else { in_result }))
            }
            Expr::InUnnest {
                expr: inner_expr,
                array_expr,
                negated,
            } => {
                let val = self.eval_expr_with_subqueries(inner_expr, schema, record)?;
                let array_val = self.eval_expr_with_subqueries(array_expr, schema, record)?;
                let in_result = if let Value::Array(arr) = array_val {
                    arr.contains(&val)
                } else {
                    false
                };
                Ok(Value::Bool(if *negated { !in_result } else { in_result }))
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_expr_with_subqueries(left, schema, record)?;
                let right_val = self.eval_expr_with_subqueries(right, schema, record)?;
                self.eval_binary_op_values(left_val, *op, right_val)
            }
            Expr::UnaryOp { op, expr: inner } => {
                let val = self.eval_expr_with_subqueries(inner, schema, record)?;
                self.eval_unary_op_value(*op, val)
            }
            Expr::ScalarFunction { name, args } => {
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| self.eval_expr_with_subqueries(a, schema, record))
                    .collect::<Result<_>>()?;
                let evaluator = IrEvaluator::new(schema)
                    .with_variables(&self.variables)
                    .with_system_variables(&self.system_variables)
                    .with_user_functions(&self.user_function_defs);
                evaluator.eval_scalar_function_with_values(name, &arg_vals)
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let val = self.eval_expr_with_subqueries(inner, schema, record)?;
                IrEvaluator::cast_value(val, data_type, *safe)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                let operand_val = operand
                    .as_ref()
                    .map(|e| self.eval_expr_with_subqueries(e, schema, record))
                    .transpose()?;

                for clause in when_clauses {
                    let condition_val = if let Some(op_val) = &operand_val {
                        let cond_val =
                            self.eval_expr_with_subqueries(&clause.condition, schema, record)?;
                        Value::Bool(op_val == &cond_val)
                    } else {
                        self.eval_expr_with_subqueries(&clause.condition, schema, record)?
                    };

                    if matches!(condition_val, Value::Bool(true)) {
                        return self.eval_expr_with_subqueries(&clause.result, schema, record);
                    }
                }

                if let Some(else_expr) = else_result {
                    self.eval_expr_with_subqueries(else_expr, schema, record)
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::Alias { expr: inner, .. } => {
                self.eval_expr_with_subqueries(inner, schema, record)
            }
            _ => {
                let evaluator = IrEvaluator::new(schema)
                    .with_variables(&self.variables)
                    .with_system_variables(&self.system_variables)
                    .with_user_functions(&self.user_function_defs);
                evaluator.evaluate(expr, record)
            }
        }
    }

    fn eval_scalar_subquery(
        &mut self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let substituted = self.substitute_outer_refs_in_plan(plan, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

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

    fn eval_scalar_subquery_as_row(&mut self, plan: &LogicalPlan) -> Result<Value> {
        let physical = optimize(plan)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

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

    fn eval_exists_subquery(
        &mut self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<bool> {
        let substituted = self.substitute_outer_refs_in_plan(plan, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;
        Ok(!result_table.is_empty())
    }

    fn eval_array_subquery(
        &mut self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let substituted = self.substitute_outer_refs_in_plan(plan, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

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

    fn eval_value_in_subquery(
        &mut self,
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
        let result_table = self.execute_plan(&executor_plan)?;

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

    fn resolve_subqueries_in_expr(&mut self, expr: &Expr) -> Result<Expr> {
        match expr {
            Expr::InSubquery {
                expr: inner_expr,
                subquery,
                negated,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner_expr)?;
                let empty_schema = Schema::new();
                let empty_record = Record::new();
                let substituted =
                    self.substitute_outer_refs_in_plan(subquery, &empty_schema, &empty_record)?;
                let physical = optimize(&substituted)?;
                let executor_plan = PhysicalPlan::from_physical(&physical);
                let result_table = self.execute_plan(&executor_plan)?;

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
                let result_table = self.execute_plan(&executor_plan)?;
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
                let result_table = self.execute_plan(&executor_plan)?;

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
                let result_table = self.execute_plan(&executor_plan)?;

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
                let resolved_left = self.resolve_subqueries_in_expr(left)?;
                let resolved_right = self.resolve_subqueries_in_expr(right)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(resolved_left),
                    op: *op,
                    right: Box::new(resolved_right),
                })
            }
            Expr::UnaryOp { op, expr: inner } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner)?;
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
                let resolved_inner = self.resolve_subqueries_in_expr(inner)?;
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
                let resolved_operand = operand
                    .as_ref()
                    .map(|e| self.resolve_subqueries_in_expr(e))
                    .transpose()?
                    .map(Box::new);

                let mut resolved_clauses = Vec::new();
                for clause in when_clauses {
                    resolved_clauses.push(yachtsql_ir::WhenClause {
                        condition: self.resolve_subqueries_in_expr(&clause.condition)?,
                        result: self.resolve_subqueries_in_expr(&clause.result)?,
                    });
                }

                let resolved_else = else_result
                    .as_ref()
                    .map(|e| self.resolve_subqueries_in_expr(e))
                    .transpose()?
                    .map(Box::new);

                Ok(Expr::Case {
                    operand: resolved_operand,
                    when_clauses: resolved_clauses,
                    else_result: resolved_else,
                })
            }
            Expr::ScalarFunction { name, args } => {
                let resolved_args = args
                    .iter()
                    .map(|a| self.resolve_subqueries_in_expr(a))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::ScalarFunction {
                    name: name.clone(),
                    args: resolved_args,
                })
            }
            Expr::Alias { expr: inner, name } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner)?;
                Ok(Expr::Alias {
                    expr: Box::new(resolved_inner),
                    name: name.clone(),
                })
            }
            Expr::IsNull {
                expr: inner,
                negated,
            } => {
                let resolved_inner = self.resolve_subqueries_in_expr(inner)?;
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
                let resolved_inner = self.resolve_subqueries_in_expr(inner)?;
                let resolved_list = list
                    .iter()
                    .map(|e| self.resolve_subqueries_in_expr(e))
                    .collect::<Result<Vec<_>>>()?;
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
                let resolved_inner = self.resolve_subqueries_in_expr(inner)?;
                let resolved_low = self.resolve_subqueries_in_expr(low)?;
                let resolved_high = self.resolve_subqueries_in_expr(high)?;
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
