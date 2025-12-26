use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Write as IoWrite};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{
    AlterTableOp, Assignment, BinaryOp, ColumnDef, CteDefinition, ExportFormat, ExportOptions, Expr,
    FunctionArg, FunctionBody, JoinType, LoadFormat, LoadOptions, MergeClause, PlanSchema,
    ProcedureArg, RaiseLevel, SetOperationType, SortExpr, UnnestColumn,
};
use yachtsql_ir::LogicalPlan;
use yachtsql_optimizer::{optimize, OptimizedLogicalPlan, SampleType};
use yachtsql_storage::{Field, FieldMode, Record, Schema, Table};

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

        Self {
            catalog,
            session,
            tables,
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
                alias: _,
                assignments,
                from: _,
                filter,
            } => self.execute_update(table_name, assignments, filter.as_ref()),
            PhysicalPlan::Delete {
                table_name,
                alias: _,
                filter,
            } => self.execute_delete(table_name, filter.as_ref()),
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
            PhysicalPlan::ExecuteImmediate { .. } => Err(Error::internal(
                "EXECUTE IMMEDIATE not yet implemented in concurrent executor",
            )),
            PhysicalPlan::Grant { .. } => Ok(Table::empty(Schema::new())),
            PhysicalPlan::Revoke { .. } => Ok(Table::empty(Schema::new())),
            PhysicalPlan::BeginTransaction => Err(Error::internal(
                "BEGIN TRANSACTION not yet implemented in concurrent executor",
            )),
            PhysicalPlan::Commit => Err(Error::internal(
                "COMMIT not yet implemented in concurrent executor",
            )),
            PhysicalPlan::Rollback => Err(Error::internal(
                "ROLLBACK not yet implemented in concurrent executor",
            )),
            PhysicalPlan::TryCatch { .. } => Err(Error::internal(
                "TRY/CATCH not yet implemented in concurrent executor",
            )),
            PhysicalPlan::GapFill { .. } => Err(Error::internal(
                "GAP_FILL not yet implemented in concurrent executor",
            )),
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

        if let Some(handle) = self.catalog.get_table_handle(table_name) {
            if let Ok(guard) = handle.read() {
                return Ok(self.apply_planned_schema(&guard, planned_schema));
            }
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

    pub(crate) fn execute_nested_loop_join(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        condition: Option<&Expr>,
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;
        let left_schema = left_table.schema().clone();
        let right_schema = right_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);

        let mut combined_schema = Schema::new();
        for field in left_schema.fields() {
            combined_schema.add_field(field.clone());
        }
        for field in right_schema.fields() {
            combined_schema.add_field(field.clone());
        }

        let evaluator = IrEvaluator::new(&combined_schema)
            .with_variables(&self.variables)
            .with_user_functions(&self.user_function_defs);

        let mut result = Table::empty(result_schema.clone());
        let left_rows = left_table.rows()?;
        let right_rows = right_table.rows()?;
        let left_width = left_schema.field_count();
        let right_width = right_schema.field_count();

        match join_type {
            JoinType::Inner => {
                for left_record in &left_rows {
                    for right_record in &right_rows {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(right_record.values().to_vec());
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            result.push_row(combined)?;
                        }
                    }
                }
            }
            JoinType::Left => {
                for left_record in &left_rows {
                    let mut found_match = false;
                    for right_record in &right_rows {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(right_record.values().to_vec());
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            found_match = true;
                            result.push_row(combined)?;
                        }
                    }
                    if !found_match {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(vec![Value::Null; right_width]);
                        result.push_row(combined)?;
                    }
                }
            }
            JoinType::Right => {
                for right_record in &right_rows {
                    let mut found_match = false;
                    for left_record in &left_rows {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(right_record.values().to_vec());
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            found_match = true;
                            result.push_row(combined)?;
                        }
                    }
                    if !found_match {
                        let mut combined = vec![Value::Null; left_width];
                        combined.extend(right_record.values().to_vec());
                        result.push_row(combined)?;
                    }
                }
            }
            JoinType::Full => {
                let mut matched_right: HashSet<usize> = HashSet::new();
                for left_record in &left_rows {
                    let mut found_match = false;
                    for (ri, right_record) in right_rows.iter().enumerate() {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(right_record.values().to_vec());
                        let combined_record = Record::from_values(combined.clone());

                        let matches = condition
                            .map(|c| evaluator.evaluate(c, &combined_record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            found_match = true;
                            matched_right.insert(ri);
                            result.push_row(combined)?;
                        }
                    }
                    if !found_match {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(vec![Value::Null; right_width]);
                        result.push_row(combined)?;
                    }
                }
                for (ri, right_record) in right_rows.iter().enumerate() {
                    if !matched_right.contains(&ri) {
                        let mut combined = vec![Value::Null; left_width];
                        combined.extend(right_record.values().to_vec());
                        result.push_row(combined)?;
                    }
                }
            }
            JoinType::Cross => {
                for left_record in &left_rows {
                    for right_record in &right_rows {
                        let mut combined = left_record.values().to_vec();
                        combined.extend(right_record.values().to_vec());
                        result.push_row(combined)?;
                    }
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_cross_join(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        schema: &PlanSchema,
    ) -> Result<Table> {
        self.execute_nested_loop_join(left, right, &JoinType::Cross, None, schema)
    }

    pub(crate) fn execute_hash_join(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        join_type: &JoinType,
        left_keys: &[Expr],
        right_keys: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let left_table = self.execute_plan(left)?;
        let right_table = self.execute_plan(right)?;
        let left_schema = left_table.schema().clone();
        let right_schema = right_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);

        match join_type {
            JoinType::Inner => {
                let left_evaluator = IrEvaluator::new(&left_schema)
                    .with_variables(&self.variables)
                    .with_user_functions(&self.user_function_defs);
                let right_evaluator = IrEvaluator::new(&right_schema)
                    .with_variables(&self.variables)
                    .with_user_functions(&self.user_function_defs);

                let right_rows = right_table.rows()?;
                let mut hash_table: HashMap<Vec<Value>, Vec<Record>> = HashMap::new();

                for right_record in right_rows {
                    let key_values: Vec<Value> = right_keys
                        .iter()
                        .map(|expr| right_evaluator.evaluate(expr, &right_record))
                        .collect::<Result<Vec<_>>>()?;

                    let has_null = key_values.iter().any(|v| matches!(v, Value::Null));
                    if has_null {
                        continue;
                    }

                    hash_table.entry(key_values).or_default().push(right_record);
                }

                let mut result = Table::empty(result_schema);
                for left_record in left_table.rows()? {
                    let key_values: Vec<Value> = left_keys
                        .iter()
                        .map(|expr| left_evaluator.evaluate(expr, &left_record))
                        .collect::<Result<Vec<_>>>()?;

                    let has_null = key_values.iter().any(|v| matches!(v, Value::Null));
                    if has_null {
                        continue;
                    }

                    if let Some(matching_rows) = hash_table.get(&key_values) {
                        for right_record in matching_rows {
                            let mut combined = left_record.values().to_vec();
                            combined.extend(right_record.values().to_vec());
                            result.push_row(combined)?;
                        }
                    }
                }

                Ok(result)
            }
            _ => {
                panic!("HashJoin only supports Inner join type currently");
            }
        }
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

    fn split_recursive_cte(query: &LogicalPlan, cte_name: &str) -> (Vec<LogicalPlan>, Vec<LogicalPlan>) {
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
            LogicalPlan::Scan { table_name: name, .. } => name.eq_ignore_ascii_case(table_name),
            LogicalPlan::Filter { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Project { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Aggregate { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Join { left, right, .. } => {
                Self::references_table(left, table_name) || Self::references_table(right, table_name)
            }
            LogicalPlan::Sort { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Limit { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::Distinct { input, .. } => Self::references_table(input, table_name),
            LogicalPlan::SetOperation { left, right, .. } => {
                Self::references_table(left, table_name) || Self::references_table(right, table_name)
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
        self.execute_filter(input, predicate)
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

    pub(crate) fn execute_insert(
        &mut self,
        table_name: &str,
        columns: &[String],
        source: &PhysicalPlan,
    ) -> Result<Table> {
        let target = self
            .tables
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        let target_schema = target.schema().clone();
        let fields = target_schema.fields().to_vec();

        let evaluator = IrEvaluator::new(&target_schema)
            .with_variables(&self.variables)
            .with_user_functions(&self.user_function_defs);
        let empty_record = Record::new();

        let mut default_values: Vec<Option<Value>> = vec![None; target_schema.field_count()];
        if let Some(defaults) = self.catalog.get_table_defaults(table_name) {
            for default in defaults {
                if let Some(idx) = target_schema.field_index(&default.column_name)
                    && let Ok(val) = evaluator.evaluate(&default.default_expr, &empty_record)
                {
                    default_values[idx] = Some(val);
                }
            }
        }

        if let PhysicalPlan::Values { values, .. } = source {
            let empty_schema = Schema::new();
            let values_evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_user_functions(&self.user_function_defs);
            let empty_rec = Record::new();

            let mut all_rows: Vec<Vec<Value>> = Vec::new();

            for row_exprs in values {
                if columns.is_empty() {
                    let mut coerced_row = Vec::with_capacity(target_schema.field_count());
                    for (i, expr) in row_exprs.iter().enumerate() {
                        if i < fields.len() {
                            let final_val = match expr {
                                Expr::Default => default_values[i].clone().unwrap_or(Value::Null),
                                _ => values_evaluator.evaluate(expr, &empty_rec)?,
                            };
                            coerced_row.push(coerce_value(final_val, &fields[i].data_type)?);
                        } else {
                            coerced_row.push(values_evaluator.evaluate(expr, &empty_rec)?);
                        }
                    }
                    all_rows.push(coerced_row);
                } else {
                    let mut row: Vec<Value> = default_values
                        .iter()
                        .map(|opt| opt.clone().unwrap_or(Value::Null))
                        .collect();
                    for (i, col_name) in columns.iter().enumerate() {
                        if let Some(col_idx) = target_schema.field_index(col_name)
                            && i < row_exprs.len()
                            && col_idx < fields.len()
                        {
                            let expr = &row_exprs[i];
                            let final_val = match expr {
                                Expr::Default => default_values[col_idx].clone().unwrap_or(Value::Null),
                                _ => values_evaluator.evaluate(expr, &empty_rec)?,
                            };
                            row[col_idx] = coerce_value(final_val, &fields[col_idx].data_type)?;
                        }
                    }
                    all_rows.push(row);
                }
            }

            let target = self
                .tables
                .get_table_mut(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
            for row in all_rows {
                target.push_row(row)?;
            }

            return Ok(Table::empty(Schema::new()));
        }

        let source_table = self.execute_plan(source)?;

        let target = self
            .tables
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        for record in source_table.rows()? {
            if columns.is_empty() {
                let mut coerced_row = Vec::with_capacity(fields.len());
                for (i, val) in record.values().iter().enumerate() {
                    if i < fields.len() {
                        let final_val = match val {
                            Value::Default => default_values[i].clone().unwrap_or(Value::Null),
                            _ => val.clone(),
                        };
                        coerced_row.push(coerce_value(final_val, &fields[i].data_type)?);
                    } else {
                        coerced_row.push(val.clone());
                    }
                }
                target.push_row(coerced_row)?;
            } else {
                let mut row: Vec<Value> = default_values
                    .iter()
                    .map(|opt| opt.clone().unwrap_or(Value::Null))
                    .collect();
                for (i, col_name) in columns.iter().enumerate() {
                    if let Some(col_idx) = target_schema.field_index(col_name)
                        && i < record.values().len()
                        && col_idx < fields.len()
                    {
                        let val = &record.values()[i];
                        let final_val = match val {
                            Value::Default => default_values[col_idx].clone().unwrap_or(Value::Null),
                            _ => val.clone(),
                        };
                        row[col_idx] = coerce_value(final_val, &fields[col_idx].data_type)?;
                    }
                }
                target.push_row(row)?;
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_update(
        &mut self,
        table_name: &str,
        assignments: &[Assignment],
        filter: Option<&Expr>,
    ) -> Result<Table> {
        let table = self
            .tables
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .clone();
        let schema = table.schema().clone();
        let evaluator = IrEvaluator::new(&schema)
            .with_variables(&self.variables)
            .with_user_functions(&self.user_function_defs);

        let mut new_table = Table::empty(schema.clone());

        for record in table.rows()? {
            let matches = filter
                .map(|f| evaluator.evaluate(f, &record))
                .transpose()?
                .map(|v| v.as_bool().unwrap_or(false))
                .unwrap_or(true);

            if matches {
                let mut new_row = record.values().to_vec();
                for assignment in assignments {
                    if let Some(idx) = schema.field_index(&assignment.column) {
                        let val = evaluator.evaluate(&assignment.value, &record)?;
                        new_row[idx] = val;
                    }
                }
                new_table.push_row(new_row)?;
            } else {
                new_table.push_row(record.values().to_vec())?;
            }
        }

        let target = self
            .tables
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        *target = new_table;

        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_delete(
        &mut self,
        table_name: &str,
        filter: Option<&Expr>,
    ) -> Result<Table> {
        let table = self
            .tables
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .clone();
        let schema = table.schema().clone();
        let evaluator = IrEvaluator::new(&schema)
            .with_variables(&self.variables)
            .with_user_functions(&self.user_function_defs);

        let mut new_table = Table::empty(schema.clone());

        for record in table.rows()? {
            let matches = filter
                .map(|f| evaluator.evaluate(f, &record))
                .transpose()?
                .map(|v| v.as_bool().unwrap_or(false))
                .unwrap_or(true);

            if !matches {
                new_table.push_row(record.values().to_vec())?;
            }
        }

        let target = self
            .tables
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        *target = new_table;

        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_merge(
        &mut self,
        _target_table: &str,
        _source: &PhysicalPlan,
        _on: &Expr,
        _clauses: &[MergeClause],
    ) -> Result<Table> {
        Err(Error::internal(
            "MERGE not yet implemented in concurrent executor",
        ))
    }

    pub(crate) fn execute_truncate(&mut self, table_name: &str) -> Result<Table> {
        let table = self
            .tables
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        table.clear();
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_create_table(
        &mut self,
        table_name: &str,
        columns: &[ColumnDef],
        if_not_exists: bool,
        or_replace: bool,
        query: Option<&PhysicalPlan>,
    ) -> Result<Table> {
        if self.catalog.table_exists(table_name) {
            if if_not_exists {
                return Ok(Table::empty(Schema::new()));
            }
            if !or_replace {
                return Err(Error::invalid_query(format!(
                    "Table already exists: {}",
                    table_name
                )));
            }
        }

        if let Some(query_plan) = query {
            let result = self.execute_plan(query_plan)?;
            let schema = result.schema().clone();
            if or_replace && self.catalog.table_exists(table_name) {
                self.catalog.create_or_replace_table(table_name, result);
            } else {
                self.catalog.insert_table(table_name, result)?;
            }
            return Ok(Table::empty(schema));
        }

        let mut schema = Schema::new();
        let mut defaults = Vec::new();
        for col in columns {
            let mode = if col.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            let mut field = Field::new(&col.name, col.data_type.clone(), mode);
            if let Some(ref collation) = col.collation {
                field = field.with_collation(collation);
            }
            schema.add_field(field);
            if let Some(ref default_expr) = col.default_value {
                defaults.push(ColumnDefault {
                    column_name: col.name.clone(),
                    default_expr: default_expr.clone(),
                });
            }
        }

        if or_replace && self.catalog.table_exists(table_name) {
            self.catalog
                .create_or_replace_table(table_name, Table::new(schema));
        } else {
            self.catalog.create_table(table_name, schema)?;
        }

        if !defaults.is_empty() {
            self.catalog.set_table_defaults(table_name, defaults);
        }

        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_tables(
        &mut self,
        table_names: &[String],
        if_exists: bool,
    ) -> Result<Table> {
        for name in table_names {
            if self.catalog.table_exists(name) {
                self.catalog.drop_table(name)?;
            } else if !if_exists {
                return Err(Error::TableNotFound(name.clone()));
            }
        }
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_alter_table(
        &mut self,
        table_name: &str,
        operation: &AlterTableOp,
        if_exists: bool,
    ) -> Result<Table> {
        let table_exists = self.tables.get_table(table_name).is_some();
        if !table_exists {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::TableNotFound(table_name.to_string()));
        }

        match operation {
            AlterTableOp::AddColumn {
                column,
                if_not_exists,
            } => {
                let table = self
                    .tables
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

                if *if_not_exists && table.schema().field(&column.name).is_some() {
                    return Ok(Table::empty(Schema::new()));
                }

                let field = if column.nullable {
                    Field::nullable(column.name.clone(), column.data_type.clone())
                } else {
                    Field::required(column.name.clone(), column.data_type.clone())
                };

                let default_value = column.default_value.as_ref().and_then(|expr| {
                    let empty_schema = Schema::new();
                    let evaluator = IrEvaluator::new(&empty_schema)
                        .with_variables(&self.variables)
                        .with_user_functions(&self.user_function_defs);
                    evaluator.evaluate(expr, &Record::new()).ok()
                });

                use yachtsql_storage::TableSchemaOps;
                table.add_column(field, default_value)?;
            }
            AlterTableOp::DropColumn {
                name,
                if_exists: column_if_exists,
            } => {
                let table = self
                    .tables
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                if *column_if_exists && table.schema().field(name).is_none() {
                    return Ok(Table::empty(Schema::new()));
                }
                table.drop_column(name)?;
            }
            AlterTableOp::RenameColumn { old_name, new_name } => {
                let table = self
                    .tables
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                table.rename_column(old_name, new_name)?;
            }
            AlterTableOp::RenameTable { new_name } => {
                self.catalog.rename_table(table_name, new_name)?;
            }
            AlterTableOp::AlterColumn { name, action } => {
                let table = self
                    .tables
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

                match action {
                    yachtsql_ir::AlterColumnAction::SetNotNull => {
                        table.set_column_not_null(name)?;
                    }
                    yachtsql_ir::AlterColumnAction::DropNotNull => {
                        table.set_column_nullable(name)?;
                    }
                    yachtsql_ir::AlterColumnAction::SetDefault { default } => {
                        let empty_schema = Schema::new();
                        let evaluator = IrEvaluator::new(&empty_schema)
                            .with_variables(&self.variables)
                            .with_user_functions(&self.user_function_defs);
                        let value = evaluator.evaluate(default, &Record::new())?;
                        table.set_column_default(name, value)?;

                        let mut defaults = self
                            .catalog
                            .get_table_defaults(table_name)
                            .unwrap_or_default();
                        defaults.retain(|d| d.column_name.to_uppercase() != name.to_uppercase());
                        defaults.push(ColumnDefault {
                            column_name: name.clone(),
                            default_expr: default.clone(),
                        });
                        self.catalog.set_table_defaults(table_name, defaults);
                    }
                    yachtsql_ir::AlterColumnAction::DropDefault => {
                        table.drop_column_default(name)?;

                        let mut defaults = self
                            .catalog
                            .get_table_defaults(table_name)
                            .unwrap_or_default();
                        defaults.retain(|d| d.column_name.to_uppercase() != name.to_uppercase());
                        self.catalog.set_table_defaults(table_name, defaults);
                    }
                    yachtsql_ir::AlterColumnAction::SetDataType { data_type } => {
                        table.set_column_data_type(name, data_type.clone())?;
                    }
                    yachtsql_ir::AlterColumnAction::SetOptions { collation } => {
                        if let Some(coll) = collation {
                            table.set_column_collation(name, coll.clone())?;
                        }
                    }
                }
            }
            AlterTableOp::SetOptions { options: _ } => {}
            AlterTableOp::AddConstraint { constraint: _ } => {}
            AlterTableOp::DropConstraint { name: _ } => {}
            AlterTableOp::DropPrimaryKey => {}
        }
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_create_view(
        &mut self,
        name: &str,
        query_sql: &str,
        column_aliases: &[String],
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Table> {
        self.catalog.create_view(
            name,
            query_sql.to_string(),
            column_aliases.to_vec(),
            or_replace,
            if_not_exists,
        )?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_view(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        self.catalog.drop_view(name, if_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_create_schema(
        &mut self,
        name: &str,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<Table> {
        if or_replace && self.catalog.schema_exists(name) {
            self.catalog.drop_schema(name, true, true)?;
        }
        self.catalog.create_schema(name, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_schema(
        &mut self,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<Table> {
        self.catalog.drop_schema(name, if_exists, cascade)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_undrop_schema(
        &mut self,
        name: &str,
        if_not_exists: bool,
    ) -> Result<Table> {
        self.catalog.undrop_schema(name, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_alter_schema(
        &mut self,
        name: &str,
        options: &[(String, String)],
    ) -> Result<Table> {
        let opts: HashMap<String, String> = options.iter().cloned().collect();
        self.catalog.alter_schema_options(name, opts)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_create_function(
        &mut self,
        name: &str,
        args: &[FunctionArg],
        return_type: &DataType,
        body: &FunctionBody,
        or_replace: bool,
        if_not_exists: bool,
        is_temp: bool,
        is_aggregate: bool,
    ) -> Result<Table> {
        if self.catalog.function_exists(name) && !or_replace {
            if if_not_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::invalid_query(format!(
                "Function already exists: {}",
                name
            )));
        }

        let func = UserFunction {
            name: name.to_string(),
            parameters: args.to_vec(),
            return_type: return_type.clone(),
            body: body.clone(),
            is_temporary: is_temp,
            is_aggregate,
        };
        self.catalog.create_function(func, or_replace)?;
        self.refresh_user_functions();
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_function(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        if !self.catalog.function_exists(name) && !if_exists {
            return Err(Error::invalid_query(format!(
                "Function not found: {}",
                name
            )));
        }
        if self.catalog.function_exists(name) {
            self.catalog.drop_function(name)?;
            self.refresh_user_functions();
        }
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_create_procedure(
        &mut self,
        name: &str,
        args: &[ProcedureArg],
        body: &[PhysicalPlan],
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Table> {
        let proc = UserProcedure {
            name: name.to_string(),
            parameters: args.to_vec(),
            body: Vec::new(),
        };
        self.catalog
            .create_procedure(proc, or_replace, if_not_exists)?;
        self.catalog.set_procedure_body(name, body.to_vec());
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_procedure(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        if !self.catalog.procedure_exists(name) && !if_exists {
            return Err(Error::invalid_query(format!(
                "Procedure not found: {}",
                name
            )));
        }
        if self.catalog.procedure_exists(name) {
            self.catalog.drop_procedure(name)?;
        }
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_call(&mut self, procedure_name: &str, args: &[Expr]) -> Result<Table> {
        use yachtsql_ir::ProcedureArgMode;

        let proc = self
            .catalog
            .get_procedure(procedure_name)
            .ok_or_else(|| Error::InvalidQuery(format!("Procedure not found: {}", procedure_name)))?
            .clone();

        let empty_schema = Schema::new();
        let empty_record = Record::new();

        let mut out_var_mappings: Vec<(String, String)> = Vec::new();

        for (i, param) in proc.parameters.iter().enumerate() {
            let param_name = param.name.to_uppercase();

            match param.mode {
                ProcedureArgMode::In => {
                    let value = if let Some(arg_expr) = args.get(i) {
                        let evaluator = IrEvaluator::new(&empty_schema)
                            .with_variables(&self.variables)
                            .with_user_functions(&self.user_function_defs);
                        evaluator.evaluate(arg_expr, &empty_record)?
                    } else {
                        default_value_for_type(&param.data_type)
                    };
                    self.variables.insert(param_name.clone(), value.clone());
                    self.session.set_variable(&param_name, value);
                }
                ProcedureArgMode::Out => {
                    let value = default_value_for_type(&param.data_type);
                    self.variables.insert(param_name.clone(), value.clone());
                    self.session.set_variable(&param_name, value);

                    if let Some(Expr::Variable { name }) = args.get(i) {
                        out_var_mappings.push((param_name.clone(), name.clone()));
                    }
                }
                ProcedureArgMode::InOut => {
                    let value = if let Some(arg_expr) = args.get(i) {
                        let evaluator = IrEvaluator::new(&empty_schema)
                            .with_variables(&self.variables)
                            .with_user_functions(&self.user_function_defs);
                        evaluator.evaluate(arg_expr, &empty_record)?
                    } else {
                        default_value_for_type(&param.data_type)
                    };
                    self.variables.insert(param_name.clone(), value.clone());
                    self.session.set_variable(&param_name, value);

                    if let Some(Expr::Variable { name }) = args.get(i) {
                        out_var_mappings.push((param_name.clone(), name.clone()));
                    }
                }
            }
        }

        let mut last_result = Table::empty(Schema::new());

        let body_plans = self
            .catalog
            .get_procedure_body(procedure_name)
            .unwrap_or_default();
        for body_plan in &body_plans {
            last_result = self.execute_plan(body_plan)?;
        }

        for (param_name, var_name) in out_var_mappings {
            if let Some(value) = self.variables.get(&param_name).cloned() {
                let var_name_upper = var_name.to_uppercase();
                self.variables.insert(var_name_upper.clone(), value.clone());
                self.session.set_variable(&var_name_upper, value);
            }
        }

        Ok(last_result)
    }

    pub(crate) fn execute_export(
        &mut self,
        options: &ExportOptions,
        query: &PhysicalPlan,
    ) -> Result<Table> {
        let data = self.execute_plan(query)?;

        let is_cloud_uri = options.uri.starts_with("gs://")
            || options.uri.starts_with("s3://")
            || options.uri.starts_with("bigtable://")
            || options.uri.starts_with("pubsub://")
            || options.uri.starts_with("spanner://")
            || options.uri.contains("bigtable.googleapis.com")
            || options.uri.contains("pubsub.googleapis.com")
            || options.uri.contains("spanner.googleapis.com");

        if is_cloud_uri {
            return Ok(Table::empty(Schema::new()));
        }

        let path = if options.uri.starts_with("file://") {
            options.uri.strip_prefix("file://").unwrap().to_string()
        } else {
            options.uri.replace('*', "data")
        };

        match options.format {
            ExportFormat::Parquet => self.export_to_parquet(&data, &path),
            ExportFormat::Json => self.export_to_json(&data, &path),
            ExportFormat::Csv => self.export_to_csv(&data, &path, options),
            ExportFormat::Avro => Err(Error::UnsupportedFeature(
                "AVRO export not yet supported".into(),
            )),
        }
    }

    fn export_to_parquet(&self, data: &Table, path: &str) -> Result<Table> {
        let schema = data.schema();
        let num_rows = data.num_rows();

        let arrow_fields: Vec<ArrowField> = schema
            .fields()
            .iter()
            .map(|f| {
                let arrow_type = Self::data_type_to_arrow(&f.data_type);
                ArrowField::new(&f.name, arrow_type, f.is_nullable())
            })
            .collect();
        let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

        let arrays = Self::build_arrow_arrays(data, schema, num_rows)?;

        let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
            .map_err(|e| Error::internal(format!("Failed to create RecordBatch: {}", e)))?;

        let file = File::create(path)
            .map_err(|e| Error::internal(format!("Failed to create file '{}': {}", path, e)))?;
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None)
            .map_err(|e| Error::internal(format!("Failed to create Parquet writer: {}", e)))?;
        writer
            .write(&batch)
            .map_err(|e| Error::internal(format!("Failed to write Parquet data: {}", e)))?;
        writer
            .close()
            .map_err(|e| Error::internal(format!("Failed to close Parquet writer: {}", e)))?;

        Ok(Table::empty(Schema::new()))
    }

    fn export_to_json(&self, data: &Table, path: &str) -> Result<Table> {
        let schema = data.schema();
        let mut file = File::create(path)
            .map_err(|e| Error::internal(format!("Failed to create file '{}': {}", path, e)))?;

        for record in data.rows()? {
            let mut obj = serde_json::Map::new();
            for (i, field) in schema.fields().iter().enumerate() {
                let val = &record.values()[i];
                obj.insert(field.name.clone(), Self::value_to_json(val));
            }
            let line = serde_json::to_string(&obj)
                .map_err(|e| Error::internal(format!("JSON serialization error: {}", e)))?;
            writeln!(file, "{}", line)
                .map_err(|e| Error::internal(format!("Failed to write JSON: {}", e)))?;
        }

        Ok(Table::empty(Schema::new()))
    }

    fn export_to_csv(&self, data: &Table, path: &str, options: &ExportOptions) -> Result<Table> {
        let schema = data.schema();
        let delimiter = options
            .field_delimiter
            .as_ref()
            .and_then(|d| d.chars().next())
            .unwrap_or(',');

        let mut file = File::create(path)
            .map_err(|e| Error::internal(format!("Failed to create file '{}': {}", path, e)))?;

        if options.header.unwrap_or(false) {
            let header: Vec<&str> = schema.fields().iter().map(|f| f.name.as_str()).collect();
            writeln!(file, "{}", header.join(&delimiter.to_string()))
                .map_err(|e| Error::internal(format!("Failed to write CSV header: {}", e)))?;
        }

        for record in data.rows()? {
            let values: Vec<String> = record
                .values()
                .iter()
                .map(Self::value_to_csv_string)
                .collect();
            writeln!(file, "{}", values.join(&delimiter.to_string()))
                .map_err(|e| Error::internal(format!("Failed to write CSV row: {}", e)))?;
        }

        Ok(Table::empty(Schema::new()))
    }

    fn value_to_json(val: &Value) -> serde_json::Value {
        match val {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int64(n) => serde_json::Value::Number((*n).into()),
            Value::Float64(f) => serde_json::json!(f.0),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Date(d) => serde_json::Value::String(d.to_string()),
            Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
            Value::Timestamp(ts) => serde_json::Value::String(ts.to_rfc3339()),
            Value::Time(t) => serde_json::Value::String(t.to_string()),
            Value::Numeric(n) => serde_json::Value::String(n.to_string()),
            Value::BigNumeric(n) => serde_json::Value::String(n.to_string()),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(Self::value_to_json).collect())
            }
            Value::Struct(fields) => {
                let obj: serde_json::Map<String, serde_json::Value> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), Self::value_to_json(v)))
                    .collect();
                serde_json::Value::Object(obj)
            }
            Value::Json(j) => j.clone(),
            Value::Bytes(_)
            | Value::Geography(_)
            | Value::Interval(_)
            | Value::Range(_)
            | Value::Default => serde_json::Value::String(format!("{:?}", val)),
        }
    }

    fn value_to_csv_string(val: &Value) -> String {
        match val {
            Value::Null => String::new(),
            Value::Bool(b) => b.to_string(),
            Value::Int64(n) => n.to_string(),
            Value::Float64(f) => f.0.to_string(),
            Value::String(s) => {
                if s.contains(',') || s.contains('"') || s.contains('\n') {
                    format!("\"{}\"", s.replace('"', "\"\""))
                } else {
                    s.clone()
                }
            }
            Value::Date(d) => d.to_string(),
            Value::DateTime(dt) => dt.to_string(),
            Value::Timestamp(ts) => ts.to_rfc3339(),
            Value::Time(t) => t.to_string(),
            Value::Numeric(n) => n.to_string(),
            Value::BigNumeric(n) => n.to_string(),
            Value::Bytes(_)
            | Value::Json(_)
            | Value::Array(_)
            | Value::Struct(_)
            | Value::Geography(_)
            | Value::Interval(_)
            | Value::Range(_)
            | Value::Default => format!("{:?}", val),
        }
    }

    fn data_type_to_arrow(data_type: &DataType) -> ArrowDataType {
        match data_type {
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::String => ArrowDataType::Utf8,
            DataType::Date => ArrowDataType::Date32,
            DataType::DateTime => {
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            DataType::Timestamp => ArrowDataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            DataType::Time => ArrowDataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            DataType::Bytes => ArrowDataType::Binary,
            DataType::Numeric(_) => ArrowDataType::Utf8,
            DataType::Unknown
            | DataType::BigNumeric
            | DataType::Geography
            | DataType::Json
            | DataType::Struct(_)
            | DataType::Array(_)
            | DataType::Interval
            | DataType::Range(_) => ArrowDataType::Utf8,
        }
    }

    fn build_arrow_arrays(
        data: &Table,
        schema: &Schema,
        _num_rows: usize,
    ) -> Result<Vec<ArrayRef>> {
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let array: ArrayRef = match &field.data_type {
                DataType::Bool => {
                    let mut builder = BooleanBuilder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        if let Value::Bool(b) = val {
                            builder.append_value(*b);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        match val {
                            Value::Int64(n) => builder.append_value(*n),
                            Value::Float64(f) => builder.append_value(f.0 as i64),
                            Value::Null
                            | Value::Bool(_)
                            | Value::Numeric(_)
                            | Value::BigNumeric(_)
                            | Value::String(_)
                            | Value::Bytes(_)
                            | Value::Date(_)
                            | Value::Time(_)
                            | Value::DateTime(_)
                            | Value::Timestamp(_)
                            | Value::Json(_)
                            | Value::Array(_)
                            | Value::Struct(_)
                            | Value::Geography(_)
                            | Value::Interval(_)
                            | Value::Range(_)
                            | Value::Default => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        match val {
                            Value::Float64(f) => builder.append_value(f.0),
                            Value::Int64(n) => builder.append_value(*n as f64),
                            Value::Null
                            | Value::Bool(_)
                            | Value::Numeric(_)
                            | Value::BigNumeric(_)
                            | Value::String(_)
                            | Value::Bytes(_)
                            | Value::Date(_)
                            | Value::Time(_)
                            | Value::DateTime(_)
                            | Value::Timestamp(_)
                            | Value::Json(_)
                            | Value::Array(_)
                            | Value::Struct(_)
                            | Value::Geography(_)
                            | Value::Interval(_)
                            | Value::Range(_)
                            | Value::Default => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::String
                | DataType::Unknown
                | DataType::Numeric(_)
                | DataType::BigNumeric
                | DataType::Json
                | DataType::Geography
                | DataType::Interval
                | DataType::Range(_)
                | DataType::Struct(_)
                | DataType::Array(_) => {
                    let mut builder = StringBuilder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        if val.is_null() {
                            builder.append_null();
                        } else {
                            builder.append_value(format!("{}", val));
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Date => {
                    let mut builder = Date32Builder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        match val {
                            Value::Date(d) => {
                                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                let days = d.signed_duration_since(epoch).num_days() as i32;
                                builder.append_value(days);
                            }
                            Value::Null
                            | Value::Bool(_)
                            | Value::Int64(_)
                            | Value::Float64(_)
                            | Value::Numeric(_)
                            | Value::BigNumeric(_)
                            | Value::String(_)
                            | Value::Bytes(_)
                            | Value::Time(_)
                            | Value::DateTime(_)
                            | Value::Timestamp(_)
                            | Value::Json(_)
                            | Value::Array(_)
                            | Value::Struct(_)
                            | Value::Geography(_)
                            | Value::Interval(_)
                            | Value::Range(_)
                            | Value::Default => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::DateTime | DataType::Timestamp => {
                    let mut builder = TimestampMicrosecondBuilder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        match val {
                            Value::DateTime(dt) => {
                                let micros = dt.and_utc().timestamp_micros();
                                builder.append_value(micros);
                            }
                            Value::Timestamp(ts) => {
                                let micros = ts.timestamp_micros();
                                builder.append_value(micros);
                            }
                            Value::Null
                            | Value::Bool(_)
                            | Value::Int64(_)
                            | Value::Float64(_)
                            | Value::Numeric(_)
                            | Value::BigNumeric(_)
                            | Value::String(_)
                            | Value::Bytes(_)
                            | Value::Date(_)
                            | Value::Time(_)
                            | Value::Json(_)
                            | Value::Array(_)
                            | Value::Struct(_)
                            | Value::Geography(_)
                            | Value::Interval(_)
                            | Value::Range(_)
                            | Value::Default => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Time | DataType::Bytes => {
                    let mut builder = StringBuilder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        if val.is_null() {
                            builder.append_null();
                        } else {
                            builder.append_value(format!("{}", val));
                        }
                    }
                    Arc::new(builder.finish())
                }
            };
            arrays.push(array);
        }

        Ok(arrays)
    }

    pub(crate) fn execute_load(
        &mut self,
        table_name: &str,
        options: &LoadOptions,
        temp_table: bool,
        temp_schema: Option<&Vec<ColumnDef>>,
    ) -> Result<Table> {
        if temp_table && let Some(col_defs) = temp_schema {
            let fields: Vec<Field> = col_defs
                .iter()
                .map(|col| Field::nullable(col.name.clone(), col.data_type.clone()))
                .collect();
            let schema = Schema::from_fields(fields);
            let _ = self.catalog.create_table(table_name, schema);
        }

        let schema = self
            .tables
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .schema()
            .clone();

        if options.overwrite
            && let Some(t) = self.tables.get_table_mut(table_name)
        {
            t.clear();
        }

        for uri in &options.uris {
            let (path, is_cloud_uri) = if uri.starts_with("file://") {
                (uri.strip_prefix("file://").unwrap().to_string(), false)
            } else if uri.starts_with("gs://") {
                (
                    uri.strip_prefix("gs://").unwrap().replace('*', "data"),
                    true,
                )
            } else if uri.starts_with("s3://") {
                (
                    uri.strip_prefix("s3://").unwrap().replace('*', "data"),
                    true,
                )
            } else {
                (uri.clone(), false)
            };

            if is_cloud_uri && !std::path::Path::new(&path).exists() {
                continue;
            }

            let rows = match options.format {
                LoadFormat::Parquet => self.load_parquet(&path, &schema)?,
                LoadFormat::Json => self.load_json(&path, &schema)?,
                LoadFormat::Csv => self.load_csv(&path, &schema, options)?,
                LoadFormat::Avro => {
                    return Err(Error::UnsupportedFeature(
                        "AVRO load not yet supported".into(),
                    ));
                }
            };

            let table = self
                .tables
                .get_table_mut(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

            for row in rows {
                table.push_row(row)?;
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn load_parquet(&self, path: &str, schema: &Schema) -> Result<Vec<Vec<Value>>> {
        use arrow::array::AsArray;

        let file = File::open(path)
            .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::internal(format!("Failed to read Parquet: {}", e)))?
            .build()
            .map_err(|e| Error::internal(format!("Failed to build Parquet reader: {}", e)))?;

        let mut rows = Vec::new();
        let target_columns: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
        let target_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type.clone())
            .collect();

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| Error::internal(format!("Failed to read batch: {}", e)))?;

            let parquet_schema = batch.schema();
            let parquet_columns: Vec<String> = parquet_schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect();

            let column_mapping: Vec<Option<usize>> = target_columns
                .iter()
                .map(|target_col| {
                    parquet_columns
                        .iter()
                        .position(|pc| pc.eq_ignore_ascii_case(target_col))
                })
                .collect();

            for row_idx in 0..batch.num_rows() {
                let mut row_values = Vec::with_capacity(target_columns.len());
                for (col_idx, parquet_col_idx) in column_mapping.iter().enumerate() {
                    let value = match parquet_col_idx {
                        Some(pci) => {
                            let array = batch.column(*pci);
                            self.arrow_array_to_value(array, row_idx, &target_types[col_idx])?
                        }
                        None => Value::null(),
                    };
                    row_values.push(value);
                }
                rows.push(row_values);
            }
        }

        Ok(rows)
    }

    fn arrow_array_to_value(
        &self,
        array: &ArrayRef,
        row_idx: usize,
        target_type: &DataType,
    ) -> Result<Value> {
        use arrow::array::AsArray;

        if array.is_null(row_idx) {
            return Ok(Value::null());
        }

        let value = match array.data_type() {
            ArrowDataType::Boolean => {
                let arr = array.as_boolean();
                Value::bool_val(arr.value(row_idx))
            }
            ArrowDataType::Int64 => {
                let arr = array.as_primitive::<arrow::datatypes::Int64Type>();
                Value::int64(arr.value(row_idx))
            }
            ArrowDataType::Int32 => {
                let arr = array.as_primitive::<arrow::datatypes::Int32Type>();
                Value::int64(arr.value(row_idx) as i64)
            }
            ArrowDataType::Float64 => {
                let arr = array.as_primitive::<arrow::datatypes::Float64Type>();
                Value::float64(arr.value(row_idx))
            }
            ArrowDataType::Float32 => {
                let arr = array.as_primitive::<arrow::datatypes::Float32Type>();
                Value::float64(arr.value(row_idx) as f64)
            }
            ArrowDataType::Utf8 => {
                let arr = array.as_string::<i32>();
                Value::string(arr.value(row_idx).to_string())
            }
            ArrowDataType::LargeUtf8 => {
                let arr = array.as_string::<i64>();
                Value::string(arr.value(row_idx).to_string())
            }
            ArrowDataType::Date32 => {
                let arr = array.as_primitive::<arrow::datatypes::Date32Type>();
                let days = arr.value(row_idx);
                let date =
                    chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163).unwrap_or_default();
                Value::date(date)
            }
            ArrowDataType::Date64 => {
                let arr = array.as_primitive::<arrow::datatypes::Date64Type>();
                let millis = arr.value(row_idx);
                let date = chrono::DateTime::from_timestamp_millis(millis)
                    .map(|dt| dt.naive_utc().date())
                    .unwrap_or_default();
                Value::date(date)
            }
            ArrowDataType::Timestamp(_, _) => {
                let arr = array.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
                let micros = arr.value(row_idx);
                match target_type {
                    DataType::DateTime => {
                        let dt = chrono::DateTime::from_timestamp_micros(micros)
                            .map(|dt| dt.naive_utc())
                            .unwrap_or_default();
                        Value::datetime(dt)
                    }
                    _ => {
                        let ts =
                            chrono::DateTime::from_timestamp_micros(micros).unwrap_or_default();
                        Value::timestamp(ts)
                    }
                }
            }
            _ => Value::string(format!("{:?}", array)),
        };

        Ok(value)
    }

    fn load_json(&self, path: &str, schema: &Schema) -> Result<Vec<Vec<Value>>> {
        let file = File::open(path)
            .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;
        let reader = BufReader::new(file);

        let target_columns: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
        let target_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type.clone())
            .collect();

        let mut rows = Vec::new();

        for line in reader.lines() {
            let line =
                line.map_err(|e| Error::internal(format!("Failed to read JSON line: {}", e)))?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let obj: HashMap<String, serde_json::Value> = serde_json::from_str(trimmed)
                .map_err(|e| Error::internal(format!("Failed to parse JSON: {}", e)))?;

            let mut row_values = Vec::with_capacity(target_columns.len());

            for (i, col_name) in target_columns.iter().enumerate() {
                let json_val = obj
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(col_name))
                    .map(|(_, v)| v);
                let value = match json_val {
                    Some(v) => self.json_to_value(v, &target_types[i])?,
                    None => Value::null(),
                };
                row_values.push(value);
            }

            rows.push(row_values);
        }

        Ok(rows)
    }

    fn json_to_value(&self, json_val: &serde_json::Value, target_type: &DataType) -> Result<Value> {
        match json_val {
            serde_json::Value::Null => Ok(Value::null()),
            serde_json::Value::Bool(b) => Ok(Value::bool_val(*b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    match target_type {
                        DataType::Float64 => Ok(Value::float64(i as f64)),
                        _ => Ok(Value::int64(i)),
                    }
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::float64(f))
                } else {
                    Ok(Value::null())
                }
            }
            serde_json::Value::String(s) => match target_type {
                DataType::Date => {
                    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .map_err(|e| Error::internal(format!("Invalid date: {}", e)))?;
                    Ok(Value::date(date))
                }
                DataType::DateTime => {
                    let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                        .map_err(|e| Error::internal(format!("Invalid datetime: {}", e)))?;
                    Ok(Value::datetime(dt))
                }
                DataType::Timestamp => {
                    let ts = chrono::DateTime::parse_from_rfc3339(s)
                        .map(|d| d.with_timezone(&chrono::Utc))
                        .map_err(|e| Error::internal(format!("Invalid timestamp: {}", e)))?;
                    Ok(Value::timestamp(ts))
                }
                _ => Ok(Value::string(s.clone())),
            },
            serde_json::Value::Array(arr) => {
                let values: Result<Vec<Value>> = arr
                    .iter()
                    .map(|v| self.json_to_value(v, &DataType::Unknown))
                    .collect();
                Ok(Value::array(values?))
            }
            serde_json::Value::Object(obj) => {
                let fields: Result<Vec<(String, Value)>> = obj
                    .iter()
                    .map(|(k, v)| {
                        let val = self.json_to_value(v, &DataType::Unknown)?;
                        Ok((k.clone(), val))
                    })
                    .collect();
                Ok(Value::Struct(fields?))
            }
        }
    }

    fn load_csv(
        &self,
        path: &str,
        schema: &Schema,
        options: &LoadOptions,
    ) -> Result<Vec<Vec<Value>>> {
        let file = File::open(path)
            .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;
        let reader = BufReader::new(file);

        let target_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type.clone())
            .collect();

        let delimiter = options
            .field_delimiter
            .as_ref()
            .and_then(|d| {
                if d == "\\t" || d == "\t" {
                    Some('\t')
                } else {
                    d.chars().next()
                }
            })
            .unwrap_or(',');

        let null_marker = options.null_marker.as_deref();
        let skip_rows = options.skip_leading_rows.unwrap_or(0) as usize;

        let mut rows = Vec::new();
        let lines = reader.lines();

        for (idx, line) in lines.enumerate() {
            if idx < skip_rows {
                continue;
            }

            let line =
                line.map_err(|e| Error::internal(format!("Failed to read CSV line: {}", e)))?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let parts = Self::parse_csv_line(trimmed, delimiter);
            let mut row_values = Vec::with_capacity(target_types.len());

            for (i, part) in parts.iter().enumerate() {
                if i >= target_types.len() {
                    break;
                }
                let value = if null_marker.is_some_and(|nm| part == nm) {
                    Value::null()
                } else {
                    self.csv_string_to_value(part, &target_types[i])?
                };
                row_values.push(value);
            }

            while row_values.len() < target_types.len() {
                row_values.push(Value::null());
            }

            rows.push(row_values);
        }

        Ok(rows)
    }

    fn parse_csv_line(line: &str, delimiter: char) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '"' {
                if in_quotes {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        current.push('"');
                    } else {
                        in_quotes = false;
                    }
                } else {
                    in_quotes = true;
                }
            } else if c == delimiter && !in_quotes {
                fields.push(current.trim().to_string());
                current = String::new();
            } else {
                current.push(c);
            }
        }
        fields.push(current.trim().to_string());
        fields
    }

    fn csv_string_to_value(&self, s: &str, target_type: &DataType) -> Result<Value> {
        if s.is_empty() {
            return Ok(Value::null());
        }

        match target_type {
            DataType::Bool => {
                let b = matches!(s.to_uppercase().as_str(), "TRUE" | "1" | "YES");
                Ok(Value::bool_val(b))
            }
            DataType::Int64 => {
                let n = s
                    .parse::<i64>()
                    .map_err(|_| Error::internal(format!("Invalid integer: {}", s)))?;
                Ok(Value::int64(n))
            }
            DataType::Float64 => {
                let f = s
                    .parse::<f64>()
                    .map_err(|_| Error::internal(format!("Invalid float: {}", s)))?;
                Ok(Value::float64(f))
            }
            DataType::Date => {
                let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|e| Error::internal(format!("Invalid date '{}': {}", s, e)))?;
                Ok(Value::date(date))
            }
            DataType::DateTime => {
                let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                    .map_err(|e| Error::internal(format!("Invalid datetime '{}': {}", s, e)))?;
                Ok(Value::datetime(dt))
            }
            DataType::Timestamp => {
                let ts = chrono::DateTime::parse_from_rfc3339(s)
                    .map(|d| d.with_timezone(&chrono::Utc))
                    .or_else(|_| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                            .or_else(|_| {
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                            })
                            .map(|dt| dt.and_utc())
                    })
                    .map_err(|e| Error::internal(format!("Invalid timestamp '{}': {}", s, e)))?;
                Ok(Value::timestamp(ts))
            }
            _ => Ok(Value::string(s.to_string())),
        }
    }

    pub(crate) fn execute_declare(
        &mut self,
        name: &str,
        data_type: &DataType,
        default: Option<&Expr>,
    ) -> Result<Table> {
        let value = if let Some(expr) = default {
            let empty_schema = Schema::new();
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_user_functions(&self.user_function_defs);
            let mut evaluated = evaluator.evaluate(expr, &Record::new())?;

            if let (DataType::Struct(type_fields), Value::Struct(value_fields)) =
                (data_type, &evaluated)
            {
                let named_fields: Vec<(String, Value)> = type_fields
                    .iter()
                    .zip(value_fields.iter())
                    .map(|(type_field, (_, v))| (type_field.name.clone(), v.clone()))
                    .collect();
                evaluated = Value::Struct(named_fields);
            }
            evaluated
        } else {
            default_value_for_type(data_type)
        };
        self.variables.insert(name.to_uppercase(), value.clone());
        self.session.set_variable(name, value);
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_set_variable(&mut self, name: &str, value: &Expr) -> Result<Table> {
        let empty_schema = Schema::new();
        let evaluator = IrEvaluator::new(&empty_schema)
            .with_variables(&self.variables)
            .with_user_functions(&self.user_function_defs);
        let val = evaluator.evaluate(value, &Record::new())?;

        if name.starts_with("@@") {
            self.session.set_system_variable(name, val);
        } else {
            self.variables.insert(name.to_uppercase(), val.clone());
            self.session.set_variable(name, val);
        }
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_set_multiple_variables(
        &mut self,
        names: &[String],
        value: &Expr,
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let evaluator = IrEvaluator::new(&empty_schema)
            .with_variables(&self.variables)
            .with_user_functions(&self.user_function_defs);
        let val = evaluator.evaluate(value, &Record::new())?;

        let field_values = match val {
            Value::Struct(fields) => fields,
            _ => {
                return Err(Error::invalid_query(
                    "SET multiple variables requires a STRUCT value",
                ));
            }
        };

        if field_values.len() != names.len() {
            return Err(Error::invalid_query(format!(
                "SET: number of struct fields ({}) doesn't match number of variables ({})",
                field_values.len(),
                names.len()
            )));
        }

        for (i, name) in names.iter().enumerate() {
            let field_val = field_values[i].1.clone();
            let upper_name = name.to_uppercase();
            self.variables.insert(upper_name.clone(), field_val.clone());
            self.session.set_variable(name, field_val);
        }

        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_if(
        &mut self,
        condition: &Expr,
        then_branch: &[PhysicalPlan],
        else_branch: Option<&[PhysicalPlan]>,
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let evaluator = IrEvaluator::new(&empty_schema)
            .with_variables(&self.variables)
            .with_user_functions(&self.user_function_defs);
        let cond = evaluator.evaluate(condition, &Record::new())?;

        let branch = if cond.as_bool().unwrap_or(false) {
            then_branch
        } else {
            else_branch.unwrap_or(&[])
        };

        let mut result = Table::empty(Schema::new());
        for stmt in branch {
            result = self.execute_plan(stmt)?;
        }
        Ok(result)
    }

    pub(crate) fn execute_while(
        &mut self,
        condition: &Expr,
        body: &[PhysicalPlan],
        label: Option<&str>,
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let mut result = Table::empty(Schema::new());
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 10000;

        'outer: loop {
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_user_functions(&self.user_function_defs);
            let cond = evaluator.evaluate(condition, &Record::new())?;

            if !cond.as_bool().unwrap_or(false) {
                break;
            }

            for stmt in body {
                match self.execute_plan(stmt) {
                    Ok(r) => result = r,
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        if msg == "BREAK outside of loop" {
                            return Ok(Table::empty(Schema::new()));
                        }
                        if let Some(lbl) = label
                            && msg == format!("BREAK:{}", lbl)
                        {
                            return Ok(Table::empty(Schema::new()));
                        }
                        return Err(Error::InvalidQuery(msg));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        if msg == "CONTINUE outside of loop" {
                            continue 'outer;
                        }
                        if let Some(lbl) = label
                            && msg == format!("CONTINUE:{}", lbl)
                        {
                            continue 'outer;
                        }
                        return Err(Error::InvalidQuery(msg));
                    }
                    Err(e) => return Err(e),
                }
            }

            iterations += 1;
            if iterations >= MAX_ITERATIONS {
                return Err(Error::invalid_query(
                    "WHILE loop exceeded maximum iterations",
                ));
            }
        }

        Ok(result)
    }

    #[allow(unused_assignments)]
    pub(crate) fn execute_loop(
        &mut self,
        body: &[PhysicalPlan],
        label: Option<&str>,
    ) -> Result<Table> {
        let mut result = Table::empty(Schema::new());
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 10000;

        'outer: loop {
            for stmt in body {
                match self.execute_plan(stmt) {
                    Ok(r) => result = r,
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        if let Some(lbl) = label
                            && (msg.contains(&format!("BREAK:{}", lbl))
                                || msg == "BREAK outside of loop")
                        {
                            return Ok(Table::empty(Schema::new()));
                        }
                        return Ok(Table::empty(Schema::new()));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        continue 'outer;
                    }
                    Err(e) => return Err(e),
                }
            }

            iterations += 1;
            if iterations >= MAX_ITERATIONS {
                return Err(Error::invalid_query("LOOP exceeded maximum iterations"));
            }
        }
    }

    pub(crate) fn execute_block(
        &mut self,
        body: &[PhysicalPlan],
        label: Option<&str>,
    ) -> Result<Table> {
        let mut last_result = Table::empty(Schema::new());
        for plan in body {
            match self.execute_plan(plan) {
                Ok(result) => {
                    last_result = result;
                }
                Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                    if let Some(lbl) = label
                        && msg == format!("BREAK:{}", lbl)
                    {
                        return Ok(last_result);
                    }
                    return Err(Error::InvalidQuery(msg));
                }
                Err(e) => return Err(e),
            }
        }
        Ok(last_result)
    }

    pub(crate) fn execute_repeat(
        &mut self,
        body: &[PhysicalPlan],
        until_condition: &Expr,
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let mut result = Table::empty(Schema::new());
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 10000;

        'outer: loop {
            for stmt in body {
                match self.execute_plan(stmt) {
                    Ok(r) => result = r,
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        return Ok(Table::empty(Schema::new()));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        continue 'outer;
                    }
                    Err(e) => return Err(e),
                }
            }

            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_user_functions(&self.user_function_defs);
            let cond = evaluator.evaluate(until_condition, &Record::new())?;

            if cond.as_bool().unwrap_or(false) {
                break;
            }

            iterations += 1;
            if iterations >= MAX_ITERATIONS {
                return Err(Error::invalid_query(
                    "REPEAT loop exceeded maximum iterations",
                ));
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_for(
        &mut self,
        variable: &str,
        query: &PhysicalPlan,
        body: &[PhysicalPlan],
    ) -> Result<Table> {
        let query_result = self.execute_plan(query)?;
        let mut result = Table::empty(Schema::new());

        for record in query_result.rows()? {
            if !record.values().is_empty() {
                self.variables
                    .insert(variable.to_uppercase(), record.values()[0].clone());
            }
            for stmt in body {
                result = self.execute_plan(stmt)?;
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_raise(
        &mut self,
        message: Option<&Expr>,
        level: RaiseLevel,
    ) -> Result<Table> {
        let msg = if let Some(expr) = message {
            let empty_schema = Schema::new();
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_user_functions(&self.user_function_defs);
            let val = evaluator.evaluate(expr, &Record::new())?;
            match val {
                Value::String(s) => s,
                _ => format!("{:?}", val),
            }
        } else {
            "Exception raised".to_string()
        };

        match level {
            RaiseLevel::Exception => Err(Error::raised_exception(msg)),
            RaiseLevel::Warning => Ok(Table::empty(Schema::new())),
            RaiseLevel::Notice => Ok(Table::empty(Schema::new())),
        }
    }

    pub(crate) fn execute_create_snapshot(
        &mut self,
        snapshot_name: &str,
        source_name: &str,
        if_not_exists: bool,
    ) -> Result<Table> {
        if self.catalog.table_exists(snapshot_name) {
            if if_not_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::invalid_query(format!(
                "Snapshot already exists: {}",
                snapshot_name
            )));
        }

        let source = self
            .tables
            .get_table(source_name)
            .ok_or_else(|| Error::TableNotFound(source_name.to_string()))?
            .clone();

        self.catalog.insert_table(snapshot_name, source)?;
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) fn execute_drop_snapshot(
        &mut self,
        snapshot_name: &str,
        if_exists: bool,
    ) -> Result<Table> {
        if !self.catalog.table_exists(snapshot_name) {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::TableNotFound(snapshot_name.to_string()));
        }
        self.catalog.drop_table(snapshot_name)?;
        Ok(Table::empty(Schema::new()))
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
                        let new_expr =
                            self.substitute_outer_refs_in_expr(&se.expr, outer_schema, outer_record)?;
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
                        let new_expr =
                            self.substitute_outer_refs_in_expr(&uc.expr, outer_schema, outer_record)?;
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
                let should_substitute = if let Some(tbl) = table {
                    outer_schema.fields().iter().any(|f| {
                        f.source_table
                            .as_ref()
                            .is_some_and(|src| src.eq_ignore_ascii_case(tbl))
                            && f.name.eq_ignore_ascii_case(name)
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

    fn expr_contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::Subquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::ArraySubquery(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery(left) || Self::expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery(expr),
            Expr::ScalarFunction { args, .. } => args.iter().any(Self::expr_contains_subquery),
            _ => false,
        }
    }
}

fn default_value_for_type(data_type: &DataType) -> Value {
    match data_type {
        DataType::Int64 => Value::Int64(0),
        DataType::Float64 => Value::Float64(ordered_float::OrderedFloat(0.0)),
        DataType::Bool => Value::Bool(false),
        DataType::String => Value::String(String::new()),
        _ => Value::Null,
    }
}
