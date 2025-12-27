mod aggregate;
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
                ..
            } => self.execute_nested_loop_join(left, right, join_type, condition.as_ref(), schema),
            PhysicalPlan::CrossJoin {
                left,
                right,
                schema,
                ..
            } => self.execute_cross_join(left, right, schema),
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
                ..
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
                ..
            } => self.execute_union(inputs, *all, schema),
            PhysicalPlan::Intersect {
                left,
                right,
                all,
                schema,
                ..
            } => self.execute_intersect(left, right, *all, schema),
            PhysicalPlan::Except {
                left,
                right,
                all,
                schema,
                ..
            } => self.execute_except(left, right, *all, schema),
            PhysicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => self.execute_window(input, window_exprs, schema),
            PhysicalPlan::WithCte { ctes, body, .. } => self.execute_cte(ctes, body),
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
            PhysicalPlan::Return { value } => {
                Err(Error::InvalidQuery("RETURN outside of function".into()))
            }
            PhysicalPlan::Raise { message, level } => self.execute_raise(message.as_ref(), *level),
            PhysicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            } => self.execute_execute_immediate(sql_expr, into_variables, using_params),
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
            PhysicalPlan::BeginTransaction => {
                self.catalog.begin_transaction();
                Ok(Table::empty(Schema::new()))
            }
            PhysicalPlan::Commit => {
                self.catalog.commit();
                Ok(Table::empty(Schema::new()))
            }
            PhysicalPlan::Rollback => {
                self.catalog.rollback();
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

    fn execute_try_catch(
        &mut self,
        try_block: &[(PhysicalPlan, Option<String>)],
        catch_block: &[PhysicalPlan],
    ) -> Result<Table> {
        let mut last_result = Table::empty(Schema::new());

        for (plan, source_sql) in try_block {
            match self.execute_plan(plan) {
                Ok(result) => {
                    last_result = result;
                }
                Err(e) => {
                    let error_message = e.to_string();
                    let stmt_text = source_sql.clone().unwrap_or_else(|| format!("{:?}", plan));
                    let error_struct = Value::Struct(vec![
                        ("message".to_string(), Value::String(error_message.clone())),
                        ("statement_text".to_string(), Value::String(stmt_text)),
                    ]);
                    self.variables
                        .insert("@@ERROR".to_string(), error_struct.clone());
                    self.session.set_system_variable("@@error", error_struct);

                    for catch_plan in catch_block {
                        last_result = self.execute_plan(catch_plan)?;
                    }
                    return Ok(last_result);
                }
            }
        }

        Ok(last_result)
    }

    fn execute_assert(&mut self, condition: &Expr, message: Option<&Expr>) -> Result<Table> {
        use yachtsql_storage::Record;

        let empty_schema = Schema::new();
        let empty_record = Record::new();

        let result = if Self::assert_expr_contains_subquery(condition) {
            self.eval_assert_expr_with_subqueries(condition, &empty_schema, &empty_record)?
        } else {
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_system_variables(self.session.system_variables())
                .with_user_functions(&self.user_function_defs);
            evaluator.evaluate(condition, &empty_record)?
        };

        match result {
            Value::Bool(true) => Ok(Table::empty(Schema::new())),
            Value::Bool(false) => {
                let msg = if let Some(msg_expr) = message {
                    let evaluator = IrEvaluator::new(&empty_schema)
                        .with_variables(&self.variables)
                        .with_system_variables(self.session.system_variables())
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

    fn assert_expr_contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Subquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::ArraySubquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::assert_expr_contains_subquery(left)
                    || Self::assert_expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::assert_expr_contains_subquery(expr),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|o| Self::assert_expr_contains_subquery(o))
                    || when_clauses.iter().any(|w| {
                        Self::assert_expr_contains_subquery(&w.condition)
                            || Self::assert_expr_contains_subquery(&w.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::assert_expr_contains_subquery(e))
            }
            _ => false,
        }
    }

    fn eval_assert_expr_with_subqueries(
        &mut self,
        expr: &Expr,
        schema: &Schema,
        record: &yachtsql_storage::Record,
    ) -> Result<Value> {
        use yachtsql_optimizer::optimize;

        match expr {
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) => {
                let physical = optimize(plan)?;
                let executor_plan = PhysicalPlan::from_physical(&physical);
                let result_table = self.execute_plan(&executor_plan)?;
                if result_table.is_empty() {
                    return Ok(Value::Null);
                }
                let rows: Vec<_> = result_table.rows()?.into_iter().collect();
                if rows.is_empty() || rows[0].values().is_empty() {
                    return Ok(Value::Null);
                }
                Ok(rows[0].values()[0].clone())
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_assert_expr_with_subqueries(left, schema, record)?;
                let right_val = self.eval_assert_expr_with_subqueries(right, schema, record)?;
                let evaluator = IrEvaluator::new(schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables());
                evaluator.eval_binary_op_with_values(*op, left_val, right_val)
            }
            _ => {
                let evaluator = IrEvaluator::new(schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables())
                    .with_user_functions(&self.user_function_defs);
                evaluator.evaluate(expr, record)
            }
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

    fn execute_gap_fill(
        &mut self,
        input: &PhysicalPlan,
        ts_column: &str,
        bucket_width: &Expr,
        value_columns: &[yachtsql_ir::GapFillColumn],
        partitioning_columns: &[String],
        origin: Option<&Expr>,
        input_schema: &yachtsql_ir::PlanSchema,
        schema: &yachtsql_ir::PlanSchema,
    ) -> Result<Table> {
        use std::collections::BTreeMap;

        use yachtsql_common::types::DataType;
        use yachtsql_ir::GapFillStrategy;
        use yachtsql_storage::Record;

        let input_table = self.execute_plan(input)?;
        let storage_schema = input_table.schema();

        let ts_idx = input_schema
            .fields
            .iter()
            .position(|f| f.name.to_uppercase() == ts_column.to_uppercase())
            .ok_or_else(|| Error::invalid_query(format!("Column not found: {}", ts_column)))?;

        let partition_indices: Vec<usize> = partitioning_columns
            .iter()
            .filter_map(|p| {
                input_schema
                    .fields
                    .iter()
                    .position(|f| f.name.to_uppercase() == p.to_uppercase())
            })
            .collect();

        let value_col_indices: Vec<(usize, GapFillStrategy)> = value_columns
            .iter()
            .filter_map(|vc| {
                input_schema
                    .fields
                    .iter()
                    .position(|f| f.name.to_uppercase() == vc.column_name.to_uppercase())
                    .map(|idx| (idx, vc.strategy))
            })
            .collect();

        let evaluator = IrEvaluator::new(storage_schema)
            .with_variables(&self.variables)
            .with_system_variables(self.session.system_variables())
            .with_user_functions(&self.user_function_defs);
        let empty_record = Record::new();

        let bucket_interval = evaluator.evaluate(bucket_width, &empty_record)?;
        let bucket_millis = match bucket_interval {
            Value::Interval(ref interval) => {
                (interval.months as i64 * 30 * 24 * 60 * 60 * 1000)
                    + (interval.days as i64 * 24 * 60 * 60 * 1000)
                    + (interval.nanos / 1_000_000)
            }
            _ => {
                return Err(Error::invalid_query("bucket_width must be an interval"));
            }
        };

        let origin_offset = if let Some(origin_expr) = origin {
            let origin_val = evaluator.evaluate(origin_expr, &empty_record)?;
            match origin_val {
                Value::DateTime(d) => d.and_utc().timestamp_millis() % bucket_millis,
                Value::Timestamp(d) => d.timestamp_millis() % bucket_millis,
                _ => 0,
            }
        } else {
            0
        };

        let mut partitions: BTreeMap<Vec<Value>, Vec<(i64, Vec<Value>)>> = BTreeMap::new();

        for row in input_table.rows()? {
            let row_values = row.values().to_vec();
            let ts_val = &row_values[ts_idx];

            let ts_millis = match ts_val {
                Value::DateTime(d) => d.and_utc().timestamp_millis(),
                Value::Timestamp(d) => d.timestamp_millis(),
                Value::Date(d) => d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis(),
                _ => continue,
            };

            let partition_key: Vec<Value> = partition_indices
                .iter()
                .map(|&idx| row_values[idx].clone())
                .collect();

            let values_for_row: Vec<Value> = value_col_indices
                .iter()
                .map(|(idx, _)| row_values[*idx].clone())
                .collect();

            partitions
                .entry(partition_key)
                .or_default()
                .push((ts_millis, values_for_row));
        }

        let output_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(output_schema.clone());

        for (partition_key, mut entries) in partitions {
            entries.sort_by_key(|(ts, _)| *ts);

            if entries.is_empty() {
                continue;
            }

            let min_original_ts = entries.first().map(|(ts, _)| *ts).unwrap();
            let max_original_ts = entries.last().map(|(ts, _)| *ts).unwrap();

            let min_bucket = {
                let floored = ((min_original_ts - origin_offset) / bucket_millis) * bucket_millis
                    + origin_offset;
                if floored < min_original_ts {
                    floored + bucket_millis
                } else {
                    floored
                }
            };
            let max_bucket =
                ((max_original_ts - origin_offset) / bucket_millis) * bucket_millis + origin_offset;

            let mut exact_match_map: BTreeMap<i64, Vec<Value>> = BTreeMap::new();
            for (ts, values) in &entries {
                let bucket_ts =
                    ((*ts - origin_offset) / bucket_millis) * bucket_millis + origin_offset;
                if *ts == bucket_ts {
                    exact_match_map.insert(bucket_ts, values.clone());
                }
            }

            let mut last_values: Vec<Option<Value>> = vec![None; value_col_indices.len()];

            let mut bucket = min_bucket;
            while bucket <= max_bucket {
                for (ts, values) in &entries {
                    if *ts <= bucket {
                        for (i, val) in values.iter().enumerate() {
                            if !val.is_null() {
                                last_values[i] = Some(val.clone());
                            }
                        }
                    }
                }

                let row_values = if let Some(existing) = exact_match_map.get(&bucket) {
                    existing.clone()
                } else {
                    value_col_indices
                        .iter()
                        .enumerate()
                        .map(|(i, (_, strategy))| match strategy {
                            GapFillStrategy::Null => Value::null(),
                            GapFillStrategy::Locf => {
                                last_values[i].clone().unwrap_or(Value::null())
                            }
                            GapFillStrategy::Linear => {
                                let prev_entry =
                                    entries.iter().filter(|(ts, _)| *ts < bucket).next_back();
                                let next_entry = entries.iter().find(|(ts, _)| *ts > bucket);

                                match (prev_entry, next_entry) {
                                    (Some((prev_ts, prev_vals)), Some((next_ts, next_vals))) => {
                                        interpolate_value(
                                            &prev_vals[i],
                                            &next_vals[i],
                                            *prev_ts,
                                            *next_ts,
                                            bucket,
                                        )
                                    }
                                    _ => Value::null(),
                                }
                            }
                        })
                        .collect()
                };

                let ts_value = match &input_schema.fields[ts_idx].data_type {
                    DataType::DateTime => Value::DateTime(
                        chrono::DateTime::from_timestamp_millis(bucket)
                            .unwrap()
                            .naive_utc(),
                    ),
                    DataType::Timestamp => {
                        Value::Timestamp(chrono::DateTime::from_timestamp_millis(bucket).unwrap())
                    }
                    _ => Value::DateTime(
                        chrono::DateTime::from_timestamp_millis(bucket)
                            .unwrap()
                            .naive_utc(),
                    ),
                };

                let mut record_values = vec![ts_value];
                record_values.extend(partition_key.clone());
                record_values.extend(row_values);

                result.push_row(record_values)?;

                bucket += bucket_millis;
            }
        }

        Ok(result)
    }
}

fn interpolate_value(
    prev: &Value,
    next: &Value,
    prev_ts: i64,
    next_ts: i64,
    current_ts: i64,
) -> Value {
    use ordered_float::OrderedFloat;
    let ratio = (current_ts - prev_ts) as f64 / (next_ts - prev_ts) as f64;

    match (prev, next) {
        (Value::Int64(p), Value::Int64(n)) => {
            let interpolated = *p as f64 + (*n as f64 - *p as f64) * ratio;
            Value::Int64(interpolated.round() as i64)
        }
        (Value::Float64(p), Value::Float64(n)) => {
            let p_val: f64 = (*p).into();
            let n_val: f64 = (*n).into();
            Value::Float64(OrderedFloat(p_val + (n_val - p_val) * ratio))
        }
        (Value::Int64(p), Value::Float64(n)) => {
            let n_val: f64 = (*n).into();
            Value::Float64(OrderedFloat(*p as f64 + (n_val - *p as f64) * ratio))
        }
        (Value::Float64(p), Value::Int64(n)) => {
            let p_val: f64 = (*p).into();
            Value::Float64(OrderedFloat(p_val + (*n as f64 - p_val) * ratio))
        }
        _ => Value::null(),
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
