use std::collections::{HashMap, HashSet};

use chrono::{DateTime, NaiveDate, NaiveTime, Timelike, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{
    AlterTableOp, Assignment, ColumnDef, CteDefinition, ExportOptions, Expr, FunctionArg,
    FunctionBody, JoinType, LoadOptions, MergeClause, PlanSchema, ProcedureArg, RaiseLevel,
    SortExpr, UnnestColumn,
};
use yachtsql_optimizer::{OptimizedLogicalPlan, SampleType};
use yachtsql_storage::{Field, FieldMode, Record, Schema, Table};

use crate::catalog::{ColumnDefault, UserFunction, UserProcedure};
use crate::concurrent_catalog::{ConcurrentCatalog, TableLockSet};
use crate::concurrent_session::ConcurrentSession;
use crate::executor::plan_schema_to_schema;
use crate::ir_evaluator::{IrEvaluator, UserFunctionDef};
use crate::plan::PhysicalPlan;

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
                        return_type: func.return_type.clone(),
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
                        return_type: func.return_type.clone(),
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
                filter,
            } => self.execute_update(table_name, assignments, filter.as_ref()),
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
            } => self.execute_create_table(table_name, columns, *if_not_exists, *or_replace),
            PhysicalPlan::DropTable {
                table_names,
                if_exists,
            } => self.execute_drop_tables(table_names, *if_exists),
            PhysicalPlan::AlterTable {
                table_name,
                operation,
            } => self.execute_alter_table(table_name, operation),
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
            PhysicalPlan::Return { value: _ } => {
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

        let table = self
            .tables
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        Ok(self.apply_planned_schema(table, planned_schema))
    }

    pub(crate) fn apply_planned_schema(
        &self,
        source_table: &Table,
        planned_schema: &PlanSchema,
    ) -> Table {
        let mut new_schema = Schema::new();
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
            new_schema.add_field(field);
        }
        source_table.with_schema(new_schema)
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
        let evaluator = IrEvaluator::new(&schema)
            .with_variables(&self.variables)
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

    pub(crate) fn execute_project(
        &mut self,
        input: &PhysicalPlan,
        expressions: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let input_schema = input_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);
        let evaluator = IrEvaluator::new(&input_schema)
            .with_variables(&self.variables)
            .with_user_functions(&self.user_function_defs);
        let mut result = Table::empty(result_schema);

        for record in input_table.rows()? {
            let mut new_row = Vec::with_capacity(expressions.len());
            for expr in expressions {
                let val = evaluator.evaluate(expr, &record)?;
                new_row.push(val);
            }
            result.push_row(new_row)?;
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
                let cmp = val_a.cmp(&val_b);
                let cmp = if sort_expr.asc { cmp } else { cmp.reverse() };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
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
            if let Some(count) = right_set.get_mut(&values) {
                if *count > 0 {
                    if all || seen.insert(values.clone()) {
                        result.push_row(values)?;
                        if all {
                            *count -= 1;
                        }
                    }
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
        _input: &PhysicalPlan,
        _group_by: &[Expr],
        _aggregates: &[Expr],
        _schema: &PlanSchema,
        _grouping_sets: Option<&Vec<Vec<usize>>>,
    ) -> Result<Table> {
        Err(Error::internal(
            "Aggregate not yet implemented in concurrent executor",
        ))
    }

    pub(crate) fn execute_window(
        &mut self,
        input: &PhysicalPlan,
        _window_exprs: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);
        for record in input_table.rows()? {
            result.push_row(record.values().to_vec())?;
        }
        Ok(result)
    }

    pub(crate) fn execute_cte(
        &mut self,
        ctes: &[CteDefinition],
        body: &PhysicalPlan,
    ) -> Result<Table> {
        for cte in ctes {
            let physical_cte = yachtsql_optimizer::optimize(&cte.query)?;
            let cte_plan = PhysicalPlan::from_physical(&physical_cte);
            let cte_result = self.execute_plan(&cte_plan)?;
            self.cte_results.insert(cte.name.clone(), cte_result);
        }
        self.execute_plan(body)
    }

    pub(crate) fn execute_unnest(
        &mut self,
        input: &PhysicalPlan,
        _columns: &[UnnestColumn],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);
        for record in input_table.rows()? {
            result.push_row(record.values().to_vec())?;
        }
        Ok(result)
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

        let source_table = self.execute_plan(source)?;

        let target = self
            .tables
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        for record in source_table.rows()? {
            if columns.is_empty() {
                target.push_row(record.values().to_vec())?;
            } else {
                let mut row = vec![Value::Null; target_schema.field_count()];
                for (i, col_name) in columns.iter().enumerate() {
                    if let Some(col_idx) = target_schema.field_index(col_name) {
                        if i < record.values().len() {
                            row[col_idx] = record.values()[i].clone();
                        }
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

        let mut schema = Schema::new();
        for col in columns {
            let mode = if col.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            schema.add_field(Field::new(&col.name, col.data_type.clone(), mode));
        }

        if or_replace && self.catalog.table_exists(table_name) {
            self.catalog
                .create_or_replace_table(table_name, Table::new(schema));
        } else {
            self.catalog.create_table(table_name, schema)?;
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
        _table_name: &str,
        _operation: &AlterTableOp,
    ) -> Result<Table> {
        Err(Error::internal(
            "ALTER TABLE not yet implemented in concurrent executor",
        ))
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
    ) -> Result<Table> {
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
    ) -> Result<Table> {
        let proc = UserProcedure {
            name: name.to_string(),
            parameters: args.to_vec(),
            body: body
                .iter()
                .map(|p| yachtsql_ir::LogicalPlan::Empty {
                    schema: yachtsql_ir::PlanSchema::default(),
                })
                .collect(),
        };
        self.catalog.create_procedure(proc, or_replace)?;
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

    pub(crate) fn execute_call(&mut self, _procedure_name: &str, _args: &[Expr]) -> Result<Table> {
        Err(Error::internal(
            "CALL not yet implemented in concurrent executor",
        ))
    }

    pub(crate) fn execute_export(
        &mut self,
        _options: &ExportOptions,
        _query: &PhysicalPlan,
    ) -> Result<Table> {
        Err(Error::internal(
            "EXPORT not yet implemented in concurrent executor",
        ))
    }

    pub(crate) fn execute_load(
        &mut self,
        _table_name: &str,
        _options: &LoadOptions,
        _temp_table: bool,
        _temp_schema: Option<&Vec<ColumnDef>>,
    ) -> Result<Table> {
        Err(Error::internal(
            "LOAD not yet implemented in concurrent executor",
        ))
    }

    pub(crate) fn execute_declare(
        &mut self,
        name: &str,
        _data_type: &DataType,
        default: Option<&Expr>,
    ) -> Result<Table> {
        let value = if let Some(expr) = default {
            let empty_schema = Schema::new();
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_user_functions(&self.user_function_defs);
            evaluator.evaluate(expr, &Record::new())?
        } else {
            Value::Null
        };
        self.variables.insert(name.to_uppercase(), value);
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
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let mut result = Table::empty(Schema::new());
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 10000;

        loop {
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_user_functions(&self.user_function_defs);
            let cond = evaluator.evaluate(condition, &Record::new())?;

            if !cond.as_bool().unwrap_or(false) {
                break;
            }

            for stmt in body {
                result = self.execute_plan(stmt)?;
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

    pub(crate) fn execute_loop(
        &mut self,
        body: &[PhysicalPlan],
        _label: Option<&str>,
    ) -> Result<Table> {
        let mut result = Table::empty(Schema::new());
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 10000;

        loop {
            for stmt in body {
                result = self.execute_plan(stmt)?;
            }

            iterations += 1;
            if iterations >= MAX_ITERATIONS {
                return Err(Error::invalid_query("LOOP exceeded maximum iterations"));
            }
        }
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

        loop {
            for stmt in body {
                result = self.execute_plan(stmt)?;
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
            RaiseLevel::Exception => Err(Error::InvalidQuery(msg)),
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
}
