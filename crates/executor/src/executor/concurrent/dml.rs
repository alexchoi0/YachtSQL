use std::collections::HashSet;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Assignment, Expr, MergeClause};
use yachtsql_storage::{Record, Schema, Table};

use super::{ConcurrentPlanExecutor, coerce_value};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl ConcurrentPlanExecutor<'_> {
    pub(crate) async fn execute_insert(
        &self,
        table_name: &str,
        columns: &[String],
        source: &PhysicalPlan,
    ) -> Result<Table> {
        let (target_schema, fields, default_values) = {
            let target = self
                .tables
                .get_table_mut(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
            let target_schema = target.schema().clone();
            let fields = target_schema.fields().to_vec();

            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&target_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);
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
            (target_schema, fields, default_values)
        };

        if let PhysicalPlan::Values { values, .. } = source {
            let empty_schema = Schema::new();
            let empty_rec = Record::new();

            let mut all_rows: Vec<Vec<Value>> = Vec::new();

            for row_exprs in values {
                if columns.is_empty() {
                    let mut coerced_row = Vec::with_capacity(target_schema.field_count());
                    for (i, expr) in row_exprs.iter().enumerate() {
                        if i < fields.len() {
                            let final_val = match expr {
                                Expr::Default => default_values[i].clone().unwrap_or(Value::Null),
                                _ if Self::expr_contains_subquery(expr) => {
                                    self.eval_expr_with_subqueries(expr, &empty_schema, &empty_rec)
                                        .await?
                                }
                                _ => {
                                    let vars = self.get_variables();
                                    let sys_vars = self.get_system_variables();
                                    let udf = self.get_user_functions();
                                    let values_evaluator = IrEvaluator::new(&empty_schema)
                                        .with_variables(&vars)
                                        .with_system_variables(&sys_vars)
                                        .with_user_functions(&udf);
                                    values_evaluator.evaluate(expr, &empty_rec)?
                                }
                            };
                            coerced_row.push(coerce_value(final_val, &fields[i].data_type)?);
                        } else if Self::expr_contains_subquery(expr) {
                            coerced_row.push(
                                self.eval_expr_with_subqueries(expr, &empty_schema, &empty_rec)
                                    .await?,
                            );
                        } else {
                            let vars = self.get_variables();
                            let sys_vars = self.get_system_variables();
                            let udf = self.get_user_functions();
                            let values_evaluator = IrEvaluator::new(&empty_schema)
                                .with_variables(&vars)
                                .with_system_variables(&sys_vars)
                                .with_user_functions(&udf);
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
                                Expr::Default => {
                                    default_values[col_idx].clone().unwrap_or(Value::Null)
                                }
                                _ if Self::expr_contains_subquery(expr) => {
                                    self.eval_expr_with_subqueries(expr, &empty_schema, &empty_rec)
                                        .await?
                                }
                                _ => {
                                    let vars = self.get_variables();
                                    let sys_vars = self.get_system_variables();
                                    let udf = self.get_user_functions();
                                    let values_evaluator = IrEvaluator::new(&empty_schema)
                                        .with_variables(&vars)
                                        .with_system_variables(&sys_vars)
                                        .with_user_functions(&udf);
                                    values_evaluator.evaluate(expr, &empty_rec)?
                                }
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

        let source_table = self.execute_plan(source).await?;

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
                            Value::Default => {
                                default_values[col_idx].clone().unwrap_or(Value::Null)
                            }
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

    pub(crate) async fn execute_update(
        &self,
        table_name: &str,
        alias: Option<&str>,
        assignments: &[Assignment],
        from: Option<&PhysicalPlan>,
        filter: Option<&Expr>,
    ) -> Result<Table> {
        let table = self
            .tables
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .clone();
        let base_schema = table.schema().clone();
        let source_name = alias.unwrap_or(table_name);
        let target_schema = Self::schema_with_source_table(&base_schema, source_name);

        let default_values = {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator_for_defaults = IrEvaluator::new(&base_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);
            let empty_record = Record::new();
            let mut default_values: Vec<Option<Value>> = vec![None; base_schema.field_count()];
            if let Some(defaults) = self.catalog.get_table_defaults(table_name) {
                for default in defaults {
                    if let Some(idx) = base_schema.field_index(&default.column_name)
                        && let Ok(val) =
                            evaluator_for_defaults.evaluate(&default.default_expr, &empty_record)
                    {
                        default_values[idx] = Some(val);
                    }
                }
            }
            default_values
        };

        let filter_has_subquery = filter
            .as_ref()
            .is_some_and(|f| Self::expr_contains_subquery(f));
        let assignments_have_subquery = assignments
            .iter()
            .any(|a| Self::expr_contains_subquery(&a.value));

        let mut new_table = Table::empty(base_schema.clone());

        match from {
            Some(from_plan) => {
                let from_data = self.execute_plan(from_plan).await?;
                let from_schema = from_data.schema().clone();

                let mut combined_schema = target_schema.clone();
                for field in from_schema.fields() {
                    combined_schema.add_field(field.clone());
                }

                let target_rows: Vec<Vec<Value>> =
                    table.rows()?.iter().map(|r| r.values().to_vec()).collect();
                let from_rows: Vec<Vec<Value>> = from_data
                    .rows()?
                    .iter()
                    .map(|r| r.values().to_vec())
                    .collect();

                let mut updated_rows: std::collections::HashMap<usize, Vec<Value>> =
                    std::collections::HashMap::new();

                for (target_idx, target_row) in target_rows.iter().enumerate() {
                    for from_row in &from_rows {
                        let mut combined_values = target_row.clone();
                        combined_values.extend(from_row.clone());
                        let combined_record = Record::from_values(combined_values);

                        let should_update = match filter {
                            Some(expr) => {
                                if filter_has_subquery {
                                    self.eval_expr_with_subqueries(
                                        expr,
                                        &combined_schema,
                                        &combined_record,
                                    )
                                    .await?
                                    .as_bool()
                                    .unwrap_or(false)
                                } else {
                                    let vars = self.get_variables();
                                    let sys_vars = self.get_system_variables();
                                    let udf = self.get_user_functions();
                                    let evaluator = IrEvaluator::new(&combined_schema)
                                        .with_variables(&vars)
                                        .with_system_variables(&sys_vars)
                                        .with_user_functions(&udf);
                                    evaluator
                                        .evaluate(expr, &combined_record)?
                                        .as_bool()
                                        .unwrap_or(false)
                                }
                            }
                            None => true,
                        };

                        if should_update && !updated_rows.contains_key(&target_idx) {
                            let mut new_row = target_row.clone();
                            for assignment in assignments {
                                let (base_col, field_path) =
                                    Self::parse_assignment_column(&assignment.column);
                                if let Some(col_idx) = target_schema.field_index(&base_col) {
                                    let new_val = match &assignment.value {
                                        Expr::Default => {
                                            default_values[col_idx].clone().unwrap_or(Value::Null)
                                        }
                                        _ => {
                                            if assignments_have_subquery {
                                                self.eval_expr_with_subqueries(
                                                    &assignment.value,
                                                    &combined_schema,
                                                    &combined_record,
                                                )
                                                .await?
                                            } else {
                                                let vars = self.get_variables();
                                                let sys_vars = self.get_system_variables();
                                                let udf = self.get_user_functions();
                                                let evaluator = IrEvaluator::new(&combined_schema)
                                                    .with_variables(&vars)
                                                    .with_system_variables(&sys_vars)
                                                    .with_user_functions(&udf);
                                                evaluator
                                                    .evaluate(&assignment.value, &combined_record)?
                                            }
                                        }
                                    };
                                    if field_path.is_empty() {
                                        new_row[col_idx] = new_val;
                                    } else {
                                        new_row[col_idx] = Self::set_nested_field(
                                            &new_row[col_idx],
                                            &field_path,
                                            new_val,
                                        )?;
                                    }
                                }
                            }
                            updated_rows.insert(target_idx, new_row);
                        }
                    }
                }

                for (idx, target_row) in target_rows.iter().enumerate() {
                    if let Some(updated_row) = updated_rows.get(&idx) {
                        new_table.push_row(updated_row.clone())?;
                    } else {
                        new_table.push_row(target_row.clone())?;
                    }
                }
            }
            None => {
                if filter_has_subquery || assignments_have_subquery {
                    for record in table.rows()? {
                        let matches = if let Some(f) = filter {
                            self.eval_expr_with_subqueries(f, &target_schema, &record)
                                .await?
                                .as_bool()
                                .unwrap_or(false)
                        } else {
                            true
                        };

                        if matches {
                            let mut new_row = record.values().to_vec();
                            for assignment in assignments {
                                let (base_col, field_path) =
                                    Self::parse_assignment_column(&assignment.column);
                                if let Some(idx) = target_schema.field_index(&base_col) {
                                    let val = match &assignment.value {
                                        Expr::Default => {
                                            default_values[idx].clone().unwrap_or(Value::Null)
                                        }
                                        _ => {
                                            self.eval_expr_with_subqueries(
                                                &assignment.value,
                                                &target_schema,
                                                &record,
                                            )
                                            .await?
                                        }
                                    };
                                    if field_path.is_empty() {
                                        new_row[idx] = val;
                                    } else {
                                        new_row[idx] = Self::set_nested_field(
                                            &new_row[idx],
                                            &field_path,
                                            val,
                                        )?;
                                    }
                                }
                            }
                            new_table.push_row(new_row)?;
                        } else {
                            new_table.push_row(record.values().to_vec())?;
                        }
                    }
                } else {
                    let vars = self.get_variables();
                    let udf = self.get_user_functions();
                    let evaluator = IrEvaluator::new(&target_schema)
                        .with_variables(&vars)
                        .with_user_functions(&udf);

                    for record in table.rows()? {
                        let matches = filter
                            .map(|f| evaluator.evaluate(f, &record))
                            .transpose()?
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(true);

                        if matches {
                            let mut new_row = record.values().to_vec();
                            for assignment in assignments {
                                let (base_col, field_path) =
                                    Self::parse_assignment_column(&assignment.column);
                                if let Some(idx) = target_schema.field_index(&base_col) {
                                    let val = match &assignment.value {
                                        Expr::Default => {
                                            default_values[idx].clone().unwrap_or(Value::Null)
                                        }
                                        _ => evaluator.evaluate(&assignment.value, &record)?,
                                    };
                                    if field_path.is_empty() {
                                        new_row[idx] = val;
                                    } else {
                                        new_row[idx] = Self::set_nested_field(
                                            &new_row[idx],
                                            &field_path,
                                            val,
                                        )?;
                                    }
                                }
                            }
                            new_table.push_row(new_row)?;
                        } else {
                            new_table.push_row(record.values().to_vec())?;
                        }
                    }
                }
            }
        }

        let target = self
            .tables
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        *target = new_table;

        Ok(Table::empty(Schema::new()))
    }

    fn parse_assignment_column(column: &str) -> (String, Vec<String>) {
        let parts: Vec<&str> = column.split('.').collect();
        if parts.len() > 1 {
            (
                parts[0].to_string(),
                parts[1..].iter().map(|s| s.to_string()).collect(),
            )
        } else {
            (column.to_string(), Vec::new())
        }
    }

    fn set_nested_field(base: &Value, path: &[String], new_val: Value) -> Result<Value> {
        if path.is_empty() {
            return Ok(new_val);
        }

        match base {
            Value::Struct(fields) => {
                let mut new_fields = fields.clone();
                let target_field = &path[0];
                let rest = &path[1..];

                for (name, value) in &mut new_fields {
                    if name.eq_ignore_ascii_case(target_field) {
                        *value = Self::set_nested_field(value, rest, new_val)?;
                        return Ok(Value::Struct(new_fields));
                    }
                }
                new_fields.push((target_field.clone(), new_val));
                Ok(Value::Struct(new_fields))
            }
            Value::Null => {
                let mut fields = Vec::new();
                if path.len() == 1 {
                    fields.push((path[0].clone(), new_val));
                } else {
                    let nested = Self::set_nested_field(&Value::Null, &path[1..], new_val)?;
                    fields.push((path[0].clone(), nested));
                }
                Ok(Value::Struct(fields))
            }
            _ => Err(Error::invalid_query(format!(
                "Cannot set field {} on non-struct value",
                path[0]
            ))),
        }
    }

    pub(crate) async fn execute_delete(
        &self,
        table_name: &str,
        alias: Option<&str>,
        filter: Option<&Expr>,
    ) -> Result<Table> {
        let table = self
            .tables
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .clone();
        let base_schema = table.schema().clone();
        let source_name = alias.unwrap_or(table_name);
        let schema = Self::schema_with_source_table(&base_schema, source_name);
        let has_subquery = filter
            .as_ref()
            .is_some_and(|f| Self::expr_contains_subquery(f));

        let mut new_table = Table::empty(base_schema.clone());

        if has_subquery {
            for record in table.rows()? {
                let matches = match filter {
                    Some(f) => self
                        .eval_expr_with_subqueries(f, &schema, &record)
                        .await?
                        .as_bool()
                        .unwrap_or(false),
                    None => true,
                };

                if !matches {
                    new_table.push_row(record.values().to_vec())?;
                }
            }
        } else {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);

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
        }

        let target = self
            .tables
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        *target = new_table;

        Ok(Table::empty(Schema::new()))
    }

    fn schema_with_source_table(schema: &Schema, table_name: &str) -> Schema {
        let mut new_schema = Schema::new();
        for field in schema.fields() {
            let mut new_field = field.clone();
            if new_field.source_table.is_none() {
                new_field.source_table = Some(table_name.to_string());
            }
            new_schema.add_field(new_field);
        }
        new_schema
    }

    pub(crate) async fn execute_merge(
        &self,
        target_table: &str,
        source: &PhysicalPlan,
        on: &Expr,
        clauses: &[MergeClause],
    ) -> Result<Table> {
        let source_table = self.execute_plan(source).await?;
        let source_schema = source_table.schema().clone();
        let source_rows: Vec<Record> = source_table.rows()?;

        let target = self
            .tables
            .get_table(target_table)
            .ok_or_else(|| Error::TableNotFound(target_table.to_string()))?;
        let target_schema = target.schema().clone();
        let target_rows: Vec<Record> = target.rows()?;

        let combined_schema = self.create_merge_combined_schema(&target_schema, &source_schema);

        let mut matched_target_indices: HashSet<usize> = HashSet::new();
        let mut matched_source_indices: HashSet<usize> = HashSet::new();
        let mut match_pairs: Vec<(usize, usize)> = Vec::new();

        let on_has_subquery = Self::expr_contains_subquery(on);
        for (t_idx, target_row) in target_rows.iter().enumerate() {
            for (s_idx, source_row) in source_rows.iter().enumerate() {
                let combined_record = self.create_combined_record(target_row, source_row);
                let val = if on_has_subquery {
                    self.eval_expr_with_subqueries(on, &combined_schema, &combined_record)
                        .await?
                } else {
                    let vars = self.get_variables();
                    let udf = self.get_user_functions();
                    let evaluator = IrEvaluator::new(&combined_schema)
                        .with_variables(&vars)
                        .with_user_functions(&udf);
                    evaluator.evaluate(on, &combined_record)?
                };
                if val.as_bool().unwrap_or(false) {
                    match_pairs.push((t_idx, s_idx));
                    matched_target_indices.insert(t_idx);
                    matched_source_indices.insert(s_idx);
                }
            }
        }

        let mut updates: Vec<(usize, Vec<Value>)> = Vec::new();
        let mut deletes: Vec<usize> = Vec::new();
        let mut inserts: Vec<Vec<Value>> = Vec::new();
        let mut claimed_target_indices: HashSet<usize> = HashSet::new();
        let mut claimed_source_indices: HashSet<usize> = HashSet::new();

        for clause in clauses {
            match clause {
                MergeClause::MatchedUpdate {
                    condition,
                    assignments,
                } => {
                    let cond_has_subquery =
                        condition.as_ref().is_some_and(Self::expr_contains_subquery);
                    let assignments_have_subquery = assignments
                        .iter()
                        .any(|a| Self::expr_contains_subquery(&a.value));

                    for &(t_idx, s_idx) in &match_pairs {
                        if claimed_target_indices.contains(&t_idx) {
                            continue;
                        }
                        let target_row = &target_rows[t_idx];
                        let source_row = &source_rows[s_idx];
                        let combined_record = self.create_combined_record(target_row, source_row);

                        let should_apply = match condition {
                            Some(pred) => {
                                if cond_has_subquery {
                                    self.eval_expr_with_subqueries(
                                        pred,
                                        &combined_schema,
                                        &combined_record,
                                    )
                                    .await?
                                    .as_bool()
                                    .unwrap_or(false)
                                } else {
                                    let vars = self.get_variables();
                                    let sys_vars = self.get_system_variables();
                                    let udf = self.get_user_functions();
                                    let evaluator = IrEvaluator::new(&combined_schema)
                                        .with_variables(&vars)
                                        .with_system_variables(&sys_vars)
                                        .with_user_functions(&udf);
                                    evaluator
                                        .evaluate(pred, &combined_record)?
                                        .as_bool()
                                        .unwrap_or(false)
                                }
                            }
                            None => true,
                        };

                        if should_apply {
                            claimed_target_indices.insert(t_idx);
                            let mut new_values = target_row.values().to_vec();
                            for assignment in assignments {
                                let col_idx = target_schema
                                    .fields()
                                    .iter()
                                    .position(|f| f.name.eq_ignore_ascii_case(&assignment.column))
                                    .ok_or_else(|| {
                                        Error::ColumnNotFound(assignment.column.clone())
                                    })?;
                                let new_val = if assignments_have_subquery {
                                    self.eval_expr_with_subqueries(
                                        &assignment.value,
                                        &combined_schema,
                                        &combined_record,
                                    )
                                    .await?
                                } else {
                                    let vars = self.get_variables();
                                    let sys_vars = self.get_system_variables();
                                    let udf = self.get_user_functions();
                                    let evaluator = IrEvaluator::new(&combined_schema)
                                        .with_variables(&vars)
                                        .with_system_variables(&sys_vars)
                                        .with_user_functions(&udf);
                                    evaluator.evaluate(&assignment.value, &combined_record)?
                                };
                                new_values[col_idx] = new_val;
                            }
                            if let Some(pos) = updates.iter().position(|(idx, _)| *idx == t_idx) {
                                updates[pos] = (t_idx, new_values);
                            } else {
                                updates.push((t_idx, new_values));
                            }
                        }
                    }
                }
                MergeClause::MatchedDelete { condition } => {
                    let cond_has_subquery =
                        condition.as_ref().is_some_and(Self::expr_contains_subquery);

                    for &(t_idx, s_idx) in &match_pairs {
                        if claimed_target_indices.contains(&t_idx) {
                            continue;
                        }
                        let target_row = &target_rows[t_idx];
                        let source_row = &source_rows[s_idx];
                        let combined_record = self.create_combined_record(target_row, source_row);

                        let should_apply = match condition {
                            Some(pred) => {
                                if cond_has_subquery {
                                    self.eval_expr_with_subqueries(
                                        pred,
                                        &combined_schema,
                                        &combined_record,
                                    )
                                    .await?
                                    .as_bool()
                                    .unwrap_or(false)
                                } else {
                                    let vars = self.get_variables();
                                    let sys_vars = self.get_system_variables();
                                    let udf = self.get_user_functions();
                                    let evaluator = IrEvaluator::new(&combined_schema)
                                        .with_variables(&vars)
                                        .with_system_variables(&sys_vars)
                                        .with_user_functions(&udf);
                                    evaluator
                                        .evaluate(pred, &combined_record)?
                                        .as_bool()
                                        .unwrap_or(false)
                                }
                            }
                            None => true,
                        };

                        if should_apply {
                            claimed_target_indices.insert(t_idx);
                            deletes.push(t_idx);
                        }
                    }
                }
                MergeClause::NotMatched {
                    condition,
                    columns,
                    values,
                } => {
                    let vars = self.get_variables();
                    let udf = self.get_user_functions();
                    let source_evaluator = IrEvaluator::new(&source_schema)
                        .with_variables(&vars)
                        .with_user_functions(&udf);

                    for (s_idx, source_row) in source_rows.iter().enumerate() {
                        if matched_source_indices.contains(&s_idx)
                            || claimed_source_indices.contains(&s_idx)
                        {
                            continue;
                        }

                        let should_apply = match condition {
                            Some(pred) => source_evaluator
                                .evaluate(pred, source_row)?
                                .as_bool()
                                .unwrap_or(false),
                            None => true,
                        };

                        if should_apply {
                            claimed_source_indices.insert(s_idx);
                            let mut row_values = vec![Value::Null; target_schema.field_count()];

                            if columns.is_empty() && values.is_empty() {
                                for (i, val) in source_row.values().iter().enumerate() {
                                    if i < row_values.len() {
                                        row_values[i] = val.clone();
                                    }
                                }
                            } else if columns.is_empty() {
                                for (expr_idx, expr) in values.iter().enumerate() {
                                    if expr_idx < row_values.len() {
                                        row_values[expr_idx] =
                                            source_evaluator.evaluate(expr, source_row)?;
                                    }
                                }
                            } else {
                                for (col_name, expr) in columns.iter().zip(values.iter()) {
                                    let col_idx = target_schema
                                        .fields()
                                        .iter()
                                        .position(|f| f.name.eq_ignore_ascii_case(col_name))
                                        .ok_or_else(|| {
                                            Error::ColumnNotFound(col_name.to_string())
                                        })?;
                                    row_values[col_idx] =
                                        source_evaluator.evaluate(expr, source_row)?;
                                }
                            }
                            inserts.push(row_values);
                        }
                    }
                }
                MergeClause::NotMatchedBySource {
                    condition,
                    assignments,
                } => {
                    let vars = self.get_variables();
                    let udf = self.get_user_functions();
                    let target_evaluator = IrEvaluator::new(&target_schema)
                        .with_variables(&vars)
                        .with_user_functions(&udf);

                    for (t_idx, target_row) in target_rows.iter().enumerate() {
                        if matched_target_indices.contains(&t_idx)
                            || claimed_target_indices.contains(&t_idx)
                        {
                            continue;
                        }

                        let should_apply = match condition {
                            Some(pred) => target_evaluator
                                .evaluate(pred, target_row)?
                                .as_bool()
                                .unwrap_or(false),
                            None => true,
                        };

                        if should_apply {
                            claimed_target_indices.insert(t_idx);
                            let mut new_values = target_row.values().to_vec();
                            for assignment in assignments {
                                let col_idx = target_schema
                                    .fields()
                                    .iter()
                                    .position(|f| f.name.eq_ignore_ascii_case(&assignment.column))
                                    .ok_or_else(|| {
                                        Error::ColumnNotFound(assignment.column.clone())
                                    })?;
                                let new_val =
                                    target_evaluator.evaluate(&assignment.value, target_row)?;
                                new_values[col_idx] = new_val;
                            }
                            if let Some(pos) = updates.iter().position(|(idx, _)| *idx == t_idx) {
                                updates[pos] = (t_idx, new_values);
                            } else {
                                updates.push((t_idx, new_values));
                            }
                        }
                    }
                }
                MergeClause::NotMatchedBySourceDelete { condition } => {
                    let vars = self.get_variables();
                    let udf = self.get_user_functions();
                    let target_evaluator = IrEvaluator::new(&target_schema)
                        .with_variables(&vars)
                        .with_user_functions(&udf);

                    for (t_idx, target_row) in target_rows.iter().enumerate() {
                        if matched_target_indices.contains(&t_idx)
                            || claimed_target_indices.contains(&t_idx)
                        {
                            continue;
                        }

                        let should_apply = match condition {
                            Some(pred) => target_evaluator
                                .evaluate(pred, target_row)?
                                .as_bool()
                                .unwrap_or(false),
                            None => true,
                        };

                        if should_apply {
                            claimed_target_indices.insert(t_idx);
                            deletes.push(t_idx);
                        }
                    }
                }
            }
        }

        let target = self
            .tables
            .get_table_mut(target_table)
            .ok_or_else(|| Error::TableNotFound(target_table.to_string()))?;

        for (idx, new_values) in &updates {
            target.update_row(*idx, new_values.clone())?;
        }

        deletes.sort();
        for idx in deletes.into_iter().rev() {
            target.remove_row(idx);
        }

        for insert_row in inserts {
            target.push_row(insert_row)?;
        }

        Ok(Table::empty(Schema::new()))
    }

    fn create_merge_combined_schema(&self, target: &Schema, source: &Schema) -> Schema {
        let mut combined = Schema::new();
        for field in target.fields() {
            combined.add_field(field.clone());
        }
        for field in source.fields() {
            combined.add_field(field.clone());
        }
        combined
    }

    fn create_combined_record(&self, target: &Record, source: &Record) -> Record {
        let mut values = target.values().to_vec();
        values.extend(source.values().to_vec());
        Record::from_values(values)
    }
}
