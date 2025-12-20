use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{Assignment, Expr, MergeClause};
use yachtsql_storage::{Schema, Table};

use super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::ExecutorPlan;

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
        _ => Ok(value),
    }
}

impl<'a> PlanExecutor<'a> {
    pub fn execute_insert(
        &mut self,
        table_name: &str,
        columns: &[String],
        source: &ExecutorPlan,
    ) -> Result<Table> {
        let target_schema = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .schema()
            .clone();

        let evaluator = IrEvaluator::new(&target_schema);
        let empty_record = yachtsql_storage::Record::new();

        let mut default_values: Vec<Option<Value>> = vec![None; target_schema.field_count()];
        if let Some(defaults) = self.catalog.get_table_defaults(table_name) {
            for default in defaults {
                if let Some(idx) = target_schema.field_index(&default.column_name) {
                    if let Ok(val) = evaluator.evaluate(&default.default_expr, &empty_record) {
                        default_values[idx] = Some(val);
                    }
                }
            }
        }

        let fields = target_schema.fields().to_vec();

        if let ExecutorPlan::Values { values, .. } = source {
            let target = self
                .catalog
                .get_table_mut(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

            let empty_schema = yachtsql_storage::Schema::new();
            let values_evaluator = IrEvaluator::new(&empty_schema);
            let empty_rec = yachtsql_storage::Record::from_values(vec![]);

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
                    target.push_row(coerced_row)?;
                } else {
                    let mut row: Vec<Value> = default_values
                        .iter()
                        .map(|opt| opt.clone().unwrap_or(Value::Null))
                        .collect();
                    for (i, col_name) in columns.iter().enumerate() {
                        if let Some(col_idx) = target_schema.field_index(col_name) {
                            if i < row_exprs.len() && col_idx < fields.len() {
                                let expr = &row_exprs[i];
                                let final_val = match expr {
                                    Expr::Default => {
                                        default_values[col_idx].clone().unwrap_or(Value::Null)
                                    }
                                    _ => values_evaluator.evaluate(expr, &empty_rec)?,
                                };
                                row[col_idx] = coerce_value(final_val, &fields[col_idx].data_type)?;
                            }
                        }
                    }
                    target.push_row(row)?;
                }
            }

            return Ok(Table::empty(Schema::new()));
        }

        let source_table = self.execute_plan(source)?;

        let target = self
            .catalog
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        for record in source_table.rows()? {
            if columns.is_empty() {
                let mut coerced_row = Vec::with_capacity(target_schema.field_count());
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
                    if let Some(col_idx) = target_schema.field_index(col_name) {
                        if i < record.values().len() && col_idx < fields.len() {
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
                }
                target.push_row(row)?;
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_update(
        &mut self,
        table_name: &str,
        assignments: &[Assignment],
        filter: Option<&Expr>,
    ) -> Result<Table> {
        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .clone();

        let schema = table.schema().clone();
        let evaluator = IrEvaluator::new(&schema);

        let mut new_table = Table::empty(schema.clone());

        for record in table.rows()? {
            let should_update = match filter {
                Some(expr) => evaluator
                    .evaluate(expr, &record)?
                    .as_bool()
                    .unwrap_or(false),
                None => true,
            };

            if should_update {
                let mut new_row = record.values().to_vec();
                for assignment in assignments {
                    if let Some(col_idx) = schema.field_index(&assignment.column) {
                        let new_val = evaluator.evaluate(&assignment.value, &record)?;
                        new_row[col_idx] = new_val;
                    }
                }
                new_table.push_row(new_row)?;
            } else {
                new_table.push_row(record.values().to_vec())?;
            }
        }

        self.catalog.replace_table(table_name, new_table)?;

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_delete(&mut self, table_name: &str, filter: Option<&Expr>) -> Result<Table> {
        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .clone();

        let schema = table.schema().clone();
        let evaluator = IrEvaluator::new(&schema);

        let mut new_table = Table::empty(schema.clone());

        for record in table.rows()? {
            let should_delete = match filter {
                Some(expr) => evaluator
                    .evaluate(expr, &record)?
                    .as_bool()
                    .unwrap_or(false),
                None => true,
            };

            if !should_delete {
                new_table.push_row(record.values().to_vec())?;
            }
        }

        self.catalog.replace_table(table_name, new_table)?;

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_merge(
        &mut self,
        target_table: &str,
        source: &ExecutorPlan,
        on: &Expr,
        clauses: &[MergeClause],
    ) -> Result<Table> {
        let source_data = self.execute_plan(source)?;

        let target_data = self
            .catalog
            .get_table(target_table)
            .ok_or_else(|| Error::TableNotFound(target_table.to_string()))?
            .clone();

        let target_schema = target_data.schema().clone();
        let source_schema = source_data.schema().clone();

        let combined_schema = {
            let mut schema = Schema::new();
            for field in target_schema.fields() {
                schema.add_field(field.clone());
            }
            for field in source_schema.fields() {
                schema.add_field(field.clone());
            }
            schema
        };

        let evaluator = IrEvaluator::new(&combined_schema);
        let target_evaluator = IrEvaluator::new(&target_schema);

        let target_rows: Vec<Vec<Value>> = target_data
            .rows()?
            .iter()
            .map(|r| r.values().to_vec())
            .collect();
        let source_rows: Vec<Vec<Value>> = source_data
            .rows()?
            .iter()
            .map(|r| r.values().to_vec())
            .collect();

        let mut target_matched: Vec<bool> = vec![false; target_rows.len()];
        let mut source_matched: Vec<bool> = vec![false; source_rows.len()];

        for (target_idx, target_row) in target_rows.iter().enumerate() {
            for (source_idx, source_row) in source_rows.iter().enumerate() {
                let mut combined_values = target_row.clone();
                combined_values.extend(source_row.clone());
                let combined_record = yachtsql_storage::Record::from_values(combined_values);

                let matches = evaluator
                    .evaluate(on, &combined_record)?
                    .as_bool()
                    .unwrap_or(false);

                if matches {
                    target_matched[target_idx] = true;
                    source_matched[source_idx] = true;
                }
            }
        }

        let mut new_rows: Vec<Vec<Value>> = Vec::new();
        let mut rows_to_delete: Vec<usize> = Vec::new();
        let mut rows_to_update: Vec<(usize, Vec<Value>)> = Vec::new();

        for (target_idx, target_row) in target_rows.iter().enumerate() {
            if target_matched[target_idx] {
                let matching_sources: Vec<&Vec<Value>> = source_rows
                    .iter()
                    .enumerate()
                    .filter(|(source_idx, source_row)| {
                        let mut combined_values = target_row.clone();
                        combined_values.extend((*source_row).clone());
                        let combined_record =
                            yachtsql_storage::Record::from_values(combined_values);
                        evaluator
                            .evaluate(on, &combined_record)
                            .ok()
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    })
                    .map(|(_, source_row)| source_row)
                    .collect();

                let mut clause_applied = false;
                for source_row in &matching_sources {
                    if clause_applied {
                        break;
                    }

                    let mut combined_values = target_row.clone();
                    combined_values.extend((*source_row).clone());
                    let combined_record =
                        yachtsql_storage::Record::from_values(combined_values.clone());

                    for clause in clauses {
                        match clause {
                            MergeClause::MatchedUpdate {
                                condition,
                                assignments,
                            } => {
                                let condition_matches = match condition {
                                    Some(cond) => evaluator
                                        .evaluate(cond, &combined_record)?
                                        .as_bool()
                                        .unwrap_or(false),
                                    None => true,
                                };

                                if condition_matches {
                                    let mut new_row = target_row.clone();
                                    for assignment in assignments {
                                        if let Some(col_idx) =
                                            target_schema.field_index(&assignment.column)
                                        {
                                            let new_val = evaluator
                                                .evaluate(&assignment.value, &combined_record)?;
                                            new_row[col_idx] = new_val;
                                        }
                                    }
                                    rows_to_update.push((target_idx, new_row));
                                    clause_applied = true;
                                    break;
                                }
                            }
                            MergeClause::MatchedDelete { condition } => {
                                let condition_matches = match condition {
                                    Some(cond) => evaluator
                                        .evaluate(cond, &combined_record)?
                                        .as_bool()
                                        .unwrap_or(false),
                                    None => true,
                                };

                                if condition_matches {
                                    rows_to_delete.push(target_idx);
                                    clause_applied = true;
                                    break;
                                }
                            }
                            MergeClause::NotMatched { .. } => {}
                            MergeClause::NotMatchedBySource { .. } => {}
                            MergeClause::NotMatchedBySourceDelete { .. } => {}
                        }
                    }
                }
            } else {
                let target_record = yachtsql_storage::Record::from_values(target_row.clone());

                for clause in clauses {
                    match clause {
                        MergeClause::NotMatchedBySource {
                            condition,
                            assignments,
                        } => {
                            let condition_matches = match condition {
                                Some(cond) => target_evaluator
                                    .evaluate(cond, &target_record)?
                                    .as_bool()
                                    .unwrap_or(false),
                                None => true,
                            };

                            if condition_matches {
                                let mut new_row = target_row.clone();
                                for assignment in assignments {
                                    if let Some(col_idx) =
                                        target_schema.field_index(&assignment.column)
                                    {
                                        let new_val = target_evaluator
                                            .evaluate(&assignment.value, &target_record)?;
                                        new_row[col_idx] = new_val;
                                    }
                                }
                                rows_to_update.push((target_idx, new_row));
                                break;
                            }
                        }
                        MergeClause::NotMatchedBySourceDelete { condition } => {
                            let condition_matches = match condition {
                                Some(cond) => target_evaluator
                                    .evaluate(cond, &target_record)?
                                    .as_bool()
                                    .unwrap_or(false),
                                None => true,
                            };

                            if condition_matches {
                                rows_to_delete.push(target_idx);
                                break;
                            }
                        }
                        MergeClause::MatchedUpdate { .. } => {}
                        MergeClause::MatchedDelete { .. } => {}
                        MergeClause::NotMatched { .. } => {}
                    }
                }
            }
        }

        for (source_idx, source_row) in source_rows.iter().enumerate() {
            if !source_matched[source_idx] {
                let target_null_row: Vec<Value> = (0..target_schema.field_count())
                    .map(|_| Value::Null)
                    .collect();
                let mut combined_values = target_null_row;
                combined_values.extend(source_row.clone());
                let combined_record =
                    yachtsql_storage::Record::from_values(combined_values.clone());
                let source_record = yachtsql_storage::Record::from_values(source_row.clone());

                for clause in clauses {
                    match clause {
                        MergeClause::NotMatched {
                            condition,
                            columns,
                            values,
                        } => {
                            let condition_matches = match condition {
                                Some(cond) => evaluator
                                    .evaluate(cond, &combined_record)?
                                    .as_bool()
                                    .unwrap_or(false),
                                None => true,
                            };

                            if condition_matches {
                                if columns.is_empty() && values.is_empty() {
                                    new_rows.push(source_row.clone());
                                } else {
                                    let mut new_row: Vec<Value> =
                                        vec![Value::Null; target_schema.field_count()];
                                    for (i, col_name) in columns.iter().enumerate() {
                                        if let Some(col_idx) = target_schema.field_index(col_name) {
                                            if i < values.len() {
                                                let val = evaluator
                                                    .evaluate(&values[i], &combined_record)?;
                                                new_row[col_idx] = val;
                                            }
                                        }
                                    }
                                    new_rows.push(new_row);
                                }
                                break;
                            }
                        }
                        MergeClause::MatchedUpdate { .. } => {}
                        MergeClause::MatchedDelete { .. } => {}
                        MergeClause::NotMatchedBySource { .. } => {}
                        MergeClause::NotMatchedBySourceDelete { .. } => {}
                    }
                }
            }
        }

        let mut updated_indices: std::collections::HashSet<usize> =
            std::collections::HashSet::new();
        for (idx, _) in &rows_to_update {
            updated_indices.insert(*idx);
        }
        let deleted_indices: std::collections::HashSet<usize> =
            rows_to_delete.into_iter().collect();

        let mut final_table = Table::empty(target_schema.clone());

        for (idx, row) in target_rows.iter().enumerate() {
            if deleted_indices.contains(&idx) {
                continue;
            }
            if let Some((_, updated_row)) = rows_to_update.iter().find(|(i, _)| *i == idx) {
                final_table.push_row(updated_row.clone())?;
            } else {
                final_table.push_row(row.clone())?;
            }
        }

        for row in new_rows {
            final_table.push_row(row)?;
        }

        self.catalog.replace_table(target_table, final_table)?;

        Ok(Table::empty(Schema::new()))
    }
}
