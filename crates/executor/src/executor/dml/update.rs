use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Assignment, Expr};
use yachtsql_storage::{Record, Schema, Table};

use super::{PlanExecutor, coerce_value, parse_assignment_column, update_struct_field};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_update(
        &mut self,
        table_name: &str,
        assignments: &[Assignment],
        from: Option<&PhysicalPlan>,
        filter: Option<&Expr>,
    ) -> Result<Table> {
        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .clone();

        let target_schema = table.schema().clone();

        let evaluator_for_defaults = IrEvaluator::new(&target_schema);
        let empty_record = yachtsql_storage::Record::new();
        let mut default_values: Vec<Option<Value>> = vec![None; target_schema.field_count()];
        if let Some(defaults) = self.catalog.get_table_defaults(table_name) {
            for default in defaults {
                if let Some(idx) = target_schema.field_index(&default.column_name)
                    && let Ok(val) =
                        evaluator_for_defaults.evaluate(&default.default_expr, &empty_record)
                {
                    default_values[idx] = Some(val);
                }
            }
        }

        match from {
            Some(from_plan) => {
                self.execute_update_with_from(
                    table_name,
                    &table,
                    &target_schema,
                    assignments,
                    from_plan,
                    filter,
                    &default_values,
                )?;
            }
            None => {
                self.execute_update_without_from(
                    table_name,
                    &table,
                    &target_schema,
                    assignments,
                    filter,
                    &default_values,
                )?;
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn execute_update_with_from(
        &mut self,
        table_name: &str,
        table: &Table,
        target_schema: &Schema,
        assignments: &[Assignment],
        from_plan: &PhysicalPlan,
        filter: Option<&Expr>,
        default_values: &[Option<Value>],
    ) -> Result<()> {
        let from_data = self.execute_plan(from_plan)?;
        let from_schema = from_data.schema().clone();

        let combined_schema = {
            let mut schema = Schema::new();
            for field in target_schema.fields() {
                schema.add_field(field.clone());
            }
            for field in from_schema.fields() {
                schema.add_field(field.clone());
            }
            schema
        };

        let evaluator = IrEvaluator::new(&combined_schema);

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
                    Some(expr) => evaluator
                        .evaluate(expr, &combined_record)?
                        .as_bool()
                        .unwrap_or(false),
                    None => true,
                };

                if should_update && !updated_rows.contains_key(&target_idx) {
                    let mut new_row = target_row.clone();
                    for assignment in assignments {
                        let (base_col, field_path) = parse_assignment_column(&assignment.column);
                        if let Some(col_idx) = target_schema.field_index(&base_col) {
                            let new_val = match &assignment.value {
                                Expr::Default => {
                                    default_values[col_idx].clone().unwrap_or(Value::Null)
                                }
                                _ => {
                                    let val =
                                        if Self::expr_contains_subquery(&assignment.value) {
                                            self.eval_expr_with_subquery(
                                                &assignment.value,
                                                &combined_schema,
                                                &combined_record,
                                            )?
                                        } else {
                                            evaluator.evaluate(&assignment.value, &combined_record)?
                                        };
                                    match val {
                                        Value::Default => {
                                            default_values[col_idx].clone().unwrap_or(Value::Null)
                                        }
                                        other => other,
                                    }
                                }
                            };

                            if field_path.is_empty() {
                                let target_type = &target_schema.fields()[col_idx].data_type;
                                new_row[col_idx] = coerce_value(new_val, target_type)?;
                            } else {
                                let field_refs: Vec<&str> =
                                    field_path.iter().map(|s| s.as_str()).collect();
                                new_row[col_idx] =
                                    update_struct_field(&new_row[col_idx], &field_refs, new_val);
                            }
                        }
                    }
                    updated_rows.insert(target_idx, new_row);
                }
            }
        }

        let mut new_table = Table::empty(target_schema.clone());
        for (idx, row) in target_rows.iter().enumerate() {
            if let Some(updated_row) = updated_rows.get(&idx) {
                new_table.push_row(updated_row.clone())?;
            } else {
                new_table.push_row(row.clone())?;
            }
        }

        self.catalog.replace_table(table_name, new_table)?;
        Ok(())
    }

    fn execute_update_without_from(
        &mut self,
        table_name: &str,
        table: &Table,
        target_schema: &Schema,
        assignments: &[Assignment],
        filter: Option<&Expr>,
        default_values: &[Option<Value>],
    ) -> Result<()> {
        let evaluator = IrEvaluator::new(target_schema);
        let has_subquery = filter.map(Self::expr_contains_subquery).unwrap_or(false);
        let assignments_have_subquery = assignments
            .iter()
            .any(|a| Self::expr_contains_subquery(&a.value));

        let target_schema_with_source = if has_subquery || assignments_have_subquery {
            let mut schema = Schema::new();
            for field in target_schema.fields() {
                let mut new_field = field.clone();
                if new_field.source_table.is_none() {
                    new_field.source_table = Some(table_name.to_string());
                }
                schema.add_field(new_field);
            }
            schema
        } else {
            target_schema.clone()
        };

        let mut new_table = Table::empty(target_schema.clone());

        for record in table.rows()? {
            let should_update = match filter {
                Some(expr) => {
                    if has_subquery {
                        self.eval_expr_with_subquery(expr, &target_schema_with_source, &record)?
                            .as_bool()
                            .unwrap_or(false)
                    } else {
                        evaluator
                            .evaluate(expr, &record)?
                            .as_bool()
                            .unwrap_or(false)
                    }
                }
                None => true,
            };

            if should_update {
                let mut new_row = record.values().to_vec();
                for assignment in assignments {
                    let (base_col, field_path) = parse_assignment_column(&assignment.column);
                    if let Some(col_idx) = target_schema.field_index(&base_col) {
                        let new_val = match &assignment.value {
                            Expr::Default => default_values[col_idx].clone().unwrap_or(Value::Null),
                            _ => {
                                let val = if Self::expr_contains_subquery(&assignment.value) {
                                    self.eval_expr_with_subquery(
                                        &assignment.value,
                                        &target_schema_with_source,
                                        &record,
                                    )?
                                } else {
                                    evaluator.evaluate(&assignment.value, &record)?
                                };
                                match val {
                                    Value::Default => {
                                        default_values[col_idx].clone().unwrap_or(Value::Null)
                                    }
                                    other => other,
                                }
                            }
                        };

                        if field_path.is_empty() {
                            let target_type = &target_schema.fields()[col_idx].data_type;
                            new_row[col_idx] = coerce_value(new_val, target_type)?;
                        } else {
                            let field_refs: Vec<&str> =
                                field_path.iter().map(|s| s.as_str()).collect();
                            new_row[col_idx] =
                                update_struct_field(&new_row[col_idx], &field_refs, new_val);
                        }
                    }
                }
                new_table.push_row(new_row)?;
            } else {
                new_table.push_row(record.values().to_vec())?;
            }
        }

        self.catalog.replace_table(table_name, new_table)?;
        Ok(())
    }
}
