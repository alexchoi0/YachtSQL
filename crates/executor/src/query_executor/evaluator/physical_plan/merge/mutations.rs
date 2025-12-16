use std::collections::HashSet;

use yachtsql_core::error::{Error, Result};
use yachtsql_storage::Schema;

use super::types::MergeReturningRow;
use crate::storage::table::Row;

pub(super) struct MergeMutations {
    updates: Vec<(usize, Row)>,
    deletes: HashSet<usize>,
    inserts: Vec<Row>,
    capture_returning: bool,
    returning_rows: Vec<MergeReturningRow>,
}

impl MergeMutations {
    pub(super) fn new(capture_returning: bool) -> Self {
        Self {
            updates: Vec::new(),
            deletes: HashSet::new(),
            inserts: Vec::new(),
            capture_returning,
            returning_rows: Vec::new(),
        }
    }

    pub(super) fn push_returning_row(
        &mut self,
        target_row: Row,
        source_row: Option<Row>,
        action: super::types::MergeAction,
    ) {
        if self.capture_returning {
            self.returning_rows.push(MergeReturningRow {
                target_row,
                source_row,
                action,
            });
        }
    }

    pub(super) fn returning_rows_count(&self) -> usize {
        self.returning_rows.len()
    }

    pub(super) fn record_update(&mut self, row_idx: usize, updated_values: Row) {
        self.updates.push((row_idx, updated_values));
    }

    pub(super) fn record_delete(&mut self, row_idx: usize) {
        self.deletes.insert(row_idx);
    }

    pub(super) fn record_insert(&mut self, row: Row) {
        self.inserts.push(row);
    }

    pub(super) fn updates(&self) -> &[(usize, Row)] {
        &self.updates
    }

    pub(super) fn inserts(&self) -> &[Row] {
        &self.inserts
    }

    pub(super) fn apply_to_table(
        self,
        storage: &mut crate::storage::Storage,
        table_name: &str,
    ) -> Result<(usize, Vec<MergeReturningRow>)> {
        let target_table = storage
            .get_table(table_name)
            .ok_or_else(|| Error::table_not_found(table_name.to_string()))?;

        self.validate_mutations(target_table)?;

        let updates_count = self.updates.len();
        let deletes_count = self.deletes.len();
        let inserts_count = self.inserts.len();

        {
            let target_table = storage
                .get_table_mut(table_name)
                .ok_or_else(|| Error::table_not_found(table_name.to_string()))?;

            let schema = target_table.schema().clone();
            for (row_idx, updated_values) in &self.updates {
                for col_name in updated_values.initialized_column_names(&schema) {
                    let value = updated_values
                        .get_by_name(&schema, col_name)
                        .ok_or_else(|| {
                            Error::InvalidOperation(format!("Column {} not found", col_name))
                        })?;
                    target_table.update_cell_at_index(*row_idx, col_name, value.clone())?;
                }
            }
        }

        let target_table = storage
            .get_table_mut(table_name)
            .ok_or_else(|| Error::table_not_found(table_name.to_string()))?;

        if !self.deletes.is_empty() {
            let keep_indices: Vec<usize> = (0..target_table.row_count())
                .filter(|i| !self.deletes.contains(i))
                .collect();

            if keep_indices.is_empty() {
                let layout = target_table.storage_layout();
                *target_table = crate::storage::table::Table::with_layout(
                    target_table.schema().clone(),
                    layout,
                );
            } else {
                let kept_table = target_table.gather(&keep_indices)?;
                *target_table = kept_table;
            }
        }

        target_table.insert_rows(self.inserts)?;

        Ok((
            updates_count + deletes_count + inserts_count,
            self.returning_rows,
        ))
    }

    fn validate_mutations(&self, target_table: &crate::storage::table::Table) -> Result<()> {
        use yachtsql_storage::constraints::validate_row_constraints;

        let schema = target_table.schema();
        let existing_rows = (0..target_table.row_count())
            .map(|idx| target_table.get_row(idx))
            .collect::<Result<Vec<_>>>()?;

        self.validate_update_mutations(target_table, schema)?;

        for insert_row in &self.inserts {
            validate_row_constraints(schema, insert_row, &existing_rows)?;
        }

        Ok(())
    }

    fn validate_update_mutations(
        &self,
        target_table: &crate::storage::table::Table,
        schema: &Schema,
    ) -> Result<()> {
        for (row_idx, updated_values) in &self.updates {
            let full_row = build_full_updated_row(target_table, *row_idx, updated_values)?;

            validate_not_null_constraints(schema, &full_row)?;
            validate_check_constraints(schema, &full_row)?;
        }
        Ok(())
    }
}

fn build_full_updated_row(
    target_table: &crate::storage::table::Table,
    row_idx: usize,
    updated_values: &Row,
) -> Result<Row> {
    let existing_row = target_table.get_row(row_idx)?;
    let mut full_row = existing_row.clone();
    let schema = target_table.schema();

    for col_name in updated_values.initialized_column_names(schema) {
        if let Some(value) = updated_values.get_by_name(schema, col_name) {
            full_row.set_by_name(schema, col_name, value.clone())?;
        }
    }

    Ok(full_row)
}

fn validate_not_null_constraints(schema: &Schema, row: &Row) -> Result<()> {
    for field in schema.fields() {
        if !field.is_nullable() {
            let is_null = match row.get_by_name(schema, &field.name) {
                Some(value) => value.is_null(),
                None => true,
            };
            if is_null {
                return Err(Error::NotNullViolation {
                    column: field.name.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_check_constraints(schema: &Schema, row: &Row) -> Result<()> {
    for check in schema.check_constraints() {
        if !check.enforced {
            continue;
        }
        if !evaluate_check_expression(&check.expression, row, schema)? {
            return Err(Error::check_constraint_violation(
                check.name.as_deref(),
                &check.expression,
            ));
        }
    }
    Ok(())
}

fn evaluate_check_expression(expression: &str, row: &Row, schema: &Schema) -> Result<bool> {
    let expr = expression.trim();

    const OPERATORS: &[&str] = &[">=", "<=", "!=", ">", "<", "="];

    for &operator in OPERATORS {
        if let Some((left, right)) = split_at_operator(expr, operator) {
            return evaluate_comparison(left.trim(), operator, right.trim(), row, schema);
        }
    }

    Ok(true)
}

fn split_at_operator<'a>(expression: &'a str, operator: &str) -> Option<(&'a str, &'a str)> {
    expression.find(operator).map(|pos| {
        let left = &expression[..pos];
        let right = &expression[pos + operator.len()..];
        (left, right)
    })
}

fn evaluate_comparison(
    left: &str,
    operator: &str,
    right: &str,
    row: &Row,
    schema: &Schema,
) -> Result<bool> {
    let left_val = match row.get_by_name(schema, left) {
        Some(value) => match value.as_i64() {
            Some(v) => v,
            None => return Ok(false),
        },
        None => return Ok(false),
    };

    let Ok(right_val) = right.parse::<i64>() else {
        return Ok(false);
    };

    let result = match operator {
        ">=" => left_val >= right_val,
        "<=" => left_val <= right_val,
        ">" => left_val > right_val,
        "<" => left_val < right_val,
        "=" => left_val == right_val,
        "!=" => left_val != right_val,
        _ => return Ok(true),
    };

    Ok(result)
}
