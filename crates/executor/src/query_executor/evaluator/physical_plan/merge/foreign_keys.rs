use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_storage::TableConstraintOps;

use super::mutations::MergeMutations;
use crate::storage::table::Row;

pub(super) fn validate_merge_foreign_keys(
    mutations: &MergeMutations,
    target_table: &crate::storage::table::Table,
    storage: &crate::storage::Storage,
    target_table_name: &str,
) -> Result<()> {
    let foreign_keys = target_table.foreign_keys();
    if foreign_keys.is_empty() {
        return Ok(());
    }

    let schema = target_table.schema();

    for insert_row in mutations.inserts() {
        validate_row_foreign_keys(insert_row, foreign_keys, storage, target_table_name, schema)?;
    }

    validate_update_foreign_keys(
        mutations.updates(),
        target_table,
        foreign_keys,
        storage,
        target_table_name,
    )?;

    Ok(())
}

fn validate_update_foreign_keys(
    updates: &[(usize, Row)],
    target_table: &crate::storage::table::Table,
    foreign_keys: &[crate::storage::foreign_keys::ForeignKey],
    storage: &crate::storage::Storage,
    target_table_name: &str,
) -> Result<()> {
    let schema = target_table.schema();
    for (row_idx, updated_values) in updates {
        if !has_fk_column_update(foreign_keys, updated_values, schema) {
            continue;
        }

        let full_row = build_updated_row(target_table, *row_idx, updated_values)?;
        validate_row_foreign_keys(&full_row, foreign_keys, storage, target_table_name, schema)?;
    }
    Ok(())
}

fn has_fk_column_update(
    foreign_keys: &[crate::storage::foreign_keys::ForeignKey],
    updated_values: &Row,
    schema: &yachtsql_storage::Schema,
) -> bool {
    foreign_keys.iter().any(|fk| {
        fk.child_columns
            .iter()
            .any(|col| updated_values.contains_column(schema, col))
    })
}

fn build_updated_row(
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

fn validate_row_foreign_keys(
    row: &Row,
    foreign_keys: &[crate::storage::foreign_keys::ForeignKey],
    storage: &crate::storage::Storage,
    target_table_name: &str,
    schema: &yachtsql_storage::Schema,
) -> Result<()> {
    for fk in foreign_keys {
        validate_single_foreign_key(row, fk, storage, target_table_name, schema)?;
    }
    Ok(())
}

fn validate_single_foreign_key(
    row: &Row,
    fk: &crate::storage::foreign_keys::ForeignKey,
    storage: &crate::storage::Storage,
    target_table_name: &str,
    schema: &yachtsql_storage::Schema,
) -> Result<()> {
    if !fk.is_enforced() {
        return Ok(());
    }

    let fk_values = extract_fk_values(row, &fk.child_columns, schema);

    if has_null_value(&fk_values) {
        return Ok(());
    }

    let parent_table = storage
        .get_table(&fk.parent_table)
        .ok_or_else(|| Error::table_not_found(fk.parent_table.clone()))?;

    if !parent_row_exists(parent_table, &fk.parent_columns, &fk_values)? {
        return Err(create_fk_violation_error(fk, target_table_name));
    }

    Ok(())
}

fn extract_fk_values(
    row: &Row,
    columns: &[String],
    schema: &yachtsql_storage::Schema,
) -> Vec<Value> {
    columns
        .iter()
        .map(|col| {
            row.get_by_name(schema, col)
                .cloned()
                .unwrap_or(Value::null())
        })
        .collect()
}

fn has_null_value(values: &[Value]) -> bool {
    values.iter().any(|v| v == &Value::null())
}

fn parent_row_exists(
    parent_table: &crate::storage::table::Table,
    parent_columns: &[String],
    fk_values: &[Value],
) -> Result<bool> {
    let schema = parent_table.schema();
    for parent_row_idx in 0..parent_table.row_count() {
        let parent_row = parent_table.get_row(parent_row_idx)?;
        if row_matches_fk_values(&parent_row, parent_columns, fk_values, schema) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn row_matches_fk_values(
    row: &Row,
    columns: &[String],
    values: &[Value],
    schema: &yachtsql_storage::Schema,
) -> bool {
    columns
        .iter()
        .enumerate()
        .all(|(i, col)| match row.get_by_name(schema, col) {
            Some(row_value) => row_value == &values[i],
            None => &Value::null() == &values[i],
        })
}

fn create_fk_violation_error(
    fk: &crate::storage::foreign_keys::ForeignKey,
    target_table_name: &str,
) -> Error {
    Error::ForeignKeyViolation {
        child_table: target_table_name.to_string(),
        fk_columns: fk.child_columns.clone(),
        parent_table: fk.parent_table.clone(),
        parent_columns: fk.parent_columns.clone(),
        message: format!(
            "Foreign key constraint violation: No matching row in parent table '{}'",
            fk.parent_table
        ),
    }
}
