use std::collections::HashSet;

use indexmap::IndexMap;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_common::types::coercion::CoercionRules;

use super::Table;
use super::indexes::TableIndexOps;
use crate::row::Row;
use crate::storage_backend::{RowStorage, StorageBackend, StorageLayout};

impl Table {
    pub fn insert_row(&mut self, mut row: Row) -> Result<()> {
        self.ensure_row_schema(&mut row)?;

        crate::constraints::apply_default_values(&self.schema, &mut row)?;

        let existing_rows = self.get_all_rows();

        crate::constraints::validate_row_constraints(&self.schema, &row, &existing_rows)?;

        self.coerce_row_to_schema(&mut row)?;

        let row_index = self.row_count();

        let row_values: Vec<Value> = row.values().to_vec();

        let schema = &self.schema;
        let storage = &mut self.storage;
        storage.as_storage_mut().insert_row(row, schema)?;

        self.update_indexes_on_insert(&row_values, row_index)?;

        Ok(())
    }

    pub fn insert_rows(&mut self, rows: Vec<Row>) -> Result<()> {
        if rows.len() > 10 {
            self.bulk_insert_rows_optimized(rows)
        } else {
            for row in rows {
                self.insert_row(row)?;
            }
            Ok(())
        }
    }

    pub fn bulk_insert_rows_optimized(&mut self, mut rows: Vec<Row>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        for row in &mut rows {
            self.ensure_row_schema(row)?;
            crate::constraints::apply_default_values(&self.schema, row)?;
        }

        let mut all_rows = self.get_all_rows();
        let mut prepared_rows = Vec::with_capacity(rows.len());

        for mut row in rows {
            crate::constraints::validate_row_constraints(&self.schema, &row, &all_rows)?;
            self.coerce_row_to_schema(&mut row)?;

            all_rows.push(row.clone());
            prepared_rows.push(row);
        }

        let starting_row_idx = self.row_count();

        let schema = &self.schema;
        let storage = &mut self.storage;
        storage
            .as_storage_mut()
            .bulk_insert_rows(prepared_rows.clone(), schema)?;

        for (i, row) in prepared_rows.iter().enumerate() {
            let row_idx = starting_row_idx + i;
            let row_values: Vec<Value> = row.values().to_vec();
            self.update_indexes_on_insert(&row_values, row_idx)?;
        }

        Ok(())
    }

    pub fn get_row(&self, index: usize) -> Result<Row> {
        self.storage().get_row(index, &self.schema)
    }

    pub fn get_all_rows(&self) -> Vec<Row> {
        self.storage().get_all_rows(&self.schema)
    }

    pub fn clear_rows(&mut self) -> Result<()> {
        self.storage_mut().clear_rows()?;

        if self.auto_increment_counter.is_some() {
            self.reset_auto_increment(1)?;
        }

        self.rebuild_all_indexes()?;

        Ok(())
    }

    pub fn filter_rows<F>(&self, predicate: F) -> Result<Vec<usize>>
    where
        F: Fn(&Row) -> Result<bool>,
    {
        let mut matching_indices = Vec::new();

        for i in 0..self.row_count() {
            let row = self.get_row(i)?;
            if predicate(&row)? {
                matching_indices.push(i);
            }
        }

        Ok(matching_indices)
    }

    pub fn update_rows<F>(
        &mut self,
        updates: &IndexMap<String, Value>,
        predicate: F,
    ) -> Result<usize>
    where
        F: Fn(&Row) -> Result<bool>,
    {
        for col_name in updates.keys() {
            if self.schema.field_index(col_name).is_none() {
                return Err(Error::column_not_found(format!(
                    "Column '{}' does not exist in table",
                    col_name
                )));
            }
        }

        let matching_indices = self.filter_rows(predicate)?;
        let update_count = matching_indices.len();

        let mut old_rows: Vec<Vec<Value>> = Vec::with_capacity(matching_indices.len());
        for &row_idx in &matching_indices {
            let row = self.get_row(row_idx)?;
            old_rows.push(row.values().to_vec());
        }

        match &mut self.storage {
            StorageBackend::Columnar(storage) => {
                for &row_idx in &matching_indices {
                    for (col_name, new_value) in updates {
                        let column = storage.columns_mut().get_mut(col_name).ok_or_else(|| {
                            Error::column_not_found(format!("Column not found: {}", col_name))
                        })?;

                        column.update(row_idx, new_value.clone())?;
                    }
                }
            }
            StorageBackend::Row(storage) => {
                let col_updates: Vec<(usize, Value)> = updates
                    .iter()
                    .filter_map(|(col_name, value)| {
                        self.schema
                            .field_index(col_name)
                            .map(|idx| (idx, value.clone()))
                    })
                    .collect();

                for &row_idx in &matching_indices {
                    for (col_idx, new_value) in &col_updates {
                        storage.update_cell(row_idx, *col_idx, new_value.clone())?;
                    }
                }
            }
        }

        for (i, &row_idx) in matching_indices.iter().enumerate() {
            let new_row = self.get_row(row_idx)?;
            let new_row_values: Vec<Value> = new_row.values().to_vec();
            self.update_indexes_on_update(&old_rows[i], &new_row_values, row_idx)?;
        }

        Ok(update_count)
    }

    pub fn update_cell_at_index(
        &mut self,
        row_idx: usize,
        col_name: &str,
        value: Value,
    ) -> Result<()> {
        let row_count = self.row_count();
        if row_idx >= row_count {
            return Err(Error::InvalidOperation(format!(
                "Row index {} out of bounds (row count: {})",
                row_idx, row_count
            )));
        }

        let col_idx = self
            .schema
            .field_index(col_name)
            .ok_or_else(|| Error::column_not_found(format!("Column '{}' not found", col_name)))?;

        match &mut self.storage {
            StorageBackend::Columnar(storage) => {
                let column = storage.columns_mut().get_mut(col_name).ok_or_else(|| {
                    Error::column_not_found(format!("Column '{}' not found", col_name))
                })?;
                column.update(row_idx, value)
            }
            StorageBackend::Row(storage) => storage.update_cell(row_idx, col_idx, value),
        }
    }

    pub fn delete_rows<F>(&mut self, predicate: F) -> Result<usize>
    where
        F: Fn(&Row) -> Result<bool>,
    {
        let matching_indices = self.filter_rows(predicate)?;
        let delete_count = matching_indices.len();

        if delete_count == 0 {
            return Ok(0);
        }

        let to_delete: HashSet<usize> = matching_indices.into_iter().collect();

        let keep_indices: Vec<usize> = (0..self.row_count())
            .filter(|i| !to_delete.contains(i))
            .collect();

        if keep_indices.is_empty() {
            self.storage_mut().clear_rows()?;
        } else if self.storage_layout() == StorageLayout::Columnar {
            let kept_table = self.gather(&keep_indices)?;
            if let Some(columnar) = kept_table.storage.as_columnar() {
                self.storage = StorageBackend::Columnar(columnar.clone());
            }
        } else {
            let mut rows = Vec::with_capacity(keep_indices.len());
            for idx in keep_indices {
                rows.push(self.get_row(idx)?);
            }
            self.storage = StorageBackend::Row(RowStorage::from_rows(rows));
        }

        self.rebuild_all_indexes()?;

        Ok(delete_count)
    }

    fn ensure_row_schema(&self, row: &mut Row) -> Result<()> {
        let expected = self.schema.field_count();
        if row.is_empty() {
            row.align_with_schema(&self.schema);
        } else if row.len() != expected {
            return Err(Error::InvalidOperation(format!(
                "Row column count {} does not match table schema columns {}",
                row.len(),
                expected
            )));
        }
        Ok(())
    }

    fn coerce_row_to_schema(&self, row: &mut Row) -> Result<()> {
        for (idx, field) in self.schema.fields().iter().enumerate() {
            let value = row.get(idx).cloned().unwrap_or(Value::null());
            let coerced = CoercionRules::coerce_value(value, &field.data_type)?;
            row.set(idx, coerced)?;
        }
        row.mark_all_initialized();
        Ok(())
    }
}
