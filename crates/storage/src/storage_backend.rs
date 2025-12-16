use indexmap::IndexMap;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

use crate::{Column, Schema};

#[derive(Debug, Clone)]
pub struct ColumnarStorage {
    columns: IndexMap<String, Column>,
    row_count: usize,
}

impl ColumnarStorage {
    pub fn new(schema: &Schema) -> Self {
        let mut columns = IndexMap::new();
        for field in schema.fields() {
            columns.insert(field.name.clone(), Column::new(&field.data_type, 100));
        }
        Self {
            columns,
            row_count: 0,
        }
    }

    pub fn from_columns(columns: IndexMap<String, Column>, row_count: usize) -> Self {
        Self { columns, row_count }
    }

    pub fn columns(&self) -> &IndexMap<String, Column> {
        &self.columns
    }

    pub fn columns_mut(&mut self) -> &mut IndexMap<String, Column> {
        &mut self.columns
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn clear_data(&mut self) {
        for column in self.columns.values_mut() {
            column.clear();
        }
        self.row_count = 0;
    }

    pub fn push_row_values(&mut self, values: &[Value], schema: &Schema) -> Result<()> {
        for (idx, field) in schema.fields().iter().enumerate() {
            let column = self
                .columns
                .get_mut(&field.name)
                .ok_or_else(|| Error::column_not_found(field.name.clone()))?;
            let value = values.get(idx).cloned().unwrap_or(Value::null());
            column.push(value)?;
        }
        self.row_count += 1;
        Ok(())
    }
}
