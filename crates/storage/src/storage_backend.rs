use indexmap::IndexMap;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use crate::{Column, Row, Schema};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageLayout {
    Columnar,
    Row,
}

pub trait TableStorage: Send {
    fn layout(&self) -> StorageLayout;
    fn row_count(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.row_count() == 0
    }

    fn insert_row(&mut self, row: Row, schema: &Schema) -> Result<()>;
    fn insert_rows(&mut self, rows: Vec<Row>, schema: &Schema) -> Result<()>;
    fn bulk_insert_rows(&mut self, rows: Vec<Row>, schema: &Schema) -> Result<()>;

    fn get_row(&self, index: usize, schema: &Schema) -> Result<Row>;
    fn get_all_rows(&self, schema: &Schema) -> Vec<Row>;
    fn clear_rows(&mut self) -> Result<()>;

    fn columns(&self) -> Option<&IndexMap<String, Column>>;
    fn columns_mut(&mut self) -> Option<&mut IndexMap<String, Column>>;
}

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

    pub fn clear_data(&mut self) {
        for column in self.columns.values_mut() {
            column.clear();
        }
        self.row_count = 0;
    }

    fn push_row_values(&mut self, values: &[Value], schema: &Schema) -> Result<()> {
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

impl TableStorage for ColumnarStorage {
    fn layout(&self) -> StorageLayout {
        StorageLayout::Columnar
    }

    fn row_count(&self) -> usize {
        self.row_count
    }

    fn insert_row(&mut self, row: Row, schema: &Schema) -> Result<()> {
        self.push_row_values(row.values(), schema)
    }

    fn insert_rows(&mut self, rows: Vec<Row>, schema: &Schema) -> Result<()> {
        for row in rows {
            self.insert_row(row, schema)?;
        }
        Ok(())
    }

    fn bulk_insert_rows(&mut self, rows: Vec<Row>, schema: &Schema) -> Result<()> {
        self.insert_rows(rows, schema)
    }

    fn get_row(&self, index: usize, schema: &Schema) -> Result<Row> {
        if index >= self.row_count {
            return Err(Error::InvalidOperation(format!(
                "Row index {} out of bounds",
                index
            )));
        }

        let mut values = Vec::with_capacity(schema.field_count());
        for field in schema.fields() {
            let column = self
                .columns
                .get(&field.name)
                .ok_or_else(|| Error::column_not_found(field.name.clone()))?;
            values.push(column.get(index)?);
        }

        let mut row = Row::from_values(values);
        row.mark_all_initialized();
        Ok(row)
    }

    fn get_all_rows(&self, schema: &Schema) -> Vec<Row> {
        (0..self.row_count)
            .filter_map(|idx| self.get_row(idx, schema).ok())
            .collect()
    }

    fn clear_rows(&mut self) -> Result<()> {
        for column in self.columns.values_mut() {
            column.clear();
        }
        self.row_count = 0;
        Ok(())
    }

    fn columns(&self) -> Option<&IndexMap<String, Column>> {
        Some(&self.columns)
    }

    fn columns_mut(&mut self) -> Option<&mut IndexMap<String, Column>> {
        Some(&mut self.columns)
    }
}

#[derive(Debug, Clone, Default)]
pub struct RowStorage {
    rows: Vec<Row>,
}

impl RowStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_rows(rows: Vec<Row>) -> Self {
        Self { rows }
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn rows_mut(&mut self) -> &mut Vec<Row> {
        &mut self.rows
    }

    pub fn update_cell(&mut self, row_idx: usize, col_idx: usize, value: Value) -> Result<()> {
        if row_idx >= self.rows.len() {
            return Err(Error::InvalidOperation(format!(
                "Row index {} out of bounds (len: {})",
                row_idx,
                self.rows.len()
            )));
        }
        self.rows[row_idx].set(col_idx, value)
    }

    pub fn add_column_with_default(&mut self, default_value: Value) -> Result<()> {
        for row in &mut self.rows {
            let len = row.len();
            row.set(len, default_value.clone())?;
        }
        Ok(())
    }

    pub fn drop_column_at_index(&mut self, col_idx: usize) -> Result<()> {
        for row in &mut self.rows {
            if col_idx >= row.len() {
                continue;
            }
            let new_values: Vec<Value> = row
                .values()
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != col_idx)
                .map(|(_, v)| v.clone())
                .collect();

            let new_initialized: Vec<bool> = (0..row.len())
                .filter(|i| *i != col_idx)
                .map(|i| row.is_initialized(i))
                .collect();

            *row = Row::from_values(new_values);
            for (i, &init) in new_initialized.iter().enumerate() {
                if !init && let Some(value) = row.get_mut(i) {
                    *value = Value::null();
                }
            }
        }
        Ok(())
    }

    pub fn project_columns(&self, col_indices: &[usize]) -> Result<Vec<Row>> {
        let mut projected_rows = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let mut new_values = Vec::with_capacity(col_indices.len());
            let mut new_initialized = Vec::with_capacity(col_indices.len());
            for &idx in col_indices {
                if idx >= row.len() {
                    return Err(Error::InvalidOperation(format!(
                        "Column index {} out of bounds (len: {})",
                        idx,
                        row.len()
                    )));
                }
                new_values.push(row.values()[idx].clone());
                new_initialized.push(row.is_initialized(idx));
            }
            let mut new_row = Row::from_values(new_values);
            for (i, &init) in new_initialized.iter().enumerate() {
                if !init && let Some(value) = new_row.get_mut(i) {
                    *value = Value::null();
                }
            }
            projected_rows.push(new_row);
        }
        Ok(projected_rows)
    }

    pub fn slice_rows(&self, start: usize, len: usize) -> Vec<Row> {
        let end = (start + len).min(self.rows.len());
        self.rows[start..end].to_vec()
    }

    pub fn gather_rows(&self, indices: &[usize]) -> Result<Vec<Row>> {
        let mut gathered = Vec::with_capacity(indices.len());
        for &idx in indices {
            if idx >= self.rows.len() {
                return Err(Error::InvalidOperation(format!(
                    "Row index {} out of bounds (len: {})",
                    idx,
                    self.rows.len()
                )));
            }
            gathered.push(self.rows[idx].clone());
        }
        Ok(gathered)
    }

    pub fn compute_column_statistics(&self, schema: &Schema) -> Vec<(usize, usize)> {
        if self.rows.is_empty() {
            return vec![(0, 0); schema.field_count()];
        }

        let col_count = schema.field_count();
        let mut null_counts = vec![0usize; col_count];

        for row in &self.rows {
            #[allow(clippy::needless_range_loop)]
            for col_idx in 0..col_count.min(row.len()) {
                if !row.is_initialized(col_idx) || row.values()[col_idx].is_null() {
                    null_counts[col_idx] += 1;
                }
            }
        }

        null_counts
            .into_iter()
            .map(|null_count| (null_count, 0))
            .collect()
    }
}

impl TableStorage for RowStorage {
    fn layout(&self) -> StorageLayout {
        StorageLayout::Row
    }

    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn insert_row(&mut self, row: Row, _schema: &Schema) -> Result<()> {
        self.rows.push(row);
        Ok(())
    }

    fn insert_rows(&mut self, rows: Vec<Row>, _schema: &Schema) -> Result<()> {
        self.rows.extend(rows);
        Ok(())
    }

    fn bulk_insert_rows(&mut self, mut rows: Vec<Row>, _schema: &Schema) -> Result<()> {
        self.rows.append(&mut rows);
        Ok(())
    }

    fn get_row(&self, index: usize, _schema: &Schema) -> Result<Row> {
        self.rows
            .get(index)
            .cloned()
            .ok_or_else(|| Error::InvalidOperation(format!("Row index {} out of bounds", index)))
    }

    fn get_all_rows(&self, _schema: &Schema) -> Vec<Row> {
        self.rows.clone()
    }

    fn clear_rows(&mut self) -> Result<()> {
        self.rows.clear();
        Ok(())
    }

    fn columns(&self) -> Option<&IndexMap<String, Column>> {
        None
    }

    fn columns_mut(&mut self) -> Option<&mut IndexMap<String, Column>> {
        None
    }
}

#[derive(Debug, Clone)]
pub enum StorageBackend {
    Columnar(ColumnarStorage),
    Row(RowStorage),
}

impl StorageBackend {
    pub fn columnar(schema: &Schema) -> Self {
        Self::Columnar(ColumnarStorage::new(schema))
    }

    pub fn row() -> Self {
        Self::Row(RowStorage::new())
    }

    pub fn as_storage(&self) -> &dyn TableStorage {
        match self {
            StorageBackend::Columnar(storage) => storage,
            StorageBackend::Row(storage) => storage,
        }
    }

    pub fn as_storage_mut(&mut self) -> &mut dyn TableStorage {
        match self {
            StorageBackend::Columnar(storage) => storage,
            StorageBackend::Row(storage) => storage,
        }
    }

    pub fn as_columnar(&self) -> Option<&ColumnarStorage> {
        match self {
            StorageBackend::Columnar(storage) => Some(storage),
            StorageBackend::Row(_) => None,
        }
    }

    pub fn as_columnar_mut(&mut self) -> Option<&mut ColumnarStorage> {
        match self {
            StorageBackend::Columnar(storage) => Some(storage),
            StorageBackend::Row(_) => None,
        }
    }

    pub fn as_row(&self) -> Option<&RowStorage> {
        match self {
            StorageBackend::Row(storage) => Some(storage),
            _ => None,
        }
    }

    pub fn as_row_mut(&mut self) -> Option<&mut RowStorage> {
        match self {
            StorageBackend::Row(storage) => Some(storage),
            _ => None,
        }
    }
}
