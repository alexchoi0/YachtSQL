//! Table data structure for query results.

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_core::types::coercion::CoercionRules;
use yachtsql_storage::{Column, Row, Schema};

#[inline]
fn are_types_compatible(col_type: &DataType, schema_type: &DataType) -> bool {
    if col_type == schema_type {
        return true;
    }

    if matches!(col_type, DataType::Unknown) || matches!(schema_type, DataType::Unknown) {
        return true;
    }

    if CoercionRules::can_implicitly_coerce(col_type, schema_type) {
        return true;
    }

    if matches!(
        (col_type, schema_type),
        (DataType::Struct(_), DataType::Custom(_))
            | (DataType::Custom(_), DataType::Struct(_))
            | (DataType::Struct(_), DataType::Struct(_))
    ) {
        return true;
    }

    matches!(
        (col_type, schema_type),
        (DataType::Float64, DataType::Int64)
            | (DataType::Float64, DataType::BigNumeric)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFormat {
    Row,
    Column,
}

#[derive(Debug, Clone)]
enum StorageData {
    Rows(Vec<Row>),
    Columns(Vec<Column>),
}

#[derive(Debug, Clone)]
pub struct Table {
    schema: Schema,
    storage: StorageData,
    num_rows: usize,
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        if self.num_rows != other.num_rows || self.schema != other.schema {
            return false;
        }

        for row_idx in 0..self.num_rows {
            let self_row = match self.row(row_idx) {
                Ok(r) => r,
                Err(_) => return false,
            };
            let other_row = match other.row(row_idx) {
                Ok(r) => r,
                Err(_) => return false,
            };
            if self_row.values() != other_row.values() {
                return false;
            }
        }
        true
    }
}

impl Table {
    pub fn new(schema: Schema, columns: Vec<Column>) -> Result<Self> {
        if columns.len() != schema.field_count() {
            return Err(Error::schema_mismatch(format!(
                "Expected {} columns, got {}",
                schema.field_count(),
                columns.len()
            )));
        }

        let num_rows = columns.first().map(|c| c.len()).unwrap_or(0);
        for (i, column) in columns.iter().enumerate() {
            if column.len() != num_rows {
                return Err(Error::schema_mismatch(format!(
                    "Column {} has {} rows, expected {}",
                    i,
                    column.len(),
                    num_rows
                )));
            }
        }

        for (column, field) in columns.iter().zip(schema.fields()) {
            let col_type = column.data_type();
            let schema_type = &field.data_type;

            if !are_types_compatible(&col_type, schema_type) {
                return Err(Error::TypeMismatch {
                    expected: schema_type.to_string(),
                    actual: col_type.to_string(),
                });
            }
        }

        Ok(Self {
            schema,
            storage: StorageData::Columns(columns),
            num_rows,
        })
    }

    pub fn from_rows(schema: Schema, rows: Vec<Row>) -> Result<Self> {
        let num_rows = rows.len();

        for (i, row) in rows.iter().enumerate() {
            if row.values().len() != schema.field_count() {
                return Err(Error::schema_mismatch(format!(
                    "Row {} has {} values, expected {}",
                    i,
                    row.values().len(),
                    schema.field_count()
                )));
            }
        }

        Ok(Self {
            schema,
            storage: StorageData::Rows(rows),
            num_rows,
        })
    }

    pub fn empty(schema: Schema) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| Column::new(&field.data_type, 0))
            .collect();

        Self {
            schema,
            storage: StorageData::Columns(columns),
            num_rows: 0,
        }
    }

    pub fn from_values(schema: Schema, values: Vec<Vec<Value>>) -> Result<Self> {
        if values.is_empty() {
            return Ok(Self::empty(schema));
        }

        let num_rows = values.len();
        let num_cols = schema.field_count();

        if values.iter().any(|row| row.len() != num_cols) {
            return Err(Error::schema_mismatch(
                "All rows must have the same number of columns".to_string(),
            ));
        }

        let mut columns: Vec<Column> = schema
            .fields()
            .iter()
            .map(|field| Column::new(&field.data_type, num_rows))
            .collect();

        for row in values {
            for (col_idx, value) in row.into_iter().enumerate() {
                columns[col_idx].push(value)?;
            }
        }

        Self::new(schema, columns)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn num_columns(&self) -> usize {
        self.schema.field_count()
    }

    pub fn column(&self, index: usize) -> Option<&Column> {
        match &self.storage {
            StorageData::Columns(columns) => columns.get(index),
            StorageData::Rows(_) => None,
        }
    }

    pub fn columns(&self) -> Option<&[Column]> {
        match &self.storage {
            StorageData::Columns(columns) => Some(columns.as_slice()),
            StorageData::Rows(_) => None,
        }
    }

    pub fn storage_format(&self) -> StorageFormat {
        match &self.storage {
            StorageData::Rows(_) => StorageFormat::Row,
            StorageData::Columns(_) => StorageFormat::Column,
        }
    }

    pub fn row(&self, index: usize) -> Result<Row> {
        if index >= self.num_rows {
            return Err(Error::InvalidOperation(format!(
                "Row index {} out of bounds (num_rows: {})",
                index, self.num_rows
            )));
        }

        match &self.storage {
            StorageData::Rows(rows) => Ok(rows[index].clone()),
            StorageData::Columns(columns) => {
                let mut values = Vec::with_capacity(self.schema.field_count());
                for column in columns {
                    values.push(column.get(index)?);
                }
                Ok(Row::from_values(values))
            }
        }
    }

    pub fn rows(&self) -> Result<Vec<Row>> {
        match &self.storage {
            StorageData::Rows(rows) => Ok(rows.clone()),
            StorageData::Columns(_) => {
                let mut result = Vec::with_capacity(self.num_rows);
                for i in 0..self.num_rows {
                    result.push(self.row(i)?);
                }
                Ok(result)
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    pub fn to_column_format(&self) -> Result<Self> {
        match &self.storage {
            StorageData::Columns(_) => Ok(self.clone()),
            StorageData::Rows(rows) => {
                if rows.is_empty() {
                    return Ok(Self::empty(self.schema.clone()));
                }

                let num_cols = self.schema.field_count();
                let mut columns: Vec<Column> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|field| Column::new(&field.data_type, self.num_rows))
                    .collect();

                for row in rows {
                    let values = row.values();
                    for (col_idx, value) in values.iter().enumerate() {
                        if col_idx < num_cols {
                            columns[col_idx].push(value.clone())?;
                        }
                    }
                }

                Ok(Self {
                    schema: self.schema.clone(),
                    storage: StorageData::Columns(columns),
                    num_rows: self.num_rows,
                })
            }
        }
    }
}
