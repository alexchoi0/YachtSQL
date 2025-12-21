use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use yachtsql_common::error::Result;
use yachtsql_common::types::Value;

use crate::{Column, Record, Schema};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    schema: Schema,
    columns: IndexMap<String, Column>,
    row_count: usize,
}

impl Table {
    pub fn new(schema: Schema) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|f| (f.name.clone(), Column::new(&f.data_type)))
            .collect();
        Self {
            schema,
            columns,
            row_count: 0,
        }
    }

    pub fn from_columns(schema: Schema, columns: IndexMap<String, Column>) -> Self {
        let row_count = columns.values().next().map(|c| c.len()).unwrap_or(0);
        Self {
            schema,
            columns,
            row_count,
        }
    }

    pub fn empty(schema: Schema) -> Self {
        Self::new(schema)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn num_rows(&self) -> usize {
        self.row_count
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn column(&self, idx: usize) -> Option<&Column> {
        self.columns.values().nth(idx)
    }

    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.columns.get(name)
    }

    pub fn columns(&self) -> &IndexMap<String, Column> {
        &self.columns
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn columns_mut(&mut self) -> &mut IndexMap<String, Column> {
        &mut self.columns
    }

    pub fn push_row(&mut self, values: Vec<Value>) -> Result<()> {
        for (col, value) in self.columns.values_mut().zip(values.into_iter()) {
            col.push(value)?;
        }
        self.row_count += 1;
        Ok(())
    }

    pub fn get_row(&self, index: usize) -> Result<Record> {
        if index >= self.row_count {
            return Err(yachtsql_common::error::Error::invalid_query(format!(
                "Row index {} out of bounds (count: {})",
                index, self.row_count
            )));
        }
        let columns: Vec<&Column> = self.columns.values().collect();
        let values: Vec<Value> = columns.iter().map(|c| c.get_value(index)).collect();
        Ok(Record::from_values(values))
    }

    pub fn to_records(&self) -> Result<Vec<Record>> {
        let mut records = Vec::with_capacity(self.row_count);
        for i in 0..self.row_count {
            records.push(self.get_row(i)?);
        }
        Ok(records)
    }

    pub fn rows(&self) -> Result<Vec<Record>> {
        self.to_records()
    }

    pub fn from_records(schema: Schema, records: Vec<Record>) -> Result<Self> {
        let mut table = Self::new(schema);
        for record in records {
            table.push_row(record.into_values())?;
        }
        Ok(table)
    }

    pub fn from_values(schema: Schema, values: Vec<Vec<Value>>) -> Result<Self> {
        let mut table = Self::new(schema);
        for row in values {
            table.push_row(row)?;
        }
        Ok(table)
    }

    pub fn clear(&mut self) {
        for col in self.columns.values_mut() {
            col.clear();
        }
        self.row_count = 0;
    }

    pub fn remove_row(&mut self, index: usize) {
        for col in self.columns.values_mut() {
            col.remove(index);
        }
        if self.row_count > 0 {
            self.row_count -= 1;
        }
    }

    pub fn update_row(&mut self, index: usize, values: Vec<Value>) -> Result<()> {
        for (col, value) in self.columns.values_mut().zip(values.into_iter()) {
            col.set(index, value)?;
        }
        Ok(())
    }

    pub fn drop_column(&mut self, name: &str) -> Result<()> {
        let upper = name.to_uppercase();
        let found = self
            .columns
            .keys()
            .find(|k| k.to_uppercase() == upper)
            .cloned();
        if let Some(key) = found {
            self.columns.shift_remove(&key);
            let fields: Vec<_> = self
                .schema
                .fields()
                .iter()
                .filter(|f| f.name.to_uppercase() != upper)
                .cloned()
                .collect();
            self.schema = Schema::from_fields(fields);
            Ok(())
        } else {
            Err(yachtsql_common::error::Error::ColumnNotFound(
                name.to_string(),
            ))
        }
    }

    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        let upper = old_name.to_uppercase();
        let found = self
            .columns
            .keys()
            .find(|k| k.to_uppercase() == upper)
            .cloned();
        if let Some(key) = found {
            if let Some(col) = self.columns.shift_remove(&key) {
                self.columns.insert(new_name.to_string(), col);
            }
            let fields: Vec<_> = self
                .schema
                .fields()
                .iter()
                .map(|f| {
                    if f.name.to_uppercase() == upper {
                        crate::Field::new(new_name.to_string(), f.data_type.clone(), f.mode)
                    } else {
                        f.clone()
                    }
                })
                .collect();
            self.schema = Schema::from_fields(fields);
            Ok(())
        } else {
            Err(yachtsql_common::error::Error::ColumnNotFound(
                old_name.to_string(),
            ))
        }
    }

    pub fn set_column_not_null(&mut self, col_name: &str) -> Result<()> {
        let upper = col_name.to_uppercase();
        let found = self
            .schema
            .fields()
            .iter()
            .any(|f| f.name.to_uppercase() == upper);
        if !found {
            return Err(yachtsql_common::error::Error::ColumnNotFound(
                col_name.to_string(),
            ));
        }
        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                if f.name.to_uppercase() == upper {
                    crate::Field::new(
                        f.name.clone(),
                        f.data_type.clone(),
                        crate::FieldMode::Required,
                    )
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    pub fn set_column_nullable(&mut self, col_name: &str) -> Result<()> {
        let upper = col_name.to_uppercase();
        let found = self
            .schema
            .fields()
            .iter()
            .any(|f| f.name.to_uppercase() == upper);
        if !found {
            return Err(yachtsql_common::error::Error::ColumnNotFound(
                col_name.to_string(),
            ));
        }
        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                if f.name.to_uppercase() == upper {
                    crate::Field::new(
                        f.name.clone(),
                        f.data_type.clone(),
                        crate::FieldMode::Nullable,
                    )
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    pub fn with_schema(&self, new_schema: Schema) -> Table {
        let mut new_columns = IndexMap::new();
        for (old_col, new_field) in self.columns.values().zip(new_schema.fields().iter()) {
            new_columns.insert(new_field.name.clone(), old_col.clone());
        }
        Table {
            schema: new_schema,
            columns: new_columns,
            row_count: self.row_count,
        }
    }

    pub fn to_query_result(&self) -> Result<yachtsql_common::QueryResult> {
        use yachtsql_common::{ColumnInfo, QueryResult, Row};

        let schema: Vec<ColumnInfo> = self
            .schema
            .fields()
            .iter()
            .map(|f| ColumnInfo::new(&f.name, f.data_type.to_bq_type()))
            .collect();

        let records = self.to_records()?;
        let rows: Vec<Row> = records
            .into_iter()
            .map(|record| Row::new(record.into_values()))
            .collect();

        Ok(QueryResult::new(schema, rows))
    }
}

pub trait TableSchemaOps {
    fn add_column(&mut self, field: crate::Field, default: Option<Value>) -> Result<()>;
}

impl TableSchemaOps for Table {
    fn add_column(&mut self, field: crate::Field, default: Option<Value>) -> Result<()> {
        let default_val = default.unwrap_or(Value::Null);
        let mut col = Column::new(&field.data_type);
        for _ in 0..self.row_count {
            col.push(default_val.clone())?;
        }
        self.columns.insert(field.name.clone(), col);
        let mut fields: Vec<_> = self.schema.fields().to_vec();
        fields.push(field);
        self.schema = Schema::from_fields(fields);
        Ok(())
    }
}
