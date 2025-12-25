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
        let found_idx = self.columns.keys().position(|k| k.to_uppercase() == upper);
        if let Some(idx) = found_idx {
            let key = self.columns.keys().nth(idx).cloned().unwrap();
            if let Some(col) = self.columns.shift_remove(&key) {
                self.columns.shift_insert(idx, new_name.to_string(), col);
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

    pub fn set_column_default(&mut self, col_name: &str, default: Value) -> Result<()> {
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
                    let mut new_field = f.clone();
                    new_field.default_value = Some(default.clone());
                    new_field
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    pub fn drop_column_default(&mut self, col_name: &str) -> Result<()> {
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
                    let mut new_field = f.clone();
                    new_field.default_value = None;
                    new_field
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    pub fn set_column_data_type(
        &mut self,
        col_name: &str,
        new_data_type: yachtsql_common::types::DataType,
    ) -> Result<()> {
        let upper = col_name.to_uppercase();
        let field_idx = self
            .schema
            .fields()
            .iter()
            .position(|f| f.name.to_uppercase() == upper);
        let field_idx = match field_idx {
            Some(idx) => idx,
            None => {
                return Err(yachtsql_common::error::Error::ColumnNotFound(
                    col_name.to_string(),
                ));
            }
        };

        let old_field = &self.schema.fields()[field_idx];
        let old_type = &old_field.data_type;

        let needs_conversion = !Self::types_compatible(old_type, &new_data_type);

        if needs_conversion {
            let col_name_key = self
                .columns
                .keys()
                .find(|k| k.to_uppercase() == upper)
                .cloned();
            if let Some(key) = col_name_key
                && let Some(old_col) = self.columns.get(&key)
            {
                let new_col = Self::convert_column(old_col, old_type, &new_data_type)?;
                self.columns.insert(key, new_col);
            }
        }

        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                if f.name.to_uppercase() == upper {
                    crate::Field::new(f.name.clone(), new_data_type.clone(), f.mode)
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    pub fn set_column_collation(&mut self, col_name: &str, collation: String) -> Result<()> {
        let upper = col_name.to_uppercase();
        let field_idx = self
            .schema
            .fields()
            .iter()
            .position(|f| f.name.to_uppercase() == upper);
        let field_idx = match field_idx {
            Some(idx) => idx,
            None => {
                return Err(yachtsql_common::error::Error::ColumnNotFound(
                    col_name.to_string(),
                ));
            }
        };

        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == field_idx {
                    let mut new_field = f.clone();
                    new_field.collation = Some(collation.clone());
                    new_field
                } else {
                    f.clone()
                }
            })
            .collect();
        self.schema = Schema::from_fields(fields);
        Ok(())
    }

    fn types_compatible(
        old_type: &yachtsql_common::types::DataType,
        new_type: &yachtsql_common::types::DataType,
    ) -> bool {
        use yachtsql_common::types::DataType;
        matches!(
            (old_type, new_type),
            (DataType::String, DataType::String)
                | (DataType::Int64, DataType::Int64)
                | (DataType::Float64, DataType::Float64)
                | (DataType::Numeric(_), DataType::Numeric(_))
        )
    }

    fn convert_column(
        old_col: &Column,
        _old_type: &yachtsql_common::types::DataType,
        new_type: &yachtsql_common::types::DataType,
    ) -> Result<Column> {
        use rust_decimal::Decimal;
        use yachtsql_common::types::DataType;

        match new_type {
            DataType::Numeric(_) => {
                let len = old_col.len();
                let mut new_data = Vec::with_capacity(len);
                let mut new_nulls = crate::NullBitmap::new_valid(len);
                for i in 0..len {
                    let val = old_col.get_value(i);
                    match val {
                        Value::Null => {
                            new_data.push(Decimal::ZERO);
                            new_nulls.set_null(i);
                        }
                        Value::Int64(v) => {
                            new_data.push(Decimal::from(v));
                        }
                        Value::Float64(v) => {
                            let f = v.into_inner();
                            new_data.push(
                                Decimal::try_from(f).unwrap_or_else(|_| Decimal::from(f as i64)),
                            );
                        }
                        Value::Numeric(d) => {
                            new_data.push(d);
                        }
                        _ => {
                            return Err(yachtsql_common::error::Error::invalid_query(format!(
                                "Cannot convert {:?} to NUMERIC",
                                val
                            )));
                        }
                    }
                }
                Ok(Column::Numeric {
                    data: new_data,
                    nulls: new_nulls,
                })
            }
            _ => Err(yachtsql_common::error::Error::UnsupportedFeature(format!(
                "Data type conversion to {:?} not yet implemented",
                new_type
            ))),
        }
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
