use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};

use super::Table;
use crate::{Column, Field, Schema};

pub trait TableSchemaOps {
    fn add_column(&mut self, field: Field, default_value: Option<Value>) -> Result<()>;
    fn drop_column(&mut self, column_name: &str) -> Result<()>;
    fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<()>;
    fn alter_column(
        &mut self,
        column_name: &str,
        new_data_type: Option<DataType>,
        set_not_null: Option<bool>,
    ) -> Result<()>;
    fn schema_mut(&mut self) -> &mut Schema;
}

impl TableSchemaOps for Table {
    fn add_column(&mut self, field: Field, default_value: Option<Value>) -> Result<()> {
        if self.schema.field_index(&field.name).is_some() {
            return Err(Error::invalid_query(format!(
                "Column '{}' already exists",
                field.name
            )));
        }

        let current_row_count = self.row_count();
        if !field.is_nullable() && default_value.is_none() && current_row_count > 0 {
            return Err(Error::invalid_query(format!(
                "Cannot add NOT NULL column '{}' without DEFAULT value to table with existing data",
                field.name
            )));
        }

        let value_to_insert = default_value.unwrap_or(Value::null());
        let mut column = Column::new(&field.data_type, 100);

        if current_row_count > 0 {
            for _ in 0..current_row_count {
                column.push(value_to_insert.clone())?;
            }
        }

        self.storage
            .columns_mut()
            .insert(field.name.clone(), column);

        let mut new_fields = self.schema.fields().to_vec();
        new_fields.push(field);
        self.schema = Schema::from_fields(new_fields);

        Ok(())
    }

    fn drop_column(&mut self, column_name: &str) -> Result<()> {
        self.schema
            .field_index(column_name)
            .ok_or_else(|| Error::invalid_query(format!("Column '{}' not found", column_name)))?;

        if self.schema.fields().len() <= 1 {
            return Err(Error::invalid_query(
                "Cannot drop last column - table must have at least one column".to_string(),
            ));
        }

        self.storage.columns_mut().shift_remove(column_name);

        let new_fields: Vec<Field> = self
            .schema
            .fields()
            .iter()
            .filter(|f| f.name != column_name)
            .cloned()
            .collect();
        self.schema = Schema::from_fields(new_fields);

        Ok(())
    }

    fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        if self.schema.field_index(old_name).is_none() {
            return Err(Error::invalid_query(format!(
                "Column '{}' not found",
                old_name
            )));
        }

        if self.schema.field_index(new_name).is_some() {
            return Err(Error::invalid_query(format!(
                "Column '{}' already exists",
                new_name
            )));
        }

        if let Some(column) = self.storage.columns_mut().shift_remove(old_name) {
            self.storage
                .columns_mut()
                .insert(new_name.to_string(), column);
        }

        let new_fields: Vec<Field> = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                if f.name == old_name {
                    Field {
                        name: new_name.to_string(),
                        data_type: f.data_type.clone(),
                        mode: f.mode,
                        description: f.description.clone(),
                        source_table: f.source_table.clone(),
                    }
                } else {
                    f.clone()
                }
            })
            .collect();

        self.schema = Schema::from_fields(new_fields);

        Ok(())
    }

    fn alter_column(
        &mut self,
        column_name: &str,
        new_data_type: Option<DataType>,
        set_not_null: Option<bool>,
    ) -> Result<()> {
        if !self.storage.columns().contains_key(column_name) {
            return Err(Error::invalid_query(format!(
                "Column '{}' not found",
                column_name
            )));
        }

        if let Some(true) = set_not_null {
            let column = self.storage.columns().get(column_name).unwrap();

            for i in 0..column.len() {
                let value = column.get(i)?;
                if value.is_null() {
                    return Err(Error::invalid_query(format!(
                        "Cannot add NOT NULL constraint to column '{}': existing data contains NULL values",
                        column_name
                    )));
                }
            }
        }

        if let Some(ref new_type) = new_data_type {
            self.convert_column_type(column_name, new_type)?;
        }

        let mut new_fields: Vec<Field> = self.schema.fields().to_vec();

        for field in &mut new_fields {
            if field.name == column_name {
                if let Some(ref new_type) = new_data_type {
                    field.data_type = new_type.clone();
                }

                if let Some(not_null) = set_not_null {
                    use crate::schema::FieldMode;
                    field.mode = if not_null {
                        FieldMode::Required
                    } else {
                        FieldMode::Nullable
                    };
                }
                break;
            }
        }

        self.schema = Schema::from_fields(new_fields);
        Ok(())
    }

    fn schema_mut(&mut self) -> &mut Schema {
        &mut self.schema
    }
}

impl Table {
    pub(crate) fn convert_column_type(
        &mut self,
        column_name: &str,
        new_type: &DataType,
    ) -> Result<()> {
        let old_column = self
            .storage
            .columns_mut()
            .get_mut(column_name)
            .ok_or_else(|| Error::invalid_query(format!("Column '{}' not found", column_name)))?;

        let row_count = old_column.len();
        let mut new_column = Column::new(new_type, row_count);

        for i in 0..row_count {
            let old_value = old_column.get(i)?;
            let new_value = convert_value_to_type(&old_value, new_type)?;
            new_column.push(new_value)?;
        }

        *old_column = new_column;

        Ok(())
    }
}

fn convert_value_to_type(value: &Value, target_type: &DataType) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    match target_type {
        DataType::Int64 => {
            if let Some(i) = value.as_i64() {
                Ok(Value::int64(i))
            } else if let Some(f) = value.as_f64() {
                Ok(Value::int64(f as i64))
            } else if let Some(s) = value.as_str() {
                s.parse::<i64>()
                    .map(Value::int64)
                    .map_err(|_| Error::invalid_query(format!("Cannot convert '{}' to INT64", s)))
            } else {
                Err(Error::invalid_query(format!(
                    "Cannot convert {:?} to INT64",
                    value
                )))
            }
        }
        DataType::Float64 => {
            if let Some(f) = value.as_f64() {
                Ok(Value::float64(f))
            } else if let Some(i) = value.as_i64() {
                Ok(Value::float64(i as f64))
            } else if let Some(s) = value.as_str() {
                s.parse::<f64>()
                    .map(Value::float64)
                    .map_err(|_| Error::invalid_query(format!("Cannot convert '{}' to FLOAT64", s)))
            } else {
                Err(Error::invalid_query(format!(
                    "Cannot convert {:?} to FLOAT64",
                    value
                )))
            }
        }
        DataType::String => Ok(Value::string(value.to_string())),
        DataType::Bool => {
            if let Some(b) = value.as_bool() {
                Ok(Value::bool_val(b))
            } else {
                Err(Error::invalid_query(format!(
                    "Cannot convert {:?} to BOOL",
                    value
                )))
            }
        }
        _ => Err(Error::unsupported_feature(format!(
            "Type conversion to {:?} not yet supported",
            target_type
        ))),
    }
}
