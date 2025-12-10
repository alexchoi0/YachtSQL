use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};

use super::Table;
use crate::storage_backend::{StorageBackend, TableStorage};
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
        set_default: Option<Value>,
        drop_default: bool,
    ) -> Result<()>;

    fn rename_table(&mut self, new_name: &str) -> Result<()>;

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

        if field.is_auto_increment {
            if self.auto_increment_counter.is_some() {
                return Err(Error::InvalidOperation(
                    "Table already has an AUTO_INCREMENT column".to_string(),
                ));
            }

            let start_value = if current_row_count > 0 {
                (current_row_count as i64) + 1
            } else {
                1
            };
            self.init_auto_increment(field.name.clone(), start_value)?;
        }

        let value_to_insert = default_value.unwrap_or(Value::null());

        let current_row_count = self.row_count();

        match &mut self.storage {
            StorageBackend::Columnar(storage) => {
                let mut column = Column::new(&field.data_type, 100);

                if current_row_count > 0 {
                    for _ in 0..current_row_count {
                        column.push(value_to_insert.clone())?;
                    }
                }

                storage.columns_mut().insert(field.name.clone(), column);
            }
            StorageBackend::Row(storage) => {
                if current_row_count > 0 {
                    storage.add_column_with_default(value_to_insert)?;
                }
            }
        }

        let mut new_fields = self.schema.fields().to_vec();
        new_fields.push(field);
        let mut new_schema = Schema::from_fields(new_fields);
        copy_constraints_to_schema(self, &mut new_schema);
        self.schema = new_schema;

        Ok(())
    }

    fn drop_column(&mut self, column_name: &str) -> Result<()> {
        let col_idx = self
            .schema
            .field_index(column_name)
            .ok_or_else(|| Error::invalid_query(format!("Column '{}' not found", column_name)))?;

        if self.schema.fields().len() <= 1 {
            return Err(Error::invalid_query(
                "Cannot drop last column - table must have at least one column".to_string(),
            ));
        }

        match &mut self.storage {
            StorageBackend::Columnar(storage) => {
                storage.columns_mut().shift_remove(column_name);
            }
            StorageBackend::Row(storage) => {
                storage.drop_column_at_index(col_idx)?;
            }
        }

        if self.auto_increment_column.as_deref() == Some(column_name) {
            self.remove_auto_increment()?;
        }

        let new_fields: Vec<Field> = self
            .schema
            .fields()
            .iter()
            .filter(|f| f.name != column_name)
            .cloned()
            .collect();
        let mut new_schema = Schema::from_fields(new_fields);
        copy_constraints_excluding_column(self, &mut new_schema, column_name);
        self.schema = new_schema;

        Ok(())
    }

    fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        use crate::schema::CheckConstraint;

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

        if let StorageBackend::Columnar(storage) = &mut self.storage
            && let Some(column) = storage.columns_mut().shift_remove(old_name)
        {
            storage.columns_mut().insert(new_name.to_string(), column);
        }

        if self.auto_increment_column.as_deref() == Some(old_name) {
            self.auto_increment_column = Some(new_name.to_string());
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
                        default_value: f.default_value.clone(),
                        is_unique: f.is_unique,
                        identity_generation: f.identity_generation,
                        identity_sequence_name: f.identity_sequence_name.clone(),
                        identity_sequence_config: f.identity_sequence_config.clone(),
                        is_auto_increment: f.is_auto_increment,
                        generated_expression: f.generated_expression.clone(),
                        collation: f.collation.clone(),
                        source_table: f.source_table.clone(),
                        domain_name: f.domain_name.clone(),
                    }
                } else {
                    f.clone()
                }
            })
            .collect();

        let mut new_schema = Schema::from_fields(new_fields);

        if let Some(pk) = self.schema.primary_key() {
            let new_pk: Vec<String> = pk
                .iter()
                .map(|col| {
                    if col == old_name {
                        new_name.to_string()
                    } else {
                        col.clone()
                    }
                })
                .collect();
            new_schema.set_primary_key(new_pk);
        }

        for unique in self.schema.unique_constraints() {
            let new_columns: Vec<String> = unique
                .columns
                .iter()
                .map(|col| {
                    if col == old_name {
                        new_name.to_string()
                    } else {
                        col.clone()
                    }
                })
                .collect();
            new_schema.add_unique_constraint(crate::schema::UniqueConstraint {
                name: unique.name.clone(),
                columns: new_columns,
                enforced: unique.enforced,
                nulls_distinct: unique.nulls_distinct,
            });
        }

        for check in self.schema.check_constraints() {
            let updated_expression =
                rename_column_in_expression(&check.expression, old_name, new_name);
            let updated_check = CheckConstraint {
                name: check.name.clone(),
                expression: updated_expression,
                enforced: check.enforced,
            };
            new_schema.add_check_constraint(updated_check);
        }

        self.schema = new_schema;

        Ok(())
    }

    fn alter_column(
        &mut self,
        column_name: &str,
        new_data_type: Option<DataType>,
        set_not_null: Option<bool>,
        _set_default: Option<Value>,
        _drop_default: bool,
    ) -> Result<()> {
        if !self.column_map().contains_key(column_name) {
            return Err(Error::invalid_query(format!(
                "Column '{}' not found",
                column_name
            )));
        }

        if let Some(true) = set_not_null {
            let col_idx = self.schema.field_index(column_name).unwrap();
            let row_count = self.row_count();

            for row_idx in 0..row_count {
                let row = self.get_row(row_idx)?;
                if row.values()[col_idx].is_null() {
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

        let mut new_schema = Schema::from_fields(new_fields);
        copy_constraints_to_schema(self, &mut new_schema);
        self.schema = new_schema;
        Ok(())
    }

    fn rename_table(&mut self, _new_name: &str) -> Result<()> {
        Ok(())
    }

    fn schema_mut(&mut self) -> &mut Schema {
        &mut self.schema
    }
}

pub(super) fn copy_constraints_to_schema(table: &Table, target_schema: &mut Schema) {
    if let Some(pk) = table.schema.primary_key() {
        target_schema.set_primary_key(pk.to_vec());
    }

    for unique in table.schema.unique_constraints() {
        target_schema.add_unique_constraint(unique.clone());
    }

    for check in table.schema.check_constraints() {
        target_schema.add_check_constraint(check.clone());
    }
}

pub(super) fn copy_constraints_excluding_column(
    table: &Table,
    target_schema: &mut Schema,
    excluded_column: &str,
) {
    if let Some(pk) = table.schema.primary_key() {
        let new_pk: Vec<String> = pk
            .iter()
            .filter(|col| *col != excluded_column)
            .cloned()
            .collect();
        if !new_pk.is_empty() {
            target_schema.set_primary_key(new_pk);
        }
    }

    for unique in table.schema.unique_constraints() {
        let new_columns: Vec<String> = unique
            .columns
            .iter()
            .filter(|col| *col != excluded_column)
            .cloned()
            .collect();
        if !new_columns.is_empty() {
            target_schema.add_unique_constraint(crate::schema::UniqueConstraint {
                name: unique.name.clone(),
                columns: new_columns,
                enforced: unique.enforced,
                nulls_distinct: unique.nulls_distinct,
            });
        }
    }

    for check in table.schema.check_constraints() {
        target_schema.add_check_constraint(check.clone());
    }
}

fn rename_column_in_expression(expression: &str, old_name: &str, new_name: &str) -> String {
    let pattern = format!(
        r"(^|[\s(,])({})($|[\s),<>=!+\-*/])",
        regex::escape(old_name)
    );

    if let Ok(re) = regex::Regex::new(&pattern) {
        re.replace_all(expression, format!("${{1}}{}${{3}}", new_name))
            .to_string()
    } else {
        expression.to_string()
    }
}

impl Table {
    pub(crate) fn convert_column_type(
        &mut self,
        column_name: &str,
        new_type: &DataType,
    ) -> Result<()> {
        match &mut self.storage {
            StorageBackend::Columnar(storage) => {
                let old_column = storage.columns_mut().get_mut(column_name).ok_or_else(|| {
                    Error::invalid_query(format!("Column '{}' not found", column_name))
                })?;

                let row_count = old_column.len();
                let mut new_column = Column::new(new_type, row_count);

                for i in 0..row_count {
                    let old_value = old_column.get(i)?;
                    let new_value = convert_value_to_type(&old_value, new_type)?;
                    new_column.push(new_value)?;
                }

                *old_column = new_column;
            }
            StorageBackend::Row(storage) => {
                let col_idx = self.schema.field_index(column_name).ok_or_else(|| {
                    Error::invalid_query(format!("Column '{}' not found", column_name))
                })?;

                let row_count = storage.row_count();
                for row_idx in 0..row_count {
                    let row = storage.get_row(row_idx, &self.schema)?;
                    let old_value = &row.values()[col_idx];
                    let new_value = convert_value_to_type(old_value, new_type)?;

                    storage.update_cell(row_idx, col_idx, new_value)?;
                }
            }
        }

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
