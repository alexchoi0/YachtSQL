use std::cell::RefCell;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_storage::{Field, Row, Schema, Storage};

use crate::record_batch::Table;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InformationSchemaTable {
    Tables,
    Columns,
    Schemata,
    Views,
    TableConstraints,
    KeyColumnUsage,
    ColumnPrivileges,
    TablePrivileges,
    ReferentialConstraints,
    CheckConstraints,
}

impl InformationSchemaTable {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "tables" => Ok(InformationSchemaTable::Tables),
            "columns" => Ok(InformationSchemaTable::Columns),
            "schemata" => Ok(InformationSchemaTable::Schemata),
            "views" => Ok(InformationSchemaTable::Views),
            "table_constraints" => Ok(InformationSchemaTable::TableConstraints),
            "key_column_usage" => Ok(InformationSchemaTable::KeyColumnUsage),
            "column_privileges" => Ok(InformationSchemaTable::ColumnPrivileges),
            "table_privileges" => Ok(InformationSchemaTable::TablePrivileges),
            "referential_constraints" => Ok(InformationSchemaTable::ReferentialConstraints),
            "check_constraints" => Ok(InformationSchemaTable::CheckConstraints),
            _ => Err(Error::table_not_found(format!(
                "information_schema.{} is not supported",
                s
            ))),
        }
    }
}

pub struct InformationSchemaProvider {
    storage: Rc<RefCell<Storage>>,
}

impl InformationSchemaProvider {
    pub fn new(storage: Rc<RefCell<Storage>>) -> Self {
        Self { storage }
    }

    pub fn query(&self, table: InformationSchemaTable) -> Result<(Schema, Vec<Row>)> {
        match table {
            InformationSchemaTable::Tables => self.get_tables(),
            InformationSchemaTable::Columns => self.get_columns(),
            InformationSchemaTable::Schemata => self.get_schemata(),
            InformationSchemaTable::Views => self.get_views(),
            InformationSchemaTable::TableConstraints => self.get_table_constraints(),
            InformationSchemaTable::KeyColumnUsage => self.get_key_column_usage(),
            InformationSchemaTable::ColumnPrivileges => self.get_column_privileges(),
            InformationSchemaTable::TablePrivileges => self.get_table_privileges(),
            InformationSchemaTable::ReferentialConstraints => self.get_referential_constraints(),
            InformationSchemaTable::CheckConstraints => self.get_check_constraints(),
        }
    }

    fn get_tables(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("table_type", DataType::String),
            Field::nullable("engine", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for table_name in dataset.tables().keys() {
                    rows.push(Row::from_values(vec![
                        Value::string(dataset_id.clone()),
                        Value::string(dataset_id.clone()),
                        Value::string(table_name.clone()),
                        Value::string("BASE TABLE".to_string()),
                        Value::string("MergeTree".to_string()),
                    ]));
                }

                for view_name in dataset.views().list_views() {
                    let is_materialized = dataset
                        .views()
                        .get_view(&view_name)
                        .map(|v| v.is_materialized())
                        .unwrap_or(false);
                    let table_type = if is_materialized {
                        "MATERIALIZED VIEW"
                    } else {
                        "VIEW"
                    };
                    rows.push(Row::from_values(vec![
                        Value::string(dataset_id.clone()),
                        Value::string(dataset_id.clone()),
                        Value::string(view_name),
                        Value::string(table_type.to_string()),
                        Value::null(),
                    ]));
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_columns(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
            Field::nullable("ordinal_position", DataType::Int64),
            Field::nullable("column_default", DataType::String),
            Field::nullable("is_nullable", DataType::String),
            Field::nullable("data_type", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();
                    for (position, field) in table_schema.fields().iter().enumerate() {
                        let is_nullable = if field.is_nullable() { "YES" } else { "NO" };
                        let default_value = field
                            .default_value
                            .as_ref()
                            .map(|d| format!("{:?}", d))
                            .map(Value::string)
                            .unwrap_or(Value::null());

                        rows.push(Row::from_values(vec![
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(table_name.clone()),
                            Value::string(field.name.clone()),
                            Value::int64((position + 1) as i64),
                            default_value,
                            Value::string(is_nullable.to_string()),
                            Value::string(field.data_type.to_string()),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_schemata(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("catalog_name", DataType::String),
            Field::nullable("schema_name", DataType::String),
            Field::nullable("schema_owner", DataType::String),
            Field::nullable("default_character_set_name", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            rows.push(Row::from_values(vec![
                Value::string(dataset_id.clone()),
                Value::string(dataset_id.clone()),
                Value::string("default".to_string()),
                Value::string("UTF-8".to_string()),
            ]));
        }

        Ok((schema, rows))
    }

    fn get_views(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("view_definition", DataType::String),
            Field::nullable("is_updatable", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for view_name in dataset.views().list_views() {
                    if let Some(view) = dataset.views().get_view(&view_name) {
                        rows.push(Row::from_values(vec![
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(view_name),
                            Value::string(view.sql.clone()),
                            Value::string("NO".to_string()),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_table_constraints(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("constraint_catalog", DataType::String),
            Field::nullable("constraint_schema", DataType::String),
            Field::nullable("constraint_name", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("constraint_type", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();

                    if let Some(pk_columns) = table_schema.primary_key() {
                        let constraint_name = format!("{}_pkey", table_name);
                        rows.push(Row::from_values(vec![
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(constraint_name),
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(table_name.clone()),
                            Value::string("PRIMARY KEY".to_string()),
                        ]));
                    }

                    for (idx, _unique_cols) in table_schema.unique_constraints().iter().enumerate()
                    {
                        let constraint_name = format!("{}_unique_{}", table_name, idx);
                        rows.push(Row::from_values(vec![
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(constraint_name),
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(table_name.clone()),
                            Value::string("UNIQUE".to_string()),
                        ]));
                    }

                    for fk in table.foreign_keys() {
                        let constraint_name = fk
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_fkey", table_name));
                        rows.push(Row::from_values(vec![
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(constraint_name),
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(table_name.clone()),
                            Value::string("FOREIGN KEY".to_string()),
                        ]));
                    }

                    for (idx, check) in table_schema.check_constraints().iter().enumerate() {
                        let constraint_name = check
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_check_{}", table_name, idx));
                        rows.push(Row::from_values(vec![
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(constraint_name),
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(table_name.clone()),
                            Value::string("CHECK".to_string()),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_key_column_usage(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("constraint_catalog", DataType::String),
            Field::nullable("constraint_schema", DataType::String),
            Field::nullable("constraint_name", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
            Field::nullable("ordinal_position", DataType::Int64),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();

                    if let Some(pk_columns) = table_schema.primary_key() {
                        let constraint_name = format!("{}_pkey", table_name);
                        for (position, col_name) in pk_columns.iter().enumerate() {
                            rows.push(Row::from_values(vec![
                                Value::string(dataset_id.clone()),
                                Value::string(dataset_id.clone()),
                                Value::string(constraint_name.clone()),
                                Value::string(dataset_id.clone()),
                                Value::string(dataset_id.clone()),
                                Value::string(table_name.clone()),
                                Value::string(col_name.clone()),
                                Value::int64((position + 1) as i64),
                            ]));
                        }
                    }

                    for fk in table.foreign_keys() {
                        let constraint_name = fk
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_fkey", table_name));
                        for (position, col_name) in fk.child_columns.iter().enumerate() {
                            rows.push(Row::from_values(vec![
                                Value::string(dataset_id.clone()),
                                Value::string(dataset_id.clone()),
                                Value::string(constraint_name.clone()),
                                Value::string(dataset_id.clone()),
                                Value::string(dataset_id.clone()),
                                Value::string(table_name.clone()),
                                Value::string(col_name.clone()),
                                Value::int64((position + 1) as i64),
                            ]));
                        }
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_column_privileges(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("grantor", DataType::String),
            Field::nullable("grantee", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
            Field::nullable("privilege_type", DataType::String),
            Field::nullable("is_grantable", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_table_privileges(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("grantor", DataType::String),
            Field::nullable("grantee", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("privilege_type", DataType::String),
            Field::nullable("is_grantable", DataType::String),
            Field::nullable("with_hierarchy", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_referential_constraints(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("constraint_catalog", DataType::String),
            Field::nullable("constraint_schema", DataType::String),
            Field::nullable("constraint_name", DataType::String),
            Field::nullable("unique_constraint_catalog", DataType::String),
            Field::nullable("unique_constraint_schema", DataType::String),
            Field::nullable("unique_constraint_name", DataType::String),
            Field::nullable("match_option", DataType::String),
            Field::nullable("update_rule", DataType::String),
            Field::nullable("delete_rule", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    for fk in table.foreign_keys() {
                        let constraint_name = fk
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_fkey", table_name));
                        let unique_constraint_name = format!("{}_pkey", fk.parent_table);

                        rows.push(Row::from_values(vec![
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(constraint_name),
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(unique_constraint_name),
                            Value::string("NONE".to_string()),
                            Value::string(format!("{:?}", fk.on_update)),
                            Value::string(format!("{:?}", fk.on_delete)),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_check_constraints(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("constraint_catalog", DataType::String),
            Field::nullable("constraint_schema", DataType::String),
            Field::nullable("constraint_name", DataType::String),
            Field::nullable("check_clause", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();
                    for (idx, check) in table_schema.check_constraints().iter().enumerate() {
                        let constraint_name = check
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_check_{}", table_name, idx));
                        rows.push(Row::from_values(vec![
                            Value::string(dataset_id.clone()),
                            Value::string(dataset_id.clone()),
                            Value::string(constraint_name),
                            Value::string(check.expression.clone()),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }
}
