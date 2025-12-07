use yachtsql_core::types::{DataType, Value};
use yachtsql_storage::{Field, Schema};

use crate::Table;

pub trait IntoTestSchema {
    fn into_schema(self) -> Schema;
}

impl IntoTestSchema for Vec<(&str, DataType)> {
    fn into_schema(self) -> Schema {
        let fields: Vec<Field> = self
            .into_iter()
            .map(|(name, dtype)| Field::nullable(name, dtype))
            .collect();
        Schema::from_fields(fields)
    }
}

impl IntoTestSchema for Schema {
    fn into_schema(self) -> Schema {
        self
    }
}

impl IntoTestSchema for &Schema {
    fn into_schema(self) -> Schema {
        self.clone()
    }
}

impl IntoTestSchema for Vec<Field> {
    fn into_schema(self) -> Schema {
        Schema::from_fields(self)
    }
}

pub fn create_batch<S>(schema_spec: S, rows: Vec<Vec<Value>>) -> Table
where
    S: IntoTestSchema,
{
    let schema = schema_spec.into_schema();
    Table::from_values(schema, rows).expect("Failed to create test batch")
}

pub fn create_single_row_batch<S>(schema_spec: S, values: Vec<Value>) -> Table
where
    S: IntoTestSchema,
{
    create_batch(schema_spec, vec![values])
}

pub fn create_empty_batch(fields: Vec<(&str, DataType)>) -> Table {
    let schema_fields: Vec<Field> = fields
        .into_iter()
        .map(|(name, dtype)| Field::nullable(name, dtype))
        .collect();
    let schema = Schema::from_fields(schema_fields);
    Table::empty(schema)
}
