//! In-memory catalog for storing table metadata and data.

use std::collections::HashMap;
use yachtsql_core::error::{Error, Result};
use yachtsql_storage::{Schema, Row};

#[derive(Debug, Clone)]
pub struct TableData {
    pub schema: Schema,
    pub rows: Vec<Row>,
}

impl TableData {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            rows: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct Catalog {
    tables: HashMap<String, TableData>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<()> {
        let key = name.to_uppercase();
        if self.tables.contains_key(&key) {
            return Err(Error::invalid_query(format!("Table already exists: {}", name)));
        }
        self.tables.insert(key, TableData::new(schema));
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        let key = name.to_uppercase();
        if self.tables.remove(&key).is_none() {
            return Err(Error::TableNotFound(name.to_string()));
        }
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&TableData> {
        self.tables.get(&name.to_uppercase())
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut TableData> {
        self.tables.get_mut(&name.to_uppercase())
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(&name.to_uppercase())
    }
}
