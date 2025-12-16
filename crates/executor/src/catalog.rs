//! In-memory catalog for storing table metadata and data.

use std::collections::HashMap;

use yachtsql_core::error::{Error, Result};
use yachtsql_storage::Schema;

use crate::record::Record;

#[derive(Debug, Clone)]
pub struct TableData {
    pub schema: Schema,
    pub rows: Vec<Record>,
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
            return Err(Error::invalid_query(format!(
                "Table already exists: {}",
                name
            )));
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

    pub fn rename_table(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        let old_key = old_name.to_uppercase();
        let new_key = new_name.to_uppercase();

        if !self.tables.contains_key(&old_key) {
            return Err(Error::TableNotFound(old_name.to_string()));
        }
        if self.tables.contains_key(&new_key) {
            return Err(Error::invalid_query(format!(
                "Table already exists: {}",
                new_name
            )));
        }

        if let Some(table_data) = self.tables.remove(&old_key) {
            self.tables.insert(new_key, table_data);
        }
        Ok(())
    }
}
