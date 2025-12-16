//! In-memory catalog for storing table metadata and data.

use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};
use yachtsql_storage::{Schema, Table};

#[derive(Debug, Clone)]
pub struct ViewDef {
    pub query: String,
    pub column_aliases: Vec<String>,
}

#[derive(Debug, Default)]
pub struct Catalog {
    tables: HashMap<String, Table>,
    views: HashMap<String, ViewDef>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            views: HashMap::new(),
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
        self.tables.insert(key, Table::new(schema));
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        let key = name.to_uppercase();
        if self.tables.remove(&key).is_none() {
            return Err(Error::TableNotFound(name.to_string()));
        }
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(&name.to_uppercase())
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
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

        if let Some(table) = self.tables.remove(&old_key) {
            self.tables.insert(new_key, table);
        }
        Ok(())
    }

    pub fn replace_table(&mut self, name: &str, table: Table) -> Result<()> {
        let key = name.to_uppercase();
        if !self.tables.contains_key(&key) {
            return Err(Error::TableNotFound(name.to_string()));
        }
        self.tables.insert(key, table);
        Ok(())
    }

    pub fn create_or_replace_table(&mut self, name: &str, table: Table) {
        let key = name.to_uppercase();
        self.tables.insert(key, table);
    }

    pub fn create_view(
        &mut self,
        name: &str,
        query: String,
        column_aliases: Vec<String>,
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<()> {
        let key = name.to_uppercase();
        if self.views.contains_key(&key) {
            if if_not_exists {
                return Ok(());
            }
            if !or_replace {
                return Err(Error::invalid_query(format!(
                    "View already exists: {}",
                    name
                )));
            }
        }
        self.views.insert(
            key,
            ViewDef {
                query,
                column_aliases,
            },
        );
        Ok(())
    }

    pub fn drop_view(&mut self, name: &str, if_exists: bool) -> Result<()> {
        let key = name.to_uppercase();
        if self.views.remove(&key).is_none() && !if_exists {
            return Err(Error::invalid_query(format!("View not found: {}", name)));
        }
        Ok(())
    }

    pub fn get_view(&self, name: &str) -> Option<&ViewDef> {
        self.views.get(&name.to_uppercase())
    }

    pub fn view_exists(&self, name: &str) -> bool {
        self.views.contains_key(&name.to_uppercase())
    }
}
