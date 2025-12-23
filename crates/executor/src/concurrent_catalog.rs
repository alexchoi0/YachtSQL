use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use dashmap::DashMap;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::Expr;
use yachtsql_storage::{Schema, Table};

use crate::catalog::{ColumnDefault, SchemaMetadata, UserFunction, UserProcedure, ViewDef};
use crate::plan::{AccessType, TableAccessSet};

pub type TableHandle = Arc<RwLock<Table>>;

pub struct TableLockSet<'a> {
    read_guards: Vec<(String, RwLockReadGuard<'a, Table>)>,
    write_guards: Vec<(String, RwLockWriteGuard<'a, Table>)>,
}

impl<'a> TableLockSet<'a> {
    pub fn new() -> Self {
        Self {
            read_guards: Vec::new(),
            write_guards: Vec::new(),
        }
    }

    pub fn add_read(&mut self, name: String, guard: RwLockReadGuard<'a, Table>) {
        self.read_guards.push((name, guard));
    }

    pub fn add_write(&mut self, name: String, guard: RwLockWriteGuard<'a, Table>) {
        self.write_guards.push((name, guard));
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        let upper = name.to_uppercase();
        for (n, guard) in &self.write_guards {
            if n == &upper {
                return Some(&**guard);
            }
        }
        for (n, guard) in &self.read_guards {
            if n == &upper {
                return Some(&**guard);
            }
        }
        None
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        let upper = name.to_uppercase();
        for (n, guard) in &mut self.write_guards {
            if n == &upper {
                return Some(&mut **guard);
            }
        }
        None
    }
}

impl Default for TableLockSet<'_> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct ConcurrentCatalog {
    tables: DashMap<String, TableHandle>,
    table_defaults: DashMap<String, Vec<ColumnDefault>>,
    functions: DashMap<String, UserFunction>,
    procedures: DashMap<String, UserProcedure>,
    views: DashMap<String, ViewDef>,
    schemas: DashMap<String, ()>,
    schema_metadata: DashMap<String, SchemaMetadata>,
    search_path: RwLock<Vec<String>>,
}

impl ConcurrentCatalog {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            table_defaults: DashMap::new(),
            functions: DashMap::new(),
            procedures: DashMap::new(),
            views: DashMap::new(),
            schemas: DashMap::new(),
            schema_metadata: DashMap::new(),
            search_path: RwLock::new(Vec::new()),
        }
    }

    fn resolve_table_name(&self, name: &str) -> String {
        let key = name.to_uppercase();
        if key.contains('.') || self.tables.contains_key(&key) {
            return key;
        }
        let search_path = self.search_path.read().unwrap();
        for schema in search_path.iter() {
            let qualified = format!("{}.{}", schema, key);
            if self.tables.contains_key(&qualified) {
                return qualified;
            }
        }
        key
    }

    pub fn get_table_handle(&self, name: &str) -> Option<TableHandle> {
        let key = self.resolve_table_name(name);
        self.tables.get(&key).map(|r| r.clone())
    }

    pub fn acquire_table_locks(&self, accesses: &TableAccessSet) -> Result<TableLockSet<'static>> {
        let mut locks = TableLockSet::new();

        for (table_name, access_type) in &accesses.accesses {
            let resolved = self.resolve_table_name(table_name);
            let handle = self
                .tables
                .get(&resolved)
                .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;
            let handle = handle.clone();

            match access_type {
                AccessType::Read => {
                    let guard = handle.read().map_err(|_| {
                        Error::internal(format!("Lock poisoned for table: {}", table_name))
                    })?;
                    let guard: RwLockReadGuard<'static, Table> =
                        unsafe { std::mem::transmute(guard) };
                    locks.add_read(resolved, guard);
                }
                AccessType::Write => {
                    let guard = handle.write().map_err(|_| {
                        Error::internal(format!("Lock poisoned for table: {}", table_name))
                    })?;
                    let guard: RwLockWriteGuard<'static, Table> =
                        unsafe { std::mem::transmute(guard) };
                    locks.add_write(resolved, guard);
                }
            }
        }

        Ok(locks)
    }

    pub fn create_schema(&self, name: &str, if_not_exists: bool) -> Result<()> {
        let key = name.to_uppercase();
        if self.schemas.contains_key(&key) {
            if if_not_exists {
                return Ok(());
            }
            return Err(Error::invalid_query(format!(
                "Schema already exists: {}",
                name
            )));
        }
        self.schemas.insert(key.clone(), ());
        self.schema_metadata.insert(key, SchemaMetadata::default());
        Ok(())
    }

    pub fn create_schema_with_options(
        &self,
        name: &str,
        if_not_exists: bool,
        options: HashMap<String, String>,
    ) -> Result<()> {
        let key = name.to_uppercase();
        if self.schemas.contains_key(&key) {
            if if_not_exists {
                return Ok(());
            }
            return Err(Error::invalid_query(format!(
                "Schema already exists: {}",
                name
            )));
        }
        self.schemas.insert(key.clone(), ());
        self.schema_metadata.insert(key, SchemaMetadata { options });
        Ok(())
    }

    pub fn drop_schema(&self, name: &str, if_exists: bool, cascade: bool) -> Result<()> {
        let key = name.to_uppercase();
        if !self.schemas.contains_key(&key) {
            if if_exists {
                return Ok(());
            }
            return Err(Error::invalid_query(format!("Schema not found: {}", name)));
        }

        let prefix = format!("{}.", key);
        let tables_in_schema: Vec<String> = self
            .tables
            .iter()
            .filter(|r| r.key().starts_with(&prefix))
            .map(|r| r.key().clone())
            .collect();

        if !tables_in_schema.is_empty() && !cascade {
            return Err(Error::invalid_query(format!(
                "Cannot drop schema '{}' because it contains objects. Use CASCADE to drop all objects.",
                name
            )));
        }

        for table_key in tables_in_schema {
            self.tables.remove(&table_key);
        }

        self.schemas.remove(&key);
        self.schema_metadata.remove(&key);
        Ok(())
    }

    pub fn schema_exists(&self, name: &str) -> bool {
        self.schemas.contains_key(&name.to_uppercase())
    }

    pub fn alter_schema_options(&self, name: &str, options: HashMap<String, String>) -> Result<()> {
        let key = name.to_uppercase();
        if !self.schemas.contains_key(&key) {
            return Err(Error::invalid_query(format!("Schema not found: {}", name)));
        }
        if let Some(mut metadata) = self.schema_metadata.get_mut(&key) {
            for (k, v) in options {
                metadata.options.insert(k, v);
            }
        }
        Ok(())
    }

    pub fn set_search_path(&self, schemas: Vec<String>) {
        *self.search_path.write().unwrap() =
            schemas.into_iter().map(|s| s.to_uppercase()).collect();
    }

    pub fn get_search_path(&self) -> Vec<String> {
        self.search_path.read().unwrap().clone()
    }

    pub fn create_table(&self, name: &str, schema: Schema) -> Result<()> {
        let key = name.to_uppercase();
        if let Some(dot_pos) = key.find('.') {
            let schema_name = &key[..dot_pos];
            if !self.schemas.contains_key(schema_name) && !schema_name.is_empty() {
                return Err(Error::invalid_query(format!(
                    "Schema not found: {}",
                    &name[..dot_pos]
                )));
            }
        }
        if self.tables.contains_key(&key) {
            return Err(Error::invalid_query(format!(
                "Table already exists: {}",
                name
            )));
        }
        let table = Table::new(schema);
        self.tables.insert(key, Arc::new(RwLock::new(table)));
        Ok(())
    }

    pub fn set_table_defaults(&self, name: &str, defaults: Vec<ColumnDefault>) {
        let key = name.to_uppercase();
        self.table_defaults.insert(key, defaults);
    }

    pub fn get_table_defaults(&self, name: &str) -> Option<Vec<ColumnDefault>> {
        let key = self.resolve_table_name(name);
        self.table_defaults.get(&key).map(|r| r.clone())
    }

    pub fn get_column_default(&self, table_name: &str, column_name: &str) -> Option<Expr> {
        let defaults = self.get_table_defaults(table_name)?;
        for default in defaults {
            if default.column_name.to_uppercase() == column_name.to_uppercase() {
                return Some(default.default_expr.clone());
            }
        }
        None
    }

    pub fn insert_table(&self, name: &str, table: Table) -> Result<()> {
        let key = name.to_uppercase();
        if let Some(dot_pos) = key.find('.') {
            let schema_name = &key[..dot_pos];
            if !self.schemas.contains_key(schema_name) {
                return Err(Error::invalid_query(format!(
                    "Schema not found: {}",
                    schema_name
                )));
            }
        }
        if self.tables.contains_key(&key) {
            return Err(Error::invalid_query(format!(
                "Table already exists: {}",
                name
            )));
        }
        self.tables.insert(key, Arc::new(RwLock::new(table)));
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> Result<()> {
        let key = self.resolve_table_name(name);
        if self.tables.remove(&key).is_none() {
            return Err(Error::TableNotFound(name.to_string()));
        }
        Ok(())
    }

    pub fn table_exists(&self, name: &str) -> bool {
        let key = self.resolve_table_name(name);
        self.tables.contains_key(&key)
    }

    pub fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()> {
        let old_key = self.resolve_table_name(old_name);
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

        if let Some((_, handle)) = self.tables.remove(&old_key) {
            self.tables.insert(new_key, handle);
        }
        Ok(())
    }

    pub fn replace_table(&self, name: &str, table: Table) -> Result<()> {
        let key = name.to_uppercase();
        if !self.tables.contains_key(&key) {
            return Err(Error::TableNotFound(name.to_string()));
        }
        self.tables.insert(key, Arc::new(RwLock::new(table)));
        Ok(())
    }

    pub fn create_or_replace_table(&self, name: &str, table: Table) {
        let key = name.to_uppercase();
        self.tables.insert(key, Arc::new(RwLock::new(table)));
    }

    pub fn create_function(&self, func: UserFunction, or_replace: bool) -> Result<()> {
        let key = func.name.to_uppercase();
        if self.functions.contains_key(&key) && !or_replace {
            return Err(Error::invalid_query(format!(
                "Function already exists: {}",
                func.name
            )));
        }
        self.functions.insert(key, func);
        Ok(())
    }

    pub fn drop_function(&self, name: &str) -> Result<()> {
        let key = name.to_uppercase();
        if self.functions.remove(&key).is_none() {
            return Err(Error::invalid_query(format!(
                "Function not found: {}",
                name
            )));
        }
        Ok(())
    }

    pub fn get_function(&self, name: &str) -> Option<UserFunction> {
        self.functions.get(&name.to_uppercase()).map(|r| r.clone())
    }

    pub fn function_exists(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_uppercase())
    }

    pub fn create_procedure(&self, proc: UserProcedure, or_replace: bool) -> Result<()> {
        let key = proc.name.to_uppercase();
        if self.procedures.contains_key(&key) && !or_replace {
            return Err(Error::invalid_query(format!(
                "Procedure already exists: {}",
                proc.name
            )));
        }
        self.procedures.insert(key, proc);
        Ok(())
    }

    pub fn drop_procedure(&self, name: &str) -> Result<()> {
        let key = name.to_uppercase();
        if self.procedures.remove(&key).is_none() {
            return Err(Error::invalid_query(format!(
                "Procedure not found: {}",
                name
            )));
        }
        Ok(())
    }

    pub fn get_procedure(&self, name: &str) -> Option<UserProcedure> {
        self.procedures.get(&name.to_uppercase()).map(|r| r.clone())
    }

    pub fn procedure_exists(&self, name: &str) -> bool {
        self.procedures.contains_key(&name.to_uppercase())
    }

    pub fn get_functions(&self) -> HashMap<String, UserFunction> {
        self.functions
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    pub fn create_view(
        &self,
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

    pub fn drop_view(&self, name: &str, if_exists: bool) -> Result<()> {
        let key = name.to_uppercase();
        if self.views.remove(&key).is_none() && !if_exists {
            return Err(Error::invalid_query(format!("View not found: {}", name)));
        }
        Ok(())
    }

    pub fn get_view(&self, name: &str) -> Option<ViewDef> {
        self.views.get(&name.to_uppercase()).map(|r| r.clone())
    }

    pub fn view_exists(&self, name: &str) -> bool {
        self.views.contains_key(&name.to_uppercase())
    }

    pub fn get_table_schema(&self, name: &str) -> Option<Schema> {
        let handle = self.get_table_handle(name)?;
        let table = handle.read().ok()?;
        Some(table.schema().clone())
    }
}

impl Default for ConcurrentCatalog {
    fn default() -> Self {
        Self::new()
    }
}
