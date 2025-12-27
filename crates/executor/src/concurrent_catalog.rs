use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use parking_lot::RwLock;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::Expr;
use yachtsql_storage::{Schema, Table};

use crate::catalog::{ColumnDefault, SchemaMetadata, UserFunction, UserProcedure, ViewDef};
use crate::plan::{AccessType, PhysicalPlan, TableAccessSet};

pub type TableHandle = Arc<RwLock<Table>>;

#[derive(Debug)]
pub struct DroppedSchemaData {
    pub metadata: SchemaMetadata,
    pub tables: Vec<(String, Table)>,
    pub table_defaults: Vec<(String, Vec<ColumnDefault>)>,
}

pub struct TableLockSet {
    read_tables: Mutex<HashMap<String, Table>>,
    write_tables: Mutex<HashMap<String, Table>>,
    catalog: Option<Arc<ConcurrentCatalog>>,
}

unsafe impl Send for TableLockSet {}
unsafe impl Sync for TableLockSet {}

impl TableLockSet {
    pub fn new() -> Self {
        Self {
            read_tables: Mutex::new(HashMap::new()),
            write_tables: Mutex::new(HashMap::new()),
            catalog: None,
        }
    }

    pub fn with_catalog(catalog: Arc<ConcurrentCatalog>) -> Self {
        Self {
            read_tables: Mutex::new(HashMap::new()),
            write_tables: Mutex::new(HashMap::new()),
            catalog: Some(catalog),
        }
    }

    pub fn set_catalog(&mut self, catalog: Arc<ConcurrentCatalog>) {
        self.catalog = Some(catalog);
    }

    pub fn add_read_table(&self, name: String, table: Table) {
        self.read_tables.lock().unwrap().insert(name, table);
    }

    pub fn add_write_table(&self, name: String, table: Table) {
        self.write_tables.lock().unwrap().insert(name, table);
    }

    pub fn get_table(&self, name: &str) -> Option<Table> {
        let upper = name.to_uppercase();
        if let Some(table) = self.write_tables.lock().unwrap().get(&upper) {
            return Some(table.clone());
        }
        if let Some(table) = self.read_tables.lock().unwrap().get(&upper) {
            return Some(table.clone());
        }
        None
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_table_mut(&self, name: &str) -> Option<&mut Table> {
        let upper = name.to_uppercase();
        let mut write_tables = self.write_tables.lock().unwrap();
        if write_tables.contains_key(&upper) {
            return Some(unsafe { &mut *(write_tables.get_mut(&upper).unwrap() as *mut Table) });
        }
        None
    }

    pub fn update_table(&self, name: &str, table: Table) {
        let upper = name.to_uppercase();
        self.write_tables.lock().unwrap().insert(upper, table);
    }

    pub fn snapshot_write_locked_tables(&self) -> HashMap<String, Table> {
        self.write_tables.lock().unwrap().clone()
    }

    pub fn commit_writes(&self) {
        if let Some(ref catalog) = self.catalog {
            let write_tables = self.write_tables.lock().unwrap();
            for (name, table) in write_tables.iter() {
                catalog.update_table(name, table.clone());
            }
        }
    }
}

impl Default for TableLockSet {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct TransactionSnapshot {
    pub tables: HashMap<String, Table>,
}

#[derive(Debug)]
pub struct ConcurrentCatalog {
    tables: DashMap<String, TableHandle>,
    table_defaults: DashMap<String, Vec<ColumnDefault>>,
    functions: DashMap<String, UserFunction>,
    procedures: DashMap<String, UserProcedure>,
    procedure_bodies: DashMap<String, Vec<PhysicalPlan>>,
    views: DashMap<String, ViewDef>,
    schemas: DashMap<String, ()>,
    schema_metadata: DashMap<String, SchemaMetadata>,
    search_path: RwLock<Vec<String>>,
    dropped_schemas: DashMap<String, DroppedSchemaData>,
    transaction_snapshot: RwLock<Option<TransactionSnapshot>>,
}

impl ConcurrentCatalog {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            table_defaults: DashMap::new(),
            functions: DashMap::new(),
            procedures: DashMap::new(),
            procedure_bodies: DashMap::new(),
            views: DashMap::new(),
            schemas: DashMap::new(),
            schema_metadata: DashMap::new(),
            search_path: RwLock::new(Vec::new()),
            dropped_schemas: DashMap::new(),
            transaction_snapshot: RwLock::new(None),
        }
    }

    pub fn begin_transaction(&self) {
        let mut tables_snapshot = HashMap::new();
        for entry in self.tables.iter() {
            if let Some(table) = entry.value().try_read() {
                tables_snapshot.insert(entry.key().clone(), table.clone());
            }
        }
        *self.transaction_snapshot.write() = Some(TransactionSnapshot {
            tables: tables_snapshot,
        });
    }

    pub fn begin_transaction_with_tables(&self, table_names: &[String]) {
        let mut tables_snapshot = HashMap::new();
        for name in table_names {
            let key = name.to_uppercase();
            if let Some(handle) = self.tables.get(&key)
                && let Some(table) = handle.try_read()
            {
                tables_snapshot.insert(key, table.clone());
            }
        }
        *self.transaction_snapshot.write() = Some(TransactionSnapshot {
            tables: tables_snapshot,
        });
    }

    pub fn snapshot_table(&self, name: &str, table_data: Table) {
        let mut snapshot_guard = self.transaction_snapshot.write();
        if let Some(ref mut snapshot) = *snapshot_guard {
            snapshot.tables.insert(name.to_uppercase(), table_data);
        } else {
            let mut tables = HashMap::new();
            tables.insert(name.to_uppercase(), table_data);
            *snapshot_guard = Some(TransactionSnapshot { tables });
        }
    }

    pub fn commit(&self) {
        *self.transaction_snapshot.write() = None;
    }

    pub fn rollback(&self) {
        let snapshot = self.transaction_snapshot.write().take();
        if let Some(snapshot) = snapshot {
            for (name, table_data) in snapshot.tables {
                if let Some(handle) = self.tables.get(&name)
                    && let Some(mut table) = handle.try_write()
                {
                    *table = table_data;
                }
            }
        }
    }

    pub fn take_transaction_snapshot(&self) -> Option<TransactionSnapshot> {
        self.transaction_snapshot.write().take()
    }

    fn resolve_table_name(&self, name: &str) -> String {
        let key = name.to_uppercase();
        if key.contains('.') || self.tables.contains_key(&key) {
            return key;
        }
        let search_path = self.search_path.read();
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

    pub fn acquire_table_locks(&self, accesses: &TableAccessSet) -> Result<TableLockSet> {
        let locks = TableLockSet::new();

        for (table_name, access_type) in &accesses.accesses {
            let resolved = self.resolve_table_name(table_name);
            let handle_opt = self.tables.get(&resolved);

            match access_type {
                AccessType::WriteOptional => {
                    if let Some(handle) = handle_opt {
                        let handle = handle.clone();
                        let table = handle.write().clone();
                        locks.add_write_table(resolved, table);
                    }
                }
                AccessType::Read => {
                    let handle = handle_opt
                        .ok_or_else(|| Error::TableNotFound(table_name.clone()))?
                        .clone();
                    let table = handle.read().clone();
                    locks.add_read_table(resolved, table);
                }
                AccessType::Write => {
                    let handle = handle_opt
                        .ok_or_else(|| Error::TableNotFound(table_name.clone()))?
                        .clone();
                    let table = handle.write().clone();
                    locks.add_write_table(resolved, table);
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
        self.dropped_schemas.remove(&key);
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
        self.dropped_schemas.remove(&key);
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

        let mut dropped_tables = Vec::new();
        let mut dropped_defaults = Vec::new();
        for table_key in tables_in_schema {
            if let Some((_, handle)) = self.tables.remove(&table_key) {
                let table = handle.read().clone();
                dropped_tables.push((table_key.clone(), table));
            }
            if let Some((_, defaults)) = self.table_defaults.remove(&table_key) {
                dropped_defaults.push((table_key, defaults));
            }
        }

        self.schemas.remove(&key);
        let metadata = self
            .schema_metadata
            .remove(&key)
            .map(|(_, m)| m)
            .unwrap_or_default();

        self.dropped_schemas.insert(
            key,
            DroppedSchemaData {
                metadata,
                tables: dropped_tables,
                table_defaults: dropped_defaults,
            },
        );
        Ok(())
    }

    pub fn is_schema_dropped(&self, name: &str) -> bool {
        self.dropped_schemas.contains_key(&name.to_uppercase())
    }

    pub fn undrop_schema(&self, name: &str, if_not_exists: bool) -> Result<()> {
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
        let dropped = self.dropped_schemas.remove(&key);
        match dropped {
            Some((_, dropped_data)) => {
                self.schemas.insert(key.clone(), ());
                self.schema_metadata.insert(key, dropped_data.metadata);
                for (table_key, table) in dropped_data.tables {
                    self.tables.insert(table_key, Arc::new(RwLock::new(table)));
                }
                for (table_key, defaults) in dropped_data.table_defaults {
                    self.table_defaults.insert(table_key, defaults);
                }
                Ok(())
            }
            None => {
                if if_not_exists {
                    return Ok(());
                }
                Err(Error::invalid_query(format!(
                    "Schema not found in drop history: {}",
                    name
                )))
            }
        }
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

    pub fn get_schema_default_collation(&self, name: &str) -> Option<String> {
        let key = name.to_uppercase();
        self.schema_metadata
            .get(&key)
            .and_then(|metadata| metadata.options.get("default_collate").cloned())
    }

    pub fn set_search_path(&self, schemas: Vec<String>) {
        *self.search_path.write() = schemas.into_iter().map(|s| s.to_uppercase()).collect();
    }

    pub fn get_search_path(&self) -> Vec<String> {
        self.search_path.read().clone()
    }

    pub fn create_table(&self, name: &str, schema: Schema) -> Result<()> {
        let key = name.to_uppercase();
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

    pub fn update_table(&self, name: &str, table: Table) {
        let key = name.to_uppercase();
        if let Some(handle) = self.tables.get(&key) {
            *handle.write() = table;
        }
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

    pub fn create_procedure(
        &self,
        proc: UserProcedure,
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<()> {
        let key = proc.name.to_uppercase();
        if self.procedures.contains_key(&key) {
            if if_not_exists {
                return Ok(());
            }
            if !or_replace {
                return Err(Error::invalid_query(format!(
                    "Procedure already exists: {}",
                    proc.name
                )));
            }
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

    pub fn set_procedure_body(&self, name: &str, body: Vec<PhysicalPlan>) {
        let key = name.to_uppercase();
        self.procedure_bodies.insert(key, body);
    }

    pub fn get_procedure_body(&self, name: &str) -> Option<Vec<PhysicalPlan>> {
        self.procedure_bodies
            .get(&name.to_uppercase())
            .map(|r| r.clone())
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
        let table = handle.read();
        Some(table.schema().clone())
    }
}

impl Default for ConcurrentCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl yachtsql_parser::CatalogProvider for ConcurrentCatalog {
    fn get_table_schema(&self, name: &str) -> Option<Schema> {
        self.get_table_schema(name)
    }

    fn get_view(&self, name: &str) -> Option<yachtsql_parser::ViewDefinition> {
        self.get_view(name)
            .map(|v| yachtsql_parser::ViewDefinition {
                query: v.query,
                column_aliases: v.column_aliases,
            })
    }

    fn get_function(&self, name: &str) -> Option<yachtsql_parser::FunctionDefinition> {
        self.get_function(name)
            .map(|f| yachtsql_parser::FunctionDefinition {
                name: f.name.clone(),
                parameters: f.parameters.clone(),
                return_type: f.return_type.clone(),
                body: f.body.clone(),
                is_aggregate: f.is_aggregate,
            })
    }
}
