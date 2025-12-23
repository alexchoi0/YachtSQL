use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{Expr, FunctionArg, FunctionBody, LogicalPlan, ProcedureArg};
use yachtsql_parser::CatalogProvider;
use yachtsql_storage::{Schema, Table};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserFunction {
    pub name: String,
    pub parameters: Vec<FunctionArg>,
    pub return_type: DataType,
    pub body: FunctionBody,
    pub is_temporary: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProcedure {
    pub name: String,
    pub parameters: Vec<ProcedureArg>,
    pub body: Vec<LogicalPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDef {
    pub query: String,
    pub column_aliases: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefault {
    pub column_name: String,
    pub default_expr: Expr,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Catalog {
    tables: HashMap<String, Table>,
    table_defaults: HashMap<String, Vec<ColumnDefault>>,
    functions: HashMap<String, UserFunction>,
    procedures: HashMap<String, UserProcedure>,
    views: HashMap<String, ViewDef>,
    schemas: HashSet<String>,
    schema_metadata: HashMap<String, SchemaMetadata>,
    search_path: Vec<String>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            table_defaults: HashMap::new(),
            functions: HashMap::new(),
            procedures: HashMap::new(),
            views: HashMap::new(),
            schemas: HashSet::new(),
            schema_metadata: HashMap::new(),
            search_path: Vec::new(),
        }
    }

    pub fn create_schema(&mut self, name: &str, if_not_exists: bool) -> Result<()> {
        let key = name.to_uppercase();
        if self.schemas.contains(&key) {
            if if_not_exists {
                return Ok(());
            }
            return Err(Error::invalid_query(format!(
                "Schema already exists: {}",
                name
            )));
        }
        self.schemas.insert(key.clone());
        self.schema_metadata.insert(key, SchemaMetadata::default());
        Ok(())
    }

    pub fn create_schema_with_options(
        &mut self,
        name: &str,
        if_not_exists: bool,
        options: HashMap<String, String>,
    ) -> Result<()> {
        let key = name.to_uppercase();
        if self.schemas.contains(&key) {
            if if_not_exists {
                return Ok(());
            }
            return Err(Error::invalid_query(format!(
                "Schema already exists: {}",
                name
            )));
        }
        self.schemas.insert(key.clone());
        self.schema_metadata.insert(key, SchemaMetadata { options });
        Ok(())
    }

    pub fn drop_schema(&mut self, name: &str, if_exists: bool, cascade: bool) -> Result<()> {
        let key = name.to_uppercase();
        if !self.schemas.contains(&key) {
            if if_exists {
                return Ok(());
            }
            return Err(Error::invalid_query(format!("Schema not found: {}", name)));
        }

        let prefix = format!("{}.", key);
        let tables_in_schema: Vec<String> = self
            .tables
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
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
        self.schemas.contains(&name.to_uppercase())
    }

    pub fn alter_schema_options(
        &mut self,
        name: &str,
        options: HashMap<String, String>,
    ) -> Result<()> {
        let key = name.to_uppercase();
        if !self.schemas.contains(&key) {
            return Err(Error::invalid_query(format!("Schema not found: {}", name)));
        }
        if let Some(metadata) = self.schema_metadata.get_mut(&key) {
            for (k, v) in options {
                metadata.options.insert(k, v);
            }
        }
        Ok(())
    }

    pub fn set_search_path(&mut self, schemas: Vec<String>) {
        self.search_path = schemas.into_iter().map(|s| s.to_uppercase()).collect();
    }

    pub fn get_search_path(&self) -> &[String] {
        &self.search_path
    }

    fn resolve_table_name(&self, name: &str) -> String {
        let key = name.to_uppercase();
        if key.contains('.') || self.tables.contains_key(&key) {
            return key;
        }
        for schema in &self.search_path {
            let qualified = format!("{}.{}", schema, key);
            if self.tables.contains_key(&qualified) {
                return qualified;
            }
        }
        key
    }

    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<()> {
        let key = name.to_uppercase();
        if let Some(dot_pos) = key.find('.') {
            let schema_name = &key[..dot_pos];
            if !self.schemas.contains(schema_name) && !schema_name.is_empty() {
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
        self.tables.insert(key, Table::new(schema));
        Ok(())
    }

    pub fn set_table_defaults(&mut self, name: &str, defaults: Vec<ColumnDefault>) {
        let key = name.to_uppercase();
        self.table_defaults.insert(key, defaults);
    }

    pub fn get_table_defaults(&self, name: &str) -> Option<&Vec<ColumnDefault>> {
        let key = self.resolve_table_name(name);
        self.table_defaults.get(&key)
    }

    pub fn get_column_default(&self, table_name: &str, column_name: &str) -> Option<&Expr> {
        let defaults = self.get_table_defaults(table_name)?;
        for default in defaults {
            if default.column_name.to_uppercase() == column_name.to_uppercase() {
                return Some(&default.default_expr);
            }
        }
        None
    }

    pub fn insert_table(&mut self, name: &str, table: Table) -> Result<()> {
        let key = name.to_uppercase();
        if let Some(dot_pos) = key.find('.') {
            let schema_name = &key[..dot_pos];
            if !self.schemas.contains(schema_name) {
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
        self.tables.insert(key, table);
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        let key = self.resolve_table_name(name);
        if self.tables.remove(&key).is_none() {
            return Err(Error::TableNotFound(name.to_string()));
        }
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        let key = self.resolve_table_name(name);
        self.tables.get(&key)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        let key = self.resolve_table_name(name);
        self.tables.get_mut(&key)
    }

    pub fn table_exists(&self, name: &str) -> bool {
        let key = self.resolve_table_name(name);
        self.tables.contains_key(&key)
    }

    pub fn rename_table(&mut self, old_name: &str, new_name: &str) -> Result<()> {
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

    pub fn create_function(&mut self, func: UserFunction, or_replace: bool) -> Result<()> {
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

    pub fn drop_function(&mut self, name: &str) -> Result<()> {
        let key = name.to_uppercase();
        if self.functions.remove(&key).is_none() {
            return Err(Error::invalid_query(format!(
                "Function not found: {}",
                name
            )));
        }
        Ok(())
    }

    pub fn get_function(&self, name: &str) -> Option<&UserFunction> {
        self.functions.get(&name.to_uppercase())
    }

    pub fn function_exists(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_uppercase())
    }

    pub fn create_procedure(&mut self, proc: UserProcedure, or_replace: bool) -> Result<()> {
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

    pub fn drop_procedure(&mut self, name: &str) -> Result<()> {
        let key = name.to_uppercase();
        if self.procedures.remove(&key).is_none() {
            return Err(Error::invalid_query(format!(
                "Procedure not found: {}",
                name
            )));
        }
        Ok(())
    }

    pub fn get_procedure(&self, name: &str) -> Option<&UserProcedure> {
        self.procedures.get(&name.to_uppercase())
    }

    pub fn procedure_exists(&self, name: &str) -> bool {
        self.procedures.contains_key(&name.to_uppercase())
    }

    pub fn get_functions(&self) -> &HashMap<String, UserFunction> {
        &self.functions
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

impl CatalogProvider for Catalog {
    fn get_table_schema(&self, name: &str) -> Option<Schema> {
        self.get_table(name).map(|t| t.schema().clone())
    }

    fn get_view(&self, name: &str) -> Option<yachtsql_parser::ViewDefinition> {
        self.views
            .get(&name.to_uppercase())
            .map(|v| yachtsql_parser::ViewDefinition {
                query: v.query.clone(),
                column_aliases: v.column_aliases.clone(),
            })
    }
}
