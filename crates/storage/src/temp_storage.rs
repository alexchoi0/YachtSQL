use indexmap::IndexMap;
use yachtsql_common::error::{Error, Result};

use crate::storage_backend::StorageLayout;
use crate::{Schema, Table};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnCommitAction {
    #[default]
    PreserveRows,
    DeleteRows,
    Drop,
}

#[derive(Debug, Clone)]
pub struct TempTableMetadata {
    pub table: Table,

    pub on_commit_action: OnCommitAction,
}

impl TempTableMetadata {
    pub fn new(table: Table, on_commit_action: OnCommitAction) -> Self {
        Self {
            table,
            on_commit_action,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TempStorage {
    tables: IndexMap<String, TempTableMetadata>,
    layout: StorageLayout,
}

impl TempStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_layout(layout: StorageLayout) -> Self {
        Self {
            tables: IndexMap::new(),
            layout,
        }
    }

    pub fn storage_layout(&self) -> StorageLayout {
        self.layout
    }

    #[inline]
    fn normalize_name(name: &str) -> String {
        name.to_lowercase()
    }

    pub fn create_table(
        &mut self,
        name: impl Into<String>,
        schema: Schema,
        on_commit_action: OnCommitAction,
    ) -> Result<()> {
        let name = Self::normalize_name(&name.into());

        if self.tables.contains_key(&name) {
            return Err(Error::InvalidOperation(format!(
                "Temporary table '{}' already exists",
                name
            )));
        }

        let table = Table::with_layout(schema, self.layout);
        let metadata = TempTableMetadata::new(table, on_commit_action);
        self.tables.insert(name, metadata);

        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        let name = Self::normalize_name(name);
        self.tables.get(&name).map(|meta| &meta.table)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        let name = Self::normalize_name(name);
        self.tables.get_mut(&name).map(|meta| &mut meta.table)
    }

    pub fn table_exists(&self, name: &str) -> bool {
        let name = Self::normalize_name(name);
        self.tables.contains_key(&name)
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        let name = Self::normalize_name(name);
        self.tables.shift_remove(&name);
        Ok(())
    }

    pub fn get_on_commit_action(&self, name: &str) -> Option<OnCommitAction> {
        let name = Self::normalize_name(name);
        self.tables.get(&name).map(|meta| meta.on_commit_action)
    }

    pub fn list_tables(&self) -> Vec<&String> {
        self.tables.keys().collect()
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub fn clear(&mut self) {
        self.tables.clear();
    }

    pub fn truncate_table(&mut self, name: &str) -> Result<()> {
        let name = Self::normalize_name(name);

        if let Some(metadata) = self.tables.get_mut(&name) {
            metadata.table.delete_rows(|_| Ok(true))?;
            Ok(())
        } else {
            Err(Error::InvalidOperation(format!(
                "Temporary table '{}' not found",
                name
            )))
        }
    }

    pub fn rename_table(&mut self, old_name: &str, new_name: impl Into<String>) -> Result<()> {
        let old_name = Self::normalize_name(old_name);
        let new_name = Self::normalize_name(&new_name.into());

        if !self.tables.contains_key(&old_name) {
            return Err(Error::InvalidOperation(format!(
                "Temporary table '{}' not found",
                old_name
            )));
        }

        if self.tables.contains_key(&new_name) {
            return Err(Error::InvalidOperation(format!(
                "Temporary table '{}' already exists",
                new_name
            )));
        }

        if let Some(metadata) = self.tables.shift_remove(&old_name) {
            self.tables.insert(new_name, metadata);
        }

        Ok(())
    }
}

impl Default for TempStorage {
    fn default() -> Self {
        Self::with_layout(StorageLayout::Columnar)
    }
}

impl Drop for TempStorage {
    fn drop(&mut self) {
        self.tables.clear();
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;

    use super::*;
    use crate::schema::Field;

    fn create_test_schema() -> Schema {
        Schema::from_fields(vec![
            Field::required("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ])
    }

    #[test]
    fn test_create_and_get_temp_table() {
        let mut storage = TempStorage::new();
        let schema = create_test_schema();

        storage
            .create_table("users", schema.clone(), OnCommitAction::PreserveRows)
            .unwrap();

        assert!(storage.table_exists("users"));
        assert!(storage.table_exists("USERS"));
        assert!(storage.get_table("users").is_some());
    }

    #[test]
    fn test_create_duplicate_fails() {
        let mut storage = TempStorage::new();
        let schema = create_test_schema();

        storage
            .create_table("users", schema.clone(), OnCommitAction::PreserveRows)
            .unwrap();

        let result = storage.create_table("users", schema, OnCommitAction::PreserveRows);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_table() {
        let mut storage = TempStorage::new();
        let schema = create_test_schema();

        storage
            .create_table("users", schema, OnCommitAction::PreserveRows)
            .unwrap();
        assert!(storage.table_exists("users"));

        storage.drop_table("users").unwrap();
        assert!(!storage.table_exists("users"));
    }

    #[test]
    fn test_list_tables() {
        let mut storage = TempStorage::new();
        let schema = create_test_schema();

        storage
            .create_table("users", schema.clone(), OnCommitAction::PreserveRows)
            .unwrap();
        storage
            .create_table("orders", schema, OnCommitAction::DeleteRows)
            .unwrap();

        let tables = storage.list_tables();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&&"users".to_string()));
        assert!(tables.contains(&&"orders".to_string()));
    }

    #[test]
    fn test_on_commit_action() {
        let mut storage = TempStorage::new();
        let schema = create_test_schema();

        storage
            .create_table("temp1", schema.clone(), OnCommitAction::PreserveRows)
            .unwrap();
        storage
            .create_table("temp2", schema, OnCommitAction::Drop)
            .unwrap();

        assert_eq!(
            storage.get_on_commit_action("temp1"),
            Some(OnCommitAction::PreserveRows)
        );
        assert_eq!(
            storage.get_on_commit_action("temp2"),
            Some(OnCommitAction::Drop)
        );
    }

    #[test]
    fn test_rename_table() {
        let mut storage = TempStorage::new();
        let schema = create_test_schema();

        storage
            .create_table("old_name", schema, OnCommitAction::PreserveRows)
            .unwrap();

        storage.rename_table("old_name", "new_name").unwrap();

        assert!(!storage.table_exists("old_name"));
        assert!(storage.table_exists("new_name"));
    }

    #[test]
    fn test_truncate_table() {
        let mut storage = TempStorage::new();
        let schema = create_test_schema();

        storage
            .create_table("data", schema, OnCommitAction::PreserveRows)
            .unwrap();

        let table = storage.get_table_mut("data").unwrap();
        let schema = table.schema().clone();
        let mut row = crate::table::Row::for_schema(&schema);
        row.set_by_name(&schema, "id", yachtsql_common::types::Value::int64(1))
            .unwrap();
        row.set_by_name(
            &schema,
            "name",
            yachtsql_common::types::Value::string("test".to_string()),
        )
        .unwrap();
        table.insert_rows(vec![row]).unwrap();

        assert_eq!(table.row_count(), 1);

        storage.truncate_table("data").unwrap();

        let table = storage.get_table("data").unwrap();
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_clear_all_tables() {
        let mut storage = TempStorage::new();
        let schema = create_test_schema();

        storage
            .create_table("temp1", schema.clone(), OnCommitAction::PreserveRows)
            .unwrap();
        storage
            .create_table("temp2", schema, OnCommitAction::PreserveRows)
            .unwrap();

        assert_eq!(storage.table_count(), 2);

        storage.clear();

        assert_eq!(storage.table_count(), 0);
        assert!(storage.is_empty());
    }
}
