//! Storage engine and table management for YachtSQL.

pub mod bitmap;
pub mod column;
pub mod column_ops;
pub mod schema;
pub mod simd;
pub mod storage_backend;
pub mod table;

pub use bitmap::NullBitmap;
pub use column::Column;
use indexmap::IndexMap;
pub use schema::{Field, FieldMode, Schema};
pub use storage_backend::ColumnarStorage;
pub use table::{ColumnStatistics, Table, TableEngine, TableSchemaOps, TableStatistics};
use yachtsql_core::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct Storage {
    datasets: IndexMap<String, Dataset>,
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            datasets: IndexMap::new(),
        }
    }
}

impl Storage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_dataset(&mut self, dataset_id: String) -> Result<()> {
        self.datasets.insert(dataset_id, Dataset::new());
        Ok(())
    }

    pub fn get_dataset(&self, dataset_id: &str) -> Option<&Dataset> {
        self.datasets.get(dataset_id)
    }

    pub fn get_dataset_mut(&mut self, dataset_id: &str) -> Option<&mut Dataset> {
        self.datasets.get_mut(dataset_id)
    }

    pub fn delete_dataset(&mut self, dataset_id: &str) -> Result<()> {
        self.datasets.shift_remove(dataset_id);
        Ok(())
    }

    pub fn rename_dataset(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        if let Some(dataset) = self.datasets.shift_remove(old_name) {
            self.datasets.insert(new_name.to_string(), dataset);
            Ok(())
        } else {
            Err(Error::DatasetNotFound(format!(
                "Dataset '{}' not found",
                old_name
            )))
        }
    }

    pub fn list_datasets(&self) -> Vec<&String> {
        self.datasets.keys().collect()
    }

    pub fn create_table(&mut self, table_name: String, schema: Schema) -> Result<()> {
        self.ensure_default_dataset();
        let dataset = self
            .datasets
            .get_mut("default")
            .expect("default dataset should exist after ensure_default_dataset()");
        dataset.create_table(table_name, schema)
    }

    pub fn get_table(&self, table_name: &str) -> Option<&Table> {
        self.datasets.get("default")?.get_table(table_name)
    }

    pub fn get_table_mut(&mut self, table_name: &str) -> Option<&mut Table> {
        self.datasets.get_mut("default")?.get_table_mut(table_name)
    }

    pub fn delete_table(&mut self, table_name: &str) -> Result<()> {
        if let Some(dataset) = self.datasets.get_mut("default") {
            dataset.delete_table(table_name)
        } else {
            Ok(())
        }
    }

    fn ensure_default_dataset(&mut self) {
        if !self.datasets.contains_key("default") {
            self.datasets.insert("default".to_string(), Dataset::new());
        }
    }
}

#[derive(Debug, Clone)]
pub struct Dataset {
    tables: IndexMap<String, Table>,
}

impl Default for Dataset {
    fn default() -> Self {
        Self::new()
    }
}

impl Dataset {
    pub fn new() -> Self {
        Self {
            tables: IndexMap::new(),
        }
    }

    pub fn create_table(&mut self, table_id: String, schema: Schema) -> Result<()> {
        let table = Table::new(schema);
        self.tables.insert(table_id, table);
        Ok(())
    }

    pub fn create_table_with_engine(
        &mut self,
        table_id: String,
        schema: Schema,
        engine: TableEngine,
    ) -> Result<()> {
        let mut table = Table::new(schema);
        table.set_engine(engine);
        self.tables.insert(table_id, table);
        Ok(())
    }

    pub fn get_table(&self, table_id: &str) -> Option<&Table> {
        self.tables.get(table_id)
    }

    pub fn get_table_mut(&mut self, table_id: &str) -> Option<&mut Table> {
        self.tables.get_mut(table_id)
    }

    pub fn delete_table(&mut self, table_id: &str) -> Result<()> {
        self.tables.shift_remove(table_id);
        Ok(())
    }

    pub fn list_tables(&self) -> Vec<&String> {
        self.tables.keys().collect()
    }

    pub fn tables(&self) -> &IndexMap<String, Table> {
        &self.tables
    }

    pub fn rename_table(&mut self, old_table_id: &str, new_table_id: &str) -> Result<()> {
        if let Some(table) = self.tables.shift_remove(old_table_id) {
            self.tables.insert(new_table_id.to_string(), table);
            Ok(())
        } else {
            Err(yachtsql_core::error::Error::table_not_found(
                old_table_id.to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;

    use super::*;

    #[test]
    fn test_storage_new() {
        let storage = Storage::new();
        assert_eq!(storage.list_datasets().len(), 0);
    }

    #[test]
    fn test_create_dataset() {
        let mut storage = Storage::new();
        storage.create_dataset("test_dataset".to_string()).unwrap();

        assert_eq!(storage.list_datasets().len(), 1);
        assert!(storage.get_dataset("test_dataset").is_some());
    }

    #[test]
    fn test_get_dataset() {
        let mut storage = Storage::new();
        storage.create_dataset("test_dataset".to_string()).unwrap();

        let dataset = storage.get_dataset("test_dataset");
        assert!(dataset.is_some());

        let non_existent = storage.get_dataset("non_existent");
        assert!(non_existent.is_none());
    }

    #[test]
    fn test_get_dataset_mut() {
        let mut storage = Storage::new();
        storage.create_dataset("test_dataset".to_string()).unwrap();

        let dataset = storage.get_dataset_mut("test_dataset");
        assert!(dataset.is_some());
    }

    #[test]
    fn test_delete_dataset() {
        let mut storage = Storage::new();
        storage.create_dataset("test_dataset".to_string()).unwrap();
        assert_eq!(storage.list_datasets().len(), 1);

        storage.delete_dataset("test_dataset").unwrap();
        assert_eq!(storage.list_datasets().len(), 0);
    }

    #[test]
    fn test_delete_non_existent_dataset() {
        let mut storage = Storage::new();
        let result = storage.delete_dataset("non_existent");
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_datasets() {
        let mut storage = Storage::new();
        storage.create_dataset("dataset1".to_string()).unwrap();
        storage.create_dataset("dataset2".to_string()).unwrap();
        storage.create_dataset("dataset3".to_string()).unwrap();

        let datasets = storage.list_datasets();
        assert_eq!(datasets.len(), 3);
        assert!(datasets.contains(&&"dataset1".to_string()));
        assert!(datasets.contains(&&"dataset2".to_string()));
        assert!(datasets.contains(&&"dataset3".to_string()));
    }

    #[test]
    fn test_dataset_new() {
        let dataset = Dataset::new();
        assert_eq!(dataset.list_tables().len(), 0);
    }

    #[test]
    fn test_create_table() {
        let mut dataset = Dataset::new();
        let schema = Schema::from_fields(vec![
            Field::required("id".to_string(), DataType::Int64),
            Field::required("name".to_string(), DataType::String),
        ]);

        dataset.create_table("users".to_string(), schema).unwrap();
        assert_eq!(dataset.list_tables().len(), 1);
        assert!(dataset.get_table("users").is_some());
    }

    #[test]
    fn test_get_table() {
        let mut dataset = Dataset::new();
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        dataset
            .create_table("test_table".to_string(), schema)
            .unwrap();

        let table = dataset.get_table("test_table");
        assert!(table.is_some());

        let non_existent = dataset.get_table("non_existent");
        assert!(non_existent.is_none());
    }

    #[test]
    fn test_get_table_mut() {
        let mut dataset = Dataset::new();
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        dataset
            .create_table("test_table".to_string(), schema)
            .unwrap();

        let table = dataset.get_table_mut("test_table");
        assert!(table.is_some());
    }

    #[test]
    fn test_delete_table() {
        let mut dataset = Dataset::new();
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        dataset
            .create_table("test_table".to_string(), schema)
            .unwrap();
        assert_eq!(dataset.list_tables().len(), 1);

        dataset.delete_table("test_table").unwrap();
        assert_eq!(dataset.list_tables().len(), 0);
    }

    #[test]
    fn test_delete_non_existent_table() {
        let mut dataset = Dataset::new();
        let result = dataset.delete_table("non_existent");
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_tables() {
        let mut dataset = Dataset::new();
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        dataset
            .create_table("table1".to_string(), schema.clone())
            .unwrap();
        dataset
            .create_table("table2".to_string(), schema.clone())
            .unwrap();
        dataset.create_table("table3".to_string(), schema).unwrap();

        let tables = dataset.list_tables();
        assert_eq!(tables.len(), 3);
        assert!(tables.contains(&&"table1".to_string()));
        assert!(tables.contains(&&"table2".to_string()));
        assert!(tables.contains(&&"table3".to_string()));
    }

    #[test]
    fn test_storage_with_dataset_and_tables() {
        let mut storage = Storage::new();
        storage.create_dataset("main".to_string()).unwrap();

        let schema = Schema::from_fields(vec![
            Field::required("id".to_string(), DataType::Int64),
            Field::required("name".to_string(), DataType::String),
        ]);

        let dataset = storage.get_dataset_mut("main").unwrap();
        dataset
            .create_table("users".to_string(), schema.clone())
            .unwrap();
        dataset
            .create_table("products".to_string(), schema)
            .unwrap();

        assert_eq!(dataset.list_tables().len(), 2);
    }

    #[test]
    fn test_multiple_datasets_with_tables() {
        let mut storage = Storage::new();
        storage.create_dataset("dataset1".to_string()).unwrap();
        storage.create_dataset("dataset2".to_string()).unwrap();

        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        let dataset1 = storage.get_dataset_mut("dataset1").unwrap();
        dataset1
            .create_table("table1".to_string(), schema.clone())
            .unwrap();

        let dataset2 = storage.get_dataset_mut("dataset2").unwrap();
        dataset2.create_table("table2".to_string(), schema).unwrap();

        assert_eq!(storage.list_datasets().len(), 2);
        assert_eq!(
            storage.get_dataset("dataset1").unwrap().list_tables().len(),
            1
        );
        assert_eq!(
            storage.get_dataset("dataset2").unwrap().list_tables().len(),
            1
        );
    }
}
