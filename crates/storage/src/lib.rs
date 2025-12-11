//! Storage engine and table management for YachtSQL.

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

pub mod bitmap;
pub mod column;
pub mod column_ops;
pub mod constraints;
pub mod custom_types;
pub mod dependency_graph;
pub mod dictionary;
pub mod domain;
pub mod extension;
pub mod features;
pub mod foreign_keys;
pub mod index;
pub mod indexes;
pub mod mvcc;
pub mod row;
pub mod schema;
pub mod sequence;
pub mod simd;
pub mod snapshot;
pub mod storage_backend;
pub mod table;
pub mod temp_storage;
pub mod transaction;
pub mod trigger;
pub mod type_registry;
pub mod view;

pub use bitmap::NullBitmap;
pub use column::Column;
pub use constraints::{apply_default_values, validate_row_constraints};
pub use custom_types::EnumType;
pub use dependency_graph::DependencyGraph;
pub use dictionary::{
    Dictionary, DictionaryColumn, DictionaryLayout, DictionaryLifetime, DictionaryRegistry,
    DictionarySource,
};
pub use domain::{DomainConstraint, DomainDefinition, DomainRegistry};
pub use extension::{ExtensionError, ExtensionMetadata, ExtensionRegistry};
pub use foreign_keys::{Deferrable, ForeignKey, ReferentialAction};
pub use index::{ColumnOrder, IndexColumn, IndexMetadata, IndexType, NullsOrder};
pub use indexes::{
    BPlusTreeIndex, HashIndex, IndexKey, RangeBound, RangeQuery, TableIndex, extract_index_key,
};
use indexmap::IndexMap;
pub use row::Row;
pub use schema::{
    CheckConstraint, CheckEvaluator, DefaultValue, Field, FieldMode, GeneratedExpression,
    GenerationMode, IdentityGeneration, Schema,
};
pub use sequence::{Sequence, SequenceConfig, SequenceRegistry};
pub use snapshot::{SnapshotRegistry, SnapshotTable};
pub use storage_backend::{StorageBackend, StorageLayout};
pub use table::{
    ColumnStatistics, PartitionSpec, PartitionType, PostgresPartitionBound, PostgresPartitionInfo,
    PostgresPartitionStrategy, Table, TableConstraintOps, TableEngine, TableIndexOps,
    TableIterator, TableSchemaOps, TableStatistics,
};
pub use temp_storage::{OnCommitAction, TempStorage, TempTableMetadata};
pub use transaction::{
    ConstraintTiming, DMLOperation, DeferredFKCheck, DeferredFKState, IsolationLevel,
    PendingChanges, Savepoint, Transaction, TransactionCharacteristics, TransactionManager,
    TransactionScope,
};
pub use trigger::{TriggerEvent, TriggerLevel, TriggerMetadata, TriggerRegistry, TriggerTiming};
pub use type_registry::{TypeDefinition, TypeRegistry, UserDefinedType};
pub use view::{ViewDefinition, ViewRegistry, WithCheckOption};
use yachtsql_core::error::Result;

#[derive(Debug, Clone, Default)]
pub struct QuotaMetadata {
    pub name: String,
}

#[derive(Debug, Clone, Default)]
pub struct RowPolicyMetadata {
    pub name: String,
    pub database: String,
    pub table: String,
}

#[derive(Debug, Clone, Default)]
pub struct SettingsProfileMetadata {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Storage {
    datasets: IndexMap<String, Dataset>,
    default_layout: StorageLayout,
    quotas: IndexMap<String, QuotaMetadata>,
    row_policies: IndexMap<String, RowPolicyMetadata>,
    settings_profiles: IndexMap<String, SettingsProfileMetadata>,
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            datasets: IndexMap::new(),
            default_layout: StorageLayout::Columnar,
            quotas: IndexMap::new(),
            row_policies: IndexMap::new(),
            settings_profiles: IndexMap::new(),
        }
    }
}

impl Storage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_layout(layout: StorageLayout) -> Self {
        Self {
            datasets: IndexMap::new(),
            default_layout: layout,
            quotas: IndexMap::new(),
            row_policies: IndexMap::new(),
            settings_profiles: IndexMap::new(),
        }
    }

    pub fn add_quota(&mut self, name: String) {
        self.quotas.insert(name.clone(), QuotaMetadata { name });
    }

    pub fn remove_quota(&mut self, name: &str) -> bool {
        self.quotas.shift_remove(name).is_some()
    }

    pub fn quotas(&self) -> impl Iterator<Item = &QuotaMetadata> {
        self.quotas.values()
    }

    pub fn add_row_policy(&mut self, name: String, database: String, table: String) {
        self.row_policies.insert(
            name.clone(),
            RowPolicyMetadata {
                name,
                database,
                table,
            },
        );
    }

    pub fn remove_row_policy(&mut self, name: &str) -> bool {
        self.row_policies.shift_remove(name).is_some()
    }

    pub fn row_policies(&self) -> impl Iterator<Item = &RowPolicyMetadata> {
        self.row_policies.values()
    }

    pub fn add_settings_profile(&mut self, name: String) {
        self.settings_profiles
            .insert(name.clone(), SettingsProfileMetadata { name });
    }

    pub fn remove_settings_profile(&mut self, name: &str) -> bool {
        self.settings_profiles.shift_remove(name).is_some()
    }

    pub fn settings_profiles(&self) -> impl Iterator<Item = &SettingsProfileMetadata> {
        self.settings_profiles.values()
    }

    pub fn create_dataset(&mut self, dataset_id: String) -> Result<()> {
        self.datasets
            .insert(dataset_id, Dataset::with_layout(self.default_layout));
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
            self.datasets.insert(
                "default".to_string(),
                Dataset::with_layout(self.default_layout),
            );
        }
    }
}

#[derive(Debug, Clone)]
pub struct Dataset {
    tables: IndexMap<String, Table>,
    sequences: SequenceRegistry,
    indexes: IndexMap<String, index::IndexMetadata>,
    triggers: TriggerRegistry,
    views: ViewRegistry,
    domains: DomainRegistry,
    types: TypeRegistry,
    dictionaries: DictionaryRegistry,
    snapshots: SnapshotRegistry,
    table_layout: StorageLayout,
}

impl Default for Dataset {
    fn default() -> Self {
        Self::new()
    }
}

impl Dataset {
    pub fn new() -> Self {
        Self::with_layout(StorageLayout::Columnar)
    }

    pub fn with_layout(layout: StorageLayout) -> Self {
        Self {
            tables: IndexMap::new(),
            sequences: SequenceRegistry::new(),
            indexes: IndexMap::new(),
            triggers: TriggerRegistry::new(),
            views: ViewRegistry::new(),
            domains: DomainRegistry::new(),
            types: TypeRegistry::new(),
            dictionaries: DictionaryRegistry::new(),
            snapshots: SnapshotRegistry::new(),
            table_layout: layout,
        }
    }

    pub fn create_table(&mut self, table_id: String, schema: Schema) -> Result<()> {
        let table = Table::with_layout(schema, self.table_layout);
        self.tables.insert(table_id, table);
        Ok(())
    }

    pub fn get_table(&self, table_id: &str) -> Option<&Table> {
        self.tables
            .get(table_id)
            .or_else(|| self.snapshots.get_snapshot(table_id).map(|s| s.get_table()))
    }

    pub fn get_table_mut(&mut self, table_id: &str) -> Option<&mut Table> {
        if self.tables.contains_key(table_id) {
            self.tables.get_mut(table_id)
        } else {
            self.snapshots
                .get_snapshot_mut(table_id)
                .map(|s| &mut s.data)
        }
    }

    pub fn is_snapshot(&self, table_id: &str) -> bool {
        !self.tables.contains_key(table_id) && self.snapshots.exists(table_id)
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

    pub fn sequences(&self) -> &SequenceRegistry {
        &self.sequences
    }

    pub fn sequences_mut(&mut self) -> &mut SequenceRegistry {
        &mut self.sequences
    }

    pub fn triggers(&self) -> &TriggerRegistry {
        &self.triggers
    }

    pub fn triggers_mut(&mut self) -> &mut TriggerRegistry {
        &mut self.triggers
    }

    pub fn views(&self) -> &ViewRegistry {
        &self.views
    }

    pub fn views_mut(&mut self) -> &mut ViewRegistry {
        &mut self.views
    }

    pub fn domains(&self) -> &DomainRegistry {
        &self.domains
    }

    pub fn domains_mut(&mut self) -> &mut DomainRegistry {
        &mut self.domains
    }

    pub fn types(&self) -> &TypeRegistry {
        &self.types
    }

    pub fn types_mut(&mut self) -> &mut TypeRegistry {
        &mut self.types
    }

    pub fn indexes(&self) -> &IndexMap<String, index::IndexMetadata> {
        &self.indexes
    }

    pub fn indexes_mut(&mut self) -> &mut IndexMap<String, index::IndexMetadata> {
        &mut self.indexes
    }

    pub fn create_index(&mut self, metadata: index::IndexMetadata) -> Result<()> {
        self.indexes.insert(metadata.index_name.clone(), metadata);
        Ok(())
    }

    pub fn drop_index(&mut self, index_name: &str) -> Result<()> {
        self.indexes.shift_remove(index_name);
        Ok(())
    }

    pub fn has_index(&self, index_name: &str) -> bool {
        self.indexes.contains_key(index_name)
    }

    pub fn get_index(&self, index_name: &str) -> Option<&index::IndexMetadata> {
        self.indexes.get(index_name)
    }

    pub fn dictionaries(&self) -> &DictionaryRegistry {
        &self.dictionaries
    }

    pub fn dictionaries_mut(&mut self) -> &mut DictionaryRegistry {
        &mut self.dictionaries
    }

    pub fn snapshots(&self) -> &SnapshotRegistry {
        &self.snapshots
    }

    pub fn snapshots_mut(&mut self) -> &mut SnapshotRegistry {
        &mut self.snapshots
    }

    pub fn table_layout(&self) -> StorageLayout {
        self.table_layout
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;

    use super::*;
    use crate::storage_backend::StorageLayout;

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
    fn test_storage_with_layout_row_applies_to_tables() {
        let mut storage = Storage::with_layout(StorageLayout::Row);
        storage.create_dataset("default".to_string()).unwrap();

        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);
        storage.create_table("tbl".to_string(), schema).unwrap();
        let table = storage.get_table("tbl").unwrap();
        assert_eq!(table.storage_layout(), StorageLayout::Row);
    }

    #[test]
    fn test_dataset_with_layout_controls_table_layout() {
        let mut dataset = Dataset::with_layout(StorageLayout::Row);
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);
        dataset
            .create_table("row_table".to_string(), schema.clone())
            .unwrap();
        assert_eq!(
            dataset.get_table("row_table").unwrap().storage_layout(),
            StorageLayout::Row
        );

        let mut columnar_dataset = Dataset::with_layout(StorageLayout::Columnar);
        columnar_dataset
            .create_table("col_table".to_string(), schema)
            .unwrap();
        assert_eq!(
            columnar_dataset
                .get_table("col_table")
                .unwrap()
                .storage_layout(),
            StorageLayout::Columnar
        );
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

    #[test]
    fn test_temp_storage_inherits_default_layout() {
        use crate::temp_storage::{OnCommitAction, TempStorage};

        let mut temp_storage = TempStorage::new();
        let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

        temp_storage
            .create_table("temp", schema.clone(), OnCommitAction::PreserveRows)
            .unwrap();

        let table = temp_storage.get_table("temp").unwrap();
        assert_eq!(table.storage_layout(), StorageLayout::Columnar);

        let mut row_temp_storage = TempStorage::with_layout(StorageLayout::Row);
        row_temp_storage
            .create_table("temp_row", schema, OnCommitAction::PreserveRows)
            .unwrap();
        let row_table = row_temp_storage.get_table("temp_row").unwrap();
        assert_eq!(row_table.storage_layout(), StorageLayout::Row);
    }
}
