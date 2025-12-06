use std::collections::HashMap;
use std::sync::Arc;

use yachtsql_optimizer::catalog::{IndexCatalog, IndexInfo, IndexType};
use yachtsql_storage::index::{IndexMetadata, IndexType as StorageIndexType};

pub struct SnapshotCatalog {
    indexes_by_table: HashMap<String, Vec<IndexInfo>>,
    indexes_by_name: HashMap<String, IndexInfo>,
}

impl SnapshotCatalog {
    pub fn new() -> Self {
        Self {
            indexes_by_table: HashMap::new(),
            indexes_by_name: HashMap::new(),
        }
    }

    pub fn from_dataset(dataset: &yachtsql_storage::Dataset) -> Self {
        let mut catalog = Self::new();

        for idx in dataset.indexes().values() {
            catalog.add_index(idx);
        }

        catalog
    }

    fn convert_index_type(storage_type: &StorageIndexType) -> IndexType {
        match storage_type {
            StorageIndexType::BTree => IndexType::BTree,
            StorageIndexType::Hash => IndexType::Hash,
            StorageIndexType::GiST => IndexType::GiST,
            StorageIndexType::GIN => IndexType::GIN,
            StorageIndexType::BRIN => IndexType::BRIN,
            StorageIndexType::SPGiST => IndexType::BTree,
            StorageIndexType::IVFFlat => IndexType::BTree,
            StorageIndexType::HNSW => IndexType::BTree,
        }
    }

    fn add_index(&mut self, idx: &IndexMetadata) {
        let info = IndexInfo::new(
            idx.index_name.clone(),
            idx.table_name.clone(),
            idx.column_names(),
        )
        .with_type(Self::convert_index_type(&idx.index_type))
        .with_unique(idx.is_unique);

        self.indexes_by_name
            .insert(idx.index_name.clone(), info.clone());
        self.indexes_by_table
            .entry(idx.table_name.clone())
            .or_default()
            .push(info);
    }

    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}

impl Default for SnapshotCatalog {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl Send for SnapshotCatalog {}
unsafe impl Sync for SnapshotCatalog {}

impl IndexCatalog for SnapshotCatalog {
    fn get_indexes_for_table(&self, table_name: &str) -> Vec<IndexInfo> {
        self.indexes_by_table
            .get(table_name)
            .cloned()
            .unwrap_or_default()
    }

    fn get_index(&self, index_name: &str) -> Option<IndexInfo> {
        self.indexes_by_name.get(index_name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_storage::index::IndexColumn;
    use yachtsql_storage::{Dataset, Field, Schema};

    use super::*;

    #[test]
    fn test_snapshot_catalog_empty() {
        let catalog = SnapshotCatalog::new();
        assert!(catalog.get_indexes_for_table("users").is_empty());
        assert!(catalog.get_index("idx").is_none());
    }

    #[test]
    fn test_snapshot_catalog_from_dataset() {
        let mut dataset = Dataset::new();

        let schema = Schema::from_fields(vec![
            Field::required("id".to_string(), DataType::Int64),
            Field::nullable("email".to_string(), DataType::String),
        ]);
        dataset.create_table("users".to_string(), schema).unwrap();

        let index = IndexMetadata::new(
            "idx_users_id".to_string(),
            "users".to_string(),
            vec![IndexColumn::simple("id".to_string())],
        );
        dataset.create_index(index).unwrap();

        let catalog = SnapshotCatalog::from_dataset(&dataset);

        let indexes = catalog.get_indexes_for_table("users");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].index_name, "idx_users_id");
        assert_eq!(indexes[0].columns, vec!["id"]);

        let idx = catalog.get_index("idx_users_id");
        assert!(idx.is_some());
        assert_eq!(idx.unwrap().table_name, "users");

        let no_idx = catalog.get_index("nonexistent");
        assert!(no_idx.is_none());

        let other_table = catalog.get_indexes_for_table("orders");
        assert!(other_table.is_empty());
    }

    #[test]
    fn test_index_type_conversion() {
        assert!(matches!(
            SnapshotCatalog::convert_index_type(&StorageIndexType::BTree),
            IndexType::BTree
        ));
        assert!(matches!(
            SnapshotCatalog::convert_index_type(&StorageIndexType::Hash),
            IndexType::Hash
        ));
        assert!(matches!(
            SnapshotCatalog::convert_index_type(&StorageIndexType::GIN),
            IndexType::GIN
        ));
    }

    #[test]
    fn test_multiple_indexes_same_table() {
        let mut dataset = Dataset::new();

        let schema = Schema::from_fields(vec![
            Field::required("id".to_string(), DataType::Int64),
            Field::nullable("email".to_string(), DataType::String),
        ]);
        dataset.create_table("users".to_string(), schema).unwrap();

        dataset
            .create_index(IndexMetadata::new(
                "idx_users_id".to_string(),
                "users".to_string(),
                vec![IndexColumn::simple("id".to_string())],
            ))
            .unwrap();

        dataset
            .create_index(IndexMetadata::new(
                "idx_users_email".to_string(),
                "users".to_string(),
                vec![IndexColumn::simple("email".to_string())],
            ))
            .unwrap();

        let catalog = SnapshotCatalog::from_dataset(&dataset);
        let indexes = catalog.get_indexes_for_table("users");
        assert_eq!(indexes.len(), 2);
    }
}
