use std::collections::HashMap;

use indexmap::IndexMap;
use yachtsql_core::error::Result;

use super::partition::PartitionSpec;
use crate::index::IndexMetadata;
use crate::indexes::TableIndex;
use crate::row::Row;
use crate::storage_backend::{
    ColumnarStorage, RowStorage, StorageBackend, StorageLayout, TableStorage,
};
use crate::{Column, Schema};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TableEngine {
    #[default]
    Memory,
}

pub struct Table {
    pub(super) schema: Schema,
    pub(super) storage: StorageBackend,
    pub(super) partition_spec: Option<PartitionSpec>,

    pub(super) indexes: HashMap<String, Box<dyn TableIndex>>,

    pub(super) index_metadata: Vec<IndexMetadata>,

    pub(super) engine: TableEngine,

    pub(super) comment: Option<String>,
}

impl Clone for Table {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            storage: self.storage.clone(),
            partition_spec: self.partition_spec.clone(),
            indexes: HashMap::new(),
            index_metadata: Vec::new(),
            engine: self.engine.clone(),
            comment: self.comment.clone(),
        }
    }
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("schema", &self.schema)
            .field("storage", &self.storage)
            .field("partition_spec", &self.partition_spec)
            .field("index_count", &self.indexes.len())
            .field("index_metadata", &self.index_metadata)
            .field("engine", &self.engine)
            .field("comment", &self.comment)
            .finish()
    }
}

impl Table {
    pub fn new(schema: Schema) -> Self {
        Self::with_layout(schema, StorageLayout::Columnar)
    }

    pub fn with_layout(schema: Schema, layout: StorageLayout) -> Self {
        let storage = match layout {
            StorageLayout::Columnar => StorageBackend::columnar(&schema),
            StorageLayout::Row => StorageBackend::row(),
        };

        Self {
            schema,
            storage,
            partition_spec: None,
            indexes: HashMap::new(),
            index_metadata: Vec::new(),
            engine: TableEngine::default(),
            comment: None,
        }
    }

    pub fn engine(&self) -> &TableEngine {
        &self.engine
    }

    pub fn set_engine(&mut self, engine: TableEngine) {
        self.engine = engine;
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn set_comment(&mut self, comment: Option<String>) {
        self.comment = comment;
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn row_count(&self) -> usize {
        self.storage().row_count()
    }

    pub fn is_empty(&self) -> bool {
        self.storage().is_empty()
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columnar_storage()
            .and_then(|storage| storage.columns().get(name))
    }

    pub fn columns(&self) -> &IndexMap<String, Column> {
        self.column_map()
    }

    pub fn partition_spec(&self) -> Option<&PartitionSpec> {
        self.partition_spec.as_ref()
    }

    pub fn set_partition_spec(&mut self, partition_spec: Option<PartitionSpec>) {
        self.partition_spec = partition_spec;
    }

    pub fn is_partitioned(&self) -> bool {
        self.partition_spec.is_some()
    }

    pub fn storage_layout(&self) -> StorageLayout {
        self.storage().layout()
    }

    pub fn to_row_layout(&self) -> Result<Self> {
        match self.storage_layout() {
            StorageLayout::Row => Ok(self.clone()),
            StorageLayout::Columnar => {
                let rows = self.get_all_rows();
                Ok(Self {
                    schema: self.schema.clone(),
                    storage: StorageBackend::Row(RowStorage::from_rows(rows)),
                    partition_spec: self.partition_spec.clone(),
                    indexes: HashMap::new(),
                    index_metadata: Vec::new(),
                    engine: self.engine.clone(),
                    comment: self.comment.clone(),
                })
            }
        }
    }

    pub fn to_column_layout(&self) -> Result<Self> {
        match self.storage_layout() {
            StorageLayout::Columnar => Ok(self.clone()),
            StorageLayout::Row => {
                let mut columnar_storage = ColumnarStorage::new(&self.schema);

                for row_idx in 0..self.row_count() {
                    let row = self.get_row(row_idx)?;
                    columnar_storage.insert_row(row, &self.schema)?;
                }

                Ok(Self {
                    schema: self.schema.clone(),
                    storage: StorageBackend::Columnar(columnar_storage),
                    partition_spec: self.partition_spec.clone(),
                    indexes: HashMap::new(),
                    index_metadata: Vec::new(),
                    engine: self.engine.clone(),
                    comment: self.comment.clone(),
                })
            }
        }
    }

    pub(super) fn storage(&self) -> &dyn TableStorage {
        self.storage.as_storage()
    }

    pub(super) fn storage_mut(&mut self) -> &mut dyn TableStorage {
        self.storage.as_storage_mut()
    }

    pub(super) fn columnar_storage(&self) -> Option<&ColumnarStorage> {
        self.storage.as_columnar()
    }

    pub(super) fn column_map(&self) -> &IndexMap<String, Column> {
        self.columnar_storage()
            .expect("columnar storage backend required")
            .columns()
    }

    pub(super) fn clone_with(
        &self,
        schema: Schema,
        columns: IndexMap<String, Column>,
        row_count: usize,
    ) -> Table {
        Table {
            schema: schema.clone(),
            storage: StorageBackend::Columnar(ColumnarStorage::from_columns(columns, row_count)),
            partition_spec: self.partition_spec.clone(),
            indexes: HashMap::new(),
            index_metadata: Vec::new(),
            engine: self.engine.clone(),
            comment: self.comment.clone(),
        }
    }

    pub(super) fn clone_with_rows(&self, schema: Schema, rows: Vec<Row>) -> Table {
        Table {
            schema,
            storage: StorageBackend::Row(RowStorage::from_rows(rows)),
            partition_spec: self.partition_spec.clone(),
            indexes: HashMap::new(),
            index_metadata: Vec::new(),
            engine: self.engine.clone(),
            comment: self.comment.clone(),
        }
    }
}
