use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use indexmap::IndexMap;
use yachtsql_core::error::Result;

use super::partition::PartitionSpec;
use crate::foreign_keys::ForeignKey;
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
    Log,
    TinyLog,
    StripeLog,
    MergeTree {
        order_by: Vec<String>,
    },
    ReplacingMergeTree {
        order_by: Vec<String>,
        version_column: Option<String>,
    },
    SummingMergeTree {
        order_by: Vec<String>,
        sum_columns: Vec<String>,
    },
    CollapsingMergeTree {
        order_by: Vec<String>,
        sign_column: String,
    },
    VersionedCollapsingMergeTree {
        order_by: Vec<String>,
        sign_column: String,
        version_column: String,
    },
    AggregatingMergeTree {
        order_by: Vec<String>,
    },
    Distributed {
        cluster: String,
        database: String,
        table: String,
        sharding_key: Option<String>,
    },
    Buffer {
        database: String,
        table: String,
    },
}

pub struct Table {
    pub(super) schema: Schema,
    pub(super) storage: StorageBackend,
    pub(super) auto_increment_counter: Option<Rc<RefCell<i64>>>,
    pub(super) auto_increment_column: Option<String>,
    pub(super) foreign_keys: Vec<ForeignKey>,
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
            auto_increment_counter: self.auto_increment_counter.clone(),
            auto_increment_column: self.auto_increment_column.clone(),
            foreign_keys: self.foreign_keys.clone(),
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
            .field("auto_increment_counter", &self.auto_increment_counter)
            .field("auto_increment_column", &self.auto_increment_column)
            .field("foreign_keys", &self.foreign_keys)
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
        let (auto_increment_counter, auto_increment_column) =
            if let Some(field) = schema.fields().iter().find(|f| f.is_auto_increment) {
                (Some(Rc::new(RefCell::new(1))), Some(field.name.clone()))
            } else {
                (None, None)
            };

        let storage = match layout {
            StorageLayout::Columnar => StorageBackend::columnar(&schema),
            StorageLayout::Row => StorageBackend::row(),
        };

        let foreign_keys = schema.foreign_keys().to_vec();

        Self {
            schema,
            storage,
            auto_increment_counter,
            auto_increment_column,
            foreign_keys,
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

    pub fn foreign_keys(&self) -> &[ForeignKey] {
        &self.foreign_keys
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

    pub fn auto_increment_column(&self) -> Option<&str> {
        self.auto_increment_column.as_deref()
    }

    pub fn next_auto_increment(&self) -> Result<i64> {
        use yachtsql_core::error::Error;
        match &self.auto_increment_counter {
            Some(counter) => {
                let mut val = counter.borrow_mut();
                let current = *val;
                *val += 1;
                Ok(current)
            }
            None => Err(Error::InvalidOperation(
                "Table does not have AUTO_INCREMENT column".to_string(),
            )),
        }
    }

    pub fn update_auto_increment_if_greater(&self, explicit_value: i64) -> Result<()> {
        match &self.auto_increment_counter {
            Some(counter) => {
                let mut val = counter.borrow_mut();
                if explicit_value >= *val {
                    *val = explicit_value + 1;
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn reset_auto_increment(&self, start_value: i64) -> Result<()> {
        match &self.auto_increment_counter {
            Some(counter) => {
                *counter.borrow_mut() = start_value;
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn set_auto_increment(&self, value: i64) -> Result<()> {
        self.reset_auto_increment(value)
    }

    pub fn init_auto_increment(&mut self, column_name: String, start_value: i64) -> Result<()> {
        use yachtsql_core::error::Error;
        if self.auto_increment_counter.is_some() {
            return Err(Error::InvalidOperation(
                "Table already has an AUTO_INCREMENT column".to_string(),
            ));
        }

        self.auto_increment_counter = Some(Rc::new(RefCell::new(start_value)));
        self.auto_increment_column = Some(column_name);
        Ok(())
    }

    pub fn remove_auto_increment(&mut self) -> Result<()> {
        self.auto_increment_counter = None;
        self.auto_increment_column = None;
        Ok(())
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
                    auto_increment_counter: self.auto_increment_counter.clone(),
                    auto_increment_column: self.auto_increment_column.clone(),
                    foreign_keys: self.foreign_keys.clone(),
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
                    auto_increment_counter: self.auto_increment_counter.clone(),
                    auto_increment_column: self.auto_increment_column.clone(),
                    foreign_keys: self.foreign_keys.clone(),
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
            auto_increment_counter: self.auto_increment_counter.clone(),
            auto_increment_column: self.auto_increment_column.clone(),
            foreign_keys: self.foreign_keys.clone(),
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
            auto_increment_counter: self.auto_increment_counter.clone(),
            auto_increment_column: self.auto_increment_column.clone(),
            foreign_keys: self.foreign_keys.clone(),
            partition_spec: self.partition_spec.clone(),
            indexes: HashMap::new(),
            index_metadata: Vec::new(),
            engine: self.engine.clone(),
            comment: self.comment.clone(),
        }
    }
}
