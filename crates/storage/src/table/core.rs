use indexmap::IndexMap;

use crate::storage_backend::ColumnarStorage;
use crate::{Column, Schema};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TableEngine {
    #[default]
    Memory,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub(super) schema: Schema,
    pub(super) storage: ColumnarStorage,
    pub(super) engine: TableEngine,
    pub(super) comment: Option<String>,
}

impl Table {
    pub fn new(schema: Schema) -> Self {
        let storage = ColumnarStorage::new(&schema);
        Self {
            schema,
            storage,
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
        self.storage.row_count()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.storage.columns().get(name)
    }

    pub fn columns(&self) -> &IndexMap<String, Column> {
        self.storage.columns()
    }

    pub fn storage(&self) -> &ColumnarStorage {
        &self.storage
    }

    pub fn storage_mut(&mut self) -> &mut ColumnarStorage {
        &mut self.storage
    }

    pub fn clone_with(
        &self,
        schema: Schema,
        columns: IndexMap<String, Column>,
        row_count: usize,
    ) -> Table {
        Table {
            schema,
            storage: ColumnarStorage::from_columns(columns, row_count),
            engine: self.engine.clone(),
            comment: self.comment.clone(),
        }
    }
}
