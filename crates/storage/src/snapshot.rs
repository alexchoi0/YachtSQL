use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};

use crate::storage_backend::StorageLayout;
use crate::{Row, Schema, Table};

#[derive(Debug, Clone)]
pub struct SnapshotTable {
    pub name: String,
    pub source_table: String,
    pub schema: Schema,
    pub data: Table,
    pub created_at: Option<String>,
    pub for_system_time: Option<String>,
    pub options: HashMap<String, String>,
}

impl SnapshotTable {
    pub fn new(
        name: String,
        source_table: String,
        schema: Schema,
        rows: Vec<Row>,
        layout: StorageLayout,
    ) -> Self {
        let mut table = Table::with_layout(schema.clone(), layout);
        for row in rows {
            let _ = table.insert_row(row);
        }
        Self {
            name,
            source_table,
            schema,
            data: table,
            created_at: None,
            for_system_time: None,
            options: HashMap::new(),
        }
    }

    pub fn with_options(
        name: String,
        source_table: String,
        schema: Schema,
        rows: Vec<Row>,
        layout: StorageLayout,
        for_system_time: Option<String>,
        options: Vec<(String, String)>,
    ) -> Self {
        let mut snapshot = Self::new(name, source_table, schema, rows, layout);
        snapshot.for_system_time = for_system_time;
        snapshot.options = options.into_iter().collect();
        snapshot
    }

    pub fn get_table(&self) -> &Table {
        &self.data
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotRegistry {
    snapshots: HashMap<String, SnapshotTable>,
}

impl SnapshotRegistry {
    pub fn new() -> Self {
        Self {
            snapshots: HashMap::new(),
        }
    }

    pub fn create_snapshot(
        &mut self,
        snapshot: SnapshotTable,
        if_not_exists: bool,
    ) -> Result<bool> {
        if self.snapshots.contains_key(&snapshot.name) {
            if if_not_exists {
                return Ok(false);
            }
            return Err(Error::invalid_query(format!(
                "Snapshot table '{}' already exists",
                snapshot.name
            )));
        }
        self.snapshots.insert(snapshot.name.clone(), snapshot);
        Ok(true)
    }

    pub fn drop_snapshot(&mut self, name: &str, if_exists: bool) -> Result<bool> {
        if let Some(_snapshot) = self.snapshots.remove(name) {
            Ok(true)
        } else if if_exists {
            Ok(false)
        } else {
            Err(Error::invalid_query(format!(
                "Snapshot table '{}' does not exist",
                name
            )))
        }
    }

    pub fn get_snapshot(&self, name: &str) -> Option<&SnapshotTable> {
        self.snapshots.get(name)
    }

    pub fn get_snapshot_mut(&mut self, name: &str) -> Option<&mut SnapshotTable> {
        self.snapshots.get_mut(name)
    }

    pub fn exists(&self, name: &str) -> bool {
        self.snapshots.contains_key(name)
    }

    pub fn list_snapshots(&self) -> Vec<String> {
        self.snapshots.keys().cloned().collect()
    }
}
