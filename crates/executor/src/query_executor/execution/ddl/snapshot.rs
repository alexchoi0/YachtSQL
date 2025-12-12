use yachtsql_common::error::{Error, Result};
use yachtsql_parser::validator::CustomStatement;
use yachtsql_storage::SnapshotTable;

use super::super::QueryExecutor;
use super::create::DdlExecutor;
use crate::Table;

pub trait SnapshotExecutor {
    fn execute_create_snapshot_table(&mut self, stmt: &CustomStatement) -> Result<Table>;

    fn execute_drop_snapshot_table(&mut self, stmt: &CustomStatement) -> Result<Table>;
}

impl SnapshotExecutor for QueryExecutor {
    fn execute_create_snapshot_table(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::CreateSnapshotTable {
            name,
            source_table,
            if_not_exists,
            for_system_time,
            options,
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a CREATE SNAPSHOT TABLE statement".to_string(),
            ));
        };

        let snapshot_name = name.to_string();
        let (dataset_id, snapshot_id) = self.parse_ddl_table_name(&snapshot_name)?;

        let source_name = source_table.to_string();
        let (source_dataset_id, source_table_id) = self.parse_ddl_table_name(&source_name)?;

        let mut storage = self.storage.borrow_mut();

        if storage.get_dataset(&dataset_id).is_none() {
            storage.create_dataset(dataset_id.clone())?;
        }

        let source_dataset = storage.get_dataset(&source_dataset_id).ok_or_else(|| {
            Error::DatasetNotFound(format!("Dataset '{}' not found", source_dataset_id))
        })?;

        let source_table_data = source_dataset
            .get_table(&source_table_id)
            .ok_or_else(|| Error::table_not_found(source_table_id.clone()))?;

        let schema = source_table_data.schema().clone();
        let rows = source_table_data.get_all_rows();
        let table_layout = source_dataset.table_layout();

        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        let snapshot = SnapshotTable::with_options(
            snapshot_id.clone(),
            source_table_id.clone(),
            schema,
            rows,
            table_layout,
            for_system_time.clone(),
            options.clone(),
        );

        dataset
            .snapshots_mut()
            .create_snapshot(snapshot, *if_not_exists)?;

        self.plan_cache.borrow_mut().invalidate_all();

        Self::empty_result()
    }

    fn execute_drop_snapshot_table(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::DropSnapshotTable { name, if_exists } = stmt else {
            return Err(Error::InternalError(
                "Not a DROP SNAPSHOT TABLE statement".to_string(),
            ));
        };

        let snapshot_name = name.to_string();
        let (dataset_id, snapshot_id) = if snapshot_name.contains('.') {
            let parts: Vec<&str> = snapshot_name.splitn(2, '.').collect();
            (parts[0].to_string(), parts[1].to_string())
        } else {
            ("default".to_string(), snapshot_name.clone())
        };

        let mut storage = self.storage.borrow_mut();

        let Some(dataset) = storage.get_dataset_mut(&dataset_id) else {
            if *if_exists {
                return Self::empty_result();
            }
            return Err(Error::DatasetNotFound(format!(
                "Dataset '{}' not found",
                dataset_id
            )));
        };

        dataset
            .snapshots_mut()
            .drop_snapshot(&snapshot_id, *if_exists)?;

        self.plan_cache.borrow_mut().invalidate_all();

        Self::empty_result()
    }
}
