use yachtsql_common::error::{Error, Result};
use yachtsql_storage::{Schema, Table};

use super::super::PlanExecutor;

impl<'a> PlanExecutor<'a> {
    pub fn execute_create_snapshot(
        &mut self,
        snapshot_name: &str,
        source_name: &str,
        if_not_exists: bool,
    ) -> Result<Table> {
        if if_not_exists && self.catalog.get_table(snapshot_name).is_some() {
            return Ok(Table::empty(Schema::new()));
        }

        let source_table = self
            .catalog
            .get_table(source_name)
            .ok_or_else(|| Error::TableNotFound(source_name.to_string()))?;

        let snapshot = source_table.clone();
        self.catalog.insert_table(snapshot_name, snapshot)?;

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_snapshot(&mut self, snapshot_name: &str, if_exists: bool) -> Result<Table> {
        if if_exists && self.catalog.get_table(snapshot_name).is_none() {
            return Ok(Table::empty(Schema::new()));
        }

        self.catalog
            .drop_table(snapshot_name)
            .map_err(|_| Error::TableNotFound(snapshot_name.to_string()))?;

        Ok(Table::empty(Schema::new()))
    }
}
