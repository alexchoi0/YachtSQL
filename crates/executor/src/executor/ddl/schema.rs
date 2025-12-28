use yachtsql_common::error::Result;
use yachtsql_storage::{Schema, Table};

use super::super::PlanExecutor;

impl<'a> PlanExecutor<'a> {
    pub fn execute_create_schema(
        &mut self,
        name: &str,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<Table> {
        if or_replace && self.catalog.schema_exists(name) {
            self.catalog.drop_schema(name, true, true)?;
        }
        self.catalog.create_schema(name, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_schema(
        &mut self,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<Table> {
        self.catalog.drop_schema(name, if_exists, cascade)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_undrop_schema(&mut self, name: &str, if_not_exists: bool) -> Result<Table> {
        self.catalog.undrop_schema(name, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_alter_schema(
        &mut self,
        name: &str,
        options: &[(String, String)],
    ) -> Result<Table> {
        let option_map: std::collections::HashMap<String, String> =
            options.iter().cloned().collect();
        self.catalog.alter_schema_options(name, option_map)?;
        Ok(Table::empty(Schema::new()))
    }
}
