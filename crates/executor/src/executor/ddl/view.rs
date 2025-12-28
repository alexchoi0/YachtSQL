use yachtsql_common::error::Result;
use yachtsql_storage::{Schema, Table};

use super::super::PlanExecutor;

impl<'a> PlanExecutor<'a> {
    pub fn execute_create_view(
        &mut self,
        name: &str,
        query_sql: &str,
        column_aliases: &[String],
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Table> {
        self.catalog.create_view(
            name,
            query_sql.to_string(),
            column_aliases.to_vec(),
            or_replace,
            if_not_exists,
        )?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_view(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        self.catalog.drop_view(name, if_exists)?;
        Ok(Table::empty(Schema::new()))
    }
}
