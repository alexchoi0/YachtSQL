use yachtsql_core::error::{Error, Result};
use yachtsql_parser::validator::CustomStatement;

use super::super::QueryExecutor;
use super::create::DdlExecutor;
use crate::Table;

pub trait MaterializedViewExecutor {
    fn execute_refresh_materialized_view(&mut self, stmt: &CustomStatement) -> Result<Table>;

    fn execute_drop_materialized_view(&mut self, stmt: &CustomStatement) -> Result<Table>;
}

impl MaterializedViewExecutor for QueryExecutor {
    fn execute_refresh_materialized_view(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::RefreshMaterializedView { name, concurrently } = stmt else {
            return Err(Error::InternalError(
                "Not a REFRESH MATERIALIZED VIEW statement".to_string(),
            ));
        };

        let view_name = name.to_string();
        let (dataset_id, view_id) = self.parse_ddl_table_name(&view_name)?;

        let _ = concurrently;

        let view_sql = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;

            let view = dataset.views().get_view(&view_id).ok_or_else(|| {
                Error::invalid_query(format!("Materialized view '{}' does not exist", view_id))
            })?;

            if !view.is_materialized() {
                return Err(Error::invalid_query(format!(
                    "'{}' is not a materialized view",
                    view_id
                )));
            }

            view.sql.clone()
        };

        let result = self.execute_sql(&view_sql)?;
        let result_schema = result.schema().clone();
        let result_rows: Vec<yachtsql_storage::Row> =
            result.rows().map(|rows| rows.to_vec()).unwrap_or_default();

        let mut storage = self.storage.borrow_mut();
        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        let view = dataset.views_mut().get_view_mut(&view_id).ok_or_else(|| {
            Error::invalid_query(format!("Materialized view '{}' does not exist", view_id))
        })?;

        view.refresh_materialized_data(result_rows, result_schema);

        Self::empty_result()
    }

    fn execute_drop_materialized_view(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::DropMaterializedView {
            name,
            if_exists,
            cascade,
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a DROP MATERIALIZED VIEW statement".to_string(),
            ));
        };

        let view_name = name.to_string();
        let (dataset_id, view_id) = self.parse_ddl_table_name(&view_name)?;

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

        let view_registry = dataset.views_mut();

        if let Some(view) = view_registry.get_view(&view_id) {
            if !view.is_materialized() {
                return Err(Error::invalid_query(format!(
                    "'{}' is not a materialized view",
                    view_id
                )));
            }
        } else {
            if *if_exists {
                return Self::empty_result();
            }
            return Err(Error::invalid_query(format!(
                "Materialized view '{}' does not exist",
                view_id
            )));
        }

        let _ = cascade;
        view_registry.drop_view(&view_id, *if_exists)?;

        Self::empty_result()
    }
}
