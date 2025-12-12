use sqlparser::ast::Statement as SqlStatement;
use yachtsql_common::error::{Error, Result};

use super::super::QueryExecutor;
use super::super::ddl::DdlExecutor;

pub trait DmlTruncateExecutor {
    fn execute_truncate(&mut self, stmt: &SqlStatement) -> Result<usize>;
}

impl DmlTruncateExecutor for QueryExecutor {
    fn execute_truncate(&mut self, stmt: &SqlStatement) -> Result<usize> {
        let SqlStatement::Truncate {
            table_names,
            partitions,
            table: _,
            identity,
            cascade: _,
            ..
        } = stmt
        else {
            return Err(Error::InternalError("Not a TRUNCATE statement".to_string()));
        };

        if partitions.is_some() {
            return Err(Error::unsupported_feature(
                "TRUNCATE with PARTITIONS is not yet supported".to_string(),
            ));
        }

        let mut total_rows_deleted = 0;

        let mut storage = self.storage.borrow_mut();

        for truncate_table in table_names {
            let table_name = truncate_table.name.to_string();

            let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name)?;

            let dataset = storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;

            let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
                Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
            })?;

            let rows_before = table.row_count();

            table.clear_rows()?;

            if let Some(sqlparser::ast::TruncateIdentityOption::Restart) = identity {
                table.reset_auto_increment(1)?;
            }

            total_rows_deleted += rows_before;
        }

        drop(storage);

        self.plan_cache.borrow_mut().invalidate_all();

        Ok(total_rows_deleted)
    }
}
