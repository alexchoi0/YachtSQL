use debug_print::debug_eprintln;
use yachtsql_core::error::{Error, Result};
use yachtsql_storage::TableIndexOps;

use super::super::QueryExecutor;
use super::create::DdlExecutor;

pub trait DdlDropExecutor {
    fn execute_drop_table(&mut self, stmt: &sqlparser::ast::Statement) -> Result<()>;

    fn execute_drop_view(&mut self, stmt: &sqlparser::ast::Statement) -> Result<()>;

    fn execute_drop_index(&mut self, stmt: &sqlparser::ast::Statement) -> Result<()>;
}

impl DdlDropExecutor for QueryExecutor {
    fn execute_drop_table(&mut self, stmt: &sqlparser::ast::Statement) -> Result<()> {
        use sqlparser::ast::Statement;

        let Statement::Drop {
            names, if_exists, ..
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a DROP TABLE statement".to_string(),
            ));
        };

        for table_name_obj in names {
            let table_name = table_name_obj.to_string();
            let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name)?;
            let table_full_name = format!("{}.{}", dataset_id, table_id);

            let mut tm = self.transaction_manager.borrow_mut();
            if let Some(_txn) = tm.get_active_transaction_mut() {
                drop(tm);

                {
                    let storage = self.storage.borrow_mut();

                    let dataset = match storage.get_dataset(&dataset_id) {
                        Some(ds) => ds,
                        None => {
                            if *if_exists {
                                continue;
                            } else {
                                return Err(Error::DatasetNotFound(format!(
                                    "Dataset '{}' not found",
                                    dataset_id
                                )));
                            }
                        }
                    };

                    if dataset.get_table(&table_id).is_none() {
                        if *if_exists {
                            continue;
                        } else {
                            return Err(Error::TableNotFound(format!(
                                "Table '{}.{}' does not exist",
                                dataset_id, table_id
                            )));
                        }
                    }
                }

                let mut tm = self.transaction_manager.borrow_mut();
                if let Some(txn) = tm.get_active_transaction_mut() {
                    txn.track_drop_table(table_full_name);
                }
            } else {
                drop(tm);

                let mut storage = self.storage.borrow_mut();

                let dataset = match storage.get_dataset_mut(&dataset_id) {
                    Some(ds) => ds,
                    None => {
                        if *if_exists {
                            continue;
                        } else {
                            return Err(Error::DatasetNotFound(format!(
                                "Dataset '{}' not found",
                                dataset_id
                            )));
                        }
                    }
                };

                if dataset.get_table(&table_id).is_none() {
                    if *if_exists {
                        continue;
                    } else {
                        return Err(Error::TableNotFound(format!(
                            "Table '{}.{}' does not exist",
                            dataset_id, table_id
                        )));
                    }
                }

                dataset.sequences_mut().drop_owned_by_table(&table_id);

                dataset.delete_table(&table_id)?;
            }
        }

        self.plan_cache.borrow_mut().invalidate_all();

        Ok(())
    }

    fn execute_drop_view(&mut self, stmt: &sqlparser::ast::Statement) -> Result<()> {
        use sqlparser::ast::Statement;

        let Statement::Drop {
            names,
            if_exists,
            cascade,
            restrict,
            ..
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a DROP VIEW statement".to_string(),
            ));
        };

        for view_name_obj in names {
            let view_name = view_name_obj.to_string();
            let (dataset_id, view_id) = self.parse_ddl_table_name(&view_name)?;

            let mut storage = self.storage.borrow_mut();

            let dataset = match storage.get_dataset_mut(&dataset_id) {
                Some(ds) => ds,
                None => {
                    if *if_exists {
                        continue;
                    } else {
                        return Err(Error::DatasetNotFound(format!(
                            "Dataset '{}' not found",
                            dataset_id
                        )));
                    }
                }
            };

            if !dataset.views().exists(&view_id) {
                if *if_exists {
                    continue;
                } else {
                    return Err(Error::InvalidQuery(format!(
                        "View '{}.{}' does not exist",
                        dataset_id, view_id
                    )));
                }
            }

            if *cascade {
                let dropped_views = dataset
                    .views_mut()
                    .drop_view_cascade(&view_id, *if_exists)
                    .map_err(|e| Error::InvalidQuery(e.to_string()))?;

                debug_eprintln!(
                    "[executor::ddl::drop] Dropped view '{}' and {} dependent view(s): {:?}",
                    view_id,
                    dropped_views.len() - 1,
                    dropped_views
                );
            } else if *restrict {
                dataset
                    .views_mut()
                    .drop_view_restrict(&view_id, *if_exists)
                    .map_err(|e| Error::InvalidQuery(e.to_string()))?;
            } else {
                dataset
                    .views_mut()
                    .drop_view_restrict(&view_id, *if_exists)
                    .map_err(|e| Error::InvalidQuery(e.to_string()))?;
            }
        }

        self.plan_cache.borrow_mut().invalidate_all();

        Ok(())
    }

    fn execute_drop_index(&mut self, stmt: &sqlparser::ast::Statement) -> Result<()> {
        use sqlparser::ast::Statement;

        let Statement::Drop {
            names, if_exists, ..
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a DROP INDEX statement".to_string(),
            ));
        };

        for index_name_obj in names {
            let index_name = index_name_obj.to_string();

            let index_id = if let Some(dot_pos) = index_name.rfind('.') {
                &index_name[dot_pos + 1..]
            } else {
                &index_name
            };

            let mut storage = self.storage.borrow_mut();

            let dataset_id = "default";

            let dataset = match storage.get_dataset_mut(dataset_id) {
                Some(ds) => ds,
                None => {
                    if *if_exists {
                        continue;
                    } else {
                        return Err(Error::DatasetNotFound(format!(
                            "Dataset '{}' not found",
                            dataset_id
                        )));
                    }
                }
            };

            if !dataset.has_index(index_id) {
                if *if_exists {
                    continue;
                } else {
                    return Err(Error::InvalidQuery(format!(
                        "Index '{}' does not exist",
                        index_id
                    )));
                }
            }

            let index_metadata = dataset.get_index(index_id).ok_or_else(|| {
                Error::InvalidQuery(format!("Index '{}' metadata not found", index_id))
            })?;
            let table_name = index_metadata.table_name.clone();

            if let Some(table) = dataset.get_table_mut(&table_name) {
                table.drop_index(index_id)?;
            }

            dataset.drop_index(index_id)?;
        }

        self.plan_cache.borrow_mut().invalidate_all();

        Ok(())
    }
}
