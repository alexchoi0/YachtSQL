use indexmap::IndexMap;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::Table;
use super::iterator::TableIterator;
use super::statistics::{ColumnStatistics, TableStatistics};
use crate::Column;
use crate::storage_backend::StorageBackend;

impl Table {
    pub fn scan(&self) -> TableIterator<'_> {
        TableIterator {
            table: self,
            current_row: 0,
        }
    }

    pub fn scan_columns(&self, column_names: &[String]) -> Result<TableIterator<'_>> {
        for name in column_names {
            if self.schema.field(name).is_none() {
                return Err(Error::column_not_found(name.clone()));
            }
        }

        Ok(TableIterator {
            table: self,
            current_row: 0,
        })
    }

    pub fn project_columns(&self, column_names: &[String]) -> Result<Table> {
        let projected_schema = self.schema.project(column_names)?;

        match &self.storage {
            StorageBackend::Columnar(storage) => {
                let mut projected_columns = IndexMap::new();

                for name in column_names {
                    let column = storage
                        .columns()
                        .get(name)
                        .ok_or_else(|| Error::column_not_found(name.clone()))?;
                    projected_columns.insert(name.clone(), column.clone());
                }

                Ok(self.clone_with(projected_schema, projected_columns, self.row_count()))
            }
            StorageBackend::Row(storage) => {
                let col_indices: Result<Vec<usize>> = column_names
                    .iter()
                    .map(|name| {
                        self.schema
                            .field_index(name)
                            .ok_or_else(|| Error::column_not_found(name.clone()))
                    })
                    .collect();
                let col_indices = col_indices?;

                let projected_rows = storage.project_columns(&col_indices)?;

                Ok(self.clone_with_rows(projected_schema, projected_rows))
            }
        }
    }

    pub fn slice(&self, start: usize, len: usize) -> Result<Table> {
        if start + len > self.row_count() {
            return Err(Error::InvalidOperation("Slice out of bounds".to_string()));
        }

        match &self.storage {
            StorageBackend::Columnar(storage) => {
                let mut sliced_columns = IndexMap::new();

                for (name, column) in storage.columns() {
                    let sliced_column = column.slice(start, len)?;
                    sliced_columns.insert(name.clone(), sliced_column);
                }

                Ok(self.clone_with(self.schema.clone(), sliced_columns, len))
            }
            StorageBackend::Row(storage) => {
                let rows = storage.slice_rows(start, len);
                Ok(self.clone_with_rows(self.schema.clone(), rows))
            }
        }
    }

    pub fn gather(&self, indices: &[usize]) -> Result<Table> {
        match &self.storage {
            StorageBackend::Columnar(storage) => {
                let mut gathered_columns = IndexMap::new();

                for (name, column) in storage.columns() {
                    let gathered_column = column.gather(indices)?;
                    gathered_columns.insert(name.clone(), gathered_column);
                }

                Ok(self.clone_with(self.schema.clone(), gathered_columns, indices.len()))
            }
            StorageBackend::Row(storage) => {
                let rows = storage.gather_rows(indices)?;
                Ok(self.clone_with_rows(self.schema.clone(), rows))
            }
        }
    }

    pub fn get_statistics(&self) -> TableStatistics {
        let mut column_stats = Vec::new();

        match &self.storage {
            StorageBackend::Columnar(storage) => {
                for (_, column) in storage.columns() {
                    column_stats.push(ColumnStatistics {
                        null_count: column.null_count(),
                        distinct_count: None,
                        min_value: None,
                        max_value: None,
                    });
                }
            }
            StorageBackend::Row(storage) => {
                let stats = storage.compute_column_statistics(&self.schema);
                for (null_count, _) in stats {
                    column_stats.push(ColumnStatistics {
                        null_count,
                        distinct_count: None,
                        min_value: None,
                        max_value: None,
                    });
                }
            }
        }

        TableStatistics {
            row_count: self.row_count(),
            total_bytes: self.estimate_bytes(),
            column_stats,
        }
    }

    pub fn compute_column_statistics(&self, column_name: &str) -> Result<ColumnStatistics> {
        let column = self
            .column(column_name)
            .ok_or_else(|| Error::column_not_found(column_name.to_string()))?;

        let (min_value, max_value) = match column {
            Column::Int64 { .. } => {
                let min = column.min_i64()?.map(Value::int64);
                let max = column.max_i64()?.map(Value::int64);
                (min, max)
            }
            _ => (None, None),
        };

        Ok(ColumnStatistics {
            null_count: column.null_count(),
            distinct_count: None,
            min_value,
            max_value,
        })
    }

    pub fn compute_statistics(&self) -> Result<TableStatistics> {
        let row_count = self.row_count();
        let total_bytes = row_count * std::mem::size_of::<Value>() * self.schema().field_count();

        let mut column_stats = Vec::new();
        for field in self.schema().fields() {
            let stats = self.compute_column_statistics(&field.name)?;
            column_stats.push(stats);
        }

        Ok(TableStatistics {
            row_count,
            total_bytes,
            column_stats,
        })
    }

    fn estimate_bytes(&self) -> usize {
        self.row_count() * self.schema.field_count() * 8
    }
}
