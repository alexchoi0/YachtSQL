use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::Table;
use crate::index::IndexMetadata;
use crate::indexes::{BPlusTreeIndex, HashIndex, IndexKey, TableIndex, extract_index_key};

pub trait TableIndexOps {
    fn index_metadata(&self) -> &[IndexMetadata];

    fn index_metadata_mut(&mut self) -> &mut Vec<IndexMetadata>;

    fn add_index(&mut self, metadata: IndexMetadata) -> Result<()>;

    fn drop_index(&mut self, index_name: &str) -> Result<()>;

    fn get_index(&self, index_name: &str) -> Option<&dyn TableIndex>;

    fn indexes(&self) -> &HashMap<String, Box<dyn TableIndex>>;

    fn index_lookup(&self, index_name: &str, key: &IndexKey) -> Vec<usize>;

    fn update_indexes_on_insert(&mut self, row_values: &[Value], row_index: usize) -> Result<()>;

    fn update_indexes_on_update(
        &mut self,
        old_row_values: &[Value],
        new_row_values: &[Value],
        row_index: usize,
    ) -> Result<()>;

    fn update_indexes_on_delete(&mut self, row_values: &[Value], row_index: usize) -> Result<()>;

    fn rebuild_all_indexes(&mut self) -> Result<()>;
}

impl TableIndexOps for Table {
    fn index_metadata(&self) -> &[IndexMetadata] {
        &self.index_metadata
    }

    fn index_metadata_mut(&mut self) -> &mut Vec<IndexMetadata> {
        &mut self.index_metadata
    }

    fn add_index(&mut self, metadata: IndexMetadata) -> Result<()> {
        use crate::index::IndexType;

        metadata.validate(self)?;

        if self.indexes.contains_key(&metadata.index_name) {
            return Err(Error::InvalidOperation(format!(
                "Index '{}' already exists",
                metadata.index_name
            )));
        }

        let column_indices: Vec<usize> = metadata
            .columns
            .iter()
            .filter_map(|col| {
                col.column_name
                    .as_ref()
                    .and_then(|name| self.schema.field_index(name))
            })
            .collect();

        let has_expressions = metadata.columns.iter().any(|col| col.expression.is_some());

        if column_indices.is_empty() && !has_expressions {
            return Err(Error::InvalidOperation(
                "Index must reference at least one column or expression".to_string(),
            ));
        }

        if has_expressions || metadata.where_clause.is_some() {
            self.index_metadata.push(metadata);
            return Ok(());
        }

        let mut index: Box<dyn TableIndex> = match metadata.index_type {
            IndexType::Hash => Box::new(HashIndex::new(column_indices.clone(), metadata.is_unique)),
            IndexType::BTree => Box::new(BPlusTreeIndex::new(
                column_indices.clone(),
                metadata.is_unique,
            )),
            _ => Box::new(BPlusTreeIndex::new(
                column_indices.clone(),
                metadata.is_unique,
            )),
        };

        build_index_from_existing_data(self, &mut *index, &column_indices)?;

        self.indexes.insert(metadata.index_name.clone(), index);
        self.index_metadata.push(metadata);

        Ok(())
    }

    fn drop_index(&mut self, index_name: &str) -> Result<()> {
        if self.indexes.remove(index_name).is_none() {
            return Err(Error::InvalidOperation(format!(
                "Index '{}' does not exist",
                index_name
            )));
        }

        self.index_metadata
            .retain(|meta| meta.index_name != index_name);

        Ok(())
    }

    fn get_index(&self, index_name: &str) -> Option<&dyn TableIndex> {
        self.indexes.get(index_name).map(|b| &**b)
    }

    fn indexes(&self) -> &HashMap<String, Box<dyn TableIndex>> {
        &self.indexes
    }

    fn index_lookup(&self, index_name: &str, key: &IndexKey) -> Vec<usize> {
        self.indexes
            .get(index_name)
            .map(|index| index.lookup_exact(key))
            .unwrap_or_default()
    }

    fn update_indexes_on_insert(&mut self, row_values: &[Value], row_index: usize) -> Result<()> {
        let index_info: Vec<(String, Vec<usize>)> = self
            .index_metadata
            .iter()
            .filter_map(|meta| {
                let column_indices: Vec<usize> = meta
                    .columns
                    .iter()
                    .filter_map(|col| {
                        col.column_name
                            .as_ref()
                            .and_then(|name| self.schema.field_index(name))
                    })
                    .collect();

                if column_indices.is_empty() {
                    None
                } else {
                    Some((meta.index_name.clone(), column_indices))
                }
            })
            .collect();

        for (index_name, column_indices) in index_info {
            if let Some(index) = self.indexes.get_mut(&index_name) {
                let key = extract_index_key(row_values, &column_indices);
                index.insert(key, row_index)?;
            }
        }

        Ok(())
    }

    fn update_indexes_on_update(
        &mut self,
        old_row_values: &[Value],
        new_row_values: &[Value],
        row_index: usize,
    ) -> Result<()> {
        let index_info: Vec<(String, Vec<usize>)> = self
            .index_metadata
            .iter()
            .filter_map(|meta| {
                let column_indices: Vec<usize> = meta
                    .columns
                    .iter()
                    .filter_map(|col| {
                        col.column_name
                            .as_ref()
                            .and_then(|name| self.schema.field_index(name))
                    })
                    .collect();

                if column_indices.is_empty() {
                    None
                } else {
                    Some((meta.index_name.clone(), column_indices))
                }
            })
            .collect();

        for (index_name, column_indices) in index_info {
            if let Some(index) = self.indexes.get_mut(&index_name) {
                let old_key = extract_index_key(old_row_values, &column_indices);
                let new_key = extract_index_key(new_row_values, &column_indices);

                if old_key != new_key {
                    index.update(&old_key, new_key, row_index)?;
                }
            }
        }

        Ok(())
    }

    fn update_indexes_on_delete(&mut self, row_values: &[Value], row_index: usize) -> Result<()> {
        let index_info: Vec<(String, Vec<usize>)> = self
            .index_metadata
            .iter()
            .filter_map(|meta| {
                let column_indices: Vec<usize> = meta
                    .columns
                    .iter()
                    .filter_map(|col| {
                        col.column_name
                            .as_ref()
                            .and_then(|name| self.schema.field_index(name))
                    })
                    .collect();

                if column_indices.is_empty() {
                    None
                } else {
                    Some((meta.index_name.clone(), column_indices))
                }
            })
            .collect();

        for (index_name, column_indices) in index_info {
            if let Some(index) = self.indexes.get_mut(&index_name) {
                let key = extract_index_key(row_values, &column_indices);
                index.delete(&key, row_index)?;
            }
        }

        Ok(())
    }

    fn rebuild_all_indexes(&mut self) -> Result<()> {
        let index_info: Vec<(String, Vec<usize>)> = self
            .index_metadata
            .iter()
            .filter_map(|meta| {
                let column_indices: Vec<usize> = meta
                    .columns
                    .iter()
                    .filter_map(|col| {
                        col.column_name
                            .as_ref()
                            .and_then(|name| self.schema.field_index(name))
                    })
                    .collect();

                if column_indices.is_empty() {
                    None
                } else {
                    Some((meta.index_name.clone(), column_indices))
                }
            })
            .collect();

        for (index_name, _) in &index_info {
            if let Some(index) = self.indexes.get_mut(index_name) {
                index.clear();
            }
        }

        let row_count = self.row_count();
        let mut all_rows: Vec<Vec<Value>> = Vec::with_capacity(row_count);
        for row_idx in 0..row_count {
            let row = self.get_row(row_idx)?;
            all_rows.push(row.values().to_vec());
        }

        for (index_name, column_indices) in index_info {
            if let Some(index) = self.indexes.get_mut(&index_name) {
                for (row_idx, row_values) in all_rows.iter().enumerate() {
                    let key = extract_index_key(row_values, &column_indices);
                    index.insert(key, row_idx)?;
                }
            }
        }

        Ok(())
    }
}

fn build_index_from_existing_data(
    table: &Table,
    index: &mut dyn TableIndex,
    column_indices: &[usize],
) -> Result<()> {
    for row_idx in 0..table.row_count() {
        let row = table.get_row(row_idx)?;
        let row_values: Vec<Value> = row.values().to_vec();
        let key = extract_index_key(&row_values, column_indices);
        index.insert(key, row_idx)?;
    }

    Ok(())
}
