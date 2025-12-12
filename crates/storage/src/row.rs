use std::ops::{Index, IndexMut};

use indexmap::IndexMap;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use crate::Schema;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    values: Vec<Value>,
    initialized: Vec<bool>,
    labels: Option<Vec<String>>,
}

impl Row {
    pub fn empty() -> Self {
        Self {
            values: Vec::new(),
            initialized: Vec::new(),
            labels: None,
        }
    }

    pub fn for_schema(schema: &Schema) -> Self {
        Self::with_len(schema.field_count())
    }
    pub fn with_len(len: usize) -> Self {
        Self {
            values: vec![Value::null(); len],
            initialized: vec![false; len],
            labels: None,
        }
    }

    pub fn from_values(values: Vec<Value>) -> Self {
        let len = values.len();
        Self {
            values,
            initialized: vec![true; len],
            labels: None,
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    pub fn align_with_schema(&mut self, schema: &Schema) {
        let len = schema.field_count();
        if self.values.len() != len {
            self.values.resize(len, Value::null());
        }
        if self.initialized.len() != len {
            self.initialized.resize(len, false);
        }
        self.labels = None;
    }

    pub fn is_initialized(&self, index: usize) -> bool {
        self.initialized.get(index).copied().unwrap_or(false)
    }

    pub fn initialized_column_names<'a>(
        &'a self,
        schema: &'a Schema,
    ) -> impl Iterator<Item = &'a str> + 'a {
        self.initialized
            .iter()
            .enumerate()
            .filter_map(move |(idx, is_set)| {
                if *is_set {
                    schema.fields().get(idx).map(|field| field.name.as_str())
                } else {
                    None
                }
            })
    }

    pub fn set(&mut self, index: usize, value: Value) -> Result<()> {
        if index >= self.values.len() {
            return Err(Error::InvalidOperation(format!(
                "Column index {} out of bounds (len: {})",
                index,
                self.values.len()
            )));
        }
        self.values[index] = value;
        if index >= self.initialized.len() {
            self.initialized.resize(self.values.len(), false);
        }
        self.initialized[index] = true;
        Ok(())
    }

    pub fn set_by_name(&mut self, schema: &Schema, column: &str, value: Value) -> Result<()> {
        let Some(index) = schema.field_index(column) else {
            return Err(Error::InvalidOperation(format!(
                "Column '{}' does not exist in schema",
                column
            )));
        };
        if self.values.len() != schema.field_count() {
            return Err(Error::InvalidOperation(format!(
                "Row length {} does not match schema column count {}",
                self.values.len(),
                schema.field_count()
            )));
        }
        self.set(index, value)
    }

    pub fn get<'a, K>(&'a self, key: K) -> Option<&'a Value>
    where
        K: RowKey<'a>,
    {
        key.lookup(self)
    }

    pub fn get_mut<'a, K>(&'a mut self, key: K) -> Option<&'a mut Value>
    where
        K: RowKeyMut<'a>,
    {
        key.lookup_mut(self)
    }

    pub fn get_by_name<'a>(&'a self, schema: &'a Schema, column: &str) -> Option<&'a Value> {
        schema
            .field_index(column)
            .and_then(|idx| self.values.get(idx))
    }

    pub fn get_mut_by_name<'a>(
        &'a mut self,
        schema: &'a Schema,
        column: &str,
    ) -> Option<&'a mut Value> {
        schema
            .field_index(column)
            .and_then(|idx| self.values.get_mut(idx))
    }

    pub fn contains_column(&self, schema: &Schema, column: &str) -> bool {
        schema
            .field_index(column)
            .map(|idx| self.is_initialized(idx))
            .unwrap_or(false)
    }

    pub fn mark_all_initialized(&mut self) {
        self.initialized.resize(self.values.len(), true);
        self.initialized.fill(true);
    }

    pub fn get_named(&self, column: &str) -> Option<&Value> {
        let idx = self
            .labels
            .as_ref()
            .and_then(|labels| labels.iter().position(|label| label == column))?;
        self.values.get(idx)
    }

    pub fn from_named_values(values: IndexMap<String, Value>) -> Self {
        let labels: Vec<String> = values.keys().cloned().collect();
        let vec_values: Vec<Value> = values.into_values().collect();
        let len = vec_values.len();
        Self {
            values: vec_values,
            initialized: vec![true; len],
            labels: Some(labels),
        }
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::empty()
    }
}

impl Index<usize> for Row {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index)
            .unwrap_or_else(|| panic!("Row index {} out of bounds", index))
    }
}

impl IndexMut<usize> for Row {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index)
            .unwrap_or_else(|| panic!("Row index {} out of bounds", index))
    }
}

impl From<IndexMap<String, Value>> for Row {
    fn from(values: IndexMap<String, Value>) -> Self {
        Row::from_named_values(values)
    }
}

impl Index<&str> for Row {
    type Output = Value;

    fn index(&self, index: &str) -> &Self::Output {
        self.get(index)
            .unwrap_or_else(|| panic!("Row column '{}' not found", index))
    }
}

impl IndexMut<&str> for Row {
    fn index_mut(&mut self, index: &str) -> &mut Self::Output {
        self.get_mut(index)
            .unwrap_or_else(|| panic!("Row column '{}' not found", index))
    }
}

pub trait RowKey<'a> {
    fn lookup(self, row: &'a Row) -> Option<&'a Value>;
}

pub trait RowKeyMut<'a> {
    fn lookup_mut(self, row: &'a mut Row) -> Option<&'a mut Value>;
}

impl<'a> RowKey<'a> for usize {
    fn lookup(self, row: &'a Row) -> Option<&'a Value> {
        row.values.get(self)
    }
}

impl<'a> RowKeyMut<'a> for usize {
    fn lookup_mut(self, row: &'a mut Row) -> Option<&'a mut Value> {
        row.values.get_mut(self)
    }
}

impl<'a> RowKey<'a> for &str {
    fn lookup(self, row: &'a Row) -> Option<&'a Value> {
        let idx = row
            .labels
            .as_ref()
            .and_then(|labels| labels.iter().position(|label| label == self))?;
        row.values.get(idx)
    }
}

impl<'a> RowKeyMut<'a> for &str {
    fn lookup_mut(self, row: &'a mut Row) -> Option<&'a mut Value> {
        let idx = row
            .labels
            .as_ref()
            .and_then(|labels| labels.iter().position(|label| label == self))?;
        row.values.get_mut(idx)
    }
}
