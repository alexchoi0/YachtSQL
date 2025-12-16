//! Record type for row-like access during query execution.

use yachtsql_core::types::Value;
use yachtsql_storage::Schema;

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Record {
    values: Vec<Value>,
}

impl Record {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }

    pub fn from_values(values: Vec<Value>) -> Self {
        Self { values }
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

    pub fn push(&mut self, value: Value) {
        self.values.push(value);
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }

    pub fn get_by_name<'a>(&'a self, schema: &'a Schema, column: &str) -> Option<&'a Value> {
        schema
            .field_index(column)
            .and_then(|idx| self.values.get(idx))
    }

    pub fn remove(&mut self, index: usize) {
        if index < self.values.len() {
            self.values.remove(index);
        }
    }
}

impl std::ops::Index<usize> for Record {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl std::ops::IndexMut<usize> for Record {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.values[index]
    }
}
