use std::sync::RwLock;

use dashmap::DashMap;
use yachtsql_common::types::Value;

#[derive(Debug)]
pub struct ConcurrentSession {
    variables: DashMap<String, Value>,
    system_variables: DashMap<String, Value>,
    current_schema: RwLock<Option<String>>,
}

impl ConcurrentSession {
    pub fn new() -> Self {
        let system_variables = DashMap::new();
        system_variables.insert("@@time_zone".to_string(), Value::String("UTC".to_string()));

        Self {
            variables: DashMap::new(),
            system_variables,
            current_schema: RwLock::new(None),
        }
    }

    pub fn get_variable(&self, name: &str) -> Option<Value> {
        self.variables.get(&name.to_uppercase()).map(|v| v.clone())
    }

    pub fn set_variable(&self, name: &str, value: Value) {
        self.variables.insert(name.to_uppercase(), value);
    }

    pub fn get_system_variable(&self, name: &str) -> Option<Value> {
        self.system_variables.get(name).map(|v| v.clone())
    }

    pub fn set_system_variable(&self, name: &str, value: Value) {
        self.system_variables.insert(name.to_string(), value);
    }

    pub fn current_schema(&self) -> Option<String> {
        self.current_schema.read().unwrap().clone()
    }

    pub fn set_current_schema(&self, schema: Option<String>) {
        *self.current_schema.write().unwrap() = schema;
    }

    pub fn clear_variables(&self) {
        self.variables.clear();
    }

    pub fn variables(&self) -> &DashMap<String, Value> {
        &self.variables
    }
}

impl Default for ConcurrentSession {
    fn default() -> Self {
        Self::new()
    }
}
