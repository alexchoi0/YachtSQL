use std::collections::HashMap;

use yachtsql_common::types::Value;

#[derive(Debug, Clone, Default)]
pub struct Session {
    variables: HashMap<String, Value>,
    system_variables: HashMap<String, Value>,
    current_schema: Option<String>,
}

impl Session {
    pub fn new() -> Self {
        let mut system_variables = HashMap::new();
        system_variables.insert("@@time_zone".to_string(), Value::String("UTC".to_string()));

        Self {
            variables: HashMap::new(),
            system_variables,
            current_schema: None,
        }
    }

    pub fn get_variable(&self, name: &str) -> Option<&Value> {
        self.variables.get(&name.to_uppercase())
    }

    pub fn set_variable(&mut self, name: &str, value: Value) {
        self.variables.insert(name.to_uppercase(), value);
    }

    pub fn get_system_variable(&self, name: &str) -> Option<&Value> {
        self.system_variables.get(name)
    }

    pub fn set_system_variable(&mut self, name: &str, value: Value) {
        self.system_variables.insert(name.to_string(), value);
    }

    pub fn current_schema(&self) -> Option<&str> {
        self.current_schema.as_deref()
    }

    pub fn set_current_schema(&mut self, schema: Option<String>) {
        self.current_schema = schema;
    }

    pub fn clear_variables(&mut self) {
        self.variables.clear();
    }

    pub fn variables(&self) -> &HashMap<String, Value> {
        &self.variables
    }
}
