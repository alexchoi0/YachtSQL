use std::collections::HashMap;

use indexmap::IndexMap;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum DictionaryLayout {
    #[default]
    Flat,
    Hashed,
    RangeHashed,
    Cache,
    ComplexKeyHashed,
    ComplexKeyCache,
    Direct,
}

#[derive(Debug, Clone, Default)]
pub struct DictionarySource {
    pub source_type: String,
    pub table: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DictionaryLifetime {
    pub min_seconds: u64,
    pub max_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct DictionaryColumn {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub is_hierarchical: bool,
    pub default_value: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct Dictionary {
    pub name: String,
    pub columns: Vec<DictionaryColumn>,
    pub primary_key_columns: Vec<String>,
    pub hierarchical_column: Option<String>,
    pub layout: DictionaryLayout,
    pub source: DictionarySource,
    pub lifetime: DictionaryLifetime,
    data: HashMap<Vec<Value>, IndexMap<String, Value>>,
    parent_index: HashMap<Value, Value>,
    children_index: HashMap<Value, Vec<Value>>,
}

impl Dictionary {
    pub fn new(name: String, columns: Vec<DictionaryColumn>) -> Self {
        let primary_key_columns = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();

        let hierarchical_column = columns
            .iter()
            .find(|c| c.is_hierarchical)
            .map(|c| c.name.clone());

        Self {
            name,
            columns,
            primary_key_columns,
            hierarchical_column,
            layout: DictionaryLayout::default(),
            source: DictionarySource::default(),
            lifetime: DictionaryLifetime::default(),
            data: HashMap::new(),
            parent_index: HashMap::new(),
            children_index: HashMap::new(),
        }
    }

    pub fn with_layout(mut self, layout: DictionaryLayout) -> Self {
        self.layout = layout;
        self
    }

    pub fn with_source(mut self, source: DictionarySource) -> Self {
        self.source = source;
        self
    }

    pub fn with_lifetime(mut self, lifetime: DictionaryLifetime) -> Self {
        self.lifetime = lifetime;
        self
    }

    pub fn insert(&mut self, key: Vec<Value>, values: IndexMap<String, Value>) {
        if let Some(hier_col) = &self.hierarchical_column
            && let Some(pk) = key.first()
            && let Some(parent) = values.get(hier_col)
            && !parent.is_null()
        {
            self.parent_index.insert(pk.clone(), parent.clone());
            self.children_index
                .entry(parent.clone())
                .or_default()
                .push(pk.clone());
        }
        self.data.insert(key, values);
    }

    pub fn get(&self, key: &[Value], column: &str) -> Option<&Value> {
        self.data.get(key).and_then(|row| row.get(column))
    }

    pub fn get_or_default(&self, key: &[Value], column: &str, default: Value) -> Value {
        self.get(key, column).cloned().unwrap_or(default)
    }

    pub fn get_or_null(&self, key: &[Value], column: &str) -> Value {
        self.get(key, column).cloned().unwrap_or(Value::null())
    }

    pub fn has(&self, key: &[Value]) -> bool {
        self.data.contains_key(key)
    }

    pub fn get_hierarchy(&self, key: &Value) -> Vec<Value> {
        let mut result = vec![];
        let mut current = key.clone();
        let mut seen = std::collections::HashSet::new();

        loop {
            if seen.contains(&current) {
                break;
            }

            let lookup_key = vec![current.clone()];
            if !self.data.contains_key(&lookup_key) {
                break;
            }

            result.push(current.clone());
            seen.insert(current.clone());

            match self.parent_index.get(&current) {
                Some(parent) if !parent.is_null() => {
                    current = parent.clone();
                }
                _ => break,
            }
        }

        result
    }

    pub fn is_in(&self, child_key: &Value, ancestor_key: &Value) -> bool {
        let hierarchy = self.get_hierarchy(child_key);
        hierarchy.iter().any(|v| v == ancestor_key)
    }

    pub fn get_children(&self, parent_key: &Value) -> Vec<Value> {
        self.children_index
            .get(parent_key)
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_descendants(&self, parent_key: &Value, max_level: Option<u32>) -> Vec<Value> {
        let mut result = Vec::new();
        let mut current_level = vec![parent_key.clone()];
        let mut level = 0;

        while !current_level.is_empty() {
            level += 1;
            if let Some(max) = max_level
                && level > max
            {
                break;
            }

            let mut next_level = Vec::new();
            for key in &current_level {
                if let Some(children) = self.children_index.get(key) {
                    for child in children {
                        result.push(child.clone());
                        next_level.push(child.clone());
                    }
                }
            }
            current_level = next_level;
        }

        result
    }

    pub fn get_all(&self, key: &[Value], column: &str) -> Value {
        if let Some(val) = self.get(key, column) {
            if let Some(arr) = val.as_array() {
                Value::array(arr.to_vec())
            } else {
                Value::array(vec![val.clone()])
            }
        } else {
            Value::null()
        }
    }

    pub fn get_column_type(&self, column: &str) -> Option<&DataType> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(column))
            .map(|c| &c.data_type)
    }
}

#[derive(Debug, Clone, Default)]
pub struct DictionaryRegistry {
    dictionaries: HashMap<String, Dictionary>,
}

impl DictionaryRegistry {
    pub fn new() -> Self {
        Self {
            dictionaries: HashMap::new(),
        }
    }

    pub fn create(&mut self, dictionary: Dictionary) -> Result<()> {
        let name = dictionary.name.clone();
        self.dictionaries.insert(name, dictionary);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&Dictionary> {
        self.dictionaries
            .get(name)
            .or_else(|| self.dictionaries.get(&name.to_lowercase()))
            .or_else(|| {
                self.dictionaries
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v)
            })
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut Dictionary> {
        if self.dictionaries.contains_key(name) {
            return self.dictionaries.get_mut(name);
        }
        let lower = name.to_lowercase();
        if self.dictionaries.contains_key(&lower) {
            return self.dictionaries.get_mut(&lower);
        }
        let key = self
            .dictionaries
            .keys()
            .find(|k| k.eq_ignore_ascii_case(name))
            .cloned();
        key.and_then(|k| self.dictionaries.get_mut(&k))
    }

    pub fn drop(&mut self, name: &str, if_exists: bool) -> Result<bool> {
        let key = self
            .dictionaries
            .keys()
            .find(|k| k.eq_ignore_ascii_case(name))
            .cloned();

        match key {
            Some(k) => {
                self.dictionaries.remove(&k);
                Ok(true)
            }
            None if if_exists => Ok(false),
            None => Err(Error::invalid_query(format!(
                "Dictionary '{}' does not exist",
                name
            ))),
        }
    }

    pub fn exists(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    pub fn list(&self) -> Vec<String> {
        self.dictionaries.keys().cloned().collect()
    }

    pub fn reload(&mut self, _name: &str) -> Result<()> {
        Ok(())
    }

    pub fn reload_all(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_dictionary() -> Dictionary {
        let columns = vec![
            DictionaryColumn {
                name: "id".to_string(),
                data_type: DataType::Int64,
                is_primary_key: true,
                is_hierarchical: false,
                default_value: None,
            },
            DictionaryColumn {
                name: "name".to_string(),
                data_type: DataType::String,
                is_primary_key: false,
                is_hierarchical: false,
                default_value: None,
            },
            DictionaryColumn {
                name: "value".to_string(),
                data_type: DataType::Float64,
                is_primary_key: false,
                is_hierarchical: false,
                default_value: None,
            },
        ];

        let mut dict = Dictionary::new("test_dict".to_string(), columns);

        let mut row1 = IndexMap::new();
        row1.insert("id".to_string(), Value::int64(1));
        row1.insert("name".to_string(), Value::string("Alice".to_string()));
        row1.insert("value".to_string(), Value::float64(100.0));
        dict.insert(vec![Value::int64(1)], row1);

        let mut row2 = IndexMap::new();
        row2.insert("id".to_string(), Value::int64(2));
        row2.insert("name".to_string(), Value::string("Bob".to_string()));
        row2.insert("value".to_string(), Value::float64(200.0));
        dict.insert(vec![Value::int64(2)], row2);

        dict
    }

    fn create_hierarchical_dictionary() -> Dictionary {
        let columns = vec![
            DictionaryColumn {
                name: "id".to_string(),
                data_type: DataType::Int64,
                is_primary_key: true,
                is_hierarchical: false,
                default_value: None,
            },
            DictionaryColumn {
                name: "parent_id".to_string(),
                data_type: DataType::Int64,
                is_primary_key: false,
                is_hierarchical: true,
                default_value: None,
            },
            DictionaryColumn {
                name: "name".to_string(),
                data_type: DataType::String,
                is_primary_key: false,
                is_hierarchical: false,
                default_value: None,
            },
        ];

        let mut dict = Dictionary::new("hierarchy_dict".to_string(), columns);

        let mut row1 = IndexMap::new();
        row1.insert("id".to_string(), Value::int64(1));
        row1.insert("parent_id".to_string(), Value::null());
        row1.insert("name".to_string(), Value::string("Root".to_string()));
        dict.insert(vec![Value::int64(1)], row1);

        let mut row2 = IndexMap::new();
        row2.insert("id".to_string(), Value::int64(2));
        row2.insert("parent_id".to_string(), Value::int64(1));
        row2.insert("name".to_string(), Value::string("Child1".to_string()));
        dict.insert(vec![Value::int64(2)], row2);

        let mut row3 = IndexMap::new();
        row3.insert("id".to_string(), Value::int64(3));
        row3.insert("parent_id".to_string(), Value::int64(1));
        row3.insert("name".to_string(), Value::string("Child2".to_string()));
        dict.insert(vec![Value::int64(3)], row3);

        let mut row4 = IndexMap::new();
        row4.insert("id".to_string(), Value::int64(4));
        row4.insert("parent_id".to_string(), Value::int64(2));
        row4.insert("name".to_string(), Value::string("Grandchild".to_string()));
        dict.insert(vec![Value::int64(4)], row4);

        dict
    }

    #[test]
    fn test_dict_get() {
        let dict = create_test_dictionary();
        let result = dict.get(&[Value::int64(1)], "name");
        assert_eq!(result, Some(&Value::string("Alice".to_string())));
    }

    #[test]
    fn test_dict_get_not_found() {
        let dict = create_test_dictionary();
        let result = dict.get(&[Value::int64(999)], "name");
        assert_eq!(result, None);
    }

    #[test]
    fn test_dict_get_or_default() {
        let dict = create_test_dictionary();
        let result = dict.get_or_default(
            &[Value::int64(999)],
            "name",
            Value::string("Default".to_string()),
        );
        assert_eq!(result, Value::string("Default".to_string()));
    }

    #[test]
    fn test_dict_get_or_null() {
        let dict = create_test_dictionary();
        let result = dict.get_or_null(&[Value::int64(999)], "name");
        assert!(result.is_null());
    }

    #[test]
    fn test_dict_has() {
        let dict = create_test_dictionary();
        assert!(dict.has(&[Value::int64(1)]));
        assert!(!dict.has(&[Value::int64(999)]));
    }

    #[test]
    fn test_dict_hierarchy() {
        let dict = create_hierarchical_dictionary();
        let hierarchy = dict.get_hierarchy(&Value::int64(4));
        assert_eq!(
            hierarchy,
            vec![Value::int64(4), Value::int64(2), Value::int64(1)]
        );
    }

    #[test]
    fn test_dict_is_in() {
        let dict = create_hierarchical_dictionary();
        assert!(dict.is_in(&Value::int64(4), &Value::int64(1)));
        assert!(dict.is_in(&Value::int64(4), &Value::int64(2)));
        assert!(!dict.is_in(&Value::int64(4), &Value::int64(3)));
    }

    #[test]
    fn test_dict_get_children() {
        let dict = create_hierarchical_dictionary();
        let children = dict.get_children(&Value::int64(1));
        assert_eq!(children.len(), 2);
        assert!(children.contains(&Value::int64(2)));
        assert!(children.contains(&Value::int64(3)));
    }

    #[test]
    fn test_dict_get_descendants() {
        let dict = create_hierarchical_dictionary();
        let descendants = dict.get_descendants(&Value::int64(1), None);
        assert_eq!(descendants.len(), 3);
        assert!(descendants.contains(&Value::int64(2)));
        assert!(descendants.contains(&Value::int64(3)));
        assert!(descendants.contains(&Value::int64(4)));
    }

    #[test]
    fn test_registry_create_and_get() {
        let mut registry = DictionaryRegistry::new();
        let dict = create_test_dictionary();
        registry.create(dict).unwrap();

        assert!(registry.exists("test_dict"));
        assert!(registry.get("test_dict").is_some());
    }

    #[test]
    fn test_registry_drop() {
        let mut registry = DictionaryRegistry::new();
        let dict = create_test_dictionary();
        registry.create(dict).unwrap();

        assert!(registry.drop("test_dict", false).unwrap());
        assert!(!registry.exists("test_dict"));
    }
}
