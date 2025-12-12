use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EnumType {
    pub name: String,

    pub schema: Option<String>,

    pub labels: Vec<String>,

    #[serde(skip)]
    label_to_ordinal: HashMap<String, u32>,
}

impl EnumType {
    pub fn new(name: String, schema: Option<String>, labels: Vec<String>) -> Result<Self> {
        if labels.is_empty() {
            return Err(Error::invalid_query(
                "ENUM type must have at least one label".to_string(),
            ));
        }

        let mut seen = HashMap::new();
        for (idx, label) in labels.iter().enumerate() {
            if let Some(prev_idx) = seen.insert(label.clone(), idx) {
                return Err(Error::invalid_query(format!(
                    "Duplicate label '{}' in ENUM type '{}' at positions {} and {}",
                    label, name, prev_idx, idx
                )));
            }
        }

        let label_to_ordinal: HashMap<String, u32> = labels
            .iter()
            .enumerate()
            .map(|(i, label)| (label.clone(), i as u32))
            .collect();

        Ok(Self {
            name,
            schema,
            labels,
            label_to_ordinal,
        })
    }

    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(s) => format!("{}.{}", s, self.name),
            None => self.name.clone(),
        }
    }

    pub fn is_valid_label(&self, label: &str) -> bool {
        self.label_to_ordinal.contains_key(label)
    }

    pub fn ordinal(&self, label: &str) -> Option<u32> {
        self.label_to_ordinal.get(label).copied()
    }

    pub fn label_at(&self, ordinal: u32) -> Option<&str> {
        self.labels.get(ordinal as usize).map(|s| s.as_str())
    }

    pub fn len(&self) -> usize {
        self.labels.len()
    }

    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    pub fn rebuild_lookup(&mut self) {
        self.label_to_ordinal = self
            .labels
            .iter()
            .enumerate()
            .map(|(i, label)| (label.clone(), i as u32))
            .collect();
    }
}

#[derive(Debug, Default, Clone)]
pub struct TypeRegistry {
    enums: HashMap<String, EnumType>,

    type_dependencies: HashMap<String, Vec<String>>,
}

impl TypeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_enum(&mut self, enum_type: EnumType, if_not_exists: bool) -> Result<()> {
        let name = enum_type.name.clone();

        if self.enums.contains_key(&name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(Error::invalid_query(format!(
                "Type '{}' already exists",
                name
            )));
        }

        self.enums.insert(name, enum_type);
        Ok(())
    }

    pub fn get_enum(&self, name: &str) -> Option<&EnumType> {
        self.enums.get(name)
    }

    pub fn get_enum_mut(&mut self, name: &str) -> Option<&mut EnumType> {
        self.enums.get_mut(name)
    }

    pub fn type_exists(&self, name: &str) -> bool {
        self.enums.contains_key(name)
    }

    pub fn register_dependency(&mut self, type_name: &str, table_name: &str) {
        self.type_dependencies
            .entry(type_name.to_string())
            .or_default()
            .push(table_name.to_string());
    }

    pub fn unregister_dependency(&mut self, type_name: &str, table_name: &str) {
        if let Some(deps) = self.type_dependencies.get_mut(type_name) {
            deps.retain(|t| t != table_name);
            if deps.is_empty() {
                self.type_dependencies.remove(type_name);
            }
        }
    }

    pub fn get_dependencies(&self, type_name: &str) -> Vec<String> {
        self.type_dependencies
            .get(type_name)
            .cloned()
            .unwrap_or_default()
    }

    pub fn drop_type(&mut self, name: &str, if_exists: bool) -> Result<()> {
        if !self.enums.contains_key(name) {
            if if_exists {
                return Ok(());
            }
            return Err(Error::invalid_query(format!(
                "Type '{}' does not exist",
                name
            )));
        }

        let deps = self.get_dependencies(name);
        if !deps.is_empty() {
            return Err(Error::invalid_query(format!(
                "Cannot drop type '{}' because other objects depend on it: {}. Use DROP TYPE ... CASCADE to drop dependent objects.",
                name,
                deps.join(", ")
            )));
        }

        self.enums.remove(name);
        Ok(())
    }

    pub fn drop_type_cascade(&mut self, name: &str, if_exists: bool) -> Result<Vec<String>> {
        if !self.enums.contains_key(name) {
            if if_exists {
                return Ok(vec![]);
            }
            return Err(Error::invalid_query(format!(
                "Type '{}' does not exist",
                name
            )));
        }

        let deps = self.get_dependencies(name);

        self.enums.remove(name);
        self.type_dependencies.remove(name);

        Ok(deps)
    }

    pub fn list_enums(&self) -> Vec<&str> {
        self.enums.keys().map(|s| s.as_str()).collect()
    }

    pub fn len(&self) -> usize {
        self.enums.len()
    }

    pub fn is_empty(&self) -> bool {
        self.enums.is_empty()
    }

    pub fn rebuild_lookups(&mut self) {
        for enum_type in self.enums.values_mut() {
            enum_type.rebuild_lookup();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enum_type_creation() {
        let mood = EnumType::new(
            "mood".to_string(),
            None,
            vec!["sad".to_string(), "ok".to_string(), "happy".to_string()],
        )
        .unwrap();

        assert_eq!(mood.name, "mood");
        assert_eq!(mood.labels.len(), 3);
        assert!(mood.is_valid_label("sad"));
        assert!(mood.is_valid_label("ok"));
        assert!(mood.is_valid_label("happy"));
        assert!(!mood.is_valid_label("angry"));
    }

    #[test]
    fn test_enum_ordinal() {
        let mood = EnumType::new(
            "mood".to_string(),
            None,
            vec!["sad".to_string(), "ok".to_string(), "happy".to_string()],
        )
        .unwrap();

        assert_eq!(mood.ordinal("sad"), Some(0));
        assert_eq!(mood.ordinal("ok"), Some(1));
        assert_eq!(mood.ordinal("happy"), Some(2));
        assert_eq!(mood.ordinal("angry"), None);
    }

    #[test]
    fn test_enum_label_at() {
        let mood = EnumType::new(
            "mood".to_string(),
            None,
            vec!["sad".to_string(), "ok".to_string(), "happy".to_string()],
        )
        .unwrap();

        assert_eq!(mood.label_at(0), Some("sad"));
        assert_eq!(mood.label_at(1), Some("ok"));
        assert_eq!(mood.label_at(2), Some("happy"));
        assert_eq!(mood.label_at(3), None);
    }

    #[test]
    fn test_enum_empty_labels_error() {
        let result = EnumType::new("empty".to_string(), None, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_enum_duplicate_labels_error() {
        let result = EnumType::new(
            "dup".to_string(),
            None,
            vec!["a".to_string(), "b".to_string(), "a".to_string()],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_registry_create_and_get() {
        let mut registry = TypeRegistry::new();
        let mood = EnumType::new(
            "mood".to_string(),
            None,
            vec!["sad".to_string(), "ok".to_string(), "happy".to_string()],
        )
        .unwrap();

        registry.create_enum(mood, false).unwrap();

        assert!(registry.type_exists("mood"));
        assert!(registry.get_enum("mood").is_some());
        assert!(registry.get_enum("nonexistent").is_none());
    }

    #[test]
    fn test_registry_duplicate_error() {
        let mut registry = TypeRegistry::new();
        let mood1 = EnumType::new(
            "mood".to_string(),
            None,
            vec!["sad".to_string(), "happy".to_string()],
        )
        .unwrap();
        let mood2 = EnumType::new(
            "mood".to_string(),
            None,
            vec!["angry".to_string(), "calm".to_string()],
        )
        .unwrap();

        registry.create_enum(mood1, false).unwrap();
        assert!(registry.create_enum(mood2, false).is_err());
    }

    #[test]
    fn test_registry_if_not_exists() {
        let mut registry = TypeRegistry::new();
        let mood1 = EnumType::new(
            "mood".to_string(),
            None,
            vec!["sad".to_string(), "happy".to_string()],
        )
        .unwrap();
        let mood2 = EnumType::new(
            "mood".to_string(),
            None,
            vec!["angry".to_string(), "calm".to_string()],
        )
        .unwrap();

        registry.create_enum(mood1, false).unwrap();

        registry.create_enum(mood2, true).unwrap();

        let mood = registry.get_enum("mood").unwrap();
        assert!(mood.is_valid_label("sad"));
        assert!(!mood.is_valid_label("angry"));
    }

    #[test]
    fn test_registry_drop() {
        let mut registry = TypeRegistry::new();
        let mood = EnumType::new(
            "mood".to_string(),
            None,
            vec!["sad".to_string(), "happy".to_string()],
        )
        .unwrap();

        registry.create_enum(mood, false).unwrap();
        assert!(registry.type_exists("mood"));

        registry.drop_type("mood", false).unwrap();
        assert!(!registry.type_exists("mood"));
    }

    #[test]
    fn test_registry_drop_if_exists() {
        let mut registry = TypeRegistry::new();

        assert!(registry.drop_type("nonexistent", false).is_err());

        registry.drop_type("nonexistent", true).unwrap();
    }

    #[test]
    fn test_registry_dependencies() {
        let mut registry = TypeRegistry::new();
        let mood = EnumType::new(
            "mood".to_string(),
            None,
            vec!["sad".to_string(), "happy".to_string()],
        )
        .unwrap();

        registry.create_enum(mood, false).unwrap();
        registry.register_dependency("mood", "person");

        assert!(registry.drop_type("mood", false).is_err());

        let deps = registry.drop_type_cascade("mood", false).unwrap();
        assert_eq!(deps, vec!["person"]);
    }
}
