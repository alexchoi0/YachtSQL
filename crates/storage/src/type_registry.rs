use std::collections::HashMap;

use indexmap::IndexMap;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::StructField;

pub use crate::custom_types::EnumType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeDefinition {
    Composite(Vec<StructField>),
}

impl TypeDefinition {
    pub fn is_composite(&self) -> bool {
        matches!(self, TypeDefinition::Composite(_))
    }

    pub fn as_composite(&self) -> Option<&Vec<StructField>> {
        match self {
            TypeDefinition::Composite(fields) => Some(fields),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserDefinedType {
    pub name: String,

    pub definition: TypeDefinition,
}

impl UserDefinedType {
    pub fn composite(name: String, fields: Vec<StructField>) -> Self {
        Self {
            name,
            definition: TypeDefinition::Composite(fields),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TypeRegistry {
    types: IndexMap<String, UserDefinedType>,

    enums: HashMap<String, EnumType>,

    type_dependencies: HashMap<String, Vec<String>>,
}

impl TypeRegistry {
    pub fn new() -> Self {
        Self {
            types: IndexMap::new(),
            enums: HashMap::new(),
            type_dependencies: HashMap::new(),
        }
    }

    pub fn create_type(
        &mut self,
        name: String,
        definition: TypeDefinition,
        if_not_exists: bool,
    ) -> Result<()> {
        let TypeDefinition::Composite(fields) = &definition;
        if fields.is_empty() {
            return Err(Error::invalid_query(
                "Composite type must have at least one field".to_string(),
            ));
        }

        let mut seen_names = std::collections::HashSet::new();
        for field in fields {
            let lower_name = field.name.to_lowercase();
            if !seen_names.insert(lower_name.clone()) {
                return Err(Error::invalid_query(format!(
                    "Duplicate field name '{}' in composite type",
                    field.name
                )));
            }
        }

        let lower_name = name.to_lowercase();

        if self.types.contains_key(&lower_name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(Error::invalid_query(format!(
                "Type '{}' already exists",
                name
            )));
        }

        self.types
            .insert(lower_name, UserDefinedType { name, definition });
        Ok(())
    }

    pub fn get_type(&self, name: &str) -> Option<&UserDefinedType> {
        self.types.get(&name.to_lowercase())
    }

    pub fn has_type(&self, name: &str) -> bool {
        self.types.contains_key(&name.to_lowercase())
    }

    pub fn drop_type(&mut self, name: &str, if_exists: bool) -> Result<()> {
        if self.enums.remove(name).is_some() {
            self.type_dependencies.remove(name);
            return Ok(());
        }

        let lower_name = name.to_lowercase();
        if self.types.shift_remove(&lower_name).is_some() {
            self.type_dependencies.remove(name);
            return Ok(());
        }

        if !if_exists {
            return Err(Error::invalid_query(format!(
                "Type '{}' does not exist",
                name
            )));
        }
        Ok(())
    }

    pub fn list_types(&self) -> Vec<&String> {
        self.types.values().map(|t| &t.name).collect()
    }

    pub fn len(&self) -> usize {
        self.types.len()
    }

    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &UserDefinedType)> {
        self.types.iter()
    }

    pub fn create_enum(&mut self, enum_type: EnumType, if_not_exists: bool) -> Result<()> {
        let name = enum_type.name.clone();

        if self.enums.contains_key(&name) || self.types.contains_key(&name.to_lowercase()) {
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
        self.enums.contains_key(name) || self.types.contains_key(&name.to_lowercase())
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

    pub fn drop_type_cascade(&mut self, name: &str, if_exists: bool) -> Result<Vec<String>> {
        let deps = self.get_dependencies(name);

        if self.enums.remove(name).is_some() {
            self.type_dependencies.remove(name);
            return Ok(deps);
        }

        let lower_name = name.to_lowercase();
        if self.types.shift_remove(&lower_name).is_some() {
            self.type_dependencies.remove(name);
            return Ok(deps);
        }

        if !if_exists {
            return Err(Error::invalid_query(format!(
                "Type '{}' does not exist",
                name
            )));
        }

        Ok(vec![])
    }

    pub fn list_enums(&self) -> Vec<&str> {
        self.enums.keys().map(|s| s.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;

    use super::*;

    fn make_field(name: &str, data_type: DataType) -> StructField {
        StructField {
            name: name.to_string(),
            data_type,
        }
    }

    #[test]
    fn test_create_composite_type() {
        let mut registry = TypeRegistry::new();

        let fields = vec![
            make_field("x", DataType::Int64),
            make_field("y", DataType::Int64),
        ];

        registry
            .create_type(
                "point".to_string(),
                TypeDefinition::Composite(fields),
                false,
            )
            .unwrap();

        assert!(registry.has_type("point"));
        assert!(registry.has_type("POINT"));
        assert!(registry.has_type("Point"));

        let udt = registry.get_type("point").unwrap();
        assert_eq!(udt.name, "point");

        let TypeDefinition::Composite(fields) = &udt.definition;
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "x");
        assert_eq!(fields[1].name, "y");
    }

    #[test]
    fn test_create_duplicate_type_error() {
        let mut registry = TypeRegistry::new();

        let fields = vec![make_field("val", DataType::Int64)];

        registry
            .create_type(
                "test_type".to_string(),
                TypeDefinition::Composite(fields.clone()),
                false,
            )
            .unwrap();

        let result = registry.create_type(
            "test_type".to_string(),
            TypeDefinition::Composite(fields),
            false,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn test_create_if_not_exists() {
        let mut registry = TypeRegistry::new();

        let fields = vec![make_field("val", DataType::Int64)];

        registry
            .create_type(
                "test_type".to_string(),
                TypeDefinition::Composite(fields.clone()),
                false,
            )
            .unwrap();

        let result = registry.create_type(
            "TEST_TYPE".to_string(),
            TypeDefinition::Composite(fields),
            true,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_empty_composite_type_error() {
        let mut registry = TypeRegistry::new();

        let result = registry.create_type(
            "empty".to_string(),
            TypeDefinition::Composite(vec![]),
            false,
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least one field")
        );
    }

    #[test]
    fn test_duplicate_field_name_error() {
        let mut registry = TypeRegistry::new();

        let fields = vec![
            make_field("x", DataType::Int64),
            make_field("X", DataType::String),
        ];

        let result = registry.create_type(
            "bad_type".to_string(),
            TypeDefinition::Composite(fields),
            false,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Duplicate field"));
    }

    #[test]
    fn test_drop_type() {
        let mut registry = TypeRegistry::new();

        let fields = vec![make_field("val", DataType::Int64)];
        registry
            .create_type(
                "to_drop".to_string(),
                TypeDefinition::Composite(fields),
                false,
            )
            .unwrap();

        assert!(registry.has_type("to_drop"));

        registry.drop_type("to_drop", false).unwrap();

        assert!(!registry.has_type("to_drop"));
    }

    #[test]
    fn test_drop_nonexistent_type_error() {
        let mut registry = TypeRegistry::new();

        let result = registry.drop_type("nonexistent", false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_drop_if_exists() {
        let mut registry = TypeRegistry::new();

        let result = registry.drop_type("nonexistent", true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_types() {
        let mut registry = TypeRegistry::new();

        registry
            .create_type(
                "type_a".to_string(),
                TypeDefinition::Composite(vec![make_field("a", DataType::Int64)]),
                false,
            )
            .unwrap();

        registry
            .create_type(
                "type_b".to_string(),
                TypeDefinition::Composite(vec![make_field("b", DataType::String)]),
                false,
            )
            .unwrap();

        let types = registry.list_types();
        assert_eq!(types.len(), 2);
        assert!(types.contains(&&"type_a".to_string()));
        assert!(types.contains(&&"type_b".to_string()));
    }
}
