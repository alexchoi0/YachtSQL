use serde::{Deserialize, Serialize};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum FieldMode {
    #[default]
    Nullable,
    Required,
    Repeated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub mode: FieldMode,
    pub description: Option<String>,
    pub source_table: Option<String>,
    pub default_value: Option<Value>,
    pub collation: Option<String>,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType, mode: FieldMode) -> Self {
        Self {
            name: name.into(),
            data_type,
            mode,
            description: None,
            source_table: None,
            default_value: None,
            collation: None,
        }
    }

    pub fn nullable(name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(name, data_type, FieldMode::Nullable)
    }

    pub fn required(name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(name, data_type, FieldMode::Required)
    }

    pub fn repeated(name: impl Into<String>, data_type: DataType) -> Self {
        Self::new(name, data_type, FieldMode::Repeated)
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_source_table(mut self, table: impl Into<String>) -> Self {
        self.source_table = Some(table.into());
        self
    }

    pub fn with_default(mut self, default_value: Value) -> Self {
        self.default_value = Some(default_value);
        self
    }

    pub fn with_collation(mut self, collation: impl Into<String>) -> Self {
        self.collation = Some(collation.into());
        self
    }

    pub fn is_nullable(&self) -> bool {
        self.mode == FieldMode::Nullable
    }

    pub fn is_repeated(&self) -> bool {
        self.mode == FieldMode::Repeated
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    pub fn from_fields(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn add_field(&mut self, field: Field) {
        self.fields.push(field);
    }

    pub fn remove_field(&mut self, index: usize) {
        if index < self.fields.len() {
            self.fields.remove(index);
        }
    }

    pub fn rename_field(&mut self, index: usize, new_name: String) {
        if let Some(field) = self.fields.get_mut(index) {
            field.name = new_name;
        }
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    pub fn field_index_qualified(&self, name: &str, table: Option<&str>) -> Option<usize> {
        match table {
            Some(tbl) => {
                let qualified_name = format!("{}.{}", tbl, name);
                if let Some(idx) = self
                    .fields
                    .iter()
                    .position(|f| f.name.eq_ignore_ascii_case(&qualified_name))
                {
                    return Some(idx);
                }
                self.fields.iter().position(|f| {
                    f.name.eq_ignore_ascii_case(name)
                        && f.source_table.as_ref().is_some_and(|src| {
                            src.eq_ignore_ascii_case(tbl)
                                || src
                                    .to_lowercase()
                                    .ends_with(&format!(".{}", tbl.to_lowercase()))
                        })
                })
            }
            None => self.field_index(name),
        }
    }

    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn validate(&self) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for field in &self.fields {
            if !seen.insert(&field.name) {
                return Err(Error::schema_mismatch(format!(
                    "Duplicate field name: {}",
                    field.name
                )));
            }
        }
        Ok(())
    }

    pub fn is_compatible_with(&self, other: &Schema) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (f1, f2) in self.fields.iter().zip(other.fields.iter()) {
            if f1.name != f2.name || f1.data_type != f2.data_type {
                return false;
            }
        }

        true
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn test_schema_creation() {
        let mut schema = Schema::new();
        schema.add_field(Field::nullable("id", DataType::Int64));
        schema.add_field(Field::nullable("name", DataType::String));

        assert_eq!(schema.field_count(), 2);
        assert!(schema.field("id").is_some());
        assert!(schema.field("name").is_some());
        assert!(schema.field("missing").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_schema_validation() {
        let mut schema = Schema::new();
        schema.add_field(Field::nullable("id", DataType::Int64));
        schema.add_field(Field::nullable("id", DataType::String));

        assert!(schema.validate().is_err());
    }
}
