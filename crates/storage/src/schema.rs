use std::collections::HashMap;
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};

use crate::index::IndexMetadata;
use crate::table::Row;

pub type CheckEvaluator = Rc<dyn Fn(&Schema, &Row, &str) -> Result<bool> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum FieldMode {
    #[default]
    Nullable,
    Required,
    Repeated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DefaultValue {
    Literal(Value),
    CurrentTimestamp,
    CurrentDate,
    GenRandomUuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IdentityGeneration {
    Always,
    ByDefault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GenerationMode {
    Stored,
    Virtual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedExpression {
    pub expression_sql: String,

    pub generation_mode: GenerationMode,

    pub dependencies: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub mode: FieldMode,
    pub description: Option<String>,
    pub default_value: Option<DefaultValue>,
    pub is_unique: bool,
    pub identity_generation: Option<IdentityGeneration>,
    pub identity_sequence_name: Option<String>,
    pub identity_sequence_config: Option<crate::SequenceConfig>,
    pub is_auto_increment: bool,
    pub generated_expression: Option<GeneratedExpression>,
    pub collation: Option<String>,
    pub source_table: Option<String>,
    pub domain_name: Option<String>,
}

impl Field {
    fn new(name: impl Into<String>, data_type: DataType, mode: FieldMode) -> Self {
        Self {
            name: name.into(),
            data_type,
            mode,
            description: None,
            default_value: None,
            is_unique: false,
            identity_generation: None,
            identity_sequence_name: None,
            identity_sequence_config: None,
            is_auto_increment: false,
            generated_expression: None,
            collation: None,
            source_table: None,
            domain_name: None,
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

    pub fn with_default(mut self, default: DefaultValue) -> Self {
        self.default_value = Some(default);
        self
    }

    pub fn with_unique(mut self) -> Self {
        self.is_unique = true;
        self
    }

    pub fn with_collation(mut self, collation: impl Into<String>) -> Self {
        self.collation = Some(collation.into());
        self
    }

    pub fn with_source_table(mut self, table: impl Into<String>) -> Self {
        self.source_table = Some(table.into());
        self
    }

    pub fn with_domain(mut self, domain_name: impl Into<String>) -> Self {
        self.domain_name = Some(domain_name.into());
        self
    }

    pub fn is_nullable(&self) -> bool {
        self.mode == FieldMode::Nullable
    }

    pub fn is_repeated(&self) -> bool {
        self.mode == FieldMode::Repeated
    }

    fn with_identity(
        mut self,
        mode: IdentityGeneration,
        sequence_name: String,
        config: Option<crate::SequenceConfig>,
    ) -> Self {
        self.identity_generation = Some(mode);
        self.identity_sequence_name = Some(sequence_name);
        self.identity_sequence_config = config;
        self.mode = FieldMode::Required;
        self
    }

    pub fn with_identity_always(
        self,
        sequence_name: String,
        config: Option<crate::SequenceConfig>,
    ) -> Self {
        self.with_identity(IdentityGeneration::Always, sequence_name, config)
    }

    pub fn with_identity_by_default(
        self,
        sequence_name: String,
        config: Option<crate::SequenceConfig>,
    ) -> Self {
        self.with_identity(IdentityGeneration::ByDefault, sequence_name, config)
    }

    pub fn is_identity(&self) -> bool {
        self.identity_generation.is_some()
    }

    pub fn is_identity_always(&self) -> bool {
        matches!(self.identity_generation, Some(IdentityGeneration::Always))
    }

    pub fn is_identity_by_default(&self) -> bool {
        matches!(
            self.identity_generation,
            Some(IdentityGeneration::ByDefault)
        )
    }

    pub fn with_generated(
        mut self,
        expression_sql: String,
        dependencies: Vec<String>,
        generation_mode: GenerationMode,
    ) -> Self {
        self.generated_expression = Some(GeneratedExpression {
            expression_sql,
            generation_mode,
            dependencies,
        });
        self
    }

    pub fn is_generated(&self) -> bool {
        self.generated_expression.is_some()
    }

    pub fn dependencies(&self) -> &[String] {
        self.generated_expression
            .as_ref()
            .map(|g| g.dependencies.as_slice())
            .unwrap_or(&[])
    }

    pub fn with_auto_increment(mut self) -> Self {
        self.is_auto_increment = true;
        self.mode = FieldMode::Required;
        self
    }

    pub fn is_auto_increment(&self) -> bool {
        self.is_auto_increment
    }

    pub fn has_auto_generation(&self) -> bool {
        self.is_identity() || self.is_auto_increment
    }

    pub fn validate_auto_increment_exclusivity(&self) -> bool {
        !(self.is_identity() && self.is_auto_increment)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckConstraint {
    pub name: Option<String>,
    pub expression: String,

    pub enforced: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintTypeTag {
    PrimaryKey,
    Unique,
    Check,
    ForeignKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintMetadata {
    pub constraint_type: ConstraintTypeTag,
    pub columns: Vec<String>,
    pub definition: String,
    pub is_valid: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Schema {
    fields: Vec<Field>,
    primary_key: Option<Vec<String>>,
    unique_constraints: Vec<Vec<String>>,
    check_constraints: Vec<CheckConstraint>,
    foreign_keys: Vec<crate::ForeignKey>,
    constraint_metadata: HashMap<String, ConstraintMetadata>,
    #[serde(skip)]
    indexes: HashMap<String, IndexMetadata>,
    #[serde(skip)]
    check_evaluator: Option<CheckEvaluator>,
}

impl std::fmt::Debug for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Schema")
            .field("fields", &self.fields)
            .field("primary_key", &self.primary_key)
            .field("unique_constraints", &self.unique_constraints)
            .field("check_constraints", &self.check_constraints)
            .field("foreign_keys", &self.foreign_keys)
            .field("constraint_metadata", &self.constraint_metadata)
            .field("indexes", &self.indexes)
            .field("check_evaluator", &"<function>")
            .finish()
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
            && self.primary_key == other.primary_key
            && self.unique_constraints == other.unique_constraints
            && self.check_constraints == other.check_constraints
            && self.foreign_keys == other.foreign_keys
            && self.constraint_metadata == other.constraint_metadata
            && self.indexes == other.indexes
    }
}

impl Schema {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            constraint_metadata: HashMap::new(),
            indexes: HashMap::new(),
            check_evaluator: None,
        }
    }

    pub fn from_fields(fields: Vec<Field>) -> Self {
        Self {
            fields,
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            constraint_metadata: HashMap::new(),
            indexes: HashMap::new(),
            check_evaluator: None,
        }
    }

    pub fn add_field(&mut self, field: Field) {
        self.fields.push(field);
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
            Some(tbl) => self.fields.iter().position(|f| {
                f.name == name
                    && f.source_table
                        .as_ref()
                        .is_some_and(|src| src == tbl || src.ends_with(&format!(".{}", tbl)))
            }),
            None => self.field_index(name),
        }
    }

    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn set_primary_key(&mut self, columns: Vec<String>) {
        self.primary_key = Some(columns);
    }

    pub fn drop_primary_key(&mut self) -> Result<()> {
        let pk_cols = match &self.primary_key {
            Some(cols) => cols.clone(),
            None => {
                return Err(Error::invalid_query(
                    "Table does not have a PRIMARY KEY constraint".to_string(),
                ));
            }
        };

        for field in &mut self.fields {
            if pk_cols.contains(&field.name) {
                let has_explicit_unique = self
                    .unique_constraints
                    .iter()
                    .any(|cols| cols.len() == 1 && cols[0] == field.name);

                if !has_explicit_unique {
                    field.is_unique = false;
                }
            }
        }

        self.primary_key = None;
        Ok(())
    }

    pub fn primary_key(&self) -> Option<&[String]> {
        self.primary_key.as_deref()
    }

    pub fn add_unique_constraint(&mut self, columns: Vec<String>) {
        self.unique_constraints.push(columns);
    }

    pub fn unique_constraints(&self) -> &[Vec<String>] {
        &self.unique_constraints
    }

    pub fn add_check_constraint(&mut self, constraint: CheckConstraint) {
        self.add_check_constraint_with_validity(constraint, true);
    }

    pub fn add_check_constraint_with_validity(
        &mut self,
        constraint: CheckConstraint,
        is_valid: bool,
    ) {
        if let Some(ref name) = constraint.name {
            self.constraint_metadata.insert(
                name.clone(),
                ConstraintMetadata {
                    constraint_type: ConstraintTypeTag::Check,
                    columns: vec![],
                    definition: constraint.expression.clone(),
                    is_valid,
                },
            );
        }
        self.check_constraints.push(constraint);
    }

    pub fn check_constraints(&self) -> &[CheckConstraint] {
        &self.check_constraints
    }

    pub fn remove_check_constraint_by_name(&mut self, name: &str) -> bool {
        if let Some(pos) = self
            .check_constraints
            .iter()
            .position(|c| c.name.as_deref() == Some(name))
        {
            self.check_constraints.remove(pos);
            self.constraint_metadata.remove(name);
            true
        } else {
            false
        }
    }

    pub fn add_foreign_key(&mut self, foreign_key: crate::ForeignKey) {
        self.add_foreign_key_with_validity(foreign_key, true);
    }

    pub fn add_foreign_key_with_validity(
        &mut self,
        foreign_key: crate::ForeignKey,
        is_valid: bool,
    ) {
        if let Some(ref name) = foreign_key.name {
            self.constraint_metadata.insert(
                name.clone(),
                ConstraintMetadata {
                    constraint_type: ConstraintTypeTag::ForeignKey,
                    columns: foreign_key.child_columns.clone(),
                    definition: format!(
                        "FOREIGN KEY ({}) REFERENCES {}({})",
                        foreign_key.child_columns.join(", "),
                        foreign_key.parent_table,
                        foreign_key.parent_columns.join(", ")
                    ),
                    is_valid,
                },
            );
        }
        self.foreign_keys.push(foreign_key);
    }

    pub fn foreign_keys(&self) -> &[crate::ForeignKey] {
        &self.foreign_keys
    }

    pub fn remove_foreign_key_by_name(&mut self, name: &str) -> bool {
        if let Some(pos) = self
            .foreign_keys
            .iter()
            .position(|fk| fk.name.as_deref() == Some(name))
        {
            self.foreign_keys.remove(pos);
            self.constraint_metadata.remove(name);
            true
        } else {
            false
        }
    }

    pub fn rename_constraint(&mut self, old_name: &str, new_name: &str) -> bool {
        if let Some(metadata) = self.constraint_metadata.remove(old_name) {
            match metadata.constraint_type {
                ConstraintTypeTag::Check => {
                    for constraint in &mut self.check_constraints {
                        if constraint.name.as_deref() == Some(old_name) {
                            constraint.name = Some(new_name.to_string());
                            break;
                        }
                    }
                }
                ConstraintTypeTag::ForeignKey => {
                    for fk in &mut self.foreign_keys {
                        if fk.name.as_deref() == Some(old_name) {
                            fk.name = Some(new_name.to_string());
                            break;
                        }
                    }
                }
                ConstraintTypeTag::Unique | ConstraintTypeTag::PrimaryKey => {}
            }
            self.constraint_metadata
                .insert(new_name.to_string(), metadata);
            true
        } else {
            false
        }
    }

    pub fn set_check_evaluator(&mut self, evaluator: CheckEvaluator) {
        self.check_evaluator = Some(evaluator);
    }

    pub fn check_evaluator(&self) -> Option<&CheckEvaluator> {
        self.check_evaluator.as_ref()
    }

    pub fn add_constraint_metadata(&mut self, name: String, metadata: ConstraintMetadata) {
        self.constraint_metadata.insert(name, metadata);
    }

    pub fn get_constraint_metadata(&self, name: &str) -> Option<&ConstraintMetadata> {
        self.constraint_metadata.get(name)
    }

    pub fn remove_constraint_metadata(&mut self, name: &str) -> Option<ConstraintMetadata> {
        self.constraint_metadata.remove(name)
    }

    pub fn has_constraint(&self, name: &str) -> bool {
        self.constraint_metadata.contains_key(name)
    }

    pub fn set_constraint_valid(&mut self, name: &str) -> bool {
        if let Some(metadata) = self.constraint_metadata.get_mut(name) {
            metadata.is_valid = true;
            true
        } else {
            false
        }
    }

    pub fn is_constraint_valid(&self, name: &str) -> Option<bool> {
        self.constraint_metadata.get(name).map(|m| m.is_valid)
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

    pub fn project(&self, field_names: &[String]) -> Result<Schema> {
        let mut projected_fields = Vec::new();

        for name in field_names {
            let field = self
                .field(name)
                .ok_or_else(|| Error::column_not_found(name.clone()))?;
            projected_fields.push(field.clone());
        }

        Ok(Schema::from_fields(projected_fields))
    }

    pub fn add_index(&mut self, index: IndexMetadata) -> Result<()> {
        if self.indexes.contains_key(&index.index_name) {
            return Err(Error::invalid_query(format!(
                "Index '{}' already exists",
                index.index_name
            )));
        }

        self.indexes.insert(index.index_name.clone(), index);
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<&IndexMetadata> {
        self.indexes.get(name)
    }

    pub fn drop_index(&mut self, name: &str) -> Result<()> {
        self.indexes
            .remove(name)
            .ok_or_else(|| Error::invalid_query(format!("Index '{}' does not exist", name)))?;
        Ok(())
    }

    pub fn list_indexes_for_table(&self, table_name: &str) -> Vec<&IndexMetadata> {
        self.indexes
            .values()
            .filter(|idx| idx.table_name == table_name)
            .collect()
    }

    pub fn list_indexes(&self) -> Vec<&IndexMetadata> {
        self.indexes.values().collect()
    }

    pub fn has_index(&self, name: &str) -> bool {
        self.indexes.contains_key(name)
    }

    pub fn with_source_table(&self, table: &str) -> Schema {
        let fields = self
            .fields
            .iter()
            .map(|f| {
                let mut field = f.clone();
                field.source_table = Some(table.to_string());
                field
            })
            .collect();
        Schema::from_fields(fields)
    }

    pub fn fields_mut(&mut self) -> &mut Vec<Field> {
        &mut self.fields
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

    #[test]
    fn test_schema_creation() {
        let mut schema = Schema::new();
        schema.add_field(Field::nullable("id", DataType::Int64));
        schema.add_field(Field::nullable("name", DataType::String));

        assert_eq!(schema.field_count(), 2);
        assert!(schema.field("id").is_some());
        assert!(schema.field("name").is_some());
        assert!(schema.field("missing").is_none());
    }

    #[test]
    fn test_schema_validation() {
        let mut schema = Schema::new();
        schema.add_field(Field::nullable("id", DataType::Int64));
        schema.add_field(Field::nullable("id", DataType::String));

        assert!(schema.validate().is_err());
    }

    #[test]
    fn test_schema_projection() {
        let mut schema = Schema::new();
        schema.add_field(Field::nullable("id", DataType::Int64));
        schema.add_field(Field::nullable("name", DataType::String));
        schema.add_field(Field::nullable("age", DataType::Int64));

        let projected = schema
            .project(&["name".to_string(), "age".to_string()])
            .unwrap();

        assert_eq!(projected.field_count(), 2);
        assert!(projected.field("name").is_some());
        assert!(projected.field("age").is_some());
        assert!(projected.field("id").is_none());
    }
}
