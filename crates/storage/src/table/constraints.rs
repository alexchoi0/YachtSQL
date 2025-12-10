use yachtsql_core::error::{Error, Result};

use super::Table;
use super::schema_ops::copy_constraints_to_schema;
use crate::foreign_keys::ForeignKey;
use crate::{Field, Schema};

pub trait TableConstraintOps {
    fn set_column_not_null(&mut self, column_name: &str) -> Result<()>;

    fn rebuild_check_constraints(
        &mut self,
        constraints: Vec<crate::schema::CheckConstraint>,
    ) -> Result<()>;

    fn remove_unique_constraint(&mut self, columns: &[String]) -> Result<()>;

    fn foreign_keys(&self) -> &[ForeignKey];

    fn foreign_keys_mut(&mut self) -> &mut [ForeignKey];

    fn add_foreign_key(&mut self, foreign_key: ForeignKey) -> Result<()>;

    fn remove_foreign_key(&mut self, constraint_name: &str) -> Result<()>;

    fn foreign_keys_to_table(&self, parent_table: &str) -> Vec<&ForeignKey>;

    fn has_foreign_keys(&self) -> bool;
}

impl TableConstraintOps for Table {
    fn set_column_not_null(&mut self, column_name: &str) -> Result<()> {
        use crate::schema::FieldMode;

        let field_idx = self
            .schema
            .field_index(column_name)
            .ok_or_else(|| Error::column_not_found(column_name.to_string()))?;

        let mut new_fields: Vec<Field> = self.schema.fields().to_vec();
        new_fields[field_idx].mode = FieldMode::Required;

        let mut new_schema = Schema::from_fields(new_fields);
        copy_constraints_to_schema(self, &mut new_schema);
        self.schema = new_schema;
        Ok(())
    }

    fn rebuild_check_constraints(
        &mut self,
        constraints: Vec<crate::schema::CheckConstraint>,
    ) -> Result<()> {
        let mut new_schema = Schema::from_fields(self.schema.fields().to_vec());

        if let Some(pk) = self.schema.primary_key() {
            new_schema.set_primary_key(pk.to_vec());
        }
        for unique in self.schema.unique_constraints() {
            new_schema.add_unique_constraint(unique.clone());
        }

        for constraint in constraints {
            new_schema.add_check_constraint(constraint);
        }

        self.schema = new_schema;
        Ok(())
    }

    fn remove_unique_constraint(&mut self, columns: &[String]) -> Result<()> {
        let mut new_schema = Schema::from_fields(self.schema.fields().to_vec());

        if let Some(pk) = self.schema.primary_key() {
            new_schema.set_primary_key(pk.to_vec());
        }

        for unique in self.schema.unique_constraints() {
            if unique.columns != columns {
                new_schema.add_unique_constraint(unique.clone());
            }
        }

        for check in self.schema.check_constraints() {
            new_schema.add_check_constraint(check.clone());
        }

        self.schema = new_schema;
        Ok(())
    }

    fn foreign_keys(&self) -> &[ForeignKey] {
        &self.foreign_keys
    }

    fn foreign_keys_mut(&mut self) -> &mut [ForeignKey] {
        &mut self.foreign_keys
    }

    fn add_foreign_key(&mut self, foreign_key: ForeignKey) -> Result<()> {
        foreign_key.validate().map_err(Error::InvalidOperation)?;

        for col in &foreign_key.child_columns {
            if self.schema.field_index(col).is_none() {
                return Err(Error::InvalidOperation(format!(
                    "Foreign key references non-existent column '{}'",
                    col
                )));
            }
        }

        self.foreign_keys.push(foreign_key);
        Ok(())
    }

    fn remove_foreign_key(&mut self, constraint_name: &str) -> Result<()> {
        let initial_len = self.foreign_keys.len();
        self.foreign_keys.retain(|fk| {
            fk.name.as_deref() != Some(constraint_name) && fk.constraint_name() != constraint_name
        });

        if self.foreign_keys.len() == initial_len {
            return Err(Error::InvalidOperation(format!(
                "Foreign key constraint '{}' not found",
                constraint_name
            )));
        }

        Ok(())
    }

    fn foreign_keys_to_table(&self, parent_table: &str) -> Vec<&ForeignKey> {
        self.foreign_keys
            .iter()
            .filter(|fk| fk.parent_table == parent_table)
            .collect()
    }

    fn has_foreign_keys(&self) -> bool {
        !self.foreign_keys.is_empty()
    }
}
