use yachtsql_core::error::{Error, Result};

use super::Table;
use super::schema_ops::copy_constraints_to_schema;
use crate::{Field, Schema};

pub trait TableConstraintOps {
    fn set_column_not_null(&mut self, column_name: &str) -> Result<()>;

    fn rebuild_check_constraints(
        &mut self,
        constraints: Vec<crate::schema::CheckConstraint>,
    ) -> Result<()>;

    fn remove_unique_constraint(&mut self, columns: &[String]) -> Result<()>;
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
}
