use sqlparser::ast::Statement as SqlStatement;
use yachtsql_core::error::{Error, Result};
use yachtsql_parser::validator::{AlterSchemaAction, CustomStatement};

use crate::{QueryExecutor, Table};

pub trait SchemaExecutor {
    fn execute_create_schema(&mut self, stmt: &SqlStatement) -> Result<()>;

    fn execute_drop_schema(&mut self, stmt: &SqlStatement) -> Result<()>;

    fn execute_alter_schema(&mut self, stmt: &CustomStatement) -> Result<Table>;
}

impl SchemaExecutor for QueryExecutor {
    fn execute_create_schema(&mut self, stmt: &SqlStatement) -> Result<()> {
        if let SqlStatement::CreateSchema {
            schema_name,
            if_not_exists,
            ..
        } = stmt
        {
            let schema_id = schema_name.to_string();

            let mut storage = self.storage.borrow_mut();

            if storage.get_dataset(&schema_id).is_some() {
                if *if_not_exists {
                    return Ok(());
                } else {
                    return Err(Error::invalid_query(format!(
                        "schema \"{}\" already exists",
                        schema_id
                    )));
                }
            }

            storage
                .create_dataset(schema_id.clone())
                .map_err(|e| Error::invalid_query(format!("Failed to create schema: {}", e)))?;

            Ok(())
        } else {
            Err(Error::InternalError(
                "Expected CREATE SCHEMA statement".to_string(),
            ))
        }
    }

    fn execute_drop_schema(&mut self, stmt: &SqlStatement) -> Result<()> {
        use sqlparser::ast::ObjectType;

        if let SqlStatement::Drop {
            object_type: ObjectType::Schema,
            names,
            if_exists,
            cascade,
            ..
        } = stmt
        {
            for name in names {
                let schema_id = name.to_string();

                let mut storage = self.storage.borrow_mut();

                let schema_exists = storage.get_dataset(&schema_id).is_some();
                let is_empty = if let Some(dataset) = storage.get_dataset(&schema_id) {
                    let has_tables = !dataset.tables().is_empty();
                    let has_views = !dataset.views().list_views().is_empty();
                    let has_sequences = dataset.sequences().has_sequences();
                    !has_tables && !has_views && !has_sequences
                } else {
                    true
                };

                if !schema_exists {
                    if *if_exists {
                        continue;
                    } else {
                        return Err(Error::invalid_query(format!(
                            "schema \"{}\" does not exist",
                            schema_id
                        )));
                    }
                }

                if !is_empty && !*cascade {
                    return Err(Error::invalid_query(format!(
                        "cannot drop schema \"{}\" because it is not empty. Use DROP SCHEMA {} CASCADE to drop all contained objects.",
                        schema_id, schema_id
                    )));
                }

                storage
                    .delete_dataset(&schema_id)
                    .map_err(|e| Error::invalid_query(format!("Failed to drop schema: {}", e)))?;
            }
            Ok(())
        } else {
            Err(Error::InternalError(
                "Expected DROP SCHEMA statement".to_string(),
            ))
        }
    }

    fn execute_alter_schema(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::AlterSchema { name, action } = stmt else {
            return Err(Error::InternalError(
                "Not an ALTER SCHEMA statement".to_string(),
            ));
        };

        let mut storage = self.storage.borrow_mut();

        if storage.get_dataset(name).is_none() {
            return Err(Error::invalid_query(format!(
                "schema \"{}\" does not exist",
                name
            )));
        }

        match action {
            AlterSchemaAction::RenameTo { new_name } => {
                if storage.get_dataset(new_name).is_some() {
                    return Err(Error::invalid_query(format!(
                        "schema \"{}\" already exists",
                        new_name
                    )));
                }
                storage.rename_dataset(name, new_name)?;
            }
            AlterSchemaAction::OwnerTo { new_owner: _ } => {}
        }

        Self::empty_result()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_schema() {
        let mut executor = QueryExecutor::new();
        let result = executor.execute_sql("CREATE SCHEMA myschema");
        assert!(
            result.is_ok(),
            "CREATE SCHEMA should succeed: {:?}",
            result.err()
        );

        let result = executor.execute_sql("CREATE TABLE myschema.test (id INT64)");
        assert!(
            result.is_ok(),
            "Creating table in schema should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_create_schema_if_not_exists() {
        let mut executor = QueryExecutor::new();

        executor.execute_sql("CREATE SCHEMA myschema").unwrap();

        let result = executor.execute_sql("CREATE SCHEMA myschema");
        assert!(result.is_err(), "Should fail without IF NOT EXISTS");

        let result = executor.execute_sql("CREATE SCHEMA IF NOT EXISTS myschema");
        assert!(
            result.is_ok(),
            "Should succeed with IF NOT EXISTS: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_drop_schema_empty() {
        let mut executor = QueryExecutor::new();

        executor.execute_sql("CREATE SCHEMA temp_schema").unwrap();

        let result = executor.execute_sql("DROP SCHEMA temp_schema");
        assert!(
            result.is_ok(),
            "DROP SCHEMA on empty schema should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_drop_schema_not_empty() {
        let mut executor = QueryExecutor::new();

        executor.execute_sql("CREATE SCHEMA myschema").unwrap();
        executor
            .execute_sql("CREATE TABLE myschema.test (id INT64)")
            .unwrap();

        let result = executor.execute_sql("DROP SCHEMA myschema");
        assert!(
            result.is_err(),
            "Should fail on non-empty schema without CASCADE"
        );

        let result = executor.execute_sql("DROP SCHEMA myschema CASCADE");
        assert!(
            result.is_ok(),
            "Should succeed with CASCADE: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_drop_schema_if_exists() {
        let mut executor = QueryExecutor::new();

        let result = executor.execute_sql("DROP SCHEMA nonexistent");
        assert!(result.is_err(), "Should fail without IF EXISTS");

        let result = executor.execute_sql("DROP SCHEMA IF EXISTS nonexistent");
        assert!(
            result.is_ok(),
            "Should succeed with IF EXISTS: {:?}",
            result.err()
        );
    }
}
