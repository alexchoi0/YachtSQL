use sqlparser::ast::Statement as SqlStatement;
use yachtsql_common::error::{Error, Result};

use crate::QueryExecutor;

pub trait ExtensionExecutor {
    fn execute_create_extension(&mut self, stmt: &SqlStatement) -> Result<()>;

    fn execute_drop_extension(&mut self, stmt: &SqlStatement) -> Result<()>;
}

impl ExtensionExecutor for QueryExecutor {
    fn execute_create_extension(&mut self, stmt: &SqlStatement) -> Result<()> {
        if let SqlStatement::CreateExtension {
            name,
            if_not_exists,
            ..
        } = stmt
        {
            let ext_name = name.value.clone();

            let result = self.session.extension_registry_mut().install(&ext_name);

            match result {
                Ok(_ext_metadata) => Ok(()),
                Err(yachtsql_storage::ExtensionError::AlreadyInstalled(_)) if *if_not_exists => {
                    Ok(())
                }
                Err(yachtsql_storage::ExtensionError::AlreadyInstalled(name)) => Err(
                    Error::invalid_query(format!("extension \"{}\" already exists", name)),
                ),
                Err(yachtsql_storage::ExtensionError::NotAvailable(name)) => {
                    Err(Error::invalid_query(format!(
                        "extension \"{}\" is not available. Available extensions: uuid-ossp, pgcrypto, pg_trgm, hstore",
                        name
                    )))
                }
                Err(e) => Err(Error::invalid_query(e.to_string())),
            }
        } else {
            Err(Error::InternalError(
                "Expected CREATE EXTENSION statement".to_string(),
            ))
        }
    }

    fn execute_drop_extension(&mut self, stmt: &SqlStatement) -> Result<()> {
        if let SqlStatement::DropExtension {
            names, if_exists, ..
        } = stmt
        {
            for name in names {
                let ext_name = name.value.clone();

                let result = self.session.extension_registry_mut().uninstall(&ext_name);

                match result {
                    Ok(()) => {}
                    Err(yachtsql_storage::ExtensionError::NotInstalled(_)) if *if_exists => {}
                    Err(yachtsql_storage::ExtensionError::NotInstalled(name)) => {
                        return Err(Error::invalid_query(format!(
                            "extension \"{}\" does not exist",
                            name
                        )));
                    }
                    Err(e) => {
                        return Err(Error::invalid_query(e.to_string()));
                    }
                }
            }
            Ok(())
        } else {
            Err(Error::InternalError(
                "Expected DROP EXTENSION statement".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_extension_uuid_ossp() {
        let mut executor = QueryExecutor::new();
        let result = executor.execute_sql("CREATE EXTENSION \"uuid-ossp\"");
        assert!(
            result.is_ok(),
            "CREATE EXTENSION uuid-ossp should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_create_extension_if_not_exists() {
        let mut executor = QueryExecutor::new();

        executor
            .execute_sql("CREATE EXTENSION \"uuid-ossp\"")
            .unwrap();

        let result = executor.execute_sql("CREATE EXTENSION \"uuid-ossp\"");
        assert!(result.is_err(), "Should fail without IF NOT EXISTS");

        let result = executor.execute_sql("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
        assert!(
            result.is_ok(),
            "Should succeed with IF NOT EXISTS: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_create_extension_unknown() {
        let mut executor = QueryExecutor::new();
        let result = executor.execute_sql("CREATE EXTENSION nonexistent");
        assert!(result.is_err(), "Unknown extension should fail");
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("not available"),
            "Error should mention 'not available': {}",
            err_str
        );
    }

    #[test]
    fn test_drop_extension() {
        let mut executor = QueryExecutor::new();

        executor.execute_sql("CREATE EXTENSION pgcrypto").unwrap();

        let result = executor.execute_sql("DROP EXTENSION pgcrypto");
        assert!(
            result.is_ok(),
            "DROP EXTENSION should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_drop_extension_if_exists() {
        let mut executor = QueryExecutor::new();

        let result = executor.execute_sql("DROP EXTENSION nonexistent");
        assert!(result.is_err(), "Should fail without IF EXISTS");

        let result = executor.execute_sql("DROP EXTENSION IF EXISTS nonexistent");
        assert!(
            result.is_ok(),
            "Should succeed with IF EXISTS: {:?}",
            result.err()
        );
    }
}
