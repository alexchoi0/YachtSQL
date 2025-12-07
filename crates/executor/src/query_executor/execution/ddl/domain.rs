use yachtsql_core::error::{Error, Result};
use yachtsql_parser::validator::{AlterDomainAction, CustomStatement};
use yachtsql_storage::{DomainConstraint, DomainDefinition};

use super::super::QueryExecutor;
use super::create::DdlExecutor;
use crate::Table;

pub trait DomainExecutor {
    fn execute_create_domain(&mut self, stmt: &CustomStatement) -> Result<Table>;

    fn execute_alter_domain(&mut self, stmt: &CustomStatement) -> Result<Table>;

    fn execute_drop_domain(&mut self, stmt: &CustomStatement) -> Result<Table>;
}

impl DomainExecutor for QueryExecutor {
    fn execute_create_domain(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::CreateDomain {
            name,
            base_type,
            default_value,
            not_null,
            constraints,
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a CREATE DOMAIN statement".to_string(),
            ));
        };

        let domain_name = name.to_string();
        let (dataset_id, domain_id) = self.parse_ddl_table_name(&domain_name)?;

        let mut domain_def = DomainDefinition::new(domain_id.clone(), base_type.clone());

        if let Some(default) = default_value {
            domain_def = domain_def.with_default(default.clone());
        }

        if *not_null {
            domain_def = domain_def.with_not_null();
        }

        for constraint in constraints {
            domain_def =
                domain_def.with_constraint(constraint.name.clone(), constraint.expression.clone());
        }

        let mut storage = self.storage.borrow_mut();

        if storage.get_dataset(&dataset_id).is_none() {
            storage.create_dataset(dataset_id.clone())?;
        }

        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        dataset.domains_mut().create_domain(domain_def)?;

        Self::empty_result()
    }

    fn execute_alter_domain(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::AlterDomain { name, action } = stmt else {
            return Err(Error::InternalError(
                "Not an ALTER DOMAIN statement".to_string(),
            ));
        };

        let domain_name = name.to_string();
        let (dataset_id, domain_id) = self.parse_ddl_table_name(&domain_name)?;

        let mut storage = self.storage.borrow_mut();

        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        let domain = dataset
            .domains_mut()
            .get_domain_mut(&domain_id)
            .ok_or_else(|| {
                Error::invalid_query(format!("Domain '{}' does not exist", domain_id))
            })?;

        match action {
            AlterDomainAction::AddConstraint { name, expression } => {
                domain.add_constraint(name.clone(), expression.clone());
            }
            AlterDomainAction::DropConstraint { name } => {
                domain.drop_constraint(name)?;
            }
            AlterDomainAction::SetDefault { value } => {
                domain.set_default(value.clone());
            }
            AlterDomainAction::DropDefault => {
                domain.drop_default();
            }
            AlterDomainAction::SetNotNull => {
                domain.set_not_null();
            }
            AlterDomainAction::DropNotNull => {
                domain.drop_not_null();
            }
            AlterDomainAction::RenameConstraint { old_name, new_name } => {
                domain.rename_constraint(old_name, new_name.clone())?;
            }
            AlterDomainAction::ValidateConstraint { name } => {
                domain.validate_constraint(name)?;
            }
        }

        Self::empty_result()
    }

    fn execute_drop_domain(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::DropDomain {
            if_exists,
            names,
            cascade,
            restrict: _,
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a DROP DOMAIN statement".to_string(),
            ));
        };

        let mut storage = self.storage.borrow_mut();

        for name in names {
            let domain_name = name.to_string();

            let (dataset_id, domain_id) = if domain_name.contains('.') {
                let parts: Vec<&str> = domain_name.splitn(2, '.').collect();
                (parts[0].to_string(), parts[1].to_string())
            } else {
                ("default".to_string(), domain_name.clone())
            };

            let Some(dataset) = storage.get_dataset_mut(&dataset_id) else {
                if *if_exists {
                    continue;
                }
                return Err(Error::DatasetNotFound(format!(
                    "Dataset '{}' not found",
                    dataset_id
                )));
            };

            let _ = cascade;

            dataset.domains_mut().drop_domain(&domain_id, *if_exists)?;
        }

        Self::empty_result()
    }
}
