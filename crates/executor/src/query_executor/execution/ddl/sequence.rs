use yachtsql_core::error::{Error, Result};
use yachtsql_parser::validator::CustomStatement;
use yachtsql_storage::sequence::SequenceConfig;

use super::super::QueryExecutor;
use super::create::DdlExecutor;
use crate::Table;

pub trait SequenceExecutor {
    fn execute_create_sequence(&mut self, stmt: &CustomStatement) -> Result<Table>;

    fn execute_alter_sequence(&mut self, stmt: &CustomStatement) -> Result<Table>;

    fn execute_drop_sequence(&mut self, stmt: &CustomStatement) -> Result<Table>;
}

impl SequenceExecutor for QueryExecutor {
    fn execute_create_sequence(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::CreateSequence {
            if_not_exists,
            name,
            start_value,
            increment,
            min_value,
            max_value,
            cycle,
            cache,
            owned_by,
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a CREATE SEQUENCE statement".to_string(),
            ));
        };

        let seq_name = name.to_string();
        let (dataset_id, seq_id) = self.parse_ddl_table_name(&seq_name)?;

        let config = SequenceConfig {
            start_value: start_value.unwrap_or(1),
            increment: increment.unwrap_or(1),
            min_value: min_value.as_ref().and_then(|v| *v),
            max_value: max_value.as_ref().and_then(|v| *v),
            cycle: cycle.unwrap_or(false),
            cache: cache.unwrap_or(1),
        };

        config.validate()?;

        let mut storage = self.storage.borrow_mut();

        if storage.get_dataset(&dataset_id).is_none() {
            storage.create_dataset(dataset_id.to_string())?;
        }

        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        dataset
            .sequences_mut()
            .create_sequence(seq_id.clone(), config, *if_not_exists)?;

        if let Some((table, column)) = owned_by {
            dataset
                .sequences_mut()
                .set_owned_by(&seq_id, table.clone(), column.clone())?;
        }

        Self::empty_result()
    }

    fn execute_alter_sequence(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::AlterSequence {
            if_exists,
            name,
            restart,
            increment,
            min_value,
            max_value,
            cycle,
            owned_by,
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not an ALTER SEQUENCE statement".to_string(),
            ));
        };

        let seq_name = name.to_string();
        let (dataset_id, seq_id) = self.parse_ddl_table_name(&seq_name)?;

        let mut storage = self.storage.borrow_mut();

        let Some(dataset) = storage.get_dataset_mut(&dataset_id) else {
            if *if_exists {
                return Self::empty_result();
            }
            return Err(Error::DatasetNotFound(format!(
                "Dataset '{}' not found",
                dataset_id
            )));
        };

        if *if_exists && dataset.sequences_mut().get_sequence(&seq_id).is_none() {
            return Self::empty_result();
        }

        dataset.sequences_mut().alter_sequence(
            &seq_id,
            *increment,
            min_value.as_ref().copied(),
            max_value.as_ref().copied(),
            *cycle,
            restart.as_ref().copied(),
        )?;

        if let Some(owned) = owned_by {
            if let Some((table, column)) = owned {
                dataset
                    .sequences_mut()
                    .set_owned_by(&seq_id, table.clone(), column.clone())?;
            } else {
                if let Some(seq) = dataset.sequences_mut().get_sequence_mut(&seq_id) {
                    seq.owned_by = None;
                }
            }
        }

        Self::empty_result()
    }

    fn execute_drop_sequence(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::DropSequence {
            if_exists,
            names,
            cascade,
            restrict: _,
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a DROP SEQUENCE statement".to_string(),
            ));
        };

        let mut storage = self.storage.borrow_mut();

        for name in names {
            let seq_name = name.to_string();
            let (dataset_id, seq_id) = self.parse_ddl_table_name(&seq_name)?;

            let Some(dataset) = storage.get_dataset_mut(&dataset_id) else {
                if *if_exists {
                    continue;
                }
                return Err(Error::DatasetNotFound(format!(
                    "Dataset '{}' not found",
                    dataset_id
                )));
            };

            dataset
                .sequences_mut()
                .drop_sequence(&seq_id, *if_exists, *cascade)?;
        }

        Self::empty_result()
    }
}
