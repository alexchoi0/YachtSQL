use sqlparser::ast::{Statement, TriggerEvent as SqlTriggerEvent};
use yachtsql_common::error::{Error, Result};
use yachtsql_storage::{TriggerEvent, TriggerLevel, TriggerMetadata, TriggerTiming};

use super::super::QueryExecutor;

pub trait TriggerExecutor {
    fn execute_create_trigger(&mut self, stmt: &Statement) -> Result<()>;

    fn execute_drop_trigger(&mut self, stmt: &Statement) -> Result<()>;

    fn parse_trigger_name(&self, name: &str) -> Result<(String, String)>;

    fn parse_trigger_table_name(&self, name: &str) -> Result<(String, String)>;
}

impl TriggerExecutor for QueryExecutor {
    fn execute_create_trigger(&mut self, stmt: &Statement) -> Result<()> {
        let Statement::CreateTrigger(create_trigger) = stmt else {
            return Err(Error::internal("Not a CREATE TRIGGER statement"));
        };

        let exec_body = create_trigger.exec_body.as_ref().ok_or_else(|| {
            Error::invalid_query("CREATE TRIGGER requires an EXECUTE body".to_string())
        })?;

        let trigger_name = create_trigger.name.to_string();
        let (dataset_id, trigger_id) = self.parse_trigger_name(&trigger_name)?;

        let table_name_str = create_trigger.table_name.to_string();
        let (table_dataset_id, table_id) = self.parse_trigger_table_name(&table_name_str)?;

        if dataset_id != table_dataset_id {
            return Err(Error::invalid_query(format!(
                "Trigger '{}' and table '{}' must be in the same dataset",
                trigger_name, create_trigger.table_name
            )));
        }

        let timing = match create_trigger.period.to_string().to_uppercase().as_str() {
            "BEFORE" => TriggerTiming::Before,
            "AFTER" => TriggerTiming::After,
            "INSTEAD OF" => TriggerTiming::InsteadOf,
            other => {
                return Err(Error::invalid_query(format!(
                    "Invalid trigger timing: {}. Must be BEFORE, AFTER, or INSTEAD OF",
                    other
                )));
            }
        };

        if create_trigger.events.is_empty() {
            return Err(Error::invalid_query(
                "At least one trigger event (INSERT, UPDATE, DELETE) is required",
            ));
        }

        if create_trigger.events.len() > 1 {
            return Err(Error::unsupported_feature(
                "Multiple events in a single trigger are not yet supported. Create separate triggers for each event.",
            ));
        }

        let event = match &create_trigger.events[0] {
            SqlTriggerEvent::Insert => TriggerEvent::Insert,
            SqlTriggerEvent::Update(cols) => {
                if cols.is_empty() {
                    TriggerEvent::Update { columns: None }
                } else {
                    let column_names: Vec<String> = cols.iter().map(|c| c.value.clone()).collect();
                    TriggerEvent::Update {
                        columns: Some(column_names),
                    }
                }
            }
            SqlTriggerEvent::Delete => TriggerEvent::Delete,
            other => {
                return Err(Error::unsupported_feature(format!(
                    "Trigger event {:?} is not yet supported",
                    other
                )));
            }
        };

        let level = match create_trigger.trigger_object {
            sqlparser::ast::TriggerObject::Row => TriggerLevel::ForEachRow,
            sqlparser::ast::TriggerObject::Statement => TriggerLevel::ForEachStatement,
        };

        let action_body = format!("{}", exec_body);

        let trigger_metadata = TriggerMetadata::new(
            trigger_id.clone(),
            table_id.clone(),
            timing,
            event,
            level,
            action_body,
        );

        let mut storage = self.storage.borrow_mut();

        if storage.get_dataset(&dataset_id).is_none() {
            storage.create_dataset(dataset_id.clone())?;
        }

        let dataset = storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
            Error::dataset_not_found(format!("Dataset '{}' not found", dataset_id))
        })?;

        if dataset.get_table(&table_id).is_none() {
            return Err(Error::table_not_found(format!(
                "Table '{}.{}' not found",
                dataset_id, table_id
            )));
        }

        let if_not_exists = create_trigger.or_replace || create_trigger.is_constraint;
        dataset
            .triggers_mut()
            .create_trigger(trigger_metadata, if_not_exists)?;

        Ok(())
    }

    fn execute_drop_trigger(&mut self, stmt: &Statement) -> Result<()> {
        let Statement::DropTrigger(drop_trigger) = stmt else {
            return Err(Error::internal("Not a DROP TRIGGER statement"));
        };

        let trigger_name_str = drop_trigger.trigger_name.to_string();
        let (dataset_id, trigger_id) = self.parse_trigger_name(&trigger_name_str)?;

        let mut storage = self.storage.borrow_mut();

        let dataset = storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
            Error::dataset_not_found(format!("Dataset '{}' not found", dataset_id))
        })?;

        dataset
            .triggers_mut()
            .drop_trigger(&trigger_id, drop_trigger.if_exists)?;

        Ok(())
    }

    fn parse_trigger_name(&self, name: &str) -> Result<(String, String)> {
        if let Some(dot_pos) = name.find('.') {
            let dataset = name[..dot_pos].to_string();
            let trigger = name[dot_pos + 1..].to_string();
            Ok((dataset, trigger))
        } else {
            Ok(("default".to_string(), name.to_string()))
        }
    }

    fn parse_trigger_table_name(&self, name: &str) -> Result<(String, String)> {
        if let Some(dot_pos) = name.find('.') {
            let dataset = name[..dot_pos].to_string();
            let table = name[dot_pos + 1..].to_string();
            Ok((dataset, table))
        } else {
            Ok(("default".to_string(), name.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DialectType;

    #[test]
    fn test_parse_trigger_name() {
        let executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

        let (dataset, trigger) = executor.parse_trigger_name("my_trigger").unwrap();
        assert_eq!(dataset, "default");
        assert_eq!(trigger, "my_trigger");

        let (dataset, trigger) = executor.parse_trigger_name("mydb.my_trigger").unwrap();
        assert_eq!(dataset, "mydb");
        assert_eq!(trigger, "my_trigger");
    }

    #[test]
    fn test_parse_trigger_table_name() {
        let executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

        let (dataset, table) = executor.parse_trigger_table_name("users").unwrap();
        assert_eq!(dataset, "default");
        assert_eq!(table, "users");

        let (dataset, table) = executor.parse_trigger_table_name("mydb.users").unwrap();
        assert_eq!(dataset, "mydb");
        assert_eq!(table, "users");
    }
}
