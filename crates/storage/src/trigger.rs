use indexmap::IndexMap;
use yachtsql_common::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

impl std::fmt::Display for TriggerTiming {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerTiming::Before => write!(f, "BEFORE"),
            TriggerTiming::After => write!(f, "AFTER"),
            TriggerTiming::InsteadOf => write!(f, "INSTEAD OF"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update { columns: Option<Vec<String>> },
    Delete,
}

impl std::fmt::Display for TriggerEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerEvent::Insert => write!(f, "INSERT"),
            TriggerEvent::Update { columns: None } => write!(f, "UPDATE"),
            TriggerEvent::Update {
                columns: Some(cols),
            } => write!(f, "UPDATE OF {}", cols.join(", ")),
            TriggerEvent::Delete => write!(f, "DELETE"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerLevel {
    ForEachRow,
    ForEachStatement,
}

impl std::fmt::Display for TriggerLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerLevel::ForEachRow => write!(f, "FOR EACH ROW"),
            TriggerLevel::ForEachStatement => write!(f, "FOR EACH STATEMENT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TriggerMetadata {
    pub name: String,

    pub table_name: String,

    pub timing: TriggerTiming,

    pub event: TriggerEvent,

    pub level: TriggerLevel,

    pub when_condition: Option<String>,

    pub action_body: String,

    pub enabled: bool,
}

impl TriggerMetadata {
    pub fn new(
        name: String,
        table_name: String,
        timing: TriggerTiming,
        event: TriggerEvent,
        level: TriggerLevel,
        action_body: String,
    ) -> Self {
        Self {
            name,
            table_name,
            timing,
            event,
            level,
            when_condition: None,
            action_body,
            enabled: true,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::invalid_query("Trigger name cannot be empty"));
        }

        if self.table_name.is_empty() {
            return Err(Error::invalid_query("Trigger table name cannot be empty"));
        }

        if self.action_body.trim().is_empty() {
            return Err(Error::invalid_query("Trigger action body cannot be empty"));
        }

        if self.timing == TriggerTiming::InsteadOf {
            return Err(Error::unsupported_feature(
                "INSTEAD OF triggers are not yet supported",
            ));
        }

        if self.level != TriggerLevel::ForEachRow {
            return Err(Error::unsupported_feature(
                "FOR EACH STATEMENT triggers are not yet supported",
            ));
        }

        if self.when_condition.is_some() {
            return Err(Error::unsupported_feature(
                "WHEN conditions in triggers are not yet supported",
            ));
        }

        Ok(())
    }

    pub fn description(&self) -> String {
        format!(
            "{} {} ON {} {}",
            self.timing, self.event, self.table_name, self.level
        )
    }
}

#[derive(Debug, Default, Clone)]
pub struct TriggerRegistry {
    triggers: IndexMap<String, TriggerMetadata>,
    table_triggers: IndexMap<String, Vec<String>>,
}

impl TriggerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_trigger(&mut self, trigger: TriggerMetadata, if_not_exists: bool) -> Result<()> {
        trigger.validate()?;

        if self.triggers.contains_key(&trigger.name) {
            if if_not_exists {
                return Ok(());
            } else {
                return Err(Error::invalid_query(format!(
                    "Trigger '{}' already exists",
                    trigger.name
                )));
            }
        }

        let trigger_name = trigger.name.clone();
        let table_name = trigger.table_name.clone();

        self.triggers.insert(trigger_name.clone(), trigger);

        self.table_triggers
            .entry(table_name)
            .or_default()
            .push(trigger_name);

        Ok(())
    }

    pub fn drop_trigger(&mut self, name: &str, if_exists: bool) -> Result<()> {
        if !self.triggers.contains_key(name) {
            if if_exists {
                return Ok(());
            } else {
                return Err(Error::invalid_query(format!(
                    "Trigger '{}' does not exist",
                    name
                )));
            }
        }

        let table_name = self.triggers.get(name).unwrap().table_name.clone();

        self.triggers.shift_remove(name);

        if let Some(trigger_names) = self.table_triggers.get_mut(&table_name) {
            trigger_names.retain(|n| n != name);

            if trigger_names.is_empty() {
                self.table_triggers.shift_remove(&table_name);
            }
        }

        Ok(())
    }

    pub fn get_trigger(&self, name: &str) -> Option<&TriggerMetadata> {
        self.triggers.get(name)
    }

    pub fn get_triggers_for_table(&self, table_name: &str) -> Vec<&TriggerMetadata> {
        if let Some(trigger_names) = self.table_triggers.get(table_name) {
            trigger_names
                .iter()
                .filter_map(|name| self.triggers.get(name))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn get_triggers_for_event(
        &self,
        table_name: &str,
        timing: TriggerTiming,
        event: &TriggerEvent,
    ) -> Vec<&TriggerMetadata> {
        self.get_triggers_for_table(table_name)
            .into_iter()
            .filter(|t| {
                t.enabled
                    && t.timing == timing
                    && match (&t.event, event) {
                        (TriggerEvent::Insert, TriggerEvent::Insert) => true,
                        (TriggerEvent::Delete, TriggerEvent::Delete) => true,
                        (
                            TriggerEvent::Update {
                                columns: trigger_cols,
                            },
                            TriggerEvent::Update {
                                columns: event_cols,
                            },
                        ) => {
                            if trigger_cols.is_none() {
                                return true;
                            }

                            if event_cols.is_none() {
                                return true;
                            }

                            let trigger_cols = trigger_cols.as_ref().unwrap();
                            let event_cols = event_cols.as_ref().unwrap();
                            trigger_cols.iter().any(|c| event_cols.contains(c))
                        }
                        _ => false,
                    }
            })
            .collect()
    }

    pub fn list_triggers(&self) -> Vec<&String> {
        self.triggers.keys().collect()
    }

    pub fn count(&self) -> usize {
        self.triggers.len()
    }

    pub fn enable_trigger(&mut self, name: &str) -> Result<()> {
        let trigger = self
            .triggers
            .get_mut(name)
            .ok_or_else(|| Error::invalid_query(format!("Trigger '{}' does not exist", name)))?;
        trigger.enabled = true;
        Ok(())
    }

    pub fn disable_trigger(&mut self, name: &str) -> Result<()> {
        let trigger = self
            .triggers
            .get_mut(name)
            .ok_or_else(|| Error::invalid_query(format!("Trigger '{}' does not exist", name)))?;
        trigger.enabled = false;
        Ok(())
    }

    pub fn drop_triggers_for_table(&mut self, table_name: &str) -> Result<()> {
        if let Some(trigger_names) = self.table_triggers.shift_remove(table_name) {
            for name in trigger_names {
                self.triggers.shift_remove(&name);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trigger_metadata_creation() {
        let trigger = TriggerMetadata::new(
            "audit_insert".to_string(),
            "users".to_string(),
            TriggerTiming::After,
            TriggerEvent::Insert,
            TriggerLevel::ForEachRow,
            "INSERT INTO audit_log VALUES (NEW.id);".to_string(),
        );

        assert_eq!(trigger.name, "audit_insert");
        assert_eq!(trigger.table_name, "users");
        assert_eq!(trigger.timing, TriggerTiming::After);
        assert!(trigger.enabled);
    }

    #[test]
    fn test_trigger_validation() {
        let trigger = TriggerMetadata::new(
            "test".to_string(),
            "users".to_string(),
            TriggerTiming::After,
            TriggerEvent::Insert,
            TriggerLevel::ForEachRow,
            "SELECT 1;".to_string(),
        );
        assert!(trigger.validate().is_ok());

        let trigger = TriggerMetadata::new(
            "".to_string(),
            "users".to_string(),
            TriggerTiming::After,
            TriggerEvent::Insert,
            TriggerLevel::ForEachRow,
            "SELECT 1;".to_string(),
        );
        assert!(trigger.validate().is_err());

        let trigger = TriggerMetadata::new(
            "test".to_string(),
            "users".to_string(),
            TriggerTiming::InsteadOf,
            TriggerEvent::Insert,
            TriggerLevel::ForEachRow,
            "SELECT 1;".to_string(),
        );
        assert!(trigger.validate().is_err());
    }

    #[test]
    fn test_trigger_registry() {
        let mut registry = TriggerRegistry::new();

        let trigger1 = TriggerMetadata::new(
            "audit_insert".to_string(),
            "users".to_string(),
            TriggerTiming::After,
            TriggerEvent::Insert,
            TriggerLevel::ForEachRow,
            "INSERT INTO audit_log VALUES (NEW.id);".to_string(),
        );

        assert!(registry.create_trigger(trigger1.clone(), false).is_ok());
        assert_eq!(registry.count(), 1);

        assert!(registry.create_trigger(trigger1.clone(), false).is_err());

        assert!(registry.create_trigger(trigger1, true).is_ok());
        assert_eq!(registry.count(), 1);

        assert!(registry.get_trigger("audit_insert").is_some());

        let triggers = registry.get_triggers_for_table("users");
        assert_eq!(triggers.len(), 1);

        assert!(registry.drop_trigger("audit_insert", false).is_ok());
        assert_eq!(registry.count(), 0);

        assert!(registry.drop_trigger("nonexistent", false).is_err());

        assert!(registry.drop_trigger("nonexistent", true).is_ok());
    }

    #[test]
    fn test_get_triggers_for_event() {
        let mut registry = TriggerRegistry::new();

        let insert_trigger = TriggerMetadata::new(
            "audit_insert".to_string(),
            "users".to_string(),
            TriggerTiming::After,
            TriggerEvent::Insert,
            TriggerLevel::ForEachRow,
            "INSERT INTO audit_log VALUES (NEW.id);".to_string(),
        );
        registry.create_trigger(insert_trigger, false).unwrap();

        let update_trigger = TriggerMetadata::new(
            "audit_update".to_string(),
            "users".to_string(),
            TriggerTiming::After,
            TriggerEvent::Update { columns: None },
            TriggerLevel::ForEachRow,
            "INSERT INTO audit_log VALUES (NEW.id);".to_string(),
        );
        registry.create_trigger(update_trigger, false).unwrap();

        let triggers =
            registry.get_triggers_for_event("users", TriggerTiming::After, &TriggerEvent::Insert);
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].name, "audit_insert");

        let triggers = registry.get_triggers_for_event(
            "users",
            TriggerTiming::After,
            &TriggerEvent::Update { columns: None },
        );
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].name, "audit_update");

        let triggers =
            registry.get_triggers_for_event("users", TriggerTiming::After, &TriggerEvent::Delete);
        assert_eq!(triggers.len(), 0);
    }

    #[test]
    fn test_enable_disable_trigger() {
        let mut registry = TriggerRegistry::new();

        let trigger = TriggerMetadata::new(
            "test".to_string(),
            "users".to_string(),
            TriggerTiming::After,
            TriggerEvent::Insert,
            TriggerLevel::ForEachRow,
            "SELECT 1;".to_string(),
        );
        registry.create_trigger(trigger, false).unwrap();

        assert!(registry.get_trigger("test").unwrap().enabled);

        registry.disable_trigger("test").unwrap();
        assert!(!registry.get_trigger("test").unwrap().enabled);

        let triggers =
            registry.get_triggers_for_event("users", TriggerTiming::After, &TriggerEvent::Insert);
        assert_eq!(triggers.len(), 0);

        registry.enable_trigger("test").unwrap();
        assert!(registry.get_trigger("test").unwrap().enabled);

        let triggers =
            registry.get_triggers_for_event("users", TriggerTiming::After, &TriggerEvent::Insert);
        assert_eq!(triggers.len(), 1);
    }
}
