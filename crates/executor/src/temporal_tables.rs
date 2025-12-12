use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use yachtsql_common::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct TemporalTableMetadata {
    pub table_name: String,
    pub is_system_versioned: bool,
    pub history_table_name: Option<String>,
    pub row_start_column: String,
    pub row_end_column: String,
    pub period_name: String,
}

impl Default for TemporalTableMetadata {
    fn default() -> Self {
        Self {
            table_name: String::new(),
            is_system_versioned: false,
            history_table_name: None,
            row_start_column: "SYS_START".to_string(),
            row_end_column: "SYS_END".to_string(),
            period_name: "SYSTEM_TIME".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemporalQueryType {
    Current,
    AsOf(DateTime<Utc>),
    Between {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
    FromTo {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
    All,
}

#[derive(Debug, Default)]
pub struct TemporalTableRegistry {
    tables: HashMap<String, TemporalTableMetadata>,
}

impl TemporalTableRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_system_versioned_table(
        &mut self,
        table_name: String,
        history_table_name: Option<String>,
    ) -> Result<()> {
        let metadata = TemporalTableMetadata {
            table_name: table_name.clone(),
            is_system_versioned: true,
            history_table_name,
            ..Default::default()
        };

        self.tables.insert(table_name, metadata);
        Ok(())
    }

    pub fn get_metadata(&self, table_name: &str) -> Option<&TemporalTableMetadata> {
        self.tables.get(table_name)
    }

    pub fn is_system_versioned(&self, table_name: &str) -> bool {
        self.tables
            .get(table_name)
            .map(|m| m.is_system_versioned)
            .unwrap_or(false)
    }

    pub fn unregister_table(&mut self, table_name: &str) {
        self.tables.remove(table_name);
    }
}

pub fn generate_temporal_predicate(
    metadata: &TemporalTableMetadata,
    query_type: &TemporalQueryType,
) -> Result<String> {
    match query_type {
        TemporalQueryType::Current => Ok(format!(
            "{} = '9999-12-31 23:59:59.999999'",
            metadata.row_end_column
        )),
        TemporalQueryType::AsOf(timestamp) => Ok(format!(
            "{} <= '{}' AND {} > '{}'",
            metadata.row_start_column,
            timestamp.format("%Y-%m-%d %H:%M:%S%.f"),
            metadata.row_end_column,
            timestamp.format("%Y-%m-%d %H:%M:%S%.f")
        )),
        TemporalQueryType::Between { start, end } => Ok(format!(
            "{} < '{}' AND {} > '{}'",
            metadata.row_start_column,
            end.format("%Y-%m-%d %H:%M:%S%.f"),
            metadata.row_end_column,
            start.format("%Y-%m-%d %H:%M:%S%.f")
        )),
        TemporalQueryType::FromTo { start, end } => Ok(format!(
            "{} >= '{}' AND {} <= '{}'",
            metadata.row_start_column,
            start.format("%Y-%m-%d %H:%M:%S%.f"),
            metadata.row_end_column,
            end.format("%Y-%m-%d %H:%M:%S%.f")
        )),
        TemporalQueryType::All => Ok("1=1".to_string()),
    }
}

pub fn handle_temporal_insert(
    metadata: &TemporalTableMetadata,
    mut row: HashMap<String, yachtsql_common::types::Value>,
) -> Result<HashMap<String, yachtsql_common::types::Value>> {
    if !metadata.is_system_versioned {
        return Ok(row);
    }

    let now = Utc::now();

    row.insert(
        metadata.row_start_column.clone(),
        yachtsql_common::types::Value::timestamp(now),
    );

    let max_timestamp =
        chrono::NaiveDateTime::parse_from_str("9999-12-31 23:59:59.999999", "%Y-%m-%d %H:%M:%S%.f")
            .map_err(|e| Error::ExecutionError(e.to_string()))?;
    row.insert(
        metadata.row_end_column.clone(),
        yachtsql_common::types::Value::timestamp(Utc.from_utc_datetime(&max_timestamp)),
    );

    Ok(row)
}

pub fn handle_temporal_update(
    metadata: &TemporalTableMetadata,
    old_row: HashMap<String, yachtsql_common::types::Value>,
    mut new_row: HashMap<String, yachtsql_common::types::Value>,
) -> Result<(
    HashMap<String, yachtsql_common::types::Value>,
    HashMap<String, yachtsql_common::types::Value>,
)> {
    if !metadata.is_system_versioned {
        return Ok((old_row, new_row));
    }

    let now = Utc::now();

    let mut history_row = old_row.clone();
    history_row.insert(
        metadata.row_end_column.clone(),
        yachtsql_common::types::Value::timestamp(now),
    );

    new_row.insert(
        metadata.row_start_column.clone(),
        yachtsql_common::types::Value::timestamp(now),
    );

    let max_timestamp =
        chrono::NaiveDateTime::parse_from_str("9999-12-31 23:59:59.999999", "%Y-%m-%d %H:%M:%S%.f")
            .map_err(|e| Error::ExecutionError(e.to_string()))?;
    new_row.insert(
        metadata.row_end_column.clone(),
        yachtsql_common::types::Value::timestamp(Utc.from_utc_datetime(&max_timestamp)),
    );

    Ok((history_row, new_row))
}

pub fn handle_temporal_delete(
    metadata: &TemporalTableMetadata,
    mut row: HashMap<String, yachtsql_common::types::Value>,
) -> Result<HashMap<String, yachtsql_common::types::Value>> {
    if !metadata.is_system_versioned {
        return Ok(row);
    }

    let now = Utc::now();

    row.insert(
        metadata.row_end_column.clone(),
        yachtsql_common::types::Value::timestamp(now),
    );

    Ok(row)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_register_system_versioned_table() {
        let mut registry = TemporalTableRegistry::new();

        registry
            .register_system_versioned_table("users".to_string(), Some("users_history".to_string()))
            .unwrap();

        assert!(registry.is_system_versioned("users"));
        assert!(!registry.is_system_versioned("orders"));

        let metadata = registry.get_metadata("users").unwrap();
        assert_eq!(metadata.table_name, "users");
        assert_eq!(
            metadata.history_table_name,
            Some("users_history".to_string())
        );
    }

    #[test]
    fn test_temporal_predicate_current() {
        let metadata = TemporalTableMetadata::default();
        let predicate =
            generate_temporal_predicate(&metadata, &TemporalQueryType::Current).unwrap();

        assert!(predicate.contains("SYS_END"));
        assert!(predicate.contains("9999-12-31"));
    }

    #[test]
    fn test_temporal_predicate_as_of() {
        let metadata = TemporalTableMetadata::default();
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let predicate =
            generate_temporal_predicate(&metadata, &TemporalQueryType::AsOf(timestamp)).unwrap();

        assert!(predicate.contains("SYS_START"));
        assert!(predicate.contains("SYS_END"));
        assert!(predicate.contains("2024-01-01"));
    }

    #[test]
    fn test_temporal_predicate_between() {
        let metadata = TemporalTableMetadata::default();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();
        let predicate =
            generate_temporal_predicate(&metadata, &TemporalQueryType::Between { start, end })
                .unwrap();

        assert!(predicate.contains("SYS_START"));
        assert!(predicate.contains("SYS_END"));
    }

    #[test]
    fn test_handle_temporal_insert() {
        let metadata = TemporalTableMetadata {
            table_name: "users".to_string(),
            is_system_versioned: true,
            ..Default::default()
        };

        let mut row = HashMap::new();
        row.insert("id".to_string(), yachtsql_common::types::Value::int64(1));
        row.insert(
            "name".to_string(),
            yachtsql_common::types::Value::string("Alice".to_string()),
        );

        let result = handle_temporal_insert(&metadata, row).unwrap();

        assert!(result.contains_key("SYS_START"));
        assert!(result.contains_key("SYS_END"));
    }

    #[test]
    fn test_unregister_table() {
        let mut registry = TemporalTableRegistry::new();

        registry
            .register_system_versioned_table("users".to_string(), None)
            .unwrap();

        assert!(registry.is_system_versioned("users"));

        registry.unregister_table("users");

        assert!(!registry.is_system_versioned("users"));
    }
}
