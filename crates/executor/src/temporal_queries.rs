use chrono::{DateTime, Utc};
use yachtsql_common::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemporalClause {
    AsOf(DateTime<Utc>),
    FromTo {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
    Between {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
    All,
    ContainedIn {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
}

#[derive(Debug, Clone)]
pub struct TemporalTableRef {
    pub table_name: String,
    pub temporal_clause: TemporalClause,
    pub alias: Option<String>,
}

impl TemporalTableRef {
    pub fn new(table_name: String, temporal_clause: TemporalClause) -> Self {
        Self {
            table_name,
            temporal_clause,
            alias: None,
        }
    }

    pub fn with_alias(mut self, alias: String) -> Self {
        self.alias = Some(alias);
        self
    }

    pub fn generate_predicate(&self, row_start_col: &str, row_end_col: &str) -> String {
        match &self.temporal_clause {
            TemporalClause::AsOf(timestamp) => {
                format!(
                    "{} <= '{}' AND {} > '{}'",
                    row_start_col,
                    timestamp.format("%Y-%m-%d %H:%M:%S%.f"),
                    row_end_col,
                    timestamp.format("%Y-%m-%d %H:%M:%S%.f")
                )
            }
            TemporalClause::FromTo { start, end } => {
                format!(
                    "{} < '{}' AND {} > '{}'",
                    row_start_col,
                    end.format("%Y-%m-%d %H:%M:%S%.f"),
                    row_end_col,
                    start.format("%Y-%m-%d %H:%M:%S%.f")
                )
            }
            TemporalClause::Between { start, end } => {
                format!(
                    "{} <= '{}' AND {} >= '{}'",
                    row_start_col,
                    end.format("%Y-%m-%d %H:%M:%S%.f"),
                    row_end_col,
                    start.format("%Y-%m-%d %H:%M:%S%.f")
                )
            }
            TemporalClause::All => "1=1".to_string(),
            TemporalClause::ContainedIn { start, end } => {
                format!(
                    "{} >= '{}' AND {} <= '{}'",
                    row_start_col,
                    start.format("%Y-%m-%d %H:%M:%S%.f"),
                    row_end_col,
                    end.format("%Y-%m-%d %H:%M:%S%.f")
                )
            }
        }
    }

    pub fn matches_condition(&self, row_start: DateTime<Utc>, row_end: DateTime<Utc>) -> bool {
        match &self.temporal_clause {
            TemporalClause::AsOf(timestamp) => row_start <= *timestamp && row_end > *timestamp,
            TemporalClause::FromTo { start, end } => row_start < *end && row_end > *start,
            TemporalClause::Between { start, end } => row_start <= *end && row_end >= *start,
            TemporalClause::All => true,
            TemporalClause::ContainedIn { start, end } => row_start >= *start && row_end <= *end,
        }
    }
}

pub struct TemporalQueryBuilder {
    table_name: String,
}

impl TemporalQueryBuilder {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }

    pub fn as_of(self, timestamp: DateTime<Utc>) -> TemporalTableRef {
        TemporalTableRef::new(self.table_name, TemporalClause::AsOf(timestamp))
    }

    pub fn from_to(self, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<TemporalTableRef> {
        if start >= end {
            return Err(Error::invalid_query("Start time must be before end time"));
        }
        Ok(TemporalTableRef::new(
            self.table_name,
            TemporalClause::FromTo { start, end },
        ))
    }

    pub fn between(self, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<TemporalTableRef> {
        if start > end {
            return Err(Error::invalid_query(
                "Start time must not be after end time",
            ));
        }
        Ok(TemporalTableRef::new(
            self.table_name,
            TemporalClause::Between { start, end },
        ))
    }

    pub fn all(self) -> TemporalTableRef {
        TemporalTableRef::new(self.table_name, TemporalClause::All)
    }

    pub fn contained_in(
        self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<TemporalTableRef> {
        if start > end {
            return Err(Error::invalid_query(
                "Start time must not be after end time",
            ));
        }
        Ok(TemporalTableRef::new(
            self.table_name,
            TemporalClause::ContainedIn { start, end },
        ))
    }
}

pub fn parse_temporal_clause(clause: &str) -> Result<TemporalClause> {
    let upper = clause.to_uppercase();

    if upper.starts_with("AS OF") {
        Err(Error::invalid_query("AS OF parsing not implemented"))
    } else if upper.starts_with("FROM") {
        Err(Error::invalid_query("FROM TO parsing not implemented"))
    } else if upper.starts_with("BETWEEN") {
        Err(Error::invalid_query("BETWEEN parsing not implemented"))
    } else if upper == "ALL" {
        Ok(TemporalClause::All)
    } else {
        Err(Error::invalid_query(format!(
            "Unknown temporal clause: {}",
            clause
        )))
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_as_of_predicate() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let table_ref = TemporalQueryBuilder::new("employees".to_string()).as_of(timestamp);

        let predicate = table_ref.generate_predicate("sys_start", "sys_end");
        assert!(predicate.contains("sys_start <="));
        assert!(predicate.contains("sys_end >"));
        assert!(predicate.contains("2024-01-01"));
    }

    #[test]
    fn test_from_to_predicate() {
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let table_ref = TemporalQueryBuilder::new("employees".to_string())
            .from_to(start, end)
            .unwrap();

        let predicate = table_ref.generate_predicate("sys_start", "sys_end");
        assert!(predicate.contains("sys_start <"));
        assert!(predicate.contains("sys_end >"));
    }

    #[test]
    fn test_between_predicate() {
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let table_ref = TemporalQueryBuilder::new("employees".to_string())
            .between(start, end)
            .unwrap();

        let predicate = table_ref.generate_predicate("sys_start", "sys_end");
        assert!(predicate.contains("sys_start <="));
        assert!(predicate.contains("sys_end >="));
    }

    #[test]
    fn test_all_predicate() {
        let table_ref = TemporalQueryBuilder::new("employees".to_string()).all();

        let predicate = table_ref.generate_predicate("sys_start", "sys_end");
        assert_eq!(predicate, "1=1");
    }

    #[test]
    fn test_matches_condition_as_of() {
        let timestamp = Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap();
        let table_ref = TemporalQueryBuilder::new("employees".to_string()).as_of(timestamp);

        let row_start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let row_end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        assert!(table_ref.matches_condition(row_start, row_end));

        let row_start_after = Utc.with_ymd_and_hms(2024, 7, 1, 0, 0, 0).unwrap();
        assert!(!table_ref.matches_condition(row_start_after, row_end));
    }

    #[test]
    fn test_from_to_validation() {
        let start = Utc.with_ymd_and_hms(2024, 12, 31, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let result = TemporalQueryBuilder::new("employees".to_string()).from_to(start, end);

        assert!(result.is_err());
    }

    #[test]
    fn test_temporal_ref_with_alias() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let table_ref = TemporalQueryBuilder::new("employees".to_string())
            .as_of(timestamp)
            .with_alias("e".to_string());

        assert_eq!(table_ref.alias, Some("e".to_string()));
        assert_eq!(table_ref.table_name, "employees");
    }

    #[test]
    fn test_contained_in() {
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let table_ref = TemporalQueryBuilder::new("employees".to_string())
            .contained_in(start, end)
            .unwrap();

        let row_start = Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap();
        let row_end = Utc.with_ymd_and_hms(2024, 6, 30, 23, 59, 59).unwrap();

        assert!(table_ref.matches_condition(row_start, row_end));

        let row_start_before = Utc.with_ymd_and_hms(2023, 12, 1, 0, 0, 0).unwrap();
        assert!(!table_ref.matches_condition(row_start_before, row_end));
    }

    #[test]
    fn test_parse_all_clause() {
        let clause = parse_temporal_clause("ALL").unwrap();
        assert_eq!(clause, TemporalClause::All);
    }
}
