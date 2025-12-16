use yachtsql_core::error::{Error, Result};

use crate::parser::DialectType;

#[derive(Debug, Clone, PartialEq)]
pub enum CustomStatement {
    GetDiagnostics {
        scope: DiagnosticsScope,
        assignments: Vec<DiagnosticsAssignment>,
    },

    ExistsTable {
        name: sqlparser::ast::ObjectName,
    },

    ExistsDatabase {
        name: sqlparser::ast::ObjectName,
    },

    Abort,

    Loop {
        label: Option<String>,
        body: String,
    },

    Repeat {
        label: Option<String>,
        body: String,
        until_condition: String,
    },

    For {
        label: Option<String>,
        variable: String,
        query: String,
        body: String,
    },

    Leave {
        label: Option<String>,
    },

    Continue {
        label: Option<String>,
    },

    Break {
        label: Option<String>,
    },

    While {
        label: Option<String>,
        condition: String,
        body: String,
    },

    BeginTransaction {
        isolation_level: Option<String>,
        read_only: Option<bool>,
        deferrable: Option<bool>,
    },

    CreateSnapshotTable {
        name: sqlparser::ast::ObjectName,
        source_table: sqlparser::ast::ObjectName,
        if_not_exists: bool,
        for_system_time: Option<String>,
        options: Vec<(String, String)>,
    },

    DropSnapshotTable {
        name: sqlparser::ast::ObjectName,
        if_exists: bool,
    },

    ExportData {
        uri: String,
        format: ExportFormat,
        overwrite: bool,
        header: bool,
        field_delimiter: Option<char>,
        compression: Option<String>,
        query: String,
    },

    LoadData {
        table_name: sqlparser::ast::ObjectName,
        overwrite: bool,
        is_temp: bool,
        temp_table_schema: Option<Vec<(String, String)>>,
        format: ExportFormat,
        uris: Vec<String>,
        allow_schema_update: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Csv,
    Json,
    Parquet,
    Avro,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticsScope {
    Current,
    Exception,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticsItem {
    ReturnedSqlstate,
    MessageText,
    RowCount,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticsAssignment {
    pub target: String,
    pub item: DiagnosticsItem,
}

pub struct StatementValidator {
    dialect: DialectType,
}

impl StatementValidator {
    pub fn new(dialect: DialectType) -> Self {
        Self { dialect }
    }

    pub fn validate_custom(&self, stmt: &CustomStatement) -> Result<()> {
        match stmt {
            CustomStatement::GetDiagnostics { assignments, .. } => {
                if assignments.is_empty() {
                    return Err(Error::invalid_query(
                        "GET DIAGNOSTICS requires at least one assignment".to_string(),
                    ));
                }
                Ok(())
            }
            CustomStatement::ExistsTable { .. } | CustomStatement::ExistsDatabase { .. } => Ok(()),
            CustomStatement::Abort => Ok(()),
            CustomStatement::BeginTransaction { .. } => Ok(()),
            CustomStatement::Loop { .. }
            | CustomStatement::Repeat { .. }
            | CustomStatement::For { .. }
            | CustomStatement::Leave { .. }
            | CustomStatement::Continue { .. }
            | CustomStatement::Break { .. }
            | CustomStatement::While { .. } => Ok(()),
            CustomStatement::CreateSnapshotTable { name, .. } => {
                self.require_bigquery("CREATE SNAPSHOT TABLE")?;
                self.validate_object_name(name, "snapshot table")?;
                Ok(())
            }
            CustomStatement::DropSnapshotTable { name, .. } => {
                self.require_bigquery("DROP SNAPSHOT TABLE")?;
                self.validate_object_name(name, "snapshot table")?;
                Ok(())
            }
            CustomStatement::ExportData { .. } => {
                self.require_bigquery("EXPORT DATA")?;
                Ok(())
            }
            CustomStatement::LoadData { .. } => {
                self.require_bigquery("LOAD DATA")?;
                Ok(())
            }
        }
    }

    fn require_bigquery(&self, feature: &str) -> Result<()> {
        if self.dialect != DialectType::BigQuery {
            return Err(Error::invalid_query(format!(
                "{} is only supported in BigQuery dialect",
                feature
            )));
        }
        Ok(())
    }

    fn validate_object_name(
        &self,
        name: &sqlparser::ast::ObjectName,
        object_type: &str,
    ) -> Result<()> {
        if name.0.is_empty() {
            return Err(Error::invalid_query(format!(
                "The {} name cannot be empty",
                object_type
            )));
        }
        Ok(())
    }
}
