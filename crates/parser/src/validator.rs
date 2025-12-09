use yachtsql_core::error::{Error, Result};

use crate::parser::DialectType;
use crate::parser::clickhouse_extensions::ClickHouseIndexType;

#[derive(Debug, Clone, PartialEq)]
pub enum CustomStatement {
    RefreshMaterializedView {
        name: sqlparser::ast::ObjectName,
        concurrently: bool,
    },

    DropMaterializedView {
        name: sqlparser::ast::ObjectName,
        if_exists: bool,
        cascade: bool,
    },

    CreateSequence {
        if_not_exists: bool,
        name: sqlparser::ast::ObjectName,
        start_value: Option<i64>,
        increment: Option<i64>,
        min_value: Option<Option<i64>>,
        max_value: Option<Option<i64>>,
        cycle: Option<bool>,
        cache: Option<u32>,
        owned_by: Option<(String, String)>,
    },

    AlterSequence {
        if_exists: bool,
        name: sqlparser::ast::ObjectName,
        restart: Option<Option<i64>>,
        increment: Option<i64>,
        min_value: Option<Option<i64>>,
        max_value: Option<Option<i64>>,
        cycle: Option<bool>,
        owned_by: Option<Option<(String, String)>>,
    },

    AlterTableRestartIdentity {
        table: sqlparser::ast::ObjectName,
        column: String,
        restart_with: Option<i64>,
    },

    DropSequence {
        if_exists: bool,
        names: Vec<sqlparser::ast::ObjectName>,
        cascade: bool,
        restrict: bool,
    },

    GetDiagnostics {
        scope: DiagnosticsScope,
        assignments: Vec<DiagnosticsAssignment>,
    },

    CreateDomain {
        name: sqlparser::ast::ObjectName,
        base_type: String,
        default_value: Option<String>,
        not_null: bool,
        constraints: Vec<DomainConstraint>,
    },

    AlterDomain {
        name: sqlparser::ast::ObjectName,
        action: AlterDomainAction,
    },

    DropDomain {
        if_exists: bool,
        names: Vec<sqlparser::ast::ObjectName>,
        cascade: bool,
        restrict: bool,
    },

    CreateType {
        if_not_exists: bool,
        name: sqlparser::ast::ObjectName,
        fields: Vec<CompositeTypeField>,
    },

    DropType {
        if_exists: bool,
        names: Vec<sqlparser::ast::ObjectName>,
        cascade: bool,
        restrict: bool,
    },

    SetConstraints {
        mode: SetConstraintsMode,
        constraints: SetConstraintsTarget,
    },

    ExistsTable {
        name: sqlparser::ast::ObjectName,
    },

    ExistsDatabase {
        name: sqlparser::ast::ObjectName,
    },

    Abort,

    BeginTransaction {
        isolation_level: Option<String>,
        read_only: Option<bool>,
        deferrable: Option<bool>,
    },

    ClickHouseCreateIndex {
        if_not_exists: bool,
        index_name: String,
        table_name: sqlparser::ast::ObjectName,
        columns: Vec<String>,
        index_type: ClickHouseIndexType,
        granularity: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetConstraintsMode {
    Immediate,
    Deferred,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetConstraintsTarget {
    All,
    Named(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainConstraint {
    pub name: Option<String>,

    pub expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterDomainAction {
    AddConstraint {
        name: Option<String>,
        expression: String,
    },
    DropConstraint {
        name: String,
    },
    SetDefault {
        value: String,
    },
    DropDefault,
    SetNotNull,
    DropNotNull,
    RenameConstraint {
        old_name: String,
        new_name: String,
    },
    ValidateConstraint {
        name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeTypeField {
    pub name: String,
    pub data_type: sqlparser::ast::DataType,
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
            CustomStatement::RefreshMaterializedView { name, .. } => {
                self.validate_refresh_materialized_view(name)
            }
            CustomStatement::DropMaterializedView { name, .. } => {
                self.validate_drop_materialized_view(name)
            }
            CustomStatement::CreateSequence { name, .. } => self.validate_create_sequence(name),
            CustomStatement::AlterSequence { name, .. } => self.validate_alter_sequence(name),
            CustomStatement::AlterTableRestartIdentity { table, column, .. } => {
                self.validate_alter_table_restart_identity(table, column)
            }
            CustomStatement::DropSequence {
                names,
                cascade,
                restrict,
                ..
            } => self.validate_drop_sequence(names, *cascade, *restrict),
            CustomStatement::GetDiagnostics { assignments, .. } => {
                if assignments.is_empty() {
                    return Err(Error::invalid_query(
                        "GET DIAGNOSTICS requires at least one assignment".to_string(),
                    ));
                }
                Ok(())
            }
            CustomStatement::CreateDomain { name, .. } => self.validate_create_domain(name),
            CustomStatement::AlterDomain { name, .. } => self.validate_alter_domain(name),
            CustomStatement::DropDomain {
                names,
                cascade,
                restrict,
                ..
            } => self.validate_drop_domain(names, *cascade, *restrict),
            CustomStatement::CreateType { name, fields, .. } => {
                self.validate_create_type(name, fields)
            }
            CustomStatement::DropType {
                names,
                cascade,
                restrict,
                ..
            } => self.validate_drop_type(names, *cascade, *restrict),
            CustomStatement::SetConstraints { .. } => self.validate_set_constraints(),
            CustomStatement::ExistsTable { .. } | CustomStatement::ExistsDatabase { .. } => Ok(()),
            CustomStatement::Abort => Ok(()),
            CustomStatement::BeginTransaction { .. } => Ok(()),
            CustomStatement::ClickHouseCreateIndex { .. } => {
                self.require_clickhouse("CREATE INDEX with TYPE")?;
                Ok(())
            }
        }
    }

    fn validate_set_constraints(&self) -> Result<()> {
        self.require_postgresql("SET CONSTRAINTS")?;
        Ok(())
    }

    fn validate_create_domain(&self, name: &sqlparser::ast::ObjectName) -> Result<()> {
        self.require_postgresql("CREATE DOMAIN")?;
        self.validate_object_name(name, "domain")?;
        Ok(())
    }

    fn validate_alter_domain(&self, name: &sqlparser::ast::ObjectName) -> Result<()> {
        self.require_postgresql("ALTER DOMAIN")?;
        self.validate_object_name(name, "domain")?;
        Ok(())
    }

    fn validate_drop_domain(
        &self,
        names: &[sqlparser::ast::ObjectName],
        cascade: bool,
        restrict: bool,
    ) -> Result<()> {
        self.require_postgresql("DROP DOMAIN")?;

        if cascade && restrict {
            return Err(Error::invalid_query(
                "DROP DOMAIN cannot have both CASCADE and RESTRICT".to_string(),
            ));
        }

        for name in names {
            self.validate_object_name(name, "domain")?;
        }

        Ok(())
    }

    fn validate_refresh_materialized_view(&self, name: &sqlparser::ast::ObjectName) -> Result<()> {
        self.require_postgresql("REFRESH MATERIALIZED VIEW")?;
        self.validate_object_name(name, "materialized view")?;
        Ok(())
    }

    fn validate_drop_materialized_view(&self, name: &sqlparser::ast::ObjectName) -> Result<()> {
        self.require_postgresql("DROP MATERIALIZED VIEW")?;
        self.validate_object_name(name, "materialized view")?;
        Ok(())
    }

    fn require_postgresql(&self, feature: &str) -> Result<()> {
        if self.dialect != DialectType::PostgreSQL {
            return Err(Error::invalid_query(format!(
                "{} is only supported in PostgreSQL dialect",
                feature
            )));
        }
        Ok(())
    }

    fn require_clickhouse(&self, feature: &str) -> Result<()> {
        if self.dialect != DialectType::ClickHouse {
            return Err(Error::invalid_query(format!(
                "{} is only supported in ClickHouse dialect",
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

    fn validate_create_sequence(&self, name: &sqlparser::ast::ObjectName) -> Result<()> {
        self.require_postgresql("CREATE SEQUENCE")?;
        self.validate_object_name(name, "sequence")?;
        Ok(())
    }

    fn validate_alter_sequence(&self, name: &sqlparser::ast::ObjectName) -> Result<()> {
        self.require_postgresql("ALTER SEQUENCE")?;
        self.validate_object_name(name, "sequence")?;
        Ok(())
    }

    fn validate_alter_table_restart_identity(
        &self,
        table: &sqlparser::ast::ObjectName,
        column: &str,
    ) -> Result<()> {
        self.require_postgresql("ALTER TABLE ... RESTART IDENTITY")?;
        self.validate_object_name(table, "table")?;
        if column.is_empty() {
            return Err(Error::invalid_query(
                "Column name cannot be empty".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_drop_sequence(
        &self,
        names: &[sqlparser::ast::ObjectName],
        cascade: bool,
        restrict: bool,
    ) -> Result<()> {
        self.require_postgresql("DROP SEQUENCE")?;

        if cascade && restrict {
            return Err(Error::invalid_query(
                "DROP SEQUENCE cannot have both CASCADE and RESTRICT".to_string(),
            ));
        }

        for name in names {
            self.validate_object_name(name, "sequence")?;
        }

        Ok(())
    }

    fn validate_create_type(
        &self,
        name: &sqlparser::ast::ObjectName,
        fields: &[CompositeTypeField],
    ) -> Result<()> {
        self.require_postgresql("CREATE TYPE")?;
        self.validate_object_name(name, "type")?;

        if fields.is_empty() {
            return Err(Error::invalid_query(
                "Composite type must have at least one field".to_string(),
            ));
        }

        let mut seen_names = std::collections::HashSet::new();
        for field in fields {
            let lower_name = field.name.to_lowercase();
            if !seen_names.insert(lower_name) {
                return Err(Error::invalid_query(format!(
                    "Duplicate field name '{}' in composite type",
                    field.name
                )));
            }
        }

        Ok(())
    }

    fn validate_drop_type(
        &self,
        names: &[sqlparser::ast::ObjectName],
        cascade: bool,
        restrict: bool,
    ) -> Result<()> {
        self.require_postgresql("DROP TYPE")?;

        if cascade && restrict {
            return Err(Error::invalid_query(
                "DROP TYPE cannot have both CASCADE and RESTRICT".to_string(),
            ));
        }

        for name in names {
            self.validate_object_name(name, "type")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
    use sqlparser::tokenizer::Span;

    use super::*;

    fn make_object_name(name: &str) -> ObjectName {
        ObjectName(vec![ObjectNamePart::Identifier(make_ident(name))])
    }

    fn make_qualified_name(schema: &str, table: &str) -> ObjectName {
        ObjectName(vec![
            ObjectNamePart::Identifier(make_ident(schema)),
            ObjectNamePart::Identifier(make_ident(table)),
        ])
    }

    fn make_ident(name: &str) -> Ident {
        Ident {
            value: name.to_string(),
            quote_style: None,
            span: Span::empty(),
        }
    }

    #[test]
    fn test_validate_refresh_materialized_view_postgresql() {
        let validator = StatementValidator::new(DialectType::PostgreSQL);
        let stmt = CustomStatement::RefreshMaterializedView {
            name: make_object_name("my_view"),
            concurrently: false,
        };

        let result = validator.validate_custom(&stmt);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_refresh_materialized_view_bigquery_fails() {
        let validator = StatementValidator::new(DialectType::BigQuery);
        let stmt = CustomStatement::RefreshMaterializedView {
            name: make_object_name("my_view"),
            concurrently: false,
        };

        let result = validator.validate_custom(&stmt);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("PostgreSQL"));
    }

    #[test]
    fn test_validate_drop_materialized_view_postgresql() {
        let validator = StatementValidator::new(DialectType::PostgreSQL);
        let stmt = CustomStatement::DropMaterializedView {
            name: make_object_name("my_view"),
            if_exists: true,
            cascade: false,
        };

        let result = validator.validate_custom(&stmt);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_drop_materialized_view_bigquery_fails() {
        let validator = StatementValidator::new(DialectType::BigQuery);
        let stmt = CustomStatement::DropMaterializedView {
            name: make_object_name("my_view"),
            if_exists: false,
            cascade: false,
        };

        let result = validator.validate_custom(&stmt);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_qualified_object_name() {
        let validator = StatementValidator::new(DialectType::PostgreSQL);
        let stmt = CustomStatement::RefreshMaterializedView {
            name: make_qualified_name("public", "my_view"),
            concurrently: false,
        };

        let result = validator.validate_custom(&stmt);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_empty_object_name_fails() {
        let validator = StatementValidator::new(DialectType::PostgreSQL);
        let stmt = CustomStatement::RefreshMaterializedView {
            name: ObjectName(vec![]),
            concurrently: false,
        };

        let result = validator.validate_custom(&stmt);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }
}
