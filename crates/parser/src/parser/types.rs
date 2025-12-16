use sqlparser::ast::Statement as SqlStatement;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum DialectType {
    BigQuery,
}

impl std::fmt::Display for DialectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DialectType::BigQuery => write!(f, "BigQuery"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandardStatement {
    statement: Box<SqlStatement>,
    merge_returning: Option<String>,
}

impl StandardStatement {
    pub fn new(statement: SqlStatement, merge_returning: Option<String>) -> Self {
        Self {
            statement: Box::new(statement),
            merge_returning,
        }
    }

    pub fn ast(&self) -> &SqlStatement {
        &self.statement
    }

    pub fn ast_mut(&mut self) -> &mut SqlStatement {
        &mut self.statement
    }

    pub fn merge_returning(&self) -> Option<&str> {
        self.merge_returning.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Standard(StandardStatement),
    Custom(crate::validator::CustomStatement),
}

impl Statement {
    pub fn unwrap_standard(&self) -> &SqlStatement {
        match self {
            Statement::Standard(std_stmt) => std_stmt.ast(),
            Statement::Custom(_) => panic!("Expected Standard statement, found Custom"),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct JsonValueRewriteOptions {
    pub returning: Option<String>,
    pub on_empty: Option<String>,
    pub on_error: Option<String>,
    pub on_empty_default: Option<String>,
    pub on_error_default: Option<String>,
}

impl JsonValueRewriteOptions {
    pub fn is_empty(&self) -> bool {
        self.returning.is_none()
            && self.on_empty.is_none()
            && self.on_error.is_none()
            && self.on_empty_default.is_none()
            && self.on_error_default.is_none()
    }
}

pub(super) struct JsonValueRewriteResult {
    pub arg1: String,
    pub arg2: String,
    pub options: JsonValueRewriteOptions,
}

pub const JSON_VALUE_OPTIONS_PREFIX: &str = "__YACHTSQL_JSON_OPTS__";
