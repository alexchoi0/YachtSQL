use std::fmt;

#[derive(Debug, Clone)]
pub enum PlannerError {
    UnsupportedStatement(String),
    UnsupportedExpression(String),
    UnsupportedTableFactor(String),
    TableNotFound(String),
    ColumnNotFound(String),
    AmbiguousColumn(String),
    TypeMismatch(String),
    InvalidLiteral(String),
    InvalidFunction(String),
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerError::UnsupportedStatement(s) => write!(f, "Unsupported statement: {}", s),
            PlannerError::UnsupportedExpression(s) => write!(f, "Unsupported expression: {}", s),
            PlannerError::UnsupportedTableFactor(s) => write!(f, "Unsupported table factor: {}", s),
            PlannerError::TableNotFound(s) => write!(f, "Table not found: {}", s),
            PlannerError::ColumnNotFound(s) => write!(f, "Column not found: {}", s),
            PlannerError::AmbiguousColumn(s) => write!(f, "Ambiguous column: {}", s),
            PlannerError::TypeMismatch(s) => write!(f, "Type mismatch: {}", s),
            PlannerError::InvalidLiteral(s) => write!(f, "Invalid literal: {}", s),
            PlannerError::InvalidFunction(s) => write!(f, "Invalid function: {}", s),
        }
    }
}

impl std::error::Error for PlannerError {}

impl From<PlannerError> for yachtsql_common::error::Error {
    fn from(e: PlannerError) -> Self {
        yachtsql_common::error::Error::parse_error(e.to_string())
    }
}
