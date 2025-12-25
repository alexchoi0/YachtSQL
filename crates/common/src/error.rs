use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub enum Error {
    ParseError(String),
    InvalidQuery(String),
    TableNotFound(String),
    FunctionNotFound(String),
    ColumnNotFound(String),
    TypeMismatch { expected: String, actual: String },
    SchemaMismatch(String),
    UnsupportedFeature(String),
    DivisionByZero,
    Overflow,
    Internal(String),
}

impl Error {
    pub fn parse_error(msg: impl Into<String>) -> Self {
        Error::ParseError(msg.into())
    }

    pub fn invalid_query(msg: impl Into<String>) -> Self {
        Error::InvalidQuery(msg.into())
    }

    pub fn table_not_found(name: impl Into<String>) -> Self {
        Error::TableNotFound(name.into())
    }

    pub fn function_not_found(name: impl Into<String>) -> Self {
        Error::FunctionNotFound(name.into())
    }

    pub fn column_not_found(name: impl Into<String>) -> Self {
        Error::ColumnNotFound(name.into())
    }

    pub fn type_mismatch(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        Error::TypeMismatch {
            expected: msg.clone(),
            actual: msg,
        }
    }

    pub fn type_mismatch_with(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Error::TypeMismatch {
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    pub fn schema_mismatch(msg: impl Into<String>) -> Self {
        Error::SchemaMismatch(msg.into())
    }

    pub fn unsupported(msg: impl Into<String>) -> Self {
        Error::UnsupportedFeature(msg.into())
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Error::Internal(msg.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ParseError(msg) => write!(f, "Parse error: {}", msg),
            Error::InvalidQuery(msg) => write!(f, "Invalid query: {}", msg),
            Error::TableNotFound(name) => write!(f, "Table not found: {}", name),
            Error::FunctionNotFound(name) => write!(f, "Function not found: {}", name),
            Error::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            Error::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, actual)
            }
            Error::SchemaMismatch(msg) => write!(f, "Schema mismatch: {}", msg),
            Error::UnsupportedFeature(msg) => write!(f, "Unsupported feature: {}", msg),
            Error::DivisionByZero => write!(f, "Division by zero"),
            Error::Overflow => write!(f, "Numeric overflow"),
            Error::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for Error {}
