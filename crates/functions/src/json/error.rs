use std::fmt;

use yachtsql_common::error::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonError {
    ParseError(String),
    InvalidPath(String),
    DepthLimitExceeded { max_depth: usize },
    SizeLimitExceeded { max_size: usize, actual: usize },
    InvalidUtf8(String),
    InvalidEscape(String),
    TypeMismatch { expected: String, got: String },
    Other(String),
}

impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonError::ParseError(msg) => write!(f, "JSON parse error: {}", msg),
            JsonError::InvalidPath(msg) => write!(f, "Invalid JSON path: {}", msg),
            JsonError::DepthLimitExceeded { max_depth } => {
                write!(f, "JSON depth limit exceeded (max: {})", max_depth)
            }
            JsonError::SizeLimitExceeded { max_size, actual } => write!(
                f,
                "JSON size limit exceeded (max: {}, actual: {})",
                max_size, actual
            ),
            JsonError::InvalidUtf8(msg) => write!(f, "Invalid UTF-8 in JSON: {}", msg),
            JsonError::InvalidEscape(msg) => write!(f, "Invalid escape sequence: {}", msg),
            JsonError::TypeMismatch { expected, got } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, got)
            }
            JsonError::Other(msg) => write!(f, "JSON error: {}", msg),
        }
    }
}

impl std::error::Error for JsonError {}

impl From<serde_json::Error> for JsonError {
    fn from(err: serde_json::Error) -> Self {
        JsonError::ParseError(err.to_string())
    }
}

impl From<JsonError> for Error {
    fn from(err: JsonError) -> Self {
        Error::InvalidOperation(err.to_string())
    }
}
