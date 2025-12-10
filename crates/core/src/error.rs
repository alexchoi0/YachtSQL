use std::fmt;

use crate::diagnostics;

pub type Result<T> = std::result::Result<T, Error>;

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Table does not exist: {0}")]
    TableNotFound(String),

    #[error("Dataset not found: {0}")]
    DatasetNotFound(String),

    #[error("Job not found: {0}")]
    JobNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("comparison involving NULL")]
    NullComparison,

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("Cannot {operation}: no active transaction")]
    NoActiveTransaction { operation: String },

    #[error("Cannot {operation}: an explicit transaction is already active")]
    ActiveSqlTransaction { operation: String },

    #[error("Savepoint not found: {name}")]
    SavepointNotFound { name: String },

    #[error("Cannot {operation}: the current transaction is aborted and must be rolled back")]
    TransactionAborted { operation: String },

    #[error("CHECK constraint violation: {message}")]
    CheckConstraintViolation {
        message: String,

        constraint: Option<String>,

        expression: String,
    },

    #[error("UNIQUE constraint violation: {0}")]
    UniqueConstraintViolation(String),

    #[error("{message}")]
    ForeignKeyViolation {
        child_table: String,

        fk_columns: Vec<String>,

        parent_table: String,

        parent_columns: Vec<String>,

        message: String,
    },

    #[error("Infinite recursion detected: {0}")]
    InfiniteRecursion(String),

    #[error("Recursion depth limit exceeded: {0}")]
    RecursionDepthExceeded(String),

    #[error("Recursive CTE has invalid structure: {0}")]
    RecursiveCteInvalidStructure(String),

    #[error("Recursive CTE schema mismatch: {0}")]
    RecursiveCteSchemaMismatch(String),

    #[error("Recursive CTE infinite loop detected: {0}")]
    RecursiveCteInfiniteLoop(String),

    #[error("Circular dependency detected in CTEs: {0}")]
    CteCircularDependency(String),

    #[error("CTE '{0}' referenced before definition")]
    CteForwardReference(String),

    #[error("CTE '{0}' not found")]
    CteUndefinedReference(String),

    #[error("Cardinality violation: expected {expected}, got {actual}")]
    CardinalityViolation { expected: usize, actual: usize },

    #[error(
        "MERGE matched multiple rows ({actual} rows) for the same target row. Each source row must match at most one target row. Check your ON condition for duplicate matches."
    )]
    MergeCardinalityViolation { actual: usize },

    #[error("Column count mismatch: expected {expected}, got {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },

    #[error("Division by zero")]
    DivisionByZero,

    #[error("Arithmetic overflow in {operation}: {left} and {right}")]
    ArithmeticOverflow {
        operation: String,

        left: String,

        right: String,
    },

    #[error("NOT NULL constraint violation for column '{column}'")]
    NotNullViolation { column: String },

    #[error("Cannot coerce {from_type} to {to_type}: {reason}")]
    TypeCoercionError {
        from_type: String,

        to_type: String,

        reason: String,
    },

    #[error("Invalid JSON path '{path}': {reason}")]
    InvalidJsonPath { path: String, reason: String },

    #[error("JSON type error: expected {expected}, got {actual}")]
    JsonTypeError { expected: String, actual: String },

    #[error("Lateral correlation error for table '{table}': {message}")]
    LateralCorrelationError { table: String, message: String },

    #[error("Ambiguous column '{column}' in tables: {tables}")]
    AmbiguousColumn { column: String, tables: String },

    #[error("Undefined collation '{name}'")]
    UndefinedCollation { name: String },

    #[error("Cannot apply collation '{collation}' to non-string type '{type_name}'")]
    CollationOnNonString {
        collation: String,

        type_name: String,
    },

    #[error(
        "{operation} column count mismatch: left has {left_count} columns, right has {right_count} columns"
    )]
    SetOperationColumnMismatch {
        operation: String,

        left_count: usize,

        right_count: usize,
    },

    #[error(
        "{operation} type mismatch at column {column_index}: left is {left_type}, right is {right_type}"
    )]
    SetOperationTypeMismatch {
        operation: String,

        column_index: usize,

        left_type: String,

        right_type: String,
    },

    #[error("Recursive CTE '{cte_name}' must be materialized")]
    RecursiveCTENotMaterialized { cte_name: String },

    #[error("Invalid join type '{join_type}' for LATERAL join")]
    LateralInvalidJoinType { join_type: String },

    #[error("Invalid cursor state: {0}")]
    InvalidCursorState(String),

    #[error("Invalid cursor name: {0}")]
    InvalidCursorName(String),

    #[error("Duplicate cursor: {0}")]
    DuplicateCursor(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Error {
    pub fn invalid_query(msg: impl fmt::Display) -> Self {
        Error::InvalidQuery(msg.to_string())
    }

    pub fn parse_error(msg: impl fmt::Display) -> Self {
        Error::ParseError(msg.to_string())
    }

    pub fn type_mismatch(expected: impl fmt::Display, actual: impl fmt::Display) -> Self {
        Error::TypeMismatch {
            expected: expected.to_string(),
            actual: actual.to_string(),
        }
    }

    pub fn internal(msg: impl fmt::Display) -> Self {
        Error::InternalError(msg.to_string())
    }

    pub fn recursive_cte_invalid_structure(msg: impl fmt::Display) -> Self {
        Error::RecursiveCteInvalidStructure(msg.to_string())
    }

    pub fn recursive_cte_schema_mismatch(msg: impl fmt::Display) -> Self {
        Error::RecursiveCteSchemaMismatch(msg.to_string())
    }

    pub fn recursive_cte_infinite_loop(msg: impl fmt::Display) -> Self {
        Error::RecursiveCteInfiniteLoop(msg.to_string())
    }

    pub fn cte_circular_dependency(msg: impl fmt::Display) -> Self {
        Error::CteCircularDependency(msg.to_string())
    }

    pub fn cte_forward_reference(cte_name: impl fmt::Display) -> Self {
        Error::CteForwardReference(cte_name.to_string())
    }

    pub fn cte_undefined_reference(cte_name: impl fmt::Display) -> Self {
        Error::CteUndefinedReference(cte_name.to_string())
    }

    pub fn type_coercion_error(
        from_type: impl fmt::Display,
        to_type: impl fmt::Display,
        reason: impl fmt::Display,
    ) -> Self {
        Error::TypeCoercionError {
            from_type: from_type.to_string(),
            to_type: to_type.to_string(),
            reason: reason.to_string(),
        }
    }

    pub fn invalid_json_path(path: impl fmt::Display, reason: impl fmt::Display) -> Self {
        Error::InvalidJsonPath {
            path: path.to_string(),
            reason: reason.to_string(),
        }
    }

    pub fn check_constraint_violation(
        constraint: Option<impl fmt::Display>,
        expression: impl fmt::Display,
    ) -> Self {
        let expression_str = expression.to_string();
        let constraint_str = constraint.map(|c| c.to_string());
        let message = match &constraint_str {
            Some(name) => format!("{} (constraint '{}')", expression_str, name),
            None => expression_str.clone(),
        };

        Error::CheckConstraintViolation {
            message,
            constraint: constraint_str,
            expression: expression_str,
        }
    }

    pub fn json_type_error(expected: impl fmt::Display, actual: impl fmt::Display) -> Self {
        Error::JsonTypeError {
            expected: expected.to_string(),
            actual: actual.to_string(),
        }
    }

    pub fn lateral_correlation_error(table: impl fmt::Display, message: impl fmt::Display) -> Self {
        Error::LateralCorrelationError {
            table: table.to_string(),
            message: message.to_string(),
        }
    }

    pub fn active_sql_transaction(operation: impl fmt::Display) -> Self {
        Error::ActiveSqlTransaction {
            operation: operation.to_string(),
        }
    }

    pub fn undefined_collation(name: impl fmt::Display) -> Self {
        Error::UndefinedCollation {
            name: name.to_string(),
        }
    }

    pub fn ambiguous_column(column: impl fmt::Display, tables: impl fmt::Display) -> Self {
        Error::AmbiguousColumn {
            column: column.to_string(),
            tables: tables.to_string(),
        }
    }

    pub fn collation_on_non_string(
        collation: impl fmt::Display,
        type_name: impl fmt::Display,
    ) -> Self {
        Error::CollationOnNonString {
            collation: collation.to_string(),
            type_name: type_name.to_string(),
        }
    }

    pub fn set_operation_column_mismatch(
        operation: impl fmt::Display,
        left_count: usize,
        right_count: usize,
    ) -> Self {
        Error::SetOperationColumnMismatch {
            operation: operation.to_string(),
            left_count,
            right_count,
        }
    }

    pub fn set_operation_type_mismatch(
        operation: impl fmt::Display,
        column_index: usize,
        left_type: impl fmt::Display,
        right_type: impl fmt::Display,
    ) -> Self {
        Error::SetOperationTypeMismatch {
            operation: operation.to_string(),
            column_index,
            left_type: left_type.to_string(),
            right_type: right_type.to_string(),
        }
    }

    pub fn recursive_cte_not_materialized(cte_name: impl fmt::Display) -> Self {
        Error::RecursiveCTENotMaterialized {
            cte_name: cte_name.to_string(),
        }
    }

    pub fn lateral_invalid_join_type(join_type: impl fmt::Display) -> Self {
        Error::LateralInvalidJoinType {
            join_type: join_type.to_string(),
        }
    }

    pub fn invalid_cursor_state(msg: impl fmt::Display) -> Self {
        Error::InvalidCursorState(msg.to_string())
    }

    pub fn invalid_cursor_name(msg: impl fmt::Display) -> Self {
        Error::InvalidCursorName(msg.to_string())
    }

    pub fn duplicate_cursor(msg: impl fmt::Display) -> Self {
        Error::DuplicateCursor(msg.to_string())
    }

    pub fn sqlstate(&self) -> &'static str {
        match self {
            Error::CardinalityViolation { .. }
            | Error::MergeCardinalityViolation { .. }
            | Error::ColumnCountMismatch { .. } => diagnostics::CARDINALITY_VIOLATION.as_str(),

            Error::TypeMismatch { .. } | Error::SchemaMismatch(_) | Error::NullComparison => {
                diagnostics::DATA_EXCEPTION.as_str()
            }
            Error::DivisionByZero => diagnostics::DIVISION_BY_ZERO.as_str(),
            Error::ArithmeticOverflow { .. } => diagnostics::NUMERIC_VALUE_OUT_OF_RANGE.as_str(),
            Error::TypeCoercionError { .. } => diagnostics::INVALID_CAST_SPECIFICATION.as_str(),
            Error::InvalidJsonPath { .. } => diagnostics::INVALID_JSON_PATH.as_str(),
            Error::JsonTypeError { .. } => diagnostics::INVALID_JSON_TEXT.as_str(),

            Error::ConstraintViolation(_) => diagnostics::INTEGRITY_CONSTRAINT_VIOLATION.as_str(),
            Error::CheckConstraintViolation { .. } => diagnostics::CHECK_VIOLATION.as_str(),
            Error::UniqueConstraintViolation(_) => diagnostics::UNIQUE_VIOLATION.as_str(),
            Error::ForeignKeyViolation { .. } => diagnostics::FOREIGN_KEY_VIOLATION.as_str(),
            Error::NotNullViolation { .. } => diagnostics::NOT_NULL_VIOLATION.as_str(),

            Error::InvalidQuery(_) | Error::ParseError(_) => diagnostics::SYNTAX_ERROR.as_str(),
            Error::TableNotFound(_)
            | Error::CteForwardReference(_)
            | Error::CteUndefinedReference(_) => diagnostics::UNDEFINED_TABLE.as_str(),
            Error::ColumnNotFound(_) => diagnostics::UNDEFINED_COLUMN.as_str(),
            Error::UnsupportedFeature(_) => diagnostics::FEATURE_NOT_SUPPORTED.as_str(),
            Error::LateralCorrelationError { .. } => diagnostics::UNDEFINED_COLUMN.as_str(),
            Error::AmbiguousColumn { .. } => diagnostics::AMBIGUOUS_COLUMN.as_str(),
            Error::UndefinedCollation { .. } => diagnostics::UNDEFINED_OBJECT.as_str(),
            Error::CollationOnNonString { .. } => diagnostics::CANNOT_COERCE.as_str(),
            Error::SetOperationColumnMismatch { .. }
            | Error::RecursiveCTENotMaterialized { .. }
            | Error::LateralInvalidJoinType { .. } => diagnostics::SYNTAX_ERROR.as_str(),
            Error::SetOperationTypeMismatch { .. } => diagnostics::DATATYPE_MISMATCH.as_str(),

            Error::NoActiveTransaction { .. } => diagnostics::INVALID_TRANSACTION_STATE.as_str(),
            Error::ActiveSqlTransaction { .. } => diagnostics::ACTIVE_SQL_TRANSACTION.as_str(),
            Error::SavepointNotFound { .. } => {
                diagnostics::INVALID_SAVEPOINT_SPECIFICATION.as_str()
            }
            Error::TransactionAborted { .. } => diagnostics::IN_FAILED_SQL_TRANSACTION.as_str(),

            Error::InvalidCursorState(_) => diagnostics::INVALID_CURSOR_STATE.as_str(),
            Error::InvalidCursorName(_) => diagnostics::INVALID_CURSOR_NAME.as_str(),
            Error::DuplicateCursor(_) => diagnostics::DUPLICATE_CURSOR.as_str(),

            Error::RecursiveCteInvalidStructure(_)
            | Error::RecursiveCteSchemaMismatch(_)
            | Error::CteCircularDependency(_) => diagnostics::INVALID_RECURSION.as_str(),

            Error::InfiniteRecursion(_)
            | Error::RecursionDepthExceeded(_)
            | Error::RecursiveCteInfiniteLoop(_) => diagnostics::STATEMENT_TOO_COMPLEX.as_str(),

            Error::DatasetNotFound(_)
            | Error::JobNotFound(_)
            | Error::InvalidOperation(_)
            | Error::ExecutionError(_) => diagnostics::SYSTEM_ERROR.as_str(),

            Error::InternalError(_) | Error::Other(_) => diagnostics::INTERNAL_ERROR.as_str(),
        }
    }

    pub fn type_mismatch_value(expected: impl fmt::Display, value: &crate::types::Value) -> Self {
        Error::TypeMismatch {
            expected: expected.to_string(),
            actual: value.data_type().to_string(),
        }
    }

    pub fn invalid_operation(msg: impl fmt::Display) -> Self {
        Error::InvalidOperation(msg.to_string())
    }

    pub fn execution_error(msg: impl fmt::Display) -> Self {
        Error::ExecutionError(msg.to_string())
    }

    pub fn unsupported_feature(msg: impl fmt::Display) -> Self {
        Error::UnsupportedFeature(msg.to_string())
    }

    pub fn schema_mismatch(msg: impl fmt::Display) -> Self {
        Error::SchemaMismatch(msg.to_string())
    }

    pub fn constraint_violation(msg: impl fmt::Display) -> Self {
        Error::ConstraintViolation(msg.to_string())
    }

    pub fn table_not_found(table: impl fmt::Display) -> Self {
        Error::TableNotFound(table.to_string())
    }

    pub fn column_not_found(column: impl fmt::Display) -> Self {
        Error::ColumnNotFound(column.to_string())
    }

    pub fn dataset_not_found(dataset: impl fmt::Display) -> Self {
        Error::DatasetNotFound(dataset.to_string())
    }

    pub fn details(&self) -> Option<String> {
        match self {
            Error::ForeignKeyViolation {
                child_table,
                fk_columns,
                parent_table,
                parent_columns,
                ..
            } => Some(format!(
                "Foreign key from {}.{} references {}.{}",
                child_table,
                fk_columns.join(", "),
                parent_table,
                parent_columns.join(", ")
            )),
            Error::TypeMismatch { expected, actual } => Some(format!(
                "Expected type: {}, Actual type: {}",
                expected, actual
            )),
            Error::TypeCoercionError {
                from_type,
                to_type,
                reason,
            } => Some(format!(
                "Cannot coerce {} to {}: {}",
                from_type, to_type, reason
            )),
            Error::InvalidJsonPath { path, reason } => {
                Some(format!("Invalid JSON path '{}': {}", path, reason))
            }
            Error::JsonTypeError { expected, actual } => {
                Some(format!("Expected JSON type: {}, Got: {}", expected, actual))
            }
            Error::LateralCorrelationError { table, message } => {
                Some(format!("Table '{}': {}", table, message))
            }
            Error::CheckConstraintViolation {
                constraint,
                expression,
                ..
            } => constraint
                .as_ref()
                .map(|name| format!("Constraint '{}' failed for expression {}", name, expression)),
            _ => None,
        }
    }
}
