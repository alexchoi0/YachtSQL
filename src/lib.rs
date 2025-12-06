//! YachtSQL - A SQL database engine.

pub use yachtsql_core::diagnostics;
pub use yachtsql_core::error::{Error, Result};
pub use yachtsql_core::types::{DataType, Value, collation};
pub use yachtsql_executor::{QueryExecutor, RecordBatch};
pub use yachtsql_parser::{CustomStatement, DialectType, Parser, Statement};
pub use yachtsql_storage::{
    Field, FieldMode, IsolationLevel, Schema, SharedTransactionState, Storage, Table, Transaction,
    TransactionManager,
};

pub mod mvcc {
    pub use yachtsql_storage::mvcc::{RowVersion, VersionStore};
}

pub use yachtsql_capability::{FeatureId, FeatureRegistry};
