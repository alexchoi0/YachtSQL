//! YachtSQL - A SQL database engine.

pub use yachtsql_common::diagnostics;
pub use yachtsql_common::error::{Error, Result};
pub use yachtsql_common::types::{DataType, Value, collation};
pub use yachtsql_executor::{QueryExecutor, Table};
pub use yachtsql_parser::{CustomStatement, DialectType, Parser, Statement};
pub use yachtsql_storage::{
    Field, FieldMode, IsolationLevel, Schema, Storage, Transaction, TransactionManager,
};

pub mod mvcc {
    pub use yachtsql_storage::mvcc::{RowVersion, VersionStore};
}

pub use yachtsql_capability::{FeatureId, FeatureRegistry};
