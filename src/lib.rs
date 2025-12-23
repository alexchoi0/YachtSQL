//! YachtSQL - A SQL database engine (BigQuery dialect).
//!
//! YachtSQL provides an in-memory SQL database with BigQuery dialect support,
//! featuring columnar storage and comprehensive SQL functionality.
//!
//! # Architecture
//!
//! The query processing pipeline is:
//! ```text
//! SQL String → Parser → LogicalPlan → Optimizer → PhysicalPlan → Executor → Result
//! ```
//!
//! The `YachtSQLEngine` creates isolated sessions that share a query plan cache.
//! Each session owns its own `Catalog` (tables, views, functions) and `Session`
//! state (variables, system state).
//!
//! # Example
//!
//! ```rust
//! use yachtsql::YachtSQLEngine;
//!
//! let engine = YachtSQLEngine::new();
//! let mut session = engine.create_session();
//!
//! // Create a table
//! session
//!     .execute_sql("CREATE TABLE users (id INT64, name STRING)")
//!     .unwrap();
//!
//! // Insert data
//! session
//!     .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
//!     .unwrap();
//!
//! // Query data
//! let result = session
//!     .execute_sql("SELECT * FROM users WHERE id = 1")
//!     .unwrap();
//! ```

use std::sync::Arc;

pub use yachtsql_common::error::{Error, Result};
pub use yachtsql_common::result::{ColumnInfo, QueryResult, Row};
pub use yachtsql_common::types::{DataType, Value};
#[cfg(feature = "concurrent")]
pub use yachtsql_executor::AsyncQueryExecutor;
pub use yachtsql_executor::{
    Catalog, QueryExecutor, Record, Session, SessionExecutor, SharedState, Table,
};
pub use yachtsql_ir::LogicalPlan;
pub use yachtsql_optimizer::OptimizedLogicalPlan;
pub use yachtsql_parser::{CatalogProvider, Planner, PlannerError, parse_and_plan, parse_sql};
pub use yachtsql_storage::{Field, FieldMode, Schema};

pub struct YachtSQLEngine {
    shared: Arc<SharedState>,
}

impl YachtSQLEngine {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(SharedState::new()),
        }
    }

    pub fn create_session(&self) -> YachtSQLSession {
        YachtSQLSession {
            executor: SessionExecutor::new(Arc::clone(&self.shared)),
        }
    }

    pub fn create_session_with_catalog(&self, catalog: Catalog) -> YachtSQLSession {
        YachtSQLSession {
            executor: SessionExecutor::with_catalog(Arc::clone(&self.shared), catalog),
        }
    }
}

impl Default for YachtSQLEngine {
    fn default() -> Self {
        Self::new()
    }
}

pub struct YachtSQLSession {
    executor: SessionExecutor,
}

impl YachtSQLSession {
    pub fn execute_sql(&mut self, sql: &str) -> Result<Table> {
        self.executor.execute_sql(sql)
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let table = self.executor.execute_sql(sql)?;
        table.to_query_result()
    }

    pub fn run(&mut self, sql: &str) -> Result<u64> {
        let table = self.executor.execute_sql(sql)?;
        Ok(table.row_count() as u64)
    }

    pub fn session(&self) -> &Session {
        self.executor.session()
    }

    pub fn session_mut(&mut self) -> &mut Session {
        self.executor.session_mut()
    }

    pub fn catalog(&self) -> &Catalog {
        self.executor.catalog()
    }

    pub fn catalog_mut(&mut self) -> &mut Catalog {
        self.executor.catalog_mut()
    }

    pub fn executor(&self) -> &SessionExecutor {
        &self.executor
    }

    pub fn executor_mut(&mut self) -> &mut SessionExecutor {
        &mut self.executor
    }
}
