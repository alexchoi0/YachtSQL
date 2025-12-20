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
//! The `YachtSQLEngine` struct orchestrates this entire pipeline and owns
//! the `Catalog` (tables, views, functions) and `Session` (variables, system state).
//!
//! # Example
//!
//! ```rust
//! use yachtsql::YachtSQLEngine;
//!
//! let mut engine = YachtSQLEngine::new();
//!
//! // Create a table
//! engine
//!     .execute("CREATE TABLE users (id INT64, name STRING)")
//!     .unwrap();
//!
//! // Insert data
//! engine
//!     .execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
//!     .unwrap();
//!
//! // Query data
//! let result = engine.execute("SELECT * FROM users WHERE id = 1").unwrap();
//! ```

pub use yachtsql_common::error::{Error, Result};
pub use yachtsql_common::types::{DataType, Value};
pub use yachtsql_executor::{Catalog, QueryExecutor, Record, Session, Table};
pub use yachtsql_ir::LogicalPlan;
pub use yachtsql_optimizer::PhysicalPlan;
pub use yachtsql_parser::{CatalogProvider, Planner, PlannerError, parse_and_plan, parse_sql};
pub use yachtsql_storage::{Field, FieldMode, Schema};

pub struct YachtSQLEngine {
    executor: QueryExecutor,
    session: Session,
}

impl YachtSQLEngine {
    pub fn new() -> Self {
        Self {
            executor: QueryExecutor::new(),
            session: Session::new(),
        }
    }

    pub fn execute(&mut self, sql: &str) -> Result<Table> {
        self.executor.execute_sql(sql)
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn session_mut(&mut self) -> &mut Session {
        &mut self.session
    }

    pub fn executor(&self) -> &QueryExecutor {
        &self.executor
    }

    pub fn executor_mut(&mut self) -> &mut QueryExecutor {
        &mut self.executor
    }
}

impl Default for YachtSQLEngine {
    fn default() -> Self {
        Self::new()
    }
}
