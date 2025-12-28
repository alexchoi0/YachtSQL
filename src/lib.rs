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
//! The `YachtSQLEngine` creates isolated sessions with their own catalog and state.
//!
//! # Example
//!
//! ```rust,ignore
//! use yachtsql::YachtSQLEngine;
//!
//! #[tokio::main]
//! async fn main() {
//!     let engine = YachtSQLEngine::new();
//!     let session = engine.create_session();
//!
//!     // Create a table
//!     session
//!         .execute_sql("CREATE TABLE users (id INT64, name STRING)")
//!         .await
//!         .unwrap();
//!
//!     // Insert data
//!     session
//!         .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
//!         .await
//!         .unwrap();
//!
//!     // Query data
//!     let result = session
//!         .execute_sql("SELECT * FROM users WHERE id = 1")
//!         .await
//!         .unwrap();
//! }
//! ```

pub use yachtsql_common::error::{Error, Result};
pub use yachtsql_common::result::{ColumnInfo, QueryResult, Row};
pub use yachtsql_common::types::{DataType, Value};
pub use yachtsql_executor::{
    AsyncQueryExecutor, ConcurrentCatalog, ConcurrentSession, Record, Table,
};
pub use yachtsql_ir::LogicalPlan;
pub use yachtsql_optimizer::OptimizedLogicalPlan;
pub use yachtsql_parser::{CatalogProvider, Planner, PlannerError, parse_and_plan, parse_sql};
pub use yachtsql_storage::{Field, FieldMode, Schema};

pub struct YachtSQLEngine;

impl YachtSQLEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn create_session(&self) -> YachtSQLSession {
        YachtSQLSession {
            executor: AsyncQueryExecutor::new(),
        }
    }
}

impl Default for YachtSQLEngine {
    fn default() -> Self {
        Self::new()
    }
}

pub struct YachtSQLSession {
    executor: AsyncQueryExecutor,
}

impl YachtSQLSession {
    pub fn new() -> Self {
        Self {
            executor: AsyncQueryExecutor::new(),
        }
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<Table> {
        self.executor.execute_sql(sql).await
    }

    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        let table = self.executor.execute_sql(sql).await?;
        table.to_query_result()
    }

    pub async fn run(&self, sql: &str) -> Result<u64> {
        let table = self.executor.execute_sql(sql).await?;
        Ok(table.row_count() as u64)
    }

    pub fn session(&self) -> &ConcurrentSession {
        self.executor.session()
    }

    pub fn catalog(&self) -> &ConcurrentCatalog {
        self.executor.catalog()
    }

    pub fn set_default_project(&self, project: Option<String>) {
        self.executor.catalog().set_default_project(project);
    }

    pub fn get_default_project(&self) -> Option<String> {
        self.executor.catalog().get_default_project()
    }

    pub fn get_projects(&self) -> Vec<String> {
        self.executor.catalog().get_projects()
    }

    pub fn get_datasets(&self, project: &str) -> Vec<String> {
        self.executor.catalog().get_datasets(project)
    }

    pub fn get_tables_in_dataset(&self, project: &str, dataset: &str) -> Vec<String> {
        self.executor
            .catalog()
            .get_tables_in_dataset(project, dataset)
    }
}

impl Default for YachtSQLSession {
    fn default() -> Self {
        Self::new()
    }
}
