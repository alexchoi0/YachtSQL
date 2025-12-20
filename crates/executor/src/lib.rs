#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

mod catalog;
mod error;
mod executor;
mod ir_evaluator;
mod js_udf;
mod plan;
mod session;

pub use catalog::{Catalog, ColumnDefault, UserFunction, UserProcedure, ViewDef};
pub use error::{Error, Result};
pub use executor::{PlanExecutor, plan_schema_to_schema};
pub use plan::ExecutorPlan;
pub use session::Session;
use yachtsql_optimizer::PhysicalPlan;
pub use yachtsql_storage::{Record, Table};

pub struct QueryExecutor {
    catalog: Catalog,
}

impl QueryExecutor {
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
        }
    }

    pub fn execute_sql(&mut self, sql: &str) -> yachtsql_common::error::Result<Table> {
        let logical = yachtsql_parser::parse_and_plan(sql, &self.catalog)?;
        let physical = yachtsql_optimizer::optimize(&logical)?;
        let mut session = Session::new();
        let mut executor = PlanExecutor::new(&mut self.catalog, &mut session);
        executor.execute(&physical)
    }

    pub fn execute(&mut self, plan: &PhysicalPlan) -> yachtsql_common::error::Result<Table> {
        let mut session = Session::new();
        let mut executor = PlanExecutor::new(&mut self.catalog, &mut session);
        executor.execute(plan)
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn catalog_mut(&mut self) -> &mut Catalog {
        &mut self.catalog
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

pub fn execute(
    catalog: &mut Catalog,
    session: &mut Session,
    plan: &PhysicalPlan,
) -> yachtsql_common::error::Result<Table> {
    let mut executor = PlanExecutor::new(catalog, session);
    executor.execute(plan)
}
