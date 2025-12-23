#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::if_same_then_else)]
#![allow(clippy::manual_strip)]

mod catalog;
mod error;
mod executor;
mod ir_evaluator;
mod js_udf;
mod plan;
mod session;

#[cfg(feature = "concurrent")]
mod async_executor;
#[cfg(feature = "concurrent")]
mod concurrent_catalog;
#[cfg(feature = "concurrent")]
mod concurrent_session;

use std::num::NonZeroUsize;

#[cfg(feature = "concurrent")]
pub use async_executor::AsyncQueryExecutor;
pub use catalog::{Catalog, ColumnDefault, UserFunction, UserProcedure, ViewDef};
#[cfg(feature = "concurrent")]
pub use concurrent_catalog::{ConcurrentCatalog, TableLockSet};
#[cfg(feature = "concurrent")]
pub use concurrent_session::ConcurrentSession;
pub use error::{Error, Result};
pub use executor::{PlanExecutor, plan_schema_to_schema};
pub use ir_evaluator::{IrEvaluator, UserFunctionDef};
use lru::LruCache;
pub use plan::PhysicalPlan;
use serde::{Deserialize, Serialize};
pub use session::Session;
use yachtsql_optimizer::OptimizedLogicalPlan;
pub use yachtsql_storage::{Record, Table};

const PLAN_CACHE_SIZE: usize = 10000;

fn default_plan_cache() -> LruCache<String, OptimizedLogicalPlan> {
    LruCache::new(NonZeroUsize::new(PLAN_CACHE_SIZE).unwrap())
}

fn is_cacheable_plan(plan: &OptimizedLogicalPlan) -> bool {
    match plan {
        OptimizedLogicalPlan::TableScan { .. }
        | OptimizedLogicalPlan::Sample { .. }
        | OptimizedLogicalPlan::Filter { .. }
        | OptimizedLogicalPlan::Project { .. }
        | OptimizedLogicalPlan::NestedLoopJoin { .. }
        | OptimizedLogicalPlan::CrossJoin { .. }
        | OptimizedLogicalPlan::HashAggregate { .. }
        | OptimizedLogicalPlan::Sort { .. }
        | OptimizedLogicalPlan::Limit { .. }
        | OptimizedLogicalPlan::TopN { .. }
        | OptimizedLogicalPlan::Distinct { .. }
        | OptimizedLogicalPlan::Union { .. }
        | OptimizedLogicalPlan::Intersect { .. }
        | OptimizedLogicalPlan::Except { .. }
        | OptimizedLogicalPlan::Window { .. }
        | OptimizedLogicalPlan::Unnest { .. }
        | OptimizedLogicalPlan::Qualify { .. }
        | OptimizedLogicalPlan::WithCte { .. }
        | OptimizedLogicalPlan::Values { .. }
        | OptimizedLogicalPlan::Empty { .. } => true,

        OptimizedLogicalPlan::Insert { .. }
        | OptimizedLogicalPlan::Update { .. }
        | OptimizedLogicalPlan::Delete { .. }
        | OptimizedLogicalPlan::Merge { .. }
        | OptimizedLogicalPlan::CreateTable { .. }
        | OptimizedLogicalPlan::DropTable { .. }
        | OptimizedLogicalPlan::AlterTable { .. }
        | OptimizedLogicalPlan::Truncate { .. }
        | OptimizedLogicalPlan::CreateView { .. }
        | OptimizedLogicalPlan::DropView { .. }
        | OptimizedLogicalPlan::CreateSchema { .. }
        | OptimizedLogicalPlan::DropSchema { .. }
        | OptimizedLogicalPlan::AlterSchema { .. }
        | OptimizedLogicalPlan::CreateFunction { .. }
        | OptimizedLogicalPlan::DropFunction { .. }
        | OptimizedLogicalPlan::CreateProcedure { .. }
        | OptimizedLogicalPlan::DropProcedure { .. }
        | OptimizedLogicalPlan::Call { .. }
        | OptimizedLogicalPlan::ExportData { .. }
        | OptimizedLogicalPlan::LoadData { .. }
        | OptimizedLogicalPlan::Declare { .. }
        | OptimizedLogicalPlan::SetVariable { .. }
        | OptimizedLogicalPlan::If { .. }
        | OptimizedLogicalPlan::While { .. }
        | OptimizedLogicalPlan::Loop { .. }
        | OptimizedLogicalPlan::Repeat { .. }
        | OptimizedLogicalPlan::For { .. }
        | OptimizedLogicalPlan::Return { .. }
        | OptimizedLogicalPlan::Raise { .. }
        | OptimizedLogicalPlan::Break
        | OptimizedLogicalPlan::Continue
        | OptimizedLogicalPlan::CreateSnapshot { .. }
        | OptimizedLogicalPlan::DropSnapshot { .. }
        | OptimizedLogicalPlan::Assert { .. }
        | OptimizedLogicalPlan::Grant { .. }
        | OptimizedLogicalPlan::Revoke { .. } => false,
    }
}

fn invalidates_cache(plan: &OptimizedLogicalPlan) -> bool {
    match plan {
        OptimizedLogicalPlan::CreateTable { .. }
        | OptimizedLogicalPlan::DropTable { .. }
        | OptimizedLogicalPlan::AlterTable { .. }
        | OptimizedLogicalPlan::Truncate { .. }
        | OptimizedLogicalPlan::CreateView { .. }
        | OptimizedLogicalPlan::DropView { .. }
        | OptimizedLogicalPlan::CreateSchema { .. }
        | OptimizedLogicalPlan::DropSchema { .. }
        | OptimizedLogicalPlan::AlterSchema { .. }
        | OptimizedLogicalPlan::CreateFunction { .. }
        | OptimizedLogicalPlan::DropFunction { .. }
        | OptimizedLogicalPlan::CreateProcedure { .. }
        | OptimizedLogicalPlan::DropProcedure { .. }
        | OptimizedLogicalPlan::CreateSnapshot { .. }
        | OptimizedLogicalPlan::DropSnapshot { .. } => true,

        OptimizedLogicalPlan::TableScan { .. }
        | OptimizedLogicalPlan::Sample { .. }
        | OptimizedLogicalPlan::Filter { .. }
        | OptimizedLogicalPlan::Project { .. }
        | OptimizedLogicalPlan::NestedLoopJoin { .. }
        | OptimizedLogicalPlan::CrossJoin { .. }
        | OptimizedLogicalPlan::HashAggregate { .. }
        | OptimizedLogicalPlan::Sort { .. }
        | OptimizedLogicalPlan::Limit { .. }
        | OptimizedLogicalPlan::TopN { .. }
        | OptimizedLogicalPlan::Distinct { .. }
        | OptimizedLogicalPlan::Union { .. }
        | OptimizedLogicalPlan::Intersect { .. }
        | OptimizedLogicalPlan::Except { .. }
        | OptimizedLogicalPlan::Window { .. }
        | OptimizedLogicalPlan::Unnest { .. }
        | OptimizedLogicalPlan::Qualify { .. }
        | OptimizedLogicalPlan::WithCte { .. }
        | OptimizedLogicalPlan::Values { .. }
        | OptimizedLogicalPlan::Empty { .. }
        | OptimizedLogicalPlan::Insert { .. }
        | OptimizedLogicalPlan::Update { .. }
        | OptimizedLogicalPlan::Delete { .. }
        | OptimizedLogicalPlan::Merge { .. }
        | OptimizedLogicalPlan::Call { .. }
        | OptimizedLogicalPlan::ExportData { .. }
        | OptimizedLogicalPlan::LoadData { .. }
        | OptimizedLogicalPlan::Declare { .. }
        | OptimizedLogicalPlan::SetVariable { .. }
        | OptimizedLogicalPlan::If { .. }
        | OptimizedLogicalPlan::While { .. }
        | OptimizedLogicalPlan::Loop { .. }
        | OptimizedLogicalPlan::Repeat { .. }
        | OptimizedLogicalPlan::For { .. }
        | OptimizedLogicalPlan::Return { .. }
        | OptimizedLogicalPlan::Raise { .. }
        | OptimizedLogicalPlan::Break
        | OptimizedLogicalPlan::Continue
        | OptimizedLogicalPlan::Assert { .. }
        | OptimizedLogicalPlan::Grant { .. }
        | OptimizedLogicalPlan::Revoke { .. } => false,
    }
}

#[derive(Serialize, Deserialize)]
pub struct QueryExecutor {
    catalog: Catalog,
    session: Session,
    #[serde(skip, default = "default_plan_cache")]
    plan_cache: LruCache<String, OptimizedLogicalPlan>,
}

impl QueryExecutor {
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
            session: Session::new(),
            plan_cache: default_plan_cache(),
        }
    }

    pub fn execute_sql(&mut self, sql: &str) -> yachtsql_common::error::Result<Table> {
        if let Some(cached_plan) = self.plan_cache.get(sql) {
            let plan = cached_plan.clone();
            let mut executor = PlanExecutor::new(&mut self.catalog, &mut self.session);
            return executor.execute(&plan);
        }

        let logical = yachtsql_parser::parse_and_plan(sql, &self.catalog)?;
        let physical = yachtsql_optimizer::optimize(&logical)?;

        if is_cacheable_plan(&physical) {
            self.plan_cache.put(sql.to_string(), physical.clone());
        }

        let mut executor = PlanExecutor::new(&mut self.catalog, &mut self.session);
        let result = executor.execute(&physical)?;

        if invalidates_cache(&physical) {
            self.plan_cache.clear();
        }

        Ok(result)
    }

    pub fn execute(
        &mut self,
        plan: &OptimizedLogicalPlan,
    ) -> yachtsql_common::error::Result<Table> {
        let mut executor = PlanExecutor::new(&mut self.catalog, &mut self.session);
        executor.execute(plan)
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn catalog_mut(&mut self) -> &mut Catalog {
        &mut self.catalog
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn session_mut(&mut self) -> &mut Session {
        &mut self.session
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
    plan: &OptimizedLogicalPlan,
) -> yachtsql_common::error::Result<Table> {
    let mut executor = PlanExecutor::new(catalog, session);
    executor.execute(plan)
}
