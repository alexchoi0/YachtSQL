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
mod py_udf;
mod session;

#[cfg(feature = "concurrent")]
mod async_executor;
#[cfg(feature = "concurrent")]
mod concurrent_catalog;
#[cfg(feature = "concurrent")]
mod concurrent_session;

use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};

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
        | OptimizedLogicalPlan::UndropSchema { .. }
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
        | OptimizedLogicalPlan::SetMultipleVariables { .. }
        | OptimizedLogicalPlan::If { .. }
        | OptimizedLogicalPlan::While { .. }
        | OptimizedLogicalPlan::Loop { .. }
        | OptimizedLogicalPlan::Block { .. }
        | OptimizedLogicalPlan::Repeat { .. }
        | OptimizedLogicalPlan::For { .. }
        | OptimizedLogicalPlan::Return { .. }
        | OptimizedLogicalPlan::Raise { .. }
        | OptimizedLogicalPlan::ExecuteImmediate { .. }
        | OptimizedLogicalPlan::Break { .. }
        | OptimizedLogicalPlan::Continue { .. }
        | OptimizedLogicalPlan::CreateSnapshot { .. }
        | OptimizedLogicalPlan::DropSnapshot { .. }
        | OptimizedLogicalPlan::Assert { .. }
        | OptimizedLogicalPlan::Grant { .. }
        | OptimizedLogicalPlan::Revoke { .. }
        | OptimizedLogicalPlan::BeginTransaction
        | OptimizedLogicalPlan::Commit
        | OptimizedLogicalPlan::Rollback
        | OptimizedLogicalPlan::TryCatch { .. }
        | OptimizedLogicalPlan::GapFill { .. } => false,
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
        | OptimizedLogicalPlan::UndropSchema { .. }
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
        | OptimizedLogicalPlan::SetMultipleVariables { .. }
        | OptimizedLogicalPlan::If { .. }
        | OptimizedLogicalPlan::While { .. }
        | OptimizedLogicalPlan::Loop { .. }
        | OptimizedLogicalPlan::Block { .. }
        | OptimizedLogicalPlan::Repeat { .. }
        | OptimizedLogicalPlan::For { .. }
        | OptimizedLogicalPlan::Return { .. }
        | OptimizedLogicalPlan::Raise { .. }
        | OptimizedLogicalPlan::ExecuteImmediate { .. }
        | OptimizedLogicalPlan::Break { .. }
        | OptimizedLogicalPlan::Continue { .. }
        | OptimizedLogicalPlan::Assert { .. }
        | OptimizedLogicalPlan::Grant { .. }
        | OptimizedLogicalPlan::Revoke { .. }
        | OptimizedLogicalPlan::BeginTransaction
        | OptimizedLogicalPlan::Commit
        | OptimizedLogicalPlan::Rollback
        | OptimizedLogicalPlan::TryCatch { .. }
        | OptimizedLogicalPlan::GapFill { .. } => false,
    }
}

pub struct SharedState {
    plan_cache: RwLock<LruCache<String, OptimizedLogicalPlan>>,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            plan_cache: RwLock::new(default_plan_cache()),
        }
    }

    pub fn get_cached_plan(&self, sql: &str) -> Option<OptimizedLogicalPlan> {
        let mut cache = self.plan_cache.write().unwrap();
        cache.get(sql).cloned()
    }

    pub fn cache_plan(&self, sql: String, plan: OptimizedLogicalPlan) {
        let mut cache = self.plan_cache.write().unwrap();
        cache.put(sql, plan);
    }

    pub fn invalidate_cache(&self) {
        let mut cache = self.plan_cache.write().unwrap();
        cache.clear();
    }
}

impl Default for SharedState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SessionExecutor {
    shared: Arc<SharedState>,
    catalog: Catalog,
    session: Session,
}

impl SessionExecutor {
    pub fn new(shared: Arc<SharedState>) -> Self {
        Self {
            shared,
            catalog: Catalog::new(),
            session: Session::new(),
        }
    }

    pub fn with_catalog(shared: Arc<SharedState>, catalog: Catalog) -> Self {
        Self {
            shared,
            catalog,
            session: Session::new(),
        }
    }

    pub fn execute_sql(&mut self, sql: &str) -> yachtsql_common::error::Result<Table> {
        if let Some(cached_plan) = self.shared.get_cached_plan(sql) {
            let mut executor = PlanExecutor::new(&mut self.catalog, &mut self.session);
            return match executor.execute(&cached_plan) {
                Ok(result) => Ok(result),
                Err(yachtsql_common::error::Error::InvalidQuery(msg))
                    if msg == "RETURN outside of function" =>
                {
                    Ok(Table::empty(yachtsql_storage::Schema::new()))
                }
                Err(e) => Err(e),
            };
        }

        let logical = yachtsql_parser::parse_and_plan(sql, &self.catalog)?;
        let physical = yachtsql_optimizer::optimize(&logical)?;

        if is_cacheable_plan(&physical) {
            self.shared.cache_plan(sql.to_string(), physical.clone());
        }

        let mut executor = PlanExecutor::new(&mut self.catalog, &mut self.session);
        let result = match executor.execute(&physical) {
            Ok(result) => result,
            Err(yachtsql_common::error::Error::InvalidQuery(msg))
                if msg == "RETURN outside of function" =>
            {
                Table::empty(yachtsql_storage::Schema::new())
            }
            Err(e) => return Err(e),
        };

        if invalidates_cache(&physical) {
            self.shared.invalidate_cache();
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
            return match executor.execute(&plan) {
                Ok(result) => Ok(result),
                Err(yachtsql_common::error::Error::InvalidQuery(msg))
                    if msg == "RETURN outside of function" =>
                {
                    Ok(Table::empty(yachtsql_storage::Schema::new()))
                }
                Err(e) => Err(e),
            };
        }

        let logical = yachtsql_parser::parse_and_plan(sql, &self.catalog)?;
        let physical = yachtsql_optimizer::optimize(&logical)?;

        if is_cacheable_plan(&physical) {
            self.plan_cache.put(sql.to_string(), physical.clone());
        }

        let mut executor = PlanExecutor::new(&mut self.catalog, &mut self.session);
        let result = match executor.execute(&physical) {
            Ok(result) => result,
            Err(yachtsql_common::error::Error::InvalidQuery(msg))
                if msg == "RETURN outside of function" =>
            {
                Table::empty(yachtsql_storage::Schema::new())
            }
            Err(e) => return Err(e),
        };

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
