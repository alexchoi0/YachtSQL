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

mod async_executor;
mod concurrent_catalog;
mod concurrent_session;
mod physical_planner;

use std::num::NonZeroUsize;

pub use async_executor::AsyncQueryExecutor;
pub use catalog::{Catalog, ColumnDefault, UserFunction, UserProcedure, ViewDef};
pub use concurrent_catalog::{ConcurrentCatalog, TableLockSet};
pub use concurrent_session::ConcurrentSession;
pub use error::{Error, Result};
pub use executor::{PlanExecutor, plan_schema_to_schema};
pub use ir_evaluator::{IrEvaluator, UserFunctionDef};
use lru::LruCache;
pub use physical_planner::PhysicalPlanner;
pub use plan::PhysicalPlan;
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
        | OptimizedLogicalPlan::HashJoin { .. }
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
        | OptimizedLogicalPlan::HashJoin { .. }
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

pub fn execute(
    catalog: &mut Catalog,
    session: &mut Session,
    plan: &OptimizedLogicalPlan,
) -> yachtsql_common::error::Result<Table> {
    let mut executor = PlanExecutor::new(catalog, session);
    executor.execute(plan)
}
