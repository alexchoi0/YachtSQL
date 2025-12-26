use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};

use lazy_static::lazy_static;
use lru::LruCache;
use regex::Regex;
use yachtsql_common::error::Result;
use yachtsql_optimizer::OptimizedLogicalPlan;
use yachtsql_storage::Table;

use crate::concurrent_catalog::ConcurrentCatalog;
use crate::concurrent_session::ConcurrentSession;
use crate::executor::concurrent::ConcurrentPlanExecutor;
use crate::plan::PhysicalPlan;

const PLAN_CACHE_SIZE: usize = 10000;

fn preprocess_range_types(sql: &str) -> String {
    lazy_static! {
        static ref RANGE_TYPE_RE: Regex =
            Regex::new(r"(?i)\bRANGE\s*<\s*(DATE|DATETIME|TIMESTAMP)\s*>").unwrap();
    }
    RANGE_TYPE_RE.replace_all(sql, "RANGE_$1").to_string()
}

fn default_plan_cache() -> LruCache<String, OptimizedLogicalPlan> {
    LruCache::new(NonZeroUsize::new(PLAN_CACHE_SIZE).unwrap())
}

fn is_cacheable_plan(plan: &OptimizedLogicalPlan) -> bool {
    matches!(
        plan,
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
    )
}

fn invalidates_cache(plan: &OptimizedLogicalPlan) -> bool {
    matches!(
        plan,
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
            | OptimizedLogicalPlan::DropSnapshot { .. }
    )
}

pub struct AsyncQueryExecutor {
    catalog: Arc<ConcurrentCatalog>,
    session: Arc<ConcurrentSession>,
    plan_cache: Arc<RwLock<LruCache<String, OptimizedLogicalPlan>>>,
}

impl AsyncQueryExecutor {
    pub fn new() -> Self {
        Self {
            catalog: Arc::new(ConcurrentCatalog::new()),
            session: Arc::new(ConcurrentSession::new()),
            plan_cache: Arc::new(RwLock::new(default_plan_cache())),
        }
    }

    pub fn from_catalog_and_session(
        catalog: ConcurrentCatalog,
        session: ConcurrentSession,
    ) -> Self {
        Self {
            catalog: Arc::new(catalog),
            session: Arc::new(session),
            plan_cache: Arc::new(RwLock::new(default_plan_cache())),
        }
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<Table> {
        let sql = preprocess_range_types(sql);
        let cached = {
            let mut cache = self.plan_cache.write().unwrap();
            cache.get(&sql).cloned()
        };

        let physical = match cached {
            Some(plan) => plan,
            None => {
                let logical = yachtsql_parser::parse_and_plan(&sql, self)?;
                let physical = yachtsql_optimizer::optimize(&logical)?;

                if is_cacheable_plan(&physical) {
                    let mut cache = self.plan_cache.write().unwrap();
                    cache.put(sql.clone(), physical.clone());
                }

                physical
            }
        };

        let executor_plan = PhysicalPlan::from_physical(&physical);
        let accesses = executor_plan.extract_table_accesses();

        let tables = self.catalog.acquire_table_locks(&accesses)?;

        let mut executor = ConcurrentPlanExecutor::new(&self.catalog, &self.session, tables);
        let result = executor.execute_plan(&executor_plan)?;

        if invalidates_cache(&physical) {
            let mut cache = self.plan_cache.write().unwrap();
            cache.clear();
        }

        Ok(result)
    }

    pub async fn execute_batch(&self, queries: Vec<String>) -> Vec<Result<Table>> {
        use futures::future::join_all;
        use tokio::task::spawn;

        let futures: Vec<_> = queries
            .into_iter()
            .map(|sql| {
                let executor = self.clone();
                spawn(async move { executor.execute_sql(&sql).await })
            })
            .collect();

        let results = join_all(futures).await;
        results
            .into_iter()
            .map(|r| {
                r.unwrap_or_else(|e| Err(yachtsql_common::error::Error::internal(e.to_string())))
            })
            .collect()
    }

    pub fn catalog(&self) -> &ConcurrentCatalog {
        &self.catalog
    }

    pub fn session(&self) -> &ConcurrentSession {
        &self.session
    }
}

impl Clone for AsyncQueryExecutor {
    fn clone(&self) -> Self {
        Self {
            catalog: Arc::clone(&self.catalog),
            session: Arc::clone(&self.session),
            plan_cache: Arc::clone(&self.plan_cache),
        }
    }
}

impl Default for AsyncQueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl yachtsql_parser::CatalogProvider for AsyncQueryExecutor {
    fn get_table_schema(&self, name: &str) -> Option<yachtsql_storage::Schema> {
        self.catalog.get_table_schema(name)
    }

    fn get_view(&self, name: &str) -> Option<yachtsql_parser::ViewDefinition> {
        self.catalog
            .get_view(name)
            .map(|v| yachtsql_parser::ViewDefinition {
                query: v.query,
                column_aliases: v.column_aliases,
            })
    }

    fn get_function(&self, name: &str) -> Option<yachtsql_parser::FunctionDefinition> {
        self.catalog
            .get_function(name)
            .map(|f| yachtsql_parser::FunctionDefinition {
                name: f.name.clone(),
                parameters: f.parameters.clone(),
                return_type: f.return_type.clone(),
                body: f.body.clone(),
                is_aggregate: f.is_aggregate,
            })
    }
}
