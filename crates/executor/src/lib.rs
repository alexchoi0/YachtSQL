//! Query execution engine for YachtSQL.

#![allow(clippy::collapsible_if)]
#![allow(clippy::needless_return)]
#![allow(clippy::single_match)]
#![allow(clippy::collapsible_match)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]
#![allow(clippy::should_implement_trait)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::unwrap_or_default)]
#![allow(clippy::single_char_add_str)]
#![allow(clippy::manual_is_multiple_of)]
#![allow(clippy::new_without_default)]
#![allow(clippy::needless_borrows_for_generic_args)]
#![allow(clippy::op_ref)]
#![allow(clippy::if_same_then_else)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::borrowed_box)]
#![allow(clippy::clone_on_copy)]
#![allow(clippy::question_mark)]
#![allow(clippy::needless_borrow)]
#![allow(clippy::let_and_return)]
#![allow(clippy::redundant_guards)]
#![allow(clippy::approx_constant)]
#![allow(clippy::redundant_pattern_matching)]
#![allow(clippy::bool_assert_comparison)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unexpected_cfgs)]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]
#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::unnecessary_unwrap)]

pub mod correlation;
pub mod record_batch;

pub mod ast_cache;
pub mod catalog_adapter;
pub mod plan_cache;
pub mod sql_normalizer;

pub mod resource_limits;

pub mod explain;
pub mod match_recognize;
pub mod materialized_view_registry;
pub mod multiset_operations;
pub(crate) mod pattern_matching;
pub mod temporal_queries;
pub mod temporal_tables;

pub mod query_executor;
pub mod trigger_execution;

pub(crate) mod sql {}
pub(crate) mod functions {
    pub use ::yachtsql_functions::*;
}

pub(crate) mod types {
    pub use ::yachtsql_core::types::*;
}
pub(crate) mod error {
    pub use ::yachtsql_core::error::*;
}
pub(crate) mod storage {
    pub use ::yachtsql_storage::*;
}
pub(crate) mod optimizer {
    pub use ::yachtsql_optimizer::*;
}

pub(crate) use ::yachtsql_core::error::Error;
pub(crate) use ::yachtsql_core::types::Value;
pub(crate) use ::yachtsql_parser::DialectType;
pub use correlation::CorrelationContext;
pub use match_recognize::{
    AfterMatchSkip, MatchMode, MatchRecognizeClause, PatternMatch, PatternMatcher,
};
pub use materialized_view_registry::{MaterializedViewMetadata, MaterializedViewRegistry};
pub use multiset_operations::Multiset;
pub use query_executor::{ExecutionContext, QueryExecutor};
pub use record_batch::Table;
pub use resource_limits::{
    CancellationToken, ResourceLimitsConfig, ResourceStats, ResourceTracker,
};
pub use sql_normalizer::{hash_sql, normalize_sql};
pub use temporal_queries::{TemporalClause, TemporalQueryBuilder, TemporalTableRef};
pub use temporal_tables::{TemporalQueryType, TemporalTableMetadata, TemporalTableRegistry};
pub use trigger_execution::{TriggerContext, TriggerExecutionResult};
pub use yachtsql_optimizer::Optimizer;

#[cfg(test)]
#[path = "../tests/support/mod.rs"]
pub(crate) mod tests_support;

#[cfg(test)]
pub mod tests {

    pub(crate) use super::tests_support as support;
}
