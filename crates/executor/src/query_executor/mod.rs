pub(crate) mod aggregator;
pub(crate) mod enforcement;
pub mod evaluator;
pub mod execution;
pub mod executor_context;
pub mod expression_evaluator;
pub mod function_validator;
pub(crate) mod logical_to_physical;
pub mod operators;
pub mod returning;
pub mod statement_validator;
pub(crate) mod window_functions;

pub use execution::QueryExecutor;
pub use executor_context::{
    CorrelationGuard, ExecutorContext, FeatureRegistryGuard, SubqueryExecutorGuard,
};
pub use function_validator::{validate_function, validate_function_with_udfs};
pub use logical_to_physical::LogicalToPhysicalPlanner;
pub use operators::ExecutionContext;
pub use statement_validator::{validate_statement, validate_statement_with_udfs};
