use yachtsql::QueryExecutor;
use yachtsql_parser::DialectType;

#[path = "../bench_helpers.rs"]
mod bench_helpers;

pub use bench_helpers::*;

pub fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::ClickHouse)
}
