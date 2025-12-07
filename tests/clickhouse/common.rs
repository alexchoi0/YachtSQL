use yachtsql::QueryExecutor;
use yachtsql_parser::DialectType;

#[path = "../test_helpers.rs"]
mod test_helpers;

pub use test_helpers::*;

pub fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::ClickHouse)
}
