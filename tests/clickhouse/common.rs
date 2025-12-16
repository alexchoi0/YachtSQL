use yachtsql::{ClickHouseExecutor, QueryExecutor};

#[path = "../test_helpers.rs"]
mod test_helpers;

pub use test_helpers::*;

pub fn create_executor() -> QueryExecutor {
    QueryExecutor::new()
}

pub fn create_clickhouse_executor() -> ClickHouseExecutor {
    ClickHouseExecutor::new()
}
