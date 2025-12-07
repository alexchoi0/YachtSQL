#[macro_use]
mod common;

use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_simple_multi_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql(
            "WITH RECURSIVE nums AS (
                SELECT 1 AS a, 2 AS b
                UNION ALL
                SELECT a + 1, b + 1
                FROM nums
                WHERE a < 5
            )
            SELECT * FROM nums ORDER BY a",
        )
        .expect("recursive CTE");

    assert_batch_eq!(result, [[1, 2], [2, 3], [3, 4], [4, 5], [5, 6],]);
}
