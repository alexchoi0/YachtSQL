use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_debug_fibonacci_simplified() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    let result = executor.execute_sql(
        "WITH RECURSIVE fib AS (
                SELECT 0 AS a, 1 AS b
                UNION ALL
                SELECT b AS a, a + b AS b
                FROM fib
                WHERE b < 10
            )
            SELECT * FROM fib",
    );

    match result {
        Ok(r) => {
            assert!(r.num_rows() > 0);
        }
        Err(e) => {
            panic!("Test failed: {:?}", e);
        }
    }
}

#[test]
fn test_debug_cte_reference() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    let result = executor.execute_sql(
        "WITH RECURSIVE nums AS (
                SELECT 1 AS n
                UNION ALL
                SELECT n + 1 FROM nums WHERE n < 3
            )
            SELECT * FROM nums",
    );

    match result {
        Ok(r) => {
            assert_eq!(r.num_rows(), 3);
        }
        Err(e) => {
            panic!("Failed to SELECT from CTE: {:?}", e);
        }
    }
}
