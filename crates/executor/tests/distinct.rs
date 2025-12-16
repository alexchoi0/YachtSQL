use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_distinct_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE colors (name STRING)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO colors VALUES ('red')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO colors VALUES ('blue')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO colors VALUES ('red')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO colors VALUES ('green')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO colors VALUES ('blue')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT DISTINCT name FROM colors");
    assert!(result.is_ok(), "DISTINCT failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3, "Expected 3 distinct colors");
}

#[test]
fn test_distinct_multiple_columns() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE pairs (first INT64, second INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO pairs VALUES (1, 2)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO pairs VALUES (1, 3)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO pairs VALUES (1, 2)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO pairs VALUES (2, 3)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT DISTINCT * FROM pairs");
    assert!(
        result.is_ok(),
        "DISTINCT multiple columns failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3, "Expected 3 distinct pairs");
}

#[test]
fn test_distinct_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO data VALUES (10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (NULL)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (NULL)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (20)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT DISTINCT value FROM data");
    assert!(result.is_ok(), "DISTINCT with NULLs failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(
        batch.num_rows(),
        3,
        "Expected 3 distinct values (10, NULL, 20)"
    );
}

#[test]
fn test_distinct_all_duplicates() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE same (id INT64)")
        .expect("CREATE failed");

    for _ in 0..5 {
        executor
            .execute_sql("INSERT INTO same VALUES (1)")
            .expect("INSERT failed");
    }

    let result = executor.execute_sql("SELECT DISTINCT id FROM same");
    assert!(
        result.is_ok(),
        "DISTINCT all duplicates failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 distinct value");
}

#[test]
fn test_distinct_no_duplicates() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE unique_vals (id INT64)")
        .expect("CREATE failed");

    for i in 1..=5 {
        executor
            .execute_sql(&format!("INSERT INTO unique_vals VALUES ({})", i))
            .expect("INSERT failed");
    }

    let result = executor.execute_sql("SELECT DISTINCT id FROM unique_vals");
    assert!(
        result.is_ok(),
        "DISTINCT no duplicates failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 5, "Expected 5 distinct values");
}

#[test]
fn test_distinct_with_where() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE filtered (category STRING, value INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO filtered VALUES ('A', 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO filtered VALUES ('A', 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO filtered VALUES ('B', 20)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO filtered VALUES ('A', 30)")
        .expect("INSERT failed");

    let result =
        executor.execute_sql("SELECT DISTINCT category, value FROM filtered WHERE category = 'A'");
    assert!(result.is_ok(), "DISTINCT with WHERE failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(
        batch.num_rows(),
        2,
        "Expected 2 distinct rows for category A"
    );
}

#[test]
fn test_distinct_with_order_by() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE ordered (val INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO ordered VALUES (30)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO ordered VALUES (10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO ordered VALUES (20)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO ordered VALUES (10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO ordered VALUES (30)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT DISTINCT val FROM ordered ORDER BY val ASC");
    assert!(
        result.is_ok(),
        "DISTINCT with ORDER BY failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3, "Expected 3 distinct values");
}

#[test]
fn test_distinct_with_limit() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE limited (num INT64)")
        .expect("CREATE failed");

    for i in 1..=10 {
        executor
            .execute_sql(&format!("INSERT INTO limited VALUES ({})", i))
            .expect("INSERT failed");

        executor
            .execute_sql(&format!("INSERT INTO limited VALUES ({})", i))
            .expect("INSERT failed");
    }

    let result = executor.execute_sql("SELECT DISTINCT num FROM limited LIMIT 5");
    assert!(result.is_ok(), "DISTINCT with LIMIT failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 5, "Expected 5 distinct values");
}

#[test]
fn test_distinct_empty_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE empty (id INT64)")
        .expect("CREATE failed");

    let result = executor.execute_sql("SELECT DISTINCT id FROM empty");
    assert!(
        result.is_ok(),
        "DISTINCT on empty table failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0, "Expected 0 rows");
}

#[test]
fn test_distinct_string_values() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE names (name STRING)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO names VALUES ('Alice')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO names VALUES ('Bob')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO names VALUES ('Alice')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO names VALUES ('Charlie')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO names VALUES ('Bob')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT DISTINCT name FROM names");
    assert!(result.is_ok(), "DISTINCT strings failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3, "Expected 3 distinct names");
}

#[test]
fn test_distinct_mixed_types() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE mixed (id INT64, name STRING, score FLOAT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO mixed VALUES (1, 'A', 90.5)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO mixed VALUES (1, 'A', 90.5)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO mixed VALUES (2, 'B', 85.0)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT DISTINCT * FROM mixed");
    assert!(result.is_ok(), "DISTINCT mixed types failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 distinct rows");
}
