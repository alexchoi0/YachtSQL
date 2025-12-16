use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_order_by_asc_single_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO numbers VALUES (1, 30)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES (2, 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES (3, 20)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM numbers ORDER BY value ASC");
    assert!(result.is_ok(), "ORDER BY ASC failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn test_order_by_desc_single_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE items (name STRING, price INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO items VALUES ('A', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO items VALUES ('B', 300)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO items VALUES ('C', 200)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM items ORDER BY price DESC");
    assert!(result.is_ok(), "ORDER BY DESC failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn test_order_by_multiple_columns() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE employees (dept STRING, name STRING, salary INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO employees VALUES ('Sales', 'Alice', 50000)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO employees VALUES ('Sales', 'Bob', 60000)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO employees VALUES ('HR', 'Charlie', 55000)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM employees ORDER BY dept ASC, salary DESC");
    assert!(
        result.is_ok(),
        "ORDER BY multiple columns failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn test_order_by_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (2, NULL)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (3, 20)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM data ORDER BY value ASC");
    assert!(result.is_ok(), "ORDER BY with NULLs failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn test_limit_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE records (id INT64)")
        .expect("CREATE failed");

    for i in 1..=10 {
        executor
            .execute_sql(&format!("INSERT INTO records VALUES ({})", i))
            .expect("INSERT failed");
    }

    let result = executor.execute_sql("SELECT * FROM records LIMIT 3");
    assert!(result.is_ok(), "LIMIT failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3, "Expected exactly 3 rows from LIMIT 3");
}

#[test]
fn test_offset_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE pages (page_num INT64)")
        .expect("CREATE failed");

    for i in 1..=10 {
        executor
            .execute_sql(&format!("INSERT INTO pages VALUES ({})", i))
            .expect("INSERT failed");
    }

    let result = executor.execute_sql("SELECT * FROM pages OFFSET 5");
    assert!(result.is_ok(), "OFFSET failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 5, "Expected 5 rows after OFFSET 5");
}

#[test]
fn test_limit_and_offset() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sequence (num INT64)")
        .expect("CREATE failed");

    for i in 1..=20 {
        executor
            .execute_sql(&format!("INSERT INTO sequence VALUES ({})", i))
            .expect("INSERT failed");
    }

    let result = executor.execute_sql("SELECT * FROM sequence LIMIT 5 OFFSET 10");
    assert!(result.is_ok(), "LIMIT + OFFSET failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(
        batch.num_rows(),
        5,
        "Expected 5 rows from LIMIT 5 OFFSET 10"
    );
}

#[test]
fn test_order_by_limit() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE scores (player STRING, score INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO scores VALUES ('Alice', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Bob', 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Charlie', 150)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Diana', 180)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM scores ORDER BY score DESC LIMIT 2");
    assert!(result.is_ok(), "ORDER BY + LIMIT failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected top 2 scores");
}

#[test]
fn test_order_by_limit_offset() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE rankings (name STRING, points INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO rankings VALUES ('A', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO rankings VALUES ('B', 90)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO rankings VALUES ('C', 80)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO rankings VALUES ('D', 70)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO rankings VALUES ('E', 60)")
        .expect("INSERT failed");

    let result =
        executor.execute_sql("SELECT * FROM rankings ORDER BY points DESC LIMIT 2 OFFSET 1");
    assert!(
        result.is_ok(),
        "ORDER BY + LIMIT + OFFSET failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows (ranks 2-3)");
}

#[test]
fn test_limit_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test (id INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO test VALUES (1)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM test LIMIT 0");
    assert!(result.is_ok(), "LIMIT 0 failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0, "LIMIT 0 should return 0 rows");
}

#[test]
fn test_order_by_string() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE names (name STRING)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO names VALUES ('Charlie')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO names VALUES ('Alice')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO names VALUES ('Bob')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM names ORDER BY name ASC");
    assert!(result.is_ok(), "ORDER BY string failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn test_order_by_with_where() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE filtered (category STRING, value INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO filtered VALUES ('A', 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO filtered VALUES ('B', 20)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO filtered VALUES ('A', 30)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO filtered VALUES ('B', 15)")
        .expect("INSERT failed");

    let result =
        executor.execute_sql("SELECT * FROM filtered WHERE category = 'A' ORDER BY value DESC");
    assert!(result.is_ok(), "ORDER BY with WHERE failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);
}
