use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_select_where_equality() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob', 25)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO users VALUES (3, 'Charlie', 30)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM users WHERE id = 2");
    assert!(result.is_ok(), "SELECT WHERE failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 row");
}

#[test]
fn test_select_where_not_equal() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO items VALUES (1, 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO items VALUES (2, 20)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO items VALUES (3, 10)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM items WHERE value != 10");
    assert!(result.is_ok(), "SELECT WHERE != failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 row with value 20");
}

#[test]
fn test_select_where_less_than() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES (1, 5)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES (2, 15)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES (3, 8)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM numbers WHERE value < 10");
    assert!(result.is_ok(), "SELECT WHERE < failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows with value < 10");
}

#[test]
fn test_select_where_greater_than() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE scores (id INT64, score INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO scores VALUES (1, 85)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES (2, 92)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES (3, 78)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM scores WHERE score > 80");
    assert!(result.is_ok(), "SELECT WHERE > failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows with score > 80");
}

#[test]
fn test_select_where_and() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE products (id INT64, price FLOAT64, stock INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO products VALUES (1, 10.5, 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES (2, 25.0, 50)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES (3, 15.0, 75)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM products WHERE price > 10.0 AND stock >= 75");
    assert!(result.is_ok(), "SELECT WHERE AND failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows matching condition");
}

#[test]
fn test_select_where_or() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, category STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO data VALUES (1, 'A')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (2, 'B')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (3, 'C')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM data WHERE category = 'A' OR category = 'C'");
    assert!(result.is_ok(), "SELECT WHERE OR failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows");
}

#[test]
fn test_select_where_is_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE nullable (id INT64, value STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO nullable VALUES (1, 'test')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO nullable VALUES (2, NULL)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO nullable VALUES (3, 'data')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM nullable WHERE value IS NULL");
    assert!(result.is_ok(), "SELECT WHERE IS NULL failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 NULL row");
}

#[test]
fn test_select_where_is_not_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE nullable (id INT64, value STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO nullable VALUES (1, 'test')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO nullable VALUES (2, NULL)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO nullable VALUES (3, 'data')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM nullable WHERE value IS NOT NULL");
    assert!(
        result.is_ok(),
        "SELECT WHERE IS NOT NULL failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 non-NULL rows");
}

#[test]
fn test_select_where_string_comparison() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE names (id INT64, name STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO names VALUES (1, 'Alice')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO names VALUES (2, 'Bob')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO names VALUES (3, 'Charlie')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM names WHERE name = 'Bob'");
    assert!(result.is_ok(), "SELECT WHERE string = failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 row with name = 'Bob'");
}

#[test]
fn test_select_where_no_matches() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test (id INT64, value INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO test VALUES (1, 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO test VALUES (2, 20)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM test WHERE value = 999");
    assert!(
        result.is_ok(),
        "SELECT WHERE no matches failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0, "Expected 0 rows");
}

#[test]
fn test_select_where_all_match() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE all_match (id INT64, status STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO all_match VALUES (1, 'active')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO all_match VALUES (2, 'active')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO all_match VALUES (3, 'active')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM all_match WHERE status = 'active'");
    assert!(
        result.is_ok(),
        "SELECT WHERE all match failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3, "Expected all 3 rows");
}

#[test]
fn test_select_where_float_comparison() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE floats (id INT64, price FLOAT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO floats VALUES (1, 9.99)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO floats VALUES (2, 19.99)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO floats VALUES (3, 14.50)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM floats WHERE price <= 15.0");
    assert!(result.is_ok(), "SELECT WHERE float <= failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows with price <= 15.0");
}

#[test]
fn test_select_where_projection() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE full (id INT64, name STRING, age INT64, city STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO full VALUES (1, 'Alice', 30, 'NYC')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO full VALUES (2, 'Bob', 25, 'LA')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO full VALUES (3, 'Charlie', 35, 'NYC')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT name, age FROM full WHERE city = 'NYC'");
    assert!(result.is_ok(), "SELECT columns WHERE failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows from NYC");
}
