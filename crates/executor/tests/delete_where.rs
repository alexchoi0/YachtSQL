use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_delete_where_single_row() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO users VALUES (3, 'Charlie')")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM users WHERE id = 2");
    assert!(result.is_ok(), "DELETE WHERE failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM users");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows remaining");

    let result = executor.execute_sql("SELECT * FROM users WHERE id = 2");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0, "Row with id=2 should be deleted");
}

#[test]
fn test_delete_where_multiple_rows() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE items (id INT64, category STRING, price FLOAT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO items VALUES (1, 'A', 10.0)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO items VALUES (2, 'B', 20.0)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO items VALUES (3, 'A', 15.0)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO items VALUES (4, 'C', 30.0)")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM items WHERE category = 'A'");
    assert!(
        result.is_ok(),
        "DELETE WHERE multiple rows failed: {:?}",
        result
    );

    let result = executor.execute_sql("SELECT * FROM items");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows remaining");

    let result = executor.execute_sql("SELECT * FROM items WHERE category = 'A'");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_delete_where_no_matches() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (2, 20)")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM data WHERE value = 999");
    assert!(
        result.is_ok(),
        "DELETE WHERE no matches failed: {:?}",
        result
    );

    let result = executor.execute_sql("SELECT * FROM data");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "All rows should remain");
}

#[test]
fn test_delete_where_all_rows() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE all_delete (id INT64, status STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO all_delete VALUES (1, 'active')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO all_delete VALUES (2, 'active')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO all_delete VALUES (3, 'active')")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM all_delete WHERE status = 'active'");
    assert!(result.is_ok(), "DELETE WHERE all rows failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM all_delete");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0, "All rows should be deleted");
}

#[test]
fn test_delete_where_greater_than() {
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
    executor
        .execute_sql("INSERT INTO scores VALUES (4, 95)")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM scores WHERE score > 90");
    assert!(result.is_ok(), "DELETE WHERE > failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM scores");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows remaining");

    let result = executor.execute_sql("SELECT * FROM scores WHERE score > 90");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_delete_where_and_condition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE products (id INT64, price FLOAT64, stock INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO products VALUES (1, 10.0, 0)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES (2, 15.0, 5)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES (3, 20.0, 0)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES (4, 25.0, 10)")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM products WHERE price > 12.0 AND stock = 0");
    assert!(result.is_ok(), "DELETE WHERE AND failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM products");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3, "Expected 3 rows remaining");

    let result = executor.execute_sql("SELECT * FROM products WHERE price = 10.0");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delete_where_or_condition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE flags (id INT64, flag STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO flags VALUES (1, 'A')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO flags VALUES (2, 'B')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO flags VALUES (3, 'C')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO flags VALUES (4, 'D')")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM flags WHERE flag = 'A' OR flag = 'C'");
    assert!(result.is_ok(), "DELETE WHERE OR failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM flags");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows remaining");

    let result = executor.execute_sql("SELECT * FROM flags WHERE flag = 'B' OR flag = 'D'");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_delete_where_is_null() {
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
    executor
        .execute_sql("INSERT INTO nullable VALUES (4, NULL)")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM nullable WHERE value IS NULL");
    assert!(result.is_ok(), "DELETE WHERE IS NULL failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM nullable");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows remaining");

    let result = executor.execute_sql("SELECT * FROM nullable WHERE value IS NULL");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_delete_where_is_not_null() {
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

    let result = executor.execute_sql("DELETE FROM nullable WHERE value IS NOT NULL");
    assert!(
        result.is_ok(),
        "DELETE WHERE IS NOT NULL failed: {:?}",
        result
    );

    let result = executor.execute_sql("SELECT * FROM nullable");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 NULL row remaining");

    let result = executor.execute_sql("SELECT * FROM nullable WHERE value IS NULL");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delete_where_less_than_equal() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES (1, 5)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES (2, 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES (3, 15)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES (4, 8)")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM numbers WHERE value <= 10");
    assert!(result.is_ok(), "DELETE WHERE <= failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM numbers");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 row remaining");

    let result = executor.execute_sql("SELECT * FROM numbers WHERE value = 15");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delete_where_string_comparison() {
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
    executor
        .execute_sql("INSERT INTO names VALUES (4, 'Alice')")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM names WHERE name = 'Alice'");
    assert!(result.is_ok(), "DELETE WHERE string = failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM names");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows remaining");

    let result = executor.execute_sql("SELECT * FROM names WHERE name = 'Alice'");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0);
}
