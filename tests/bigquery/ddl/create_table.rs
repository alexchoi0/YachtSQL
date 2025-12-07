use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_simple_table() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_create_table_with_multiple_columns() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE products (id INT64, name STRING, price FLOAT64, in_stock BOOLEAN)",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Laptop', 999.99, true)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM products").unwrap();
    assert_table_eq!(result, [[1, "Laptop", 999.99, true]]);
}

#[test]
fn test_create_table_with_primary_key() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE items (id INT64 PRIMARY KEY, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO items VALUES (1, 'Widget')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM items WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [[1, "Widget"]]);
}

#[test]
fn test_create_table_with_not_null() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE required (id INT64, name STRING NOT NULL)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO required VALUES (1, 'Test')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM required").unwrap();
    assert_table_eq!(result, [[1, "Test"]]);
}

#[test]
fn test_create_table_with_default() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE defaults (id INT64, status STRING DEFAULT 'pending')")
        .unwrap();

    executor
        .execute_sql("INSERT INTO defaults (id) VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM defaults").unwrap();
    assert_table_eq!(result, [[1, "pending"]]);
}

#[test]
fn test_create_table_if_not_exists() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE test_table (id INT64)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE IF NOT EXISTS test_table (id INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO test_table VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM test_table").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_check_constraint() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE positive (id INT64, value INT64 CHECK (value > 0))")
        .unwrap();

    executor
        .execute_sql("INSERT INTO positive VALUES (1, 10)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM positive").unwrap();
    assert_table_eq!(result, [[1, 10]]);
}

#[test]
fn test_create_table_with_composite_primary_key() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE composite (a INT64, b INT64, c STRING, PRIMARY KEY (a, b))")
        .unwrap();

    executor
        .execute_sql("INSERT INTO composite VALUES (1, 2, 'test')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM composite").unwrap();
    assert_table_eq!(result, [[1, 2, "test"]]);
}
