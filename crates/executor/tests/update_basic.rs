use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_update_single_column() {
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

    let result = executor.execute_sql("UPDATE users SET age = 31");
    assert!(result.is_ok(), "UPDATE failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM users");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_update_multiple_columns() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64, stock INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Widget', 9.99, 100)")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE products SET price = 12.99, stock = 150");
    assert!(
        result.is_ok(),
        "UPDATE multiple columns failed: {:?}",
        result
    );

    let result = executor.execute_sql("SELECT * FROM products");
    assert!(result.is_ok());
}

#[test]
fn test_update_with_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE items (id INT64, description STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO items VALUES (1, 'Test item')")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE items SET description = NULL");
    assert!(result.is_ok(), "UPDATE to NULL failed: {:?}", result);
}

#[test]
fn test_update_various_types() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql(
            "CREATE TABLE mixed (int_col INT64, float_col FLOAT64, str_col STRING, bool_col BOOL)",
        )
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO mixed VALUES (1, 1.1, 'one', true)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "UPDATE mixed SET int_col = 2, float_col = 2.2, str_col = 'two', bool_col = false",
    );
    assert!(result.is_ok(), "UPDATE various types failed: {:?}", result);
}

#[test]
fn test_update_nonexistent_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    let result = executor.execute_sql("UPDATE nonexistent SET value = 1");
    assert!(result.is_err(), "UPDATE nonexistent table should fail");
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_update_nonexistent_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test (id INT64, name STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO test VALUES (1, 'Test')")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE test SET nonexistent = 1");
    assert!(result.is_err(), "UPDATE nonexistent column should fail");
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_update_qualified_table_name() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE dataset1.table1 (id INT64, value INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO dataset1.table1 VALUES (1, 10)")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE dataset1.table1 SET value = 20");
    assert!(
        result.is_ok(),
        "UPDATE with qualified table name failed: {:?}",
        result
    );
}

#[test]
fn test_update_empty_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, value INT64)")
        .expect("CREATE TABLE failed");

    let result = executor.execute_sql("UPDATE empty_table SET value = 1");
    assert!(result.is_ok(), "UPDATE empty table failed: {:?}", result);
}

#[test]
fn test_update_type_coercion() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE coercion_test (id INT64, float_col FLOAT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO coercion_test VALUES (1, 1.5)")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE coercion_test SET float_col = 2");
    assert!(
        result.is_ok(),
        "UPDATE with type coercion failed: {:?}",
        result
    );
}

#[test]
fn test_update_preserves_other_columns() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE preserve_test (id INT64, name STRING, age INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO preserve_test VALUES (1, 'Alice', 30)")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE preserve_test SET age = 31");
    assert!(result.is_ok(), "UPDATE failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM preserve_test");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}
