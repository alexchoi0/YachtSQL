use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_insert_single_row() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .expect("CREATE TABLE failed");

    let result = executor.execute_sql("INSERT INTO users VALUES (1, 'Alice')");
    assert!(result.is_ok(), "INSERT failed: {:?}", result);
}

#[test]
fn test_insert_multiple_rows() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE products (id INT64, price FLOAT64)")
        .expect("CREATE TABLE failed");

    let result =
        executor.execute_sql("INSERT INTO products VALUES (1, 9.99), (2, 19.99), (3, 29.99)");
    assert!(result.is_ok(), "INSERT multiple rows failed: {:?}", result);
}

#[test]
fn test_insert_with_column_list() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE orders (id INT64, amount FLOAT64, customer STRING)")
        .expect("CREATE TABLE failed");

    let result =
        executor.execute_sql("INSERT INTO orders (id, customer, amount) VALUES (1, 'Bob', 100.50)");
    assert!(
        result.is_ok(),
        "INSERT with column list failed: {:?}",
        result
    );
}

#[test]
fn test_insert_with_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE items (id INT64, description STRING)")
        .expect("CREATE TABLE failed");

    let result = executor.execute_sql("INSERT INTO items VALUES (1, NULL)");
    assert!(result.is_ok(), "INSERT with NULL failed: {:?}", result);
}

#[test]
fn test_insert_into_nonexistent_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    let result = executor.execute_sql("INSERT INTO nonexistent VALUES (1, 'test')");
    assert!(result.is_err(), "INSERT into nonexistent table should fail");
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_insert_column_count_mismatch() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test (id INT64, name STRING)")
        .expect("CREATE TABLE failed");

    let result = executor.execute_sql("INSERT INTO test VALUES (1)");
    assert!(
        result.is_err(),
        "INSERT with wrong column count should fail"
    );
    assert!(result.unwrap_err().to_string().contains("mismatch"));
}

#[test]
fn test_insert_qualified_table_name() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE dataset1.table1 (id INT64)")
        .expect("CREATE TABLE failed");

    let result = executor.execute_sql("INSERT INTO dataset1.table1 VALUES (42)");
    assert!(
        result.is_ok(),
        "INSERT with qualified table name failed: {:?}",
        result
    );
}

#[test]
fn test_insert_various_types() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql(
            "CREATE TABLE mixed (int_col INT64, float_col FLOAT64, str_col STRING, bool_col BOOL)",
        )
        .expect("CREATE TABLE failed");

    let result = executor.execute_sql("INSERT INTO mixed VALUES (100, 3.14, 'hello', true)");
    assert!(
        result.is_ok(),
        "INSERT with various types failed: {:?}",
        result
    );
}
