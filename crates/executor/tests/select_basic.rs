use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_select_star_from_table() {
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

    let result = executor.execute_sql("SELECT * FROM users");
    assert!(result.is_ok(), "SELECT * failed: {:?}", result);

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows");
    assert_eq!(batch.num_columns(), 3, "Expected 3 columns");
}

#[test]
fn test_select_specific_columns() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64)")
        .expect("CREATE TABLE failed");

    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Widget', 9.99)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES (2, 'Gadget', 19.99)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT name, price FROM products");
    assert!(result.is_ok(), "SELECT columns failed: {:?}", result);

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows");
    assert_eq!(batch.num_columns(), 2, "Expected 2 columns");
}

#[test]
fn test_select_single_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE items (id INT64, description STRING)")
        .expect("CREATE TABLE failed");

    executor
        .execute_sql("INSERT INTO items VALUES (1, 'First item')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO items VALUES (2, 'Second item')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT description FROM items");
    assert!(result.is_ok(), "SELECT single column failed: {:?}", result);

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows");
    assert_eq!(batch.num_columns(), 1, "Expected 1 column");
}

#[test]
fn test_select_from_empty_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, name STRING)")
        .expect("CREATE TABLE failed");

    let result = executor.execute_sql("SELECT * FROM empty_table");
    assert!(
        result.is_ok(),
        "SELECT from empty table failed: {:?}",
        result
    );

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0, "Expected 0 rows");
}

#[test]
fn test_select_from_nonexistent_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    let result = executor.execute_sql("SELECT * FROM nonexistent");
    assert!(result.is_err(), "SELECT from nonexistent table should fail");
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_select_nonexistent_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test (id INT64, name STRING)")
        .expect("CREATE TABLE failed");

    executor
        .execute_sql("INSERT INTO test VALUES (1, 'Test')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT nonexistent FROM test");
    assert!(result.is_err(), "SELECT nonexistent column should fail");
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_select_qualified_table_name() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE dataset1.table1 (id INT64, value STRING)")
        .expect("CREATE TABLE failed");

    executor
        .execute_sql("INSERT INTO dataset1.table1 VALUES (1, 'test')")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM dataset1.table1");
    assert!(
        result.is_ok(),
        "SELECT with qualified table name failed: {:?}",
        result
    );

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 row");
}

#[test]
fn test_select_various_types() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql(
            "CREATE TABLE mixed (int_col INT64, float_col FLOAT64, str_col STRING, bool_col BOOL)",
        )
        .expect("CREATE TABLE failed");

    executor
        .execute_sql("INSERT INTO mixed VALUES (100, 3.14, 'hello', true)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO mixed VALUES (200, 2.71, 'world', false)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM mixed");
    assert!(result.is_ok(), "SELECT various types failed: {:?}", result);

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows");
    assert_eq!(batch.num_columns(), 4, "Expected 4 columns");
}

#[test]
fn test_select_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE nullable (id INT64, value STRING)")
        .expect("CREATE TABLE failed");

    executor
        .execute_sql("INSERT INTO nullable VALUES (1, 'data')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO nullable VALUES (2, NULL)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT * FROM nullable");
    assert!(result.is_ok(), "SELECT with NULL failed: {:?}", result);

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows");
}

#[test]
fn test_select_column_order() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE ordered (a INT64, b INT64, c INT64)")
        .expect("CREATE TABLE failed");

    executor
        .execute_sql("INSERT INTO ordered VALUES (1, 2, 3)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT c, a, b FROM ordered");
    assert!(result.is_ok(), "SELECT column order failed: {:?}", result);

    let batch = result.unwrap();
    assert_eq!(batch.num_columns(), 3, "Expected 3 columns");

    let schema = batch.schema();
    assert_eq!(schema.fields()[0].name, "c", "First column should be c");
    assert_eq!(schema.fields()[1].name, "a", "Second column should be a");
    assert_eq!(schema.fields()[2].name, "b", "Third column should be b");
}
