use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_create_simple_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    let result = executor.execute_sql("CREATE TABLE users (id INT64, name STRING)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
}

#[test]
fn test_create_table_with_not_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    let result = executor.execute_sql("CREATE TABLE products (id INT64 NOT NULL, price FLOAT64)");
    assert!(
        result.is_ok(),
        "CREATE TABLE with NOT NULL failed: {:?}",
        result
    );
}

#[test]
fn test_create_table_if_not_exists() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    let result1 = executor.execute_sql("CREATE TABLE IF NOT EXISTS items (id INT64)");
    assert!(
        result1.is_ok(),
        "First CREATE TABLE IF NOT EXISTS failed: {:?}",
        result1
    );

    let result2 = executor.execute_sql("CREATE TABLE IF NOT EXISTS items (id INT64)");
    assert!(
        result2.is_ok(),
        "Second CREATE TABLE IF NOT EXISTS failed: {:?}",
        result2
    );
}

#[test]
fn test_create_duplicate_table_fails() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE orders (id INT64)")
        .expect("First CREATE failed");

    let result = executor.execute_sql("CREATE TABLE orders (id INT64)");
    assert!(result.is_err(), "Duplicate CREATE TABLE should fail");
    assert!(result.unwrap_err().to_string().contains("already exists"));
}

#[test]
fn test_create_table_various_types() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    let sql = r#"
        CREATE TABLE all_types (
            col_int INT64,
            col_float FLOAT64,
            col_bool BOOL,
            col_string STRING,
            col_bytes BYTEA,
            col_date DATE,
            col_timestamp TIMESTAMP
        )
    "#;
    let result = executor.execute_sql(sql);
    assert!(
        result.is_ok(),
        "CREATE TABLE with various types failed: {:?}",
        result
    );
}

#[test]
fn test_create_table_with_numeric() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    let result = executor.execute_sql("CREATE TABLE finances (amount NUMERIC(10, 2))");
    assert!(
        result.is_ok(),
        "CREATE TABLE with NUMERIC failed: {:?}",
        result
    );
}

#[test]
fn test_create_table_qualified_name() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    let result = executor.execute_sql("CREATE TABLE mydataset.mytable (id INT64)");
    assert!(
        result.is_ok(),
        "CREATE TABLE with qualified name failed: {:?}",
        result
    );
}
