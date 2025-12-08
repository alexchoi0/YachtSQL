use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_drop_existing_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE users (id INT64)")
        .expect("CREATE failed");

    let result = executor.execute_sql("DROP TABLE users");
    assert!(result.is_ok(), "DROP TABLE failed: {:?}", result);
}

#[test]
fn test_drop_nonexistent_table_fails() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("DROP TABLE nonexistent");
    assert!(result.is_err(), "DROP nonexistent table should fail");
    let err_msg = result.unwrap_err().to_string();

    assert!(
        err_msg.contains("does not exist") || err_msg.contains("not found"),
        "Expected error about table not existing, got: {}",
        err_msg
    );
}

#[test]
fn test_drop_table_if_exists_success() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE products (id INT64)")
        .expect("CREATE failed");
    let result = executor.execute_sql("DROP TABLE IF EXISTS products");
    assert!(result.is_ok(), "DROP TABLE IF EXISTS failed: {:?}", result);
}

#[test]
fn test_drop_table_if_exists_nonexistent() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("DROP TABLE IF EXISTS nonexistent");
    assert!(
        result.is_ok(),
        "DROP TABLE IF EXISTS should succeed even if table doesn't exist: {:?}",
        result
    );
}

#[test]
fn test_drop_qualified_table_name() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE mydataset.mytable (id INT64)")
        .expect("CREATE failed");
    let result = executor.execute_sql("DROP TABLE mydataset.mytable");
    assert!(result.is_ok(), "DROP qualified table failed: {:?}", result);
}

#[test]
fn test_drop_multiple_tables() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE table1 (id INT64)")
        .expect("CREATE table1 failed");
    executor
        .execute_sql("CREATE TABLE table2 (id INT64)")
        .expect("CREATE table2 failed");
    executor
        .execute_sql("CREATE TABLE table3 (id INT64)")
        .expect("CREATE table3 failed");

    let result = executor.execute_sql("DROP TABLE table1, table2, table3");
    assert!(result.is_ok(), "DROP multiple tables failed: {:?}", result);
}

#[test]
fn test_drop_and_recreate_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE temp (id INT64)")
        .expect("First CREATE failed");

    executor
        .execute_sql("DROP TABLE temp")
        .expect("DROP failed");

    let result = executor.execute_sql("CREATE TABLE temp (id INT64, name STRING)");
    assert!(result.is_ok(), "Recreate after drop failed: {:?}", result);
}

#[test]
fn test_drop_table_cascade() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE parent (id INT64)")
        .expect("CREATE failed");

    let result = executor.execute_sql("DROP TABLE parent CASCADE");
    assert!(
        result.is_ok(),
        "DROP TABLE CASCADE should succeed: {:?}",
        result
    );
}
