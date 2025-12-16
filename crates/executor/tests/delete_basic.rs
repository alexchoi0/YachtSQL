use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_delete_all_rows() {
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

    let result = executor.execute_sql("DELETE FROM users");
    assert!(result.is_ok(), "DELETE failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM users");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0, "Expected 0 rows after DELETE");
}

#[test]
fn test_delete_from_empty_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, value STRING)")
        .expect("CREATE TABLE failed");

    let result = executor.execute_sql("DELETE FROM empty_table");
    assert!(
        result.is_ok(),
        "DELETE from empty table failed: {:?}",
        result
    );
}

#[test]
fn test_delete_nonexistent_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    let result = executor.execute_sql("DELETE FROM nonexistent");
    assert!(result.is_err(), "DELETE from nonexistent table should fail");
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_delete_qualified_table_name() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE dataset1.table1 (id INT64, value STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO dataset1.table1 VALUES (1, 'test')")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM dataset1.table1");
    assert!(
        result.is_ok(),
        "DELETE with qualified table name failed: {:?}",
        result
    );

    let result = executor.execute_sql("SELECT * FROM dataset1.table1");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_delete_and_reinsert() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE reinsert_test (id INT64, name STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO reinsert_test VALUES (1, 'First')")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM reinsert_test");
    assert!(result.is_ok(), "DELETE failed: {:?}", result);

    let result = executor.execute_sql("INSERT INTO reinsert_test VALUES (2, 'Second')");
    assert!(result.is_ok(), "Re-INSERT failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM reinsert_test");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delete_multiple_batches() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE batch_test (id INT64, value STRING)")
        .expect("CREATE TABLE failed");

    executor
        .execute_sql("INSERT INTO batch_test VALUES (1, 'one')")
        .expect("INSERT 1 failed");
    executor
        .execute_sql("INSERT INTO batch_test VALUES (2, 'two')")
        .expect("INSERT 2 failed");
    executor
        .execute_sql("INSERT INTO batch_test VALUES (3, 'three')")
        .expect("INSERT 3 failed");
    executor
        .execute_sql("INSERT INTO batch_test VALUES (4, 'four')")
        .expect("INSERT 4 failed");

    let result = executor.execute_sql("DELETE FROM batch_test");
    assert!(result.is_ok(), "DELETE multiple rows failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM batch_test");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0, "Expected 0 rows after DELETE");
}

#[test]
fn test_delete_various_types() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql(
            "CREATE TABLE mixed (int_col INT64, float_col FLOAT64, str_col STRING, bool_col BOOL)",
        )
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO mixed VALUES (1, 1.1, 'one', true)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO mixed VALUES (2, 2.2, 'two', false)")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM mixed");
    assert!(result.is_ok(), "DELETE various types failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM mixed");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_delete_with_nulls() {
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

    let result = executor.execute_sql("DELETE FROM nullable");
    assert!(result.is_ok(), "DELETE with NULL failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM nullable");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_delete_single_row_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE single_row (id INT64, name STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO single_row VALUES (1, 'Only')")
        .expect("INSERT failed");

    let result = executor.execute_sql("DELETE FROM single_row");
    assert!(result.is_ok(), "DELETE single row failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM single_row");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0);
}
