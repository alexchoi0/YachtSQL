use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_truncate_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE truncate_test (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO truncate_test VALUES (1), (2), (3)")
        .unwrap();
    executor
        .execute_sql("TRUNCATE TABLE truncate_test")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM truncate_test")
        .unwrap();
    assert_table_eq!(result, [[crate::common::0]]);
}

#[test]
fn test_truncate_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("TRUNCATE TABLE IF EXISTS nonexistent_table")
        .unwrap();
}

#[test]
fn test_truncate_database_qualified() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE truncate_db").unwrap();
    executor
        .execute_sql("CREATE TABLE truncate_db.trunc_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO truncate_db.trunc_table VALUES (1), (2)")
        .unwrap();
    executor
        .execute_sql("TRUNCATE TABLE truncate_db.trunc_table")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM truncate_db.trunc_table")
        .unwrap();
    assert_table_eq!(result, [[crate::common::0]]);
}

#[test]
fn test_truncate_preserves_schema() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE schema_test (id INT64, name String, value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO schema_test VALUES (1, 'test', 1.5)")
        .unwrap();
    executor.execute_sql("TRUNCATE TABLE schema_test").unwrap();
    executor
        .execute_sql("INSERT INTO schema_test VALUES (2, 'after', 2.5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, value FROM schema_test")
        .unwrap();
    assert_table_eq!(result, [[2, "after", 2.5]]);
}

#[test]
fn test_truncate_multiple_times() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE multi_truncate (id INT64)")
        .unwrap();

    for i in 0..3 {
        executor
            .execute_sql(&format!("INSERT INTO multi_truncate VALUES ({})", i))
            .unwrap();
        executor
            .execute_sql("TRUNCATE TABLE multi_truncate")
            .unwrap();
        let result = executor
            .execute_sql("SELECT COUNT(*) FROM multi_truncate")
            .unwrap();
        assert_table_eq!(result, [[crate::common::0]]);
    }
}

#[test]
fn test_truncate_with_indexes() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE indexed_truncate (id INT64, INDEX idx_id id TYPE minmax GRANULARITY 1)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO indexed_truncate VALUES (1), (2), (3)")
        .unwrap();
    executor
        .execute_sql("TRUNCATE TABLE indexed_truncate")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM indexed_truncate")
        .unwrap();
    assert_table_eq!(result, [[crate::common::0]]);
}
