use crate::common::create_executor;

#[test]
fn test_drop_table() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE test_table (id INTEGER)")
        .unwrap();

    executor.execute_sql("DROP TABLE test_table").unwrap();

    let result = executor.execute_sql("SELECT * FROM test_table");
    assert!(result.is_err());
}

#[test]
fn test_drop_table_if_exists() {
    let mut executor = create_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS nonexistent_table")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE existing_table (id INTEGER)")
        .unwrap();

    executor
        .execute_sql("DROP TABLE IF EXISTS existing_table")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM existing_table");
    assert!(result.is_err());
}

#[test]
fn test_drop_multiple_tables() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE table1 (id INTEGER)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE table2 (id INTEGER)")
        .unwrap();

    executor.execute_sql("DROP TABLE table1, table2").unwrap();

    let result1 = executor.execute_sql("SELECT * FROM table1");
    let result2 = executor.execute_sql("SELECT * FROM table2");
    assert!(result1.is_err());
    assert!(result2.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_drop_table_cascade() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE child (id INTEGER, parent_id INTEGER REFERENCES parent(id))")
        .unwrap();

    executor.execute_sql("DROP TABLE parent CASCADE").unwrap();

    let result = executor.execute_sql("SELECT * FROM parent");
    assert!(result.is_err());
}

#[test]
fn test_drop_table_restrict() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE standalone (id INTEGER)")
        .unwrap();

    executor
        .execute_sql("DROP TABLE standalone RESTRICT")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM standalone");
    assert!(result.is_err());
}
