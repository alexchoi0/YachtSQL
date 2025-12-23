use crate::common::create_session;

#[test]
fn test_drop_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE test_table (id INT64)")
        .unwrap();

    session.execute_sql("DROP TABLE test_table").unwrap();

    let result = session.execute_sql("SELECT * FROM test_table");
    assert!(result.is_err());
}

#[test]
fn test_drop_table_if_exists() {
    let mut session = create_session();

    session
        .execute_sql("DROP TABLE IF EXISTS nonexistent_table")
        .unwrap();

    session
        .execute_sql("CREATE TABLE existing_table (id INT64)")
        .unwrap();

    session
        .execute_sql("DROP TABLE IF EXISTS existing_table")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM existing_table");
    assert!(result.is_err());
}

#[test]
fn test_drop_multiple_tables() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE table1 (id INT64)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE table2 (id INT64)")
        .unwrap();

    session.execute_sql("DROP TABLE table1, table2").unwrap();

    let result1 = session.execute_sql("SELECT * FROM table1");
    let result2 = session.execute_sql("SELECT * FROM table2");
    assert!(result1.is_err());
    assert!(result2.is_err());
}

#[test]
fn test_drop_table_restrict() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE standalone (id INT64)")
        .unwrap();

    session
        .execute_sql("DROP TABLE standalone RESTRICT")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM standalone");
    assert!(result.is_err());
}
