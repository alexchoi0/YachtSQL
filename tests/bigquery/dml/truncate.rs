use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_truncate_table() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE to_truncate (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO to_truncate VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let before = session
        .execute_sql("SELECT COUNT(*) FROM to_truncate")
        .unwrap();
    assert_table_eq!(before, [[2]]);

    session.execute_sql("TRUNCATE TABLE to_truncate").unwrap();

    let after = session
        .execute_sql("SELECT COUNT(*) FROM to_truncate")
        .unwrap();
    assert_table_eq!(after, [[0]]);
}

#[test]
fn test_truncate_preserves_schema() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE schema_test (id INT64, value STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO schema_test VALUES (1, 'test')")
        .unwrap();

    session.execute_sql("TRUNCATE TABLE schema_test").unwrap();

    session
        .execute_sql("INSERT INTO schema_test VALUES (2, 'new')")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM schema_test").unwrap();
    assert_table_eq!(result, [[2, "new"]]);
}

#[test]
fn test_truncate_empty_table() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE empty_table (id INT64)")
        .unwrap();

    let result = session.execute_sql("TRUNCATE TABLE empty_table");
    assert!(result.is_ok());
}

#[test]
fn test_truncate_large_table() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE large_table (id INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO large_table SELECT n FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS n")
        .unwrap();

    let before = session
        .execute_sql("SELECT COUNT(*) FROM large_table")
        .unwrap();
    assert_table_eq!(before, [[1000]]);

    session.execute_sql("TRUNCATE TABLE large_table").unwrap();

    let after = session
        .execute_sql("SELECT COUNT(*) FROM large_table")
        .unwrap();
    assert_table_eq!(after, [[0]]);
}

#[test]
fn test_truncate_nonexistent_table() {
    let mut session = create_session();
    let result = session.execute_sql("TRUNCATE TABLE nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_truncate_multiple_tables_sequentially() {
    let mut session = create_session();
    session.execute_sql("CREATE TABLE t1 (id INT64)").unwrap();
    session.execute_sql("CREATE TABLE t2 (id INT64)").unwrap();
    session.execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    session.execute_sql("INSERT INTO t2 VALUES (2)").unwrap();

    session.execute_sql("TRUNCATE TABLE t1").unwrap();
    session.execute_sql("TRUNCATE TABLE t2").unwrap();

    let r1 = session.execute_sql("SELECT COUNT(*) FROM t1").unwrap();
    let r2 = session.execute_sql("SELECT COUNT(*) FROM t2").unwrap();
    assert_table_eq!(r1, [[0]]);
    assert_table_eq!(r2, [[0]]);
}

#[test]
fn test_truncate_with_qualified_name() {
    let mut session = create_session();
    session.execute_sql("CREATE SCHEMA test_schema").unwrap();
    session
        .execute_sql("CREATE TABLE test_schema.my_table (id INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO test_schema.my_table VALUES (1)")
        .unwrap();

    session
        .execute_sql("TRUNCATE TABLE test_schema.my_table")
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM test_schema.my_table")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}
