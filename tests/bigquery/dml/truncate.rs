use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE to_truncate (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO to_truncate VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let before = session
        .execute_sql("SELECT COUNT(*) FROM to_truncate")
        .await
        .unwrap();
    assert_table_eq!(before, [[2]]);

    session
        .execute_sql("TRUNCATE TABLE to_truncate")
        .await
        .unwrap();

    let after = session
        .execute_sql("SELECT COUNT(*) FROM to_truncate")
        .await
        .unwrap();
    assert_table_eq!(after, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_preserves_schema() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE schema_test (id INT64, value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO schema_test VALUES (1, 'test')")
        .await
        .unwrap();

    session
        .execute_sql("TRUNCATE TABLE schema_test")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO schema_test VALUES (2, 'new')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM schema_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, "new"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_empty_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE empty_table (id INT64)")
        .await
        .unwrap();

    let result = session.execute_sql("TRUNCATE TABLE empty_table").await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_large_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE large_table (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO large_table SELECT n FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS n")
        .await
        .unwrap();

    let before = session
        .execute_sql("SELECT COUNT(*) FROM large_table")
        .await
        .unwrap();
    assert_table_eq!(before, [[1000]]);

    session
        .execute_sql("TRUNCATE TABLE large_table")
        .await
        .unwrap();

    let after = session
        .execute_sql("SELECT COUNT(*) FROM large_table")
        .await
        .unwrap();
    assert_table_eq!(after, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_nonexistent_table() {
    let session = create_session();
    let result = session.execute_sql("TRUNCATE TABLE nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_multiple_tables_sequentially() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE t1 (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE t2 (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t1 VALUES (1)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t2 VALUES (2)")
        .await
        .unwrap();

    session.execute_sql("TRUNCATE TABLE t1").await.unwrap();
    session.execute_sql("TRUNCATE TABLE t2").await.unwrap();

    let r1 = session
        .execute_sql("SELECT COUNT(*) FROM t1")
        .await
        .unwrap();
    let r2 = session
        .execute_sql("SELECT COUNT(*) FROM t2")
        .await
        .unwrap();
    assert_table_eq!(r1, [[0]]);
    assert_table_eq!(r2, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_with_qualified_name() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA test_schema")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE test_schema.my_table (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO test_schema.my_table VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql("TRUNCATE TABLE test_schema.my_table")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM test_schema.my_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}
