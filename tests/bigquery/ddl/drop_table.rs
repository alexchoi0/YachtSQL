use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_drop_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE test_table (id INT64)")
        .await
        .unwrap();

    session.execute_sql("DROP TABLE test_table").await.unwrap();

    let result = session.execute_sql("SELECT * FROM test_table").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_table_if_exists() {
    let session = create_session();

    session
        .execute_sql("DROP TABLE IF EXISTS nonexistent_table")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE existing_table (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("DROP TABLE IF EXISTS existing_table")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM existing_table").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_multiple_tables() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE table1 (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE table2 (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("DROP TABLE table1, table2")
        .await
        .unwrap();

    let result1 = session.execute_sql("SELECT * FROM table1").await;
    let result2 = session.execute_sql("SELECT * FROM table2").await;
    assert!(result1.is_err());
    assert!(result2.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_table_restrict() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE standalone (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("DROP TABLE standalone RESTRICT")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM standalone").await;
    assert!(result.is_err());
}
