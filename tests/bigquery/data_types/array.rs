use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_integers() {
    let session = create_session();
    let result = session.execute_sql("SELECT [1, 2, 3]").await.unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_literal_strings() {
    let session = create_session();
    let result = session.execute_sql("SELECT ['a', 'b', 'c']").await.unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_empty_array() {
    let session = create_session();
    let result = session.execute_sql("SELECT []").await.unwrap();
    assert_table_eq!(result, [[[]]]); // empty array
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, values ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, [10, 20, 30])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT values FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[[10, 20, 30]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_subscript() {
    let session = create_session();
    let result = session.execute_sql("SELECT [10, 20, 30][1]").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_with_() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, values ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, [1, NULL, 3])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT values FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[[1, null, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_null_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, values ARRAY<INT64>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE values IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT [[1, 2], [3, 4]]")
        .await
        .unwrap();
    assert_table_eq!(result, [[[[1, 2], [3, 4]]]]);
}
