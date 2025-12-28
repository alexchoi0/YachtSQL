use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_between_integers() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_lower_bound() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 1 BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_upper_bound() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 10 BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_outside_range() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 15 BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_between() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 15 NOT BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_strings() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'cat' BETWEEN 'apple' AND 'dog'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE scores (id INT64, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES (1, 50), (2, 75), (3, 90), (4, 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM scores WHERE score BETWEEN 40 AND 80 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_between_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 5), (2, 15), (3, 25), (4, 35)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE value NOT BETWEEN 10 AND 30 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL BETWEEN 1 AND 10")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_negative_numbers() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT -5 BETWEEN -10 AND 0")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between_with_expressions() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 10 BETWEEN 2 * 2 AND 5 * 3")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
