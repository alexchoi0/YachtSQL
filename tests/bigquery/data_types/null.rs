use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_null_literal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL IS NULL").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 IS NOT NULL").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_in_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 100), (2, NULL), (3, 300)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable WHERE value IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_not_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULL = NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_arithmetic() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 + NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_with_() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, NULL, 'default')")
        .await
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_first_not_() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE('first', 'second')")
        .await
        .unwrap();
    assert_table_eq!(result, [["first"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IFNULL(NULL, 'default')")
        .await
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ifnull_not_() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IFNULL('value', 'default')")
        .await
        .unwrap();
    assert_table_eq!(result, [["value"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(5, 5)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_not_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(5, 10)").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, status STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 'active'), (2, NULL), (3, 'inactive'), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE status IS NOT NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data ORDER BY value NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_order_by_last() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data ORDER BY value NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [[3], [1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_ignores_() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(value) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_star_includes_() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}
