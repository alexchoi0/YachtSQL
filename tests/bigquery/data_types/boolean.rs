use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_true() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val BOOLEAN)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (true)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_false() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val BOOLEAN)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (false)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val BOOLEAN)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_and() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT true AND true, true AND false, false AND false")
        .await
        .unwrap();

    assert_table_eq!(result, [[true, false, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_or() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT true OR true, true OR false, false OR false")
        .await
        .unwrap();

    assert_table_eq!(result, [[true, true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_not() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT NOT true, NOT false")
        .await
        .unwrap();

    assert_table_eq!(result, [[false, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_filter() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (id INT64, active BOOLEAN)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, true), (2, false), (3, true)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE active ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_comparison_result() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 1 = 1, 1 = 2, 1 < 2, 1 > 2")
        .await
        .unwrap();

    assert_table_eq!(result, [[true, false, true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_case_when() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT val, CASE WHEN val > 1 THEN true ELSE false END AS gt1 FROM t ORDER BY val",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, false], [2, true], [3, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_null_and() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT NULL AND true, NULL AND false")
        .await
        .unwrap();

    assert_table_eq!(result, [[null, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_boolean_null_or() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT NULL OR true, NULL OR false")
        .await
        .unwrap();

    assert_table_eq!(result, [[true, null]]);
}
