use crate::assert_table_eq;
use crate::common::{create_session, n};

#[tokio::test(flavor = "current_thread")]
async fn test_int64_basic() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (42)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_negative() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (-100)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[-100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_zero() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (0)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_basic() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (3.11)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[3.11]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_scientific() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1.5e10)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[1.5e10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_basic() {
    let session = create_session();

    let result = session.execute_sql("SELECT 123.456").await.unwrap();

    assert_table_eq!(result, [[n("123.456")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_arithmetic() {
    let session = create_session();

    let result = session.execute_sql("SELECT 10.5 + 2.3").await.unwrap();

    assert_table_eq!(result, [[n("12.8")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val INT64)")
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
async fn test_numeric_comparison() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nums (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (10, 20), (30, 30), (50, 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a FROM nums WHERE a >= b ORDER BY a")
        .await
        .unwrap();

    assert_table_eq!(result, [[30], [50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_in_expression() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10), (20), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val * 2, val + 5 FROM t ORDER BY val")
        .await
        .unwrap();

    assert_table_eq!(result, [[20, 15], [40, 25], [60, 35],]);
}
