#![allow(clippy::approx_constant)]

use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_stddev() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (2), (4), (4), (4), (5), (5), (7), (9)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(STDDEV(val), 6) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.138090]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_stddev_pop() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(STDDEV_POP(val), 6) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.414214]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_stddev_samp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(STDDEV_SAMP(val), 6) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.581139]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variance() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT VARIANCE(val) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_var_pop() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT VAR_POP(val) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_var_samp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT VAR_SAMP(val) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_corr() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(CORR(x, y), 6) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_covar_pop() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 2), (2, 4), (3, 6)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ROUND(COVAR_POP(x, y), 6) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1.333333]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_covar_samp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (x INT64, y INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 2), (2, 4), (3, 6)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COVAR_SAMP(x, y) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_statistical_with_group_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (region STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES ('East', 100), ('East', 150), ('East', 200), ('West', 50), ('West', 75), ('West', 100)").await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT region, ROUND(STDDEV(amount), 2) AS std FROM sales GROUP BY region ORDER BY region",
        ).await
        .unwrap();
    assert_table_eq!(result, [["East", 50.0], ["West", 25.0]]);
}
