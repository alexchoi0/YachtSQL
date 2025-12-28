use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NUMERIC '123.456' AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123.456]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bignumeric_literal() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BIGNUMERIC '12345678901234567890.12345678901234567890' > 0")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_in_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE prices (id INT64, price NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO prices VALUES (1, 19.99), (2, 29.99), (3, 39.99)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM prices WHERE price > 25 ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bignumeric_in_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE big_values (id INT64, val BIGNUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO big_values VALUES (1, BIGNUMERIC '99999999999999999999.99999999999999999999')").await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM big_values")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_arithmetic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NUMERIC '10.5' + NUMERIC '5.5' AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[16.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_multiplication() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NUMERIC '10.5' * NUMERIC '2' AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[21.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_division() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(CAST(NUMERIC '10' / NUMERIC '3' AS FLOAT64), 6)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.333333]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_sum() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE amounts (val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO amounts VALUES (10.5), (20.25), (30.75)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CAST(SUM(val) AS FLOAT64) FROM amounts")
        .await
        .unwrap();
    assert_table_eq!(result, [[61.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_avg() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE amounts (val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO amounts VALUES (10), (20), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CAST(AVG(val) AS FLOAT64) FROM amounts")
        .await
        .unwrap();
    assert_table_eq!(result, [[20.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_comparison() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '10.5' > NUMERIC '10.4'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_equality() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '10.50' = NUMERIC '10.5'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_cast_from_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(CAST(100 AS NUMERIC) AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[100.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_cast_from_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ROUND(CAST(CAST(3.12131 AS NUMERIC) AS FLOAT64), 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.12131]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_cast_from_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(CAST('123.456' AS NUMERIC) AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123.456]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_round() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(ROUND(NUMERIC '123.456', 2) AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123.46]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_trunc() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(TRUNC(NUMERIC '123.789', 1) AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123.7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_abs() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(ABS(NUMERIC '-123.45') AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123.45]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE amounts (id INT64, val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO amounts VALUES (1, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM amounts WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE amounts (id INT64, val NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO amounts VALUES (1, 30.5), (2, 10.25), (3, 20.75)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM amounts ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_group_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (category STRING, amount NUMERIC)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES ('A', 10.5), ('A', 20.5), ('B', 30.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, CAST(SUM(amount) AS FLOAT64) FROM sales GROUP BY category ORDER BY category").await
        .unwrap();
    assert_table_eq!(result, [["A", 31.0], ["B", 30.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_numeric_precision_preservation() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NUMERIC '0.123456789012345678901234567890123456789' > 0")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bignumeric_precision_preservation() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BIGNUMERIC '0.12345678901234567890123456789012345678901234567890' > 0")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
