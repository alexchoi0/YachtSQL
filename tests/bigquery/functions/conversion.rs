use crate::assert_table_eq;
use crate::common::{create_session, date};

#[tokio::test(flavor = "current_thread")]
async fn test_cast_int_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(123 AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_string_to_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('456' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[456]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_float_to_int() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(3.7 AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_int_to_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(5 AS FLOAT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_string_to_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('true' AS BOOL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_bool_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(TRUE AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [["true"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_valid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('123' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_cast_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('abc' AS INT64)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(NULL AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (100), (200), (300)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CAST(val AS STRING) FROM data ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [["100"], ["200"], ["300"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES ('10'), ('20'), ('30')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM data WHERE CAST(val AS INT64) > 15 ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [["20"], ["30"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_date_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(DATE '2024-01-15' AS STRING)")
        .await
        .unwrap();
    assert_table_eq!(result, [["2024-01-15"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cast_string_to_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST('2024-06-15' AS DATE)")
        .await
        .unwrap();
    assert_table_eq!(result, [[(date(2024, 6, 15))]]);
}
