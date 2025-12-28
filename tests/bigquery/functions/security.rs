use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_generate_uuid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT GENERATE_UUID() IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_uuid_uniqueness() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT GENERATE_UUID() != GENERATE_UUID()")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_uuid_lowercase() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT uuid = LOWER(uuid) FROM (SELECT GENERATE_UUID() AS uuid)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_uuid_hyphen_positions() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT
                LENGTH(uuid) = 36
                AND SUBSTR(uuid, 9, 1) = '-'
                AND SUBSTR(uuid, 14, 1) = '-'
                AND SUBSTR(uuid, 19, 1) = '-'
                AND SUBSTR(uuid, 24, 1) = '-'
            FROM (SELECT GENERATE_UUID() AS uuid)",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_session_user() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SESSION_USER() IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_current_user() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CURRENT_USER() IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%s has %d items', 'cart', 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [["cart has 5 items"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%.2f', 3.12131)")
        .await
        .unwrap();
    assert_table_eq!(result, [["3.12"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%t', DATE '2024-01-15') IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10, 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_by_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10, 0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_multiply() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_MULTIPLY(1000000000000, 1000000000000)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_negate() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_NEGATE(-9223372036854775808)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_add() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_ADD(9223372036854775807, 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_subtract() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_SUBTRACT(-9223372036854775808, 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_convert_bytes_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_error_function() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 1 > 0 THEN 'ok' ELSE ERROR('should not happen') END")
        .await
        .unwrap();
    assert_table_eq!(result, [["ok"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ifnull() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IFNULL(NULL, 'default')")
        .await
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(5, 5)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_not_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(5, 3)").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_uuid_in_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id STRING, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO items VALUES (GENERATE_UUID(), 'item1'), (GENERATE_UUID(), 'item2')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT id) FROM items")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}
