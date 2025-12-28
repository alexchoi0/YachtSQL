use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_bit_count() {
    let session = create_session();
    let result = session.execute_sql("SELECT BIT_COUNT(5)").await.unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_count_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT BIT_COUNT(0)").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_count_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BIT_COUNT(b'\\xFF')")
        .await
        .unwrap();
    assert_table_eq!(result, [[8]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BYTE_LENGTH('hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_length_unicode() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BYTE_LENGTH('日本語')")
        .await
        .unwrap();
    assert_table_eq!(result, [[9]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_length_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BYTE_LENGTH(b'\\x00\\x01\\x02')")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_and_aggregate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bits (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bits VALUES (7), (3), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_AND(val) FROM bits")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_or_aggregate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bits (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bits VALUES (1), (2), (4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_OR(val) FROM bits")
        .await
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_xor_aggregate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bits (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bits VALUES (5), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT BIT_XOR(val) FROM bits")
        .await
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_operations_with_group_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE flags (category STRING, flag INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO flags VALUES ('A', 1), ('A', 2), ('A', 4), ('B', 3), ('B', 5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, BIT_OR(flag) FROM flags GROUP BY category ORDER BY category")
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 7], ["B", 7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bit_count_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT BIT_COUNT(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}
