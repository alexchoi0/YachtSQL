#![allow(clippy::approx_constant)]

use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_json_extract() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_EXTRACT('{\"name\": \"alice\"}', '$.name')")
        .await
        .unwrap();
    assert_table_eq!(result, [["\"alice\""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_query() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_QUERY('{\"items\": [1, 2, 3]}', '$.items')")
        .await
        .unwrap();
    assert_table_eq!(result, [["[1,2,3]"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_value() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE('{\"name\": \"bob\"}', '$.name')")
        .await
        .unwrap();
    assert_table_eq!(result, [["bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, json_data STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO data VALUES (1, '{\"name\": \"alice\"}'), (2, '{\"name\": \"bob\"}')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, JSON_EXTRACT_SCALAR(json_data, '$.name') AS name FROM data ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "alice"], [2, "bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_EXTRACT(NULL, '$.name')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_nested() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT JSON_EXTRACT_SCALAR('{\"user\": {\"name\": \"alice\"}}', '$.user.name')",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_array_element() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT JSON_EXTRACT_SCALAR('{\"items\": [\"a\", \"b\", \"c\"]}', '$.items[0]')",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["a"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_extract_scalar() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_EXTRACT_SCALAR('{\"name\": \"alice\"}', '$.name')")
        .await
        .unwrap();
    assert_table_eq!(result, [["alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_to_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_JSON(STRUCT(1 AS a, 'hello' AS b))")
        .await
        .unwrap();
    assert_table_eq!(result, [["{\"a\":1,\"b\":\"hello\"}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_to_json_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_JSON_STRING(STRUCT(1 AS a, 'hello' AS b))")
        .await
        .unwrap();
    assert_table_eq!(result, [["{\"a\":1,\"b\":\"hello\"}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_parse_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(PARSE_JSON('{\"a\": 1, \"b\": \"hello\"}'), '$.b')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_type() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON '{\"a\": 1}')")
        .await
        .unwrap();
    assert_table_eq!(result, [["object"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_extract_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('{\"items\": [1, 2, 3]}', '$.items'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_extract_string_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(JSON_EXTRACT_STRING_ARRAY('{\"items\": [\"a\", \"b\"]}', '$.items'))").await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_query_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(JSON_QUERY_ARRAY('{\"items\": [1, 2, 3]}', '$.items'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_value_array() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT ARRAY_LENGTH(JSON_VALUE_ARRAY('{\"items\": [\"a\", \"b\"]}', '$.items'))",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_object() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON_OBJECT('key1', 1, 'key2', 'value'), '$.key2')")
        .await
        .unwrap();
    assert_table_eq!(result, [["value"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_JSON_STRING(JSON_ARRAY(1, 2, 'three'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["[1,2,\"three\"]"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_set() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON_SET(JSON '{\"a\": 1}', '$.b', 2), '$.b')")
        .await
        .unwrap();
    assert_table_eq!(result, [["2"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_strip_nulls() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_STRIP_NULLS(JSON '{\"a\": 1, \"b\": null}')")
        .await
        .unwrap();
    assert_table_eq!(result, [["{\"a\":1}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_remove() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT JSON_REMOVE(JSON '{\"a\": 1, \"b\": 2}', '$.b')")
        .await
        .unwrap();
    assert_table_eq!(result, [["{\"a\":1}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_bool_from_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BOOL(JSON 'true')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_int64_from_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INT64(JSON '123')")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_float64_from_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FLOAT64(JSON '3.14')")
        .await
        .unwrap();
    assert_table_eq!(result, [[3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_from_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRING(JSON '\"hello\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_bool() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_BOOL(JSON '\"true\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lax_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LAX_INT64(JSON '\"123\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}
