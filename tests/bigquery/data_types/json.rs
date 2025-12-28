#![allow(clippy::approx_constant)]

use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_json_literal() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"name\": \"Alice\", \"age\": 30}', '$.name')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE json_data (id INT64, data JSON)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO json_data VALUES (1, JSON '{\"key\": \"value\"}')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM json_data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON 'null')")
        .await
        .unwrap();
    assert_table_eq!(result, [["null"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_boolean() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT BOOL(JSON 'true')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_number() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT INT64(JSON '42')")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_array() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON '[1, 2, 3]')")
        .await
        .unwrap();
    assert_table_eq!(result, [["array"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_nested() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT JSON_QUERY(JSON '{\"outer\": {\"inner\": [1, 2, 3]}}', '$.outer.inner')",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["[1,2,3]"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_access_field() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"name\": \"Bob\"}', '$.name')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_access_array_element() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '[10, 20, 30]', '$[1]')")
        .await
        .unwrap();
    assert_table_eq!(result, [["20"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_dot_notation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, info JSON)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO users VALUES (1, JSON '{\"name\": \"Alice\", \"city\": \"NYC\"}')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING(info.name) FROM users")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_subscript_notation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, payload JSON)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, JSON '{\"items\": [1, 2, 3]}')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT INT64(payload['items'][0]) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_parse_json() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(PARSE_JSON('{\"a\": 1}'), '$.a')")
        .await
        .unwrap();
    assert_table_eq!(result, [["1"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_to_json() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(TO_JSON(STRUCT(1 AS a, 'hello' AS b)), '$.b')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_to_json_string() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT TO_JSON_STRING(STRUCT(1 AS x, 2 AS y))")
        .await
        .unwrap();
    assert_table_eq!(result, [["{\"x\":1,\"y\":2}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_type() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON '\"hello\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [["string"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_type_object() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON '{}')")
        .await
        .unwrap();
    assert_table_eq!(result, [["object"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_type_array() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON '[]')")
        .await
        .unwrap();
    assert_table_eq!(result, [["array"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_type_number() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON '123')")
        .await
        .unwrap();
    assert_table_eq!(result, [["number"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_type_boolean() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON 'false')")
        .await
        .unwrap();
    assert_table_eq!(result, [["boolean"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_query() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_QUERY(JSON '{\"a\": {\"b\": 1}}', '$.a')")
        .await
        .unwrap();
    assert_table_eq!(result, [["{\"b\":1}"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_query_array() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(JSON_QUERY_ARRAY(JSON '[1, 2, 3]', '$'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_value_array() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(JSON_VALUE_ARRAY(JSON '[\"a\", \"b\", \"c\"]', '$'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
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
        .execute_sql("SELECT INT64(JSON '42')")
        .await
        .unwrap();
    assert_table_eq!(result, [[42]]);
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
async fn test_json_object() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON_OBJECT('key1', 1, 'key2', 'value'), '$.key2')")
        .await
        .unwrap();
    assert_table_eq!(result, [["value"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_array_func() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT TO_JSON_STRING(JSON_ARRAY(1, 2, 3))")
        .await
        .unwrap();
    assert_table_eq!(result, [["[1,2,3]"]]);
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
async fn test_lax_json_value() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT LAX_INT64(JSON '\"123\"')")
        .await
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_in_table() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE products (
                id INT64,
                name STRING,
                metadata JSON
            )",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO products VALUES
            (1, 'Widget', JSON '{\"color\": \"red\", \"size\": \"large\"}'),
            (2, 'Gadget', JSON '{\"color\": \"blue\", \"size\": \"small\"}')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM products WHERE JSON_VALUE(metadata, '$.color') = 'red'")
        .await
        .unwrap();
    assert_table_eq!(result, [["Widget"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_json_keys() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(JSON_KEYS(JSON '{\"a\": 1, \"b\": 2, \"c\": 3}'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}
