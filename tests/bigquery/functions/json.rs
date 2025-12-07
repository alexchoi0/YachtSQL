#![allow(clippy::approx_constant)]

use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_json_extract() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_EXTRACT('{\"name\": \"alice\"}', '$.name')")
        .unwrap();
    assert_table_eq!(result, [["\"alice\""]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_query() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_QUERY('{\"items\": [1, 2, 3]}', '$.items')")
        .unwrap();
    assert_table_eq!(result, [["[1,2,3]"]]);
}

#[test]
fn test_json_value() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_VALUE('{\"name\": \"bob\"}', '$.name')")
        .unwrap();
    assert_table_eq!(result, [["bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_with_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, json_data STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO data VALUES (1, '{\"name\": \"alice\"}'), (2, '{\"name\": \"bob\"}')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, JSON_EXTRACT_SCALAR(json_data, '$.name') AS name FROM data ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "alice"], [2, "bob"]]);
}

#[test]
fn test_json_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_EXTRACT(NULL, '$.name')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_nested() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT JSON_EXTRACT_SCALAR('{\"user\": {\"name\": \"alice\"}}', '$.user.name')",
        )
        .unwrap();
    assert_table_eq!(result, [["alice"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_array_element() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT JSON_EXTRACT_SCALAR('{\"items\": [\"a\", \"b\", \"c\"]}', '$.items[0]')",
        )
        .unwrap();
    assert_table_eq!(result, [["a"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_extract_scalar() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_EXTRACT_SCALAR('{\"name\": \"alice\"}', '$.name')")
        .unwrap();
    assert_table_eq!(result, [["alice"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_to_json() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_JSON(STRUCT(1 AS a, 'hello' AS b))")
        .unwrap();
    assert_table_eq!(result, [["{\"a\":1,\"b\":\"hello\"}"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_to_json_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_JSON_STRING(STRUCT(1 AS a, 'hello' AS b))")
        .unwrap();
    assert_table_eq!(result, [["{\"a\":1,\"b\":\"hello\"}"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_parse_json() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_VALUE(PARSE_JSON('{\"a\": 1, \"b\": \"hello\"}'), '$.b')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_json_type() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_TYPE(JSON '{\"a\": 1}')")
        .unwrap();
    assert_table_eq!(result, [["object"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_extract_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(JSON_EXTRACT_ARRAY('{\"items\": [1, 2, 3]}', '$.items'))")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_extract_string_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(JSON_EXTRACT_STRING_ARRAY('{\"items\": [\"a\", \"b\"]}', '$.items'))")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_query_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(JSON_QUERY_ARRAY('{\"items\": [1, 2, 3]}', '$.items'))")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_value_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ARRAY_LENGTH(JSON_VALUE_ARRAY('{\"items\": [\"a\", \"b\"]}', '$.items'))",
        )
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_json_object() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_VALUE(JSON_OBJECT('key1', 1, 'key2', 'value'), '$.key2')")
        .unwrap();
    assert_table_eq!(result, [["value"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_ARRAY(1, 2, 'three')")
        .unwrap();
    assert_table_eq!(result, [["[1,2,\"three\"]"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_set() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_VALUE(JSON_SET(JSON '{\"a\": 1}', '$.b', 2), '$.b')")
        .unwrap();
    assert_table_eq!(result, [["2"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_strip_nulls() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_STRIP_NULLS(JSON '{\"a\": 1, \"b\": null}')")
        .unwrap();
    assert_table_eq!(result, [["{\"a\":1}"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_remove() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_REMOVE(JSON '{\"a\": 1, \"b\": 2}', '$.b')")
        .unwrap();
    assert_table_eq!(result, [["{\"a\":1}"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bool_from_json() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT BOOL(JSON 'true')").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_int64_from_json() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT INT64(JSON '123')").unwrap();
    assert_table_eq!(result, [[123]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_float64_from_json() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT FLOAT64(JSON '3.14')").unwrap();
    assert_table_eq!(result, [[3.14]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_string_from_json() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRING(JSON '\"hello\"')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_lax_bool() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LAX_BOOL(JSON '\"true\"')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_lax_int64() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LAX_INT64(JSON '\"123\"')")
        .unwrap();
    assert_table_eq!(result, [[123]]);
}
