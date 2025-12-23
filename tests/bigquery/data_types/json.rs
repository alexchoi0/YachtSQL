#![allow(clippy::approx_constant)]

use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_json_literal() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"name\": \"Alice\", \"age\": 30}', '$.name')")
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_json_column() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE json_data (id INT64, data JSON)")
        .unwrap();
    session
        .execute_sql("INSERT INTO json_data VALUES (1, JSON '{\"key\": \"value\"}')")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM json_data").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_json_() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON 'null')")
        .unwrap();
    assert_table_eq!(result, [["null"]]);
}

#[test]
fn test_json_boolean() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT BOOL(JSON 'true')").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_json_number() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT INT64(JSON '42')").unwrap();
    assert_table_eq!(result, [[42]]);
}

#[test]
fn test_json_array() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON '[1, 2, 3]')")
        .unwrap();
    assert_table_eq!(result, [["array"]]);
}

#[test]
fn test_json_nested() {
    let mut session = create_session();

    let result = session
        .execute_sql(
            "SELECT JSON_QUERY(JSON '{\"outer\": {\"inner\": [1, 2, 3]}}', '$.outer.inner')",
        )
        .unwrap();
    assert_table_eq!(result, [["[1,2,3]"]]);
}

#[test]
fn test_json_access_field() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '{\"name\": \"Bob\"}', '$.name')")
        .unwrap();
    assert_table_eq!(result, [["Bob"]]);
}

#[test]
fn test_json_access_array_element() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON '[10, 20, 30]', '$[1]')")
        .unwrap();
    assert_table_eq!(result, [["20"]]);
}

#[test]
fn test_json_dot_notation() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, info JSON)")
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO users VALUES (1, JSON '{\"name\": \"Alice\", \"city\": \"NYC\"}')",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT STRING(info.name) FROM users")
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_json_subscript_notation() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, payload JSON)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, JSON '{\"items\": [1, 2, 3]}')")
        .unwrap();

    let result = session
        .execute_sql("SELECT INT64(payload['items'][0]) FROM data")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_parse_json() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(PARSE_JSON('{\"a\": 1}'), '$.a')")
        .unwrap();
    assert_table_eq!(result, [["1"]]);
}

#[test]
fn test_to_json() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(TO_JSON(STRUCT(1 AS a, 'hello' AS b)), '$.b')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_to_json_string() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT TO_JSON_STRING(STRUCT(1 AS x, 2 AS y))")
        .unwrap();
    assert_table_eq!(result, [["{\"x\":1,\"y\":2}"]]);
}

#[test]
fn test_json_type() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON '\"hello\"')")
        .unwrap();
    assert_table_eq!(result, [["string"]]);
}

#[test]
fn test_json_type_object() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT JSON_TYPE(JSON '{}')").unwrap();
    assert_table_eq!(result, [["object"]]);
}

#[test]
fn test_json_type_array() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT JSON_TYPE(JSON '[]')").unwrap();
    assert_table_eq!(result, [["array"]]);
}

#[test]
fn test_json_type_number() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT JSON_TYPE(JSON '123')").unwrap();
    assert_table_eq!(result, [["number"]]);
}

#[test]
fn test_json_type_boolean() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_TYPE(JSON 'false')")
        .unwrap();
    assert_table_eq!(result, [["boolean"]]);
}

#[test]
fn test_json_query() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_QUERY(JSON '{\"a\": {\"b\": 1}}', '$.a')")
        .unwrap();
    assert_table_eq!(result, [["{\"b\":1}"]]);
}

#[test]
fn test_json_query_array() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(JSON_QUERY_ARRAY(JSON '[1, 2, 3]', '$'))")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_json_value_array() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(JSON_VALUE_ARRAY(JSON '[\"a\", \"b\", \"c\"]', '$'))")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_bool_from_json() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT BOOL(JSON 'true')").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_int64_from_json() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT INT64(JSON '42')").unwrap();
    assert_table_eq!(result, [[42]]);
}

#[test]
fn test_float64_from_json() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT FLOAT64(JSON '3.14')").unwrap();
    assert_table_eq!(result, [[3.14]]);
}

#[test]
fn test_string_from_json() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT STRING(JSON '\"hello\"')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_json_object() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON_OBJECT('key1', 1, 'key2', 'value'), '$.key2')")
        .unwrap();
    assert_table_eq!(result, [["value"]]);
}

#[test]
fn test_json_array_func() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT TO_JSON_STRING(JSON_ARRAY(1, 2, 3))")
        .unwrap();
    assert_table_eq!(result, [["[1,2,3]"]]);
}

#[test]
fn test_json_set() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_VALUE(JSON_SET(JSON '{\"a\": 1}', '$.b', 2), '$.b')")
        .unwrap();
    assert_table_eq!(result, [["2"]]);
}

#[test]
fn test_json_strip_nulls() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_STRIP_NULLS(JSON '{\"a\": 1, \"b\": null}')")
        .unwrap();
    assert_table_eq!(result, [["{\"a\":1}"]]);
}

#[test]
fn test_json_remove() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT JSON_REMOVE(JSON '{\"a\": 1, \"b\": 2}', '$.b')")
        .unwrap();
    assert_table_eq!(result, [["{\"a\":1}"]]);
}

#[test]
fn test_lax_json_value() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT LAX_INT64(JSON '\"123\"')")
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[test]
fn test_json_in_table() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TABLE products (
                id INT64,
                name STRING,
                metadata JSON
            )",
        )
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO products VALUES
            (1, 'Widget', JSON '{\"color\": \"red\", \"size\": \"large\"}'),
            (2, 'Gadget', JSON '{\"color\": \"blue\", \"size\": \"small\"}')",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM products WHERE JSON_VALUE(metadata, '$.color') = 'red'")
        .unwrap();
    assert_table_eq!(result, [["Widget"]]);
}

#[test]
fn test_json_keys() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(JSON_KEYS(JSON '{\"a\": 1, \"b\": 2, \"c\": 3}'))")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}
