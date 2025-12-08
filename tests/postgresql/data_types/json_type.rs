use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_json_literal_object() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"name": "Alice", "age": 30}'::JSON"#)
        .unwrap();
    assert_table_eq!(result, [[r#"{"name": "Alice", "age": 30}"#]]);
}

#[test]
fn test_json_literal_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '[1, 2, 3, 4, 5]'::JSON"#)
        .unwrap();
    assert_table_eq!(result, [["[1, 2, 3, 4, 5]"]]);
}

#[test]
fn test_jsonb_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"name": "Bob"}'::JSONB"#)
        .unwrap();
    assert_table_eq!(result, [[r#"{"name": "Bob"}"#]]);
}

#[test]
fn test_json_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE json_data (id INTEGER, data JSON)")
        .unwrap();
    executor
        .execute_sql(r#"INSERT INTO json_data VALUES (1, '{"key": "value"}')"#)
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM json_data").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_jsonb_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE jsonb_data (id INTEGER, data JSONB)")
        .unwrap();
    executor
        .execute_sql(r#"INSERT INTO jsonb_data VALUES (1, '{"key": "value"}')"#)
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM jsonb_data").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_json_arrow_operator() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"name": "Alice"}'::JSON -> 'name'"#)
        .unwrap();
    assert_table_eq!(result, [[r#""Alice""#]]);
}

#[test]
fn test_json_double_arrow_operator() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"name": "Alice"}'::JSON ->> 'name'"#)
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_json_array_index() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '[10, 20, 30]'::JSON -> 1"#)
        .unwrap();
    assert_table_eq!(result, [["20"]]);
}

#[test]
fn test_json_array_index_text() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '["a", "b", "c"]'::JSON ->> 0"#)
        .unwrap();
    assert_table_eq!(result, [["a"]]);
}

#[test]
fn test_json_path_operator() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": {"b": "c"}}'::JSON #> '{a,b}'"#)
        .unwrap();
    assert_table_eq!(result, [[r#""c""#]]);
}

#[test]
fn test_json_path_text_operator() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": {"b": "deep"}}'::JSON #>> '{a,b}'"#)
        .unwrap();
    assert_table_eq!(result, [["deep"]]);
}

#[test]
fn test_jsonb_contains() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": 1, "b": 2}'::JSONB @> '{"a": 1}'::JSONB"#)
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_jsonb_contained_by() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": 1}'::JSONB <@ '{"a": 1, "b": 2}'::JSONB"#)
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_jsonb_key_exists() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": 1, "b": 2}'::JSONB ? 'a'"#)
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_jsonb_any_key_exists() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": 1, "b": 2}'::JSONB ?| ARRAY['a', 'c']"#)
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_jsonb_all_keys_exist() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": 1, "b": 2}'::JSONB ?& ARRAY['a', 'b']"#)
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_jsonb_concat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": 1}'::JSONB || '{"b": 2}'::JSONB"#)
        .unwrap();
    assert_table_eq!(result, [[r#"{"a": 1, "b": 2}"#]]);
}

#[test]
fn test_jsonb_delete_key() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": 1, "b": 2}'::JSONB - 'a'"#)
        .unwrap();
    assert_table_eq!(result, [[r#"{"b": 2}"#]]);
}

#[test]
fn test_jsonb_delete_index() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '[1, 2, 3]'::JSONB - 1"#)
        .unwrap();
    assert_table_eq!(result, [["[1, 3]"]]);
}

#[test]
fn test_jsonb_delete_path() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": {"b": 1}}'::JSONB #- '{a,b}'"#)
        .unwrap();
    assert_table_eq!(result, [[r#"{"a": {}}"#]]);
}

#[test]
fn test_json_typeof() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSON_TYPEOF('{"a": 1}'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [["object"]]);
}

#[test]
fn test_json_typeof_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSON_TYPEOF('[1,2,3]'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [["array"]]);
}

#[test]
fn test_json_typeof_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSON_TYPEOF('"hello"'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [["string"]]);
}

#[test]
fn test_json_typeof_number() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSON_TYPEOF('42'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [["number"]]);
}

#[test]
fn test_json_typeof_boolean() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSON_TYPEOF('true'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [["boolean"]]);
}

#[test]
fn test_json_typeof_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSON_TYPEOF('null'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [["null"]]);
}

#[test]
fn test_json_array_length() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSON_ARRAY_LENGTH('[1,2,3,4,5]'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_json_object_keys() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSON_OBJECT_KEYS('{"a":1,"b":2}'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [["a"], ["b"]]);
}

#[test]
fn test_json_build_object() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_BUILD_OBJECT('name', 'Alice', 'age', 30)")
        .unwrap();
    assert_table_eq!(result, [[r#"{"name": "Alice", "age": 30}"#]]);
}

#[test]
fn test_json_build_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSON_BUILD_ARRAY(1, 2, 'three')")
        .unwrap();
    assert_table_eq!(result, [[r#"[1, 2, "three"]"#]]);
}

#[test]
fn test_to_json() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_JSON('hello'::TEXT)")
        .unwrap();
    assert_table_eq!(result, [[r#""hello""#]]);
}

#[test]
fn test_to_jsonb() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TO_JSONB('hello'::TEXT)")
        .unwrap();
    assert_table_eq!(result, [[r#""hello""#]]);
}

#[test]
fn test_jsonb_pretty() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSONB_PRETTY('{"a":1,"b":2}'::JSONB)"#)
        .unwrap();
    assert_table_eq!(
        result,
        [[r#"{
    "a": 1,
    "b": 2
}"#]]
    );
}

#[test]
fn test_json_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE json_null (id INTEGER, data JSON)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO json_null VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT data IS NULL FROM json_null")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_jsonb_set() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSONB_SET('{"a":1}'::JSONB, '{a}', '2')"#)
        .unwrap();
    assert_table_eq!(result, [[r#"{"a": 2}"#]]);
}

#[test]
fn test_jsonb_insert() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSONB_INSERT('[1,2,3]'::JSONB, '{1}', '10')"#)
        .unwrap();
    assert_table_eq!(result, [["[1, 10, 2, 3]"]]);
}

#[test]
fn test_json_strip_nulls() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT JSON_STRIP_NULLS('{"a":1,"b":null}'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [[r#"{"a": 1}"#]]);
}
