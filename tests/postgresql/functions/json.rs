use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_json_object_access() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"name": "John"}'::JSON -> 'name'"#)
        .unwrap();
    assert_table_eq!(result, [["\"John\""]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_text_access() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"name": "John"}'::JSON ->> 'name'"#)
        .unwrap();
    assert_table_eq!(result, [["John"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_array_access() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '[1, 2, 3]'::JSON -> 0"#)
        .unwrap();
    assert_table_eq!(result, [["1"]]);
}

#[test]
fn test_json_array_text() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '["a", "b", "c"]'::JSON ->> 1"#)
        .unwrap();
    assert_table_eq!(result, [["b"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_nested_access() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"user": {"name": "John"}}'::JSON -> 'user' ->> 'name'"#)
        .unwrap();
    assert_table_eq!(result, [["John"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, data JSON)")
        .unwrap();
    executor
        .execute_sql(r#"INSERT INTO users VALUES (1, '{"name": "Alice", "age": 30}')"#)
        .unwrap();

    let result = executor
        .execute_sql("SELECT data ->> 'name' FROM users")
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_path_access() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": {"b": {"c": 1}}}'::JSON #> '{a,b,c}'"#)
        .unwrap();
    assert_table_eq!(result, [["1"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_contains() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": 1, "b": 2}'::JSONB @> '{"a": 1}'::JSONB"#)
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_exists() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT '{"a": 1}'::JSONB ? 'a'"#)
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_typeof() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT json_typeof('{"a": 1}'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [["object"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_typeof_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT json_typeof('[1,2,3]'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [["array"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_typeof_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT json_typeof('"hello"'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [["string"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_typeof_number() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT json_typeof('42'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [["number"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_json_array_length() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT json_array_length('[1, 2, 3, 4, 5]'::JSON)"#)
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_jsonb_pretty() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r#"SELECT jsonb_pretty('{"a":1}'::JSONB)"#)
        .unwrap();
    assert_table_eq!(result, [["{\n    \"a\": 1\n}"]]);
}
