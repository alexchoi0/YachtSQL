#![allow(dead_code)]
#![allow(unused_variables)]

use yachtsql::{DialectType, QueryExecutor, RecordBatch};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

fn wrap_query(inner_sql: &str) -> String {
    format!("SELECT * FROM ({}) t", inner_sql)
}

fn get_string(result: &RecordBatch, col: usize, row: usize) -> String {
    let value = result.column(col).unwrap().get(row).unwrap();
    value.as_str().unwrap().to_string()
}

fn get_i64(result: &RecordBatch, col: usize, row: usize) -> i64 {
    result
        .column(col)
        .unwrap()
        .get(row)
        .unwrap()
        .as_i64()
        .unwrap()
}

fn is_null(result: &RecordBatch, col: usize, row: usize) -> bool {
    result.column(col).unwrap().get(row).unwrap().is_null()
}

fn get_json_string(result: &RecordBatch, col: usize, row: usize) -> String {
    let value = result.column(col).unwrap().get(row).unwrap();
    if let Some(j) = value.as_json() {
        serde_json::to_string(j).unwrap()
    } else if let Some(s) = value.as_str() {
        s.to_string()
    } else {
        format!("{:?}", value)
    }
}

fn setup_json_docs(executor: &mut QueryExecutor) {
    let _ = executor.execute_sql("DROP TABLE IF EXISTS docs");
    executor
        .execute_sql("CREATE TABLE docs (id INT64, data JSON)")
        .unwrap();
}

#[test]
fn test_json_extract_simple_field() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"name": "Alice", "age": 30}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$.name') as name FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("Alice"),
        "Expected 'Alice' in result, got: {}",
        json_str
    );
}

#[test]
fn test_json_extract_nested_field() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"user": {"name": "Bob", "age": 25}}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$.user.name') as name FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("Bob"),
        "Expected 'Bob' in result, got: {}",
        json_str
    );
}

#[test]
fn test_json_extract_array_element() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '[10, 20, 30, 40]')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$[1]') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("20"),
        "Expected '20' in result, got: {}",
        json_str
    );
}

#[test]
fn test_json_extract_array_in_object() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"scores": [85, 92, 78]}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$.scores[1]') as score FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("92"),
        "Expected '92' in result, got: {}",
        json_str
    );
}

#[test]
fn test_json_extract_deep_nesting() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"a": {"b": {"c": {"d": 42}}}}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$.a.b.c.d') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("42"),
        "Expected '42' in result, got: {}",
        json_str
    );
}

#[test]
fn test_json_extract_nonexistent_path() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"name": "Alice"}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$.missing') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert!(is_null(&result, 0, 0), "Missing path should return NULL");
}

#[test]
fn test_json_extract_large_number() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"bignum": 9223372036854775807}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$.bignum') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("9223372036854775807"),
        "Expected large number in result, got: {}",
        json_str
    );
}

#[test]
fn test_json_extract_floating_point() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"pi": 3.141592653589793}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$.pi') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("3.14"),
        "Expected pi value in result, got: {}",
        json_str
    );
}

#[test]
fn test_json_extract_deeply_nested() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"l1": {"l2": {"l3": {"l4": {"l5": {"l6": {"l7": {"l8": {"l9": {"l10": "deep"}}}}}}}}}}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$.l1.l2.l3.l4.l5.l6.l7.l8.l9.l10') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("deep"),
        "Expected 'deep' in result, got: {}",
        json_str
    );
}

#[test]
fn test_json_value_extract_string() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"name": "Alice"}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_VALUE(data, '$.name') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, 0, 0), "Alice");
}

#[test]
fn test_json_value_extract_number() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"age": 30}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$.age') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("30"),
        "Expected '30' in result, got: {}",
        json_str
    );
}

#[test]
fn test_json_value_extract_boolean() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"active": true}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_VALUE(data, '$.active') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, 0, 0), "true");
}

#[test]
fn test_json_value_extract_null() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"value": null}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_VALUE(data, '$.value') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);

    let val = result.column(0).unwrap().get(0).unwrap();
    assert!(
        val.is_null() || val.as_str() == Some("null"),
        "Expected NULL or 'null', got: {:?}",
        val
    );
}

#[test]
fn test_json_object_simple() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE people (name STRING, age INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO people VALUES ('Alice', 30)")
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_OBJECT('name', name, 'age', age) as obj FROM people",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("Alice") && json_str.contains("30"),
        "Expected object with name and age, got: {}",
        json_str
    );
}

#[test]
fn test_json_object_empty() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dummy (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dummy VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query("SELECT JSON_OBJECT() as obj FROM dummy"))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(json_str == "{}", "Expected empty object, got: {}", json_str);
}

#[test]
fn test_json_array_simple() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (1, 2, 3)")
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query("SELECT JSON_ARRAY(a, b, c) as arr FROM nums"))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("1") && json_str.contains("2") && json_str.contains("3"),
        "Expected array with 1, 2, 3, got: {}",
        json_str
    );
}

#[test]
fn test_json_array_empty() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dummy (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dummy VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query("SELECT JSON_ARRAY() as arr FROM dummy"))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(json_str == "[]", "Expected empty array, got: {}", json_str);
}

#[test]
fn test_json_query_all_elements() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '[1, 2, 3, 4]')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_QUERY(data, '$[*]') as vals FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_json_keys_simple() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(
            r#"INSERT INTO docs VALUES (1, '{"name": "Alice", "age": 30, "city": "NYC"}')"#,
        )
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_KEYS(data) as keys FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("name") && json_str.contains("age") && json_str.contains("city"),
        "Expected keys array with name, age, city, got: {}",
        json_str
    );
}

#[test]
fn test_json_keys_empty_object() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_KEYS(data) as keys FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str == "[]" || json_str.is_empty(),
        "Expected empty array for empty object keys, got: {}",
        json_str
    );
}

#[test]
fn test_json_length_array() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '[1, 2, 3, 4, 5]')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_LENGTH(data) as len FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_i64(&result, 0, 0), 5);
}

#[test]
fn test_json_length_object() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"a": 1, "b": 2, "c": 3}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_LENGTH(data) as len FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_i64(&result, 0, 0), 3);
}

#[test]
fn test_json_length_empty_array() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '[]')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_LENGTH(data) as len FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_i64(&result, 0, 0), 0);
}

#[test]
fn test_json_length_nested_path() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"items": [1, 2, 3, 4]}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_LENGTH(JSON_EXTRACT(data, '$.items')) as len FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_i64(&result, 0, 0), 4);
}

#[test]
fn test_json_length_scalar() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '"hello"')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_LENGTH(data) as len FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);

    let len = get_i64(&result, 0, 0);
    assert!(
        len == 1 || len == 5,
        "Expected 1 (for scalar) or 5 (for string length), got: {}",
        len
    );
}

#[test]
fn test_json_value_in_where_clause() {
    let mut executor = create_executor();
    let _ = executor.execute_sql("DROP TABLE IF EXISTS users");
    executor
        .execute_sql("CREATE TABLE users (id INT64, data JSON)")
        .unwrap();
    executor
        .execute_sql(r#"INSERT INTO users VALUES (1, '{"name": "Alice", "age": 30}')"#)
        .unwrap();
    executor
        .execute_sql(r#"INSERT INTO users VALUES (2, '{"name": "Bob", "age": 25}')"#)
        .unwrap();
    executor
        .execute_sql(r#"INSERT INTO users VALUES (3, '{"name": "Charlie", "age": 35}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT id FROM users WHERE CAST(JSON_VALUE(data, '$.age') AS INT64) > 28 ORDER BY id",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_i64(&result, 0, 0), 1);
    assert_eq!(get_i64(&result, 0, 1), 3);
}

#[test]
fn test_json_object_from_columns() {
    let mut executor = create_executor();
    let _ = executor.execute_sql("DROP TABLE IF EXISTS people");
    executor
        .execute_sql("CREATE TABLE people (name STRING, age INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO people VALUES ('Alice', 30)")
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_OBJECT('person_name', name, 'person_age', age) as json FROM people",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("person_name") && json_str.contains("Alice"),
        "Expected JSON object with person_name and Alice, got: {}",
        json_str
    );
}

#[test]
fn test_nested_json_functions() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '[1, 2, 3]')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(JSON_OBJECT('data', data), '$.data[1]') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("2"),
        "Expected '2' from nested extraction, got: {}",
        json_str
    );
}

#[test]
fn test_json_extract_unicode() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"emoji": "😀🎉", "chinese": "你好"}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$.chinese') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("你好"),
        "Expected Chinese characters, got: {}",
        json_str
    );
}

#[test]
fn test_json_value_missing_key() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"name": "Alice"}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_VALUE(data, '$.missing') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert!(
        is_null(&result, 0, 0),
        "Missing key in JSON_VALUE should return NULL"
    );
}

#[test]
fn test_json_extract_returns_object() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"user": {"name": "Bob", "age": 25}}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_EXTRACT(data, '$.user') as val FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);

    assert!(
        json_str.contains("name") && json_str.contains("Bob"),
        "Expected nested object, got: {}",
        json_str
    );
}

#[test]
fn test_multiple_json_extracts() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"name": "Alice", "city": "NYC"}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_VALUE(data, '$.name') as name, JSON_VALUE(data, '$.city') as city FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, 0, 0), "Alice");
    assert_eq!(get_string(&result, 1, 0), "NYC");
}

#[test]
fn test_json_length_with_path() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"users": ["Alice", "Bob", "Charlie"]}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_LENGTH(JSON_EXTRACT(data, '$.users')) as len FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_i64(&result, 0, 0), 3);
}

#[test]
fn test_json_keys_nested_path() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"user": {"name": "Bob", "age": 25}}')"#)
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query(
            "SELECT JSON_KEYS(JSON_EXTRACT(data, '$.user')) as keys FROM docs WHERE id = 1",
        ))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("name") && json_str.contains("age"),
        "Expected keys from nested object, got: {}",
        json_str
    );
}

#[test]
fn test_json_array_mixed_types() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mixed (s STRING, n INT64, f FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed VALUES ('hello', 42, 3.14)")
        .unwrap();

    let result = executor
        .execute_sql(&wrap_query("SELECT JSON_ARRAY(s, n, f) as arr FROM mixed"))
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let json_str = get_json_string(&result, 0, 0);
    assert!(
        json_str.contains("hello") && json_str.contains("42") && json_str.contains("3.14"),
        "Expected array with mixed types, got: {}",
        json_str
    );
}

#[test]
fn test_json_extract_multiple_rows() {
    let mut executor = create_executor();
    setup_json_docs(&mut executor);
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (1, '{"name": "Alice"}')"#)
        .unwrap();
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (2, '{"name": "Bob"}')"#)
        .unwrap();
    executor
        .execute_sql(r#"INSERT INTO docs VALUES (3, '{"name": "Charlie"}')"#)
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM (SELECT id, JSON_VALUE(data, '$.name') as val FROM docs ORDER BY id) t",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    assert_eq!(get_string(&result, 1, 0), "Alice");
    assert_eq!(get_string(&result, 1, 1), "Bob");
    assert_eq!(get_string(&result, 1, 2), "Charlie");
}
