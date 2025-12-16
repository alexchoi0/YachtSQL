use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_struct_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRUCT(1 AS a, 'hello' AS b).a, STRUCT(1 AS a, 'hello' AS b).b")
        .unwrap();
    assert_table_eq!(result, [[1, "hello"]]);
}

#[test]
fn test_struct_field_access() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRUCT(1 AS a, 'hello' AS b).a")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_struct_string_field_access() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRUCT(1 AS a, 'hello' AS b).b")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_struct_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, info STRUCT<name STRING, age INT64>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, STRUCT('Alice' AS name, 30 AS age)), (2, STRUCT('Bob' AS name, 25 AS age))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, info.name FROM users ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[test]
fn test_struct_nested() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRUCT(STRUCT(1 AS x, 2 AS y) AS point, 'origin' AS name).point.x")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_struct_nested_access() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRUCT(STRUCT(1 AS x, 2 AS y) AS point, 'origin' AS name).point.x")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_struct_in_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH([STRUCT(1 AS a, 'x' AS b), STRUCT(2 AS a, 'y' AS b)])")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_struct_with_null_field() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRUCT(1 AS a, NULL AS b).a, STRUCT(1 AS a, NULL AS b).b IS NULL")
        .unwrap();
    assert_table_eq!(result, [[1, true]]);
}

#[test]
fn test_struct_comparison() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRUCT(1, 2) = STRUCT(1, 2)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_struct_comparison_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRUCT(1, 2) = STRUCT(1, 3)")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_struct_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, info STRUCT<name STRING, value INT64>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, STRUCT('a' AS name, 10 AS value)), (2, STRUCT('b' AS name, 20 AS value)), (3, STRUCT('c' AS name, 30 AS value))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM data WHERE info.value > 15 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_struct_in_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, info STRUCT<category STRING, value INT64>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, STRUCT('A' AS category, 10 AS value)), (2, STRUCT('A' AS category, 20 AS value)), (3, STRUCT('B' AS category, 30 AS value))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT info.category, SUM(info.value) FROM data GROUP BY info.category ORDER BY info.category")
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_struct_in_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, info STRUCT<name STRING, priority INT64>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, STRUCT('c' AS name, 3 AS priority)), (2, STRUCT('a' AS name, 1 AS priority)), (3, STRUCT('b' AS name, 2 AS priority))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM data ORDER BY info.priority")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[test]
fn test_struct_with_array_field() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(STRUCT([1, 2, 3] AS numbers, 'test' AS name).numbers)")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_struct_unnest() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT s.a, s.b FROM UNNEST([STRUCT(1 AS a, 'x' AS b), STRUCT(2 AS a, 'y' AS b)]) AS s ORDER BY s.a")
        .unwrap();
    assert_table_eq!(result, [[1, "x"], [2, "y"]]);
}

#[test]
fn test_struct_as_function_result() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT STRUCT(id AS id, value AS value, id + value AS sum).sum FROM data ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[11], [22]]);
}

#[test]
fn test_struct_is_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRUCT(NULL AS a, NULL AS b) IS NULL")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_null_struct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, info STRUCT<name STRING, value INT64>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM data WHERE info IS NULL")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_struct_concat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CONCAT(STRUCT('hello' AS a, 'world' AS b).a, ' ', STRUCT('hello' AS a, 'world' AS b).b)")
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}
