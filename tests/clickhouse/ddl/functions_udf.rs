use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Fix me!"]
#[test]
fn test_create_function_simple() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION double AS (x) -> x * 2")
        .unwrap();

    let result = executor.execute_sql("SELECT double(5)").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[ignore = "Fix me!"]
#[test]
fn test_create_function_two_args() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION add_values AS (a, b) -> a + b")
        .unwrap();

    let result = executor.execute_sql("SELECT add_values(3, 7)").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[ignore = "Fix me!"]
#[test]
fn test_create_function_string() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION greeting AS (name) -> concat('Hello, ', name)")
        .unwrap();

    let result = executor.execute_sql("SELECT greeting('World')").unwrap();
    assert_table_eq!(result, [["Hello, World"]]);
}

#[test]
fn test_create_function_if_not_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION IF NOT EXISTS test_func AS (x) -> x + 1")
        .unwrap();
    executor
        .execute_sql("CREATE FUNCTION IF NOT EXISTS test_func AS (x) -> x + 1")
        .unwrap();
}

#[test]
fn test_drop_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION drop_func AS (x) -> x")
        .unwrap();
    executor.execute_sql("DROP FUNCTION drop_func").unwrap();
}

#[test]
fn test_drop_function_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP FUNCTION IF EXISTS nonexistent_func")
        .unwrap();
}

#[ignore = "Fix me!"]
#[test]
fn test_create_function_with_case() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION classify AS (x) -> CASE WHEN x > 0 THEN 'positive' WHEN x < 0 THEN 'negative' ELSE 'zero' END")
        .unwrap();

    let result = executor
        .execute_sql("SELECT classify(5), classify(-3), classify(0)")
        .unwrap();
    assert_table_eq!(result, [["positive", "negative", "zero"]]);
}

#[ignore = "Fix me!"]
#[test]
fn test_create_function_with_builtin() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION safe_sqrt AS (x) -> if(x >= 0, sqrt(x), NULL)")
        .unwrap();

    let result = executor.execute_sql("SELECT safe_sqrt(16)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Fix me!"]
#[test]
fn test_function_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION is_even AS (x) -> x % 2 = 0")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE func_where (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO func_where VALUES (1), (2), (3), (4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM func_where WHERE is_even(id) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);
}

#[ignore = "Fix me!"]
#[test]
fn test_function_in_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION negate AS (x) -> -x")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE func_order (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO func_order VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM func_order ORDER BY negate(id)")
        .unwrap();
    assert_table_eq!(result, [[3], [2], [1]]);
}

#[ignore = "Fix me!"]
#[test]
fn test_nested_function_call() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION square AS (x) -> x * x")
        .unwrap();
    executor
        .execute_sql("CREATE FUNCTION add_one AS (x) -> x + 1")
        .unwrap();

    let result = executor.execute_sql("SELECT square(add_one(2))").unwrap();
    assert_table_eq!(result, [[9]]);
}

#[ignore = "Fix me!"]
#[test]
fn test_function_with_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION coalesce_zero AS (x) -> ifNull(x, 0)")
        .unwrap();

    let result = executor.execute_sql("SELECT coalesce_zero(NULL)").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[ignore = "Fix me!"]
#[test]
fn test_or_replace_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION replace_func AS (x) -> x + 1")
        .unwrap();
    executor
        .execute_sql("CREATE OR REPLACE FUNCTION replace_func AS (x) -> x + 10")
        .unwrap();

    let result = executor.execute_sql("SELECT replace_func(5)").unwrap();
    assert_table_eq!(result, [[15]]);
}
