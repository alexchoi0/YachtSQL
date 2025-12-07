use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_like_prefix() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello world' LIKE 'hello%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_suffix() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello world' LIKE '%world'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_contains() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello world' LIKE '%lo wo%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_exact() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' LIKE 'hello'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_single_char() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'cat' LIKE 'c_t'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_single_char_false() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'cart' LIKE 'c_t'").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_not_like() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello world' NOT LIKE 'goodbye%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, email STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@test.org'), (3, 'Charlie', 'charlie@example.com')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM users WHERE email LIKE '%@example.com' ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Charlie"]]);
}

#[test]
fn test_like_prefix_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Apple iPhone'), (2, 'Samsung Galaxy'), (3, 'Apple Watch'), (4, 'Google Pixel')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM products WHERE name LIKE 'Apple%' ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Apple Watch"], ["Apple iPhone"]]);
}

#[test]
fn test_ilike_case_insensitive() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'Hello World' ILIKE 'hello%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_empty_pattern() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '' LIKE ''").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_all_wildcard() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'anything' LIKE '%'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_multiple_wildcards() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'abcdef' LIKE 'a%c%f'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
