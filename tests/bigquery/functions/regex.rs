use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_regexp_contains() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_CONTAINS('hello world', 'hello')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_regexp_contains_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_CONTAINS('hello world', 'goodbye')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_regexp_contains_pattern() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_CONTAINS('abc123', '[0-9]+')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_regexp_extract() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_EXTRACT('email@example.com', r'@(.+)')")
        .unwrap();
    assert_table_eq!(result, [["example.com"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_regexp_extract_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(REGEXP_EXTRACT_ALL('a1b2c3', '[0-9]'))")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_regexp_replace() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_REPLACE('hello world', 'world', 'there')")
        .unwrap();
    assert_table_eq!(result, [["hello there"]]);
}

#[test]
fn test_regexp_replace_pattern() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_REPLACE('abc123def', '[0-9]+', 'X')")
        .unwrap();
    assert_table_eq!(result, [["abcXdef"]]);
}

#[test]
fn test_regexp_instr() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_INSTR('hello world', 'world')")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
fn test_regexp_instr_not_found() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_INSTR('hello world', 'foo')")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_regexp_substr() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_SUBSTR('hello123world', '[0-9]+')")
        .unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[test]
fn test_regexp_contains_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE emails (id INT64, email STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO emails VALUES (1, 'user@gmail.com'), (2, 'user@yahoo.com'), (3, 'user@example.org')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM emails WHERE REGEXP_CONTAINS(email, 'gmail|yahoo') ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_regexp_with_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_CONTAINS(NULL, 'pattern')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_regexp_case_insensitive() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_CONTAINS('HELLO', '(?i)hello')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_regexp_extract_with_group() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_EXTRACT('John Smith', r'(\\w+) (\\w+)', 2)")
        .unwrap();
    assert_table_eq!(result, [["Smith"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_regexp_replace_with_backreference() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_REPLACE('John Smith', r'(\\w+) (\\w+)', r'\\2, \\1')")
        .unwrap();
    assert_table_eq!(result, [["Smith, John"]]);
}
