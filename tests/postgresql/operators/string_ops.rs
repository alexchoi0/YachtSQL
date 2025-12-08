use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_string_concat_operator() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'Hello' || ' ' || 'World'")
        .unwrap();
    assert_table_eq!(result, [["Hello World"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_string_concat_with_number() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'Value: ' || 42").unwrap();
    assert_table_eq!(result, [["Value: 42"]]);
}

#[test]
fn test_string_concat_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'Hello' || NULL").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_like_simple() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' LIKE 'hello'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_percent() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello world' LIKE 'hello%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_underscore() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' LIKE 'h_llo'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_escape() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '50%' LIKE '%\\%%' ESCAPE '\\'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_not_like() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello' NOT LIKE 'world%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_ilike() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'Hello' ILIKE 'hello'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_not_ilike() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'Hello' NOT ILIKE 'world'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_similar_to() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'abc' SIMILAR TO '(abc|def)'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_similar_to_percent() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'abcdef' SIMILAR TO 'abc%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_not_similar_to() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'xyz' NOT SIMILAR TO '(abc|def)'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_regex_match() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' ~ 'ell'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_regex_match_case_insensitive() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'Hello' ~* 'hello'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_regex_not_match() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' !~ 'xyz'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_regex_not_match_case_insensitive() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'Hello' !~* 'xyz'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_string_comparison_equal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'abc' = 'abc'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_string_comparison_not_equal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'abc' <> 'def'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_string_comparison_less_than() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'abc' < 'abd'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_string_comparison_greater_than() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'abd' > 'abc'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_string_comparison_less_equal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'abc' <= 'abc'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_string_comparison_greater_equal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'abc' >= 'abc'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_collate() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'abc' COLLATE \"en_US\" = 'abc'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_string_in_list() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'b' IN ('a', 'b', 'c')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_string_not_in_list() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'x' NOT IN ('a', 'b', 'c')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_string_between() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'b' BETWEEN 'a' AND 'c'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_string_not_between() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'x' NOT BETWEEN 'a' AND 'c'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_starts_with_operator() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' ^@ 'hel'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_pattern_with_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE patterns (id INTEGER, val TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO patterns VALUES (1, 'apple'), (2, 'apricot'), (3, 'banana')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM patterns WHERE val LIKE 'ap%'")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_regex_with_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE emails (id INTEGER, email TEXT)")
        .unwrap();
    executor.execute_sql("INSERT INTO emails VALUES (1, 'test@test.com'), (2, 'invalid'), (3, 'user@domain.org')").unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM emails WHERE email ~ '@'")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}
