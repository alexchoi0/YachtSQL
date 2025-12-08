use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_regexp_replace_basic() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT REGEXP_REPLACE('hello world', 'world', 'there')")
        .unwrap();

    assert_table_eq!(result, [["hello there"]]);
}

#[test]
fn test_regexp_replace_with_pattern() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT REGEXP_REPLACE('abc123def456', '[0-9]+', 'X')")
        .unwrap();

    assert_table_eq!(result, [["abcXdefX"]]);
}

#[test]
fn test_tilde_operator() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT 'hello' ~ 'ell'").unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tilde_operator_no_match() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT 'hello' ~ 'xyz'").unwrap();

    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_tilde_star_case_insensitive() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT 'Hello' ~* 'hello'").unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_not_tilde_operator() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT 'hello' !~ 'xyz'").unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_not_tilde_star_case_insensitive() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT 'Hello' !~* 'xyz'").unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_similar_to() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 'abc' SIMILAR TO 'a%'")
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_similar_to_alternation() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 'abc' SIMILAR TO '(abc|def)'")
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_similar_to_character_class() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 'a1b' SIMILAR TO 'a[0-9]b'")
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_not_similar_to() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 'xyz' NOT SIMILAR TO 'a%'")
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_regexp_with_table() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE emails (email TEXT)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO emails VALUES ('test@example.com'), ('invalid'), ('user@domain.org')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT email FROM emails WHERE email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$' ORDER BY email")
        .unwrap();

    assert_table_eq!(result, [["test@example.com"], ["user@domain.org"]]);
}
