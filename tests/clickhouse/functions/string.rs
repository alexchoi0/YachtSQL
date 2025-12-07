use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_upper() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT UPPER('hello')").unwrap();

    assert_table_eq!(result, [["HELLO"]]);
}

#[test]
fn test_lower() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT LOWER('HELLO')").unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_length() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT LENGTH('hello')").unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_trim() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT TRIM('  hello  ')").unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_ltrim() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT LTRIM('  hello')").unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_rtrim() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT RTRIM('hello  ')").unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_concat() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT CONCAT('hello', ' ', 'world')")
        .unwrap();

    assert_table_eq!(result, [["hello world"]]);
}

#[test]
fn test_substring() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT SUBSTRING('hello world', 1, 5)")
        .unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_replace() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT REPLACE('hello world', 'world', 'there')")
        .unwrap();

    assert_table_eq!(result, [["hello there"]]);
}

#[test]
fn test_coalesce() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT COALESCE(NULL, 'default')")
        .unwrap();

    assert_table_eq!(result, [["default"]]);
}

#[test]
fn test_coalesce_first_non_null() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT COALESCE('first', 'second')")
        .unwrap();

    assert_table_eq!(result, [["first"]]);
}

#[test]
fn test_nullif() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT NULLIF('a', 'a')").unwrap();

    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_nullif_not_equal() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT NULLIF('a', 'b')").unwrap();

    assert_table_eq!(result, [["a"]]);
}

#[test]
fn test_left() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT LEFT('hello', 3)").unwrap();

    assert_table_eq!(result, [["hel"]]);
}

#[test]
fn test_right() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT RIGHT('hello', 3)").unwrap();

    assert_table_eq!(result, [["llo"]]);
}

#[test]
fn test_reverse() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT REVERSE('hello')").unwrap();

    assert_table_eq!(result, [["olleh"]]);
}

#[test]
fn test_repeat() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT REPEAT('ab', 3)").unwrap();

    assert_table_eq!(result, [["ababab"]]);
}

#[test]
fn test_string_functions_on_table() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE words (word STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('Hello'), ('World')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT UPPER(word), LENGTH(word) FROM words ORDER BY word")
        .unwrap();

    assert_table_eq!(result, [["HELLO", 5], ["WORLD", 5],]);
}
