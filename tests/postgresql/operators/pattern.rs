use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_like_basic() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' LIKE 'hello'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_percent_wildcard() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello world' LIKE 'hello%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_percent_middle() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello world' LIKE '%wor%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_underscore_wildcard() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'cat' LIKE 'c_t'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_no_match() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' LIKE 'world'").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_not_like() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello' NOT LIKE 'world'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_ilike_case_insensitive() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'Hello' ILIKE 'hello'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_ilike_with_wildcard() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'HELLO WORLD' ILIKE '%world'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('apple'), ('banana'), ('apricot'), ('cherry')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM items WHERE name LIKE 'ap%' ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["apple"], ["apricot"]]);
}

#[test]
fn test_like_ends_with() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE files (name TEXT)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO files VALUES ('doc.txt'), ('image.png'), ('data.txt'), ('photo.jpg')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM files WHERE name LIKE '%.txt' ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["data.txt"], ["doc.txt"]]);
}

#[test]
fn test_like_single_char() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE codes (code TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO codes VALUES ('A1'), ('B2'), ('AB'), ('A12')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT code FROM codes WHERE code LIKE '_1' ORDER BY code")
        .unwrap();
    assert_table_eq!(result, [["A1"]]);
}

#[test]
fn test_like_combined_wildcards() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'test123' LIKE 't_st%'")
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
fn test_like_percent_only() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'anything' LIKE '%'").unwrap();
    assert_table_eq!(result, [[true]]);
}
