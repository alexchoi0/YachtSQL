use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_like_prefix() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' LIKE 'hel%'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_suffix() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' LIKE '%llo'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_contains() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' LIKE '%ell%'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_exact() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' LIKE 'hello'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_no_match() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello' LIKE 'world%'")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_like_underscore_single_char() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'cat' LIKE 'c_t'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_multiple_underscores() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' LIKE 'h___o'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_in_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE names (name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO names VALUES ('alice'), ('bob'), ('alex'), ('anna')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM names WHERE name LIKE 'a%' ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["alex"], ["alice"], ["anna"]]);
}

#[test]
fn test_not_like() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (item STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('apple'), ('banana'), ('apricot'), ('orange')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT item FROM items WHERE item NOT LIKE 'a%' ORDER BY item")
        .unwrap();
    assert_table_eq!(result, [["banana"], ["orange"]]);
}

#[test]
fn test_like_with_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NULL LIKE 'test%'").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_like_empty_string() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '' LIKE '%'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_mixed_wildcards() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'testing' LIKE 't_st%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_case_sensitive() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'Hello' LIKE 'hello'").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_like_only_percent() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'anything' LIKE '%'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_any_basic() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'apple' LIKE ANY ('app%', 'ban%', 'ora%')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_any_no_match() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'grape' LIKE ANY ('app%', 'ban%', 'ora%')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_any_multiple_matches() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'apple' LIKE ANY ('a%', '%pple', 'app%')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_all_basic() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'apple' LIKE ALL ('a%', '%e', '%ppl%')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_all_partial_match() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'apple' LIKE ALL ('a%', '%z', '%ppl%')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_all_no_match() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'grape' LIKE ALL ('a%', 'b%', 'c%')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_not_like_any() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'grape' NOT LIKE ANY ('app%', 'ban%')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_not_like_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'grape' NOT LIKE ALL ('app%', 'ban%', 'ora%')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_any_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fruits (name STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO fruits VALUES ('apple'), ('banana'), ('cherry'), ('apricot'), ('orange')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM fruits WHERE name LIKE ANY ('a%', 'b%') ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["apple"], ["apricot"], ["banana"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_all_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE codes (code STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO codes VALUES ('ABC123'), ('ABC456'), ('DEF123'), ('ABCDEF')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT code FROM codes WHERE code LIKE ALL ('A%', '%C%') ORDER BY code")
        .unwrap();
    assert_table_eq!(result, [["ABC123"], ["ABC456"], ["ABCDEF"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_any_with_underscore() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'cat' LIKE ANY ('c_t', 'd_g', 'b_t')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_all_with_underscore() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'cat' LIKE ALL ('c__', '_at', 'c_t')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_any_empty_pattern() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '' LIKE ANY ('%', 'a%')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_like_any_with_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NULL LIKE ANY ('a%', 'b%')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_like_all_with_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NULL LIKE ALL ('a%', 'b%')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}
