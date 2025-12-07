use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_match() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT match('hello world', 'world')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_match_no_match() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT match('hello world', 'foo')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_match_regex_pattern() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT match('hello123', '\\\\d+')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_match_any() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiMatchAny('hello world', ['foo', 'world', 'bar'])")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_match_any_index() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiMatchAnyIndex('hello world', ['foo', 'world', 'bar'])")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_match_all_indices() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiMatchAllIndices('hello world hello', ['hello', 'world'])")
        .unwrap();
    assert_table_eq!(result, [[[1, 2]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_extract() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT extract('hello123world', '\\\\d+')")
        .unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_extract_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT extractAll('a1b2c3', '\\\\d')")
        .unwrap();
    assert_table_eq!(result, [[["1", "2", "3"]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_extract_groups() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT extractGroups('hello123world456', '(\\\\w+)(\\\\d+)')")
        .unwrap();
    assert_table_eq!(result, [[["hello", "123"]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_replace_regexp_one() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT replaceRegexpOne('hello123world', '\\\\d+', 'XXX')")
        .unwrap();
    assert_table_eq!(result, [["helloXXXworld"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_replace_regexp_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT replaceRegexpAll('a1b2c3', '\\\\d', 'X')")
        .unwrap();
    assert_table_eq!(result, [["aXbXcX"]]);
}

#[test]
fn test_like() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello world' LIKE '%world'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_not_like() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello world' NOT LIKE '%foo%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_ilike() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'Hello World' ILIKE '%world'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_not_ilike() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'Hello World' NOT ILIKE '%foo%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_count_matches() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT countMatches('aaa', 'a')")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_count_matches_case_insensitive() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT countMatchesCaseInsensitive('AaA', 'a')")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_regexp_quote_meta() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT regexpQuoteMeta('a.b*c?d')")
        .unwrap();
    assert_table_eq!(result, [["a\\.b\\*c\\?d"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_regexp_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE regex_test (id INT64, email STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO regex_test VALUES (1, 'alice@example.com'), (2, 'bob@test.org'), (3, 'invalid')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM regex_test WHERE match(email, '@.*\\\\.com$') ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_search_any() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiSearchAny('hello world', ['foo', 'world'])")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_search_first_index() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiSearchFirstIndex('hello world', ['foo', 'hello', 'world'])")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_search_first_position() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiSearchFirstPosition('hello world', ['foo', 'world'])")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_search_all_positions() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiSearchAllPositions('hello world', ['hello', 'world'])")
        .unwrap();
    assert_table_eq!(result, [[[1, 7]]]);
}
