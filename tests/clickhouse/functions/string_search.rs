use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_position() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT position('hello world', 'world')")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
fn test_position_not_found() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT position('hello world', 'xyz')")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_position_case_sensitive() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT position('Hello World', 'world')")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_position_case_insensitive() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT positionCaseInsensitive('Hello World', 'world')")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_position_utf8() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT positionUTF8('hello мир', 'мир')")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_position_case_insensitive_utf8() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT positionCaseInsensitiveUTF8('Hello МИР', 'мир')")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_locate() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT locate('hello world', 'world')")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_search_any() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiSearchAny('hello world', ['foo', 'world', 'bar'])")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_search_first_index() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiSearchFirstIndex('hello world', ['world', 'hello'])")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_search_first_position() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiSearchFirstPosition('hello world', ['world', 'hello'])")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_search_all_positions() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiSearchAllPositions('hello world hello', ['hello', 'world'])")
        .unwrap();
    assert_table_eq!(result, [[[[1, 13], [7]]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_match() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT match('hello world', 'w.*d')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_match_any() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiMatchAny('hello world', ['h.*o', 'w.*d'])")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_match_any_index() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiMatchAnyIndex('hello world', ['foo', 'w.*d', 'bar'])")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_match_all_indices() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT multiMatchAllIndices('hello world', ['h.*o', 'w.*d', 'xyz'])")
        .unwrap();
    assert_table_eq!(result, [[[1, 2]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_extract() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT extract('hello 123 world', '[0-9]+')")
        .unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_extract_groups() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT extractGroups('key=value', '([a-z]+)=([a-z]+)')")
        .unwrap();
    assert_table_eq!(result, [[["key", "value"]]]);
}

#[test]
fn test_like() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello world' LIKE '%world%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_not_like() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'hello world' NOT LIKE '%xyz%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_ilike() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'Hello World' ILIKE '%world%'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_count_substrings() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT countSubstrings('hello hello hello', 'hello')")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_count_substrings_case_insensitive() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT countSubstringsCaseInsensitive('Hello HELLO hello', 'hello')")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_count_matches() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT countMatches('abc123def456', '[0-9]+')")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_has_token() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT hasToken('hello world test', 'world')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_has_token_case_insensitive() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT hasTokenCaseInsensitive('Hello World Test', 'world')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_starts_with() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT startsWith('hello world', 'hello')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_ends_with() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT endsWith('hello world', 'world')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_ngram_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ngramDistance('hello', 'hallo')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_ngram_search() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ngramSearch('hello world', 'world')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_search_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE text_search (id UInt32, content String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO text_search VALUES
            (1, 'The quick brown fox'),
            (2, 'jumps over the lazy dog'),
            (3, 'Hello World example')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, content
            FROM text_search
            WHERE position(content, 'the') > 0
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[2, "jumps over the lazy dog"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_fuzzy_search() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE products (name String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO products VALUES
            ('iPhone'),
            ('iPad'),
            ('MacBook'),
            ('iPod')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT name, ngramDistance(name, 'iFone') AS distance
            FROM products
            ORDER BY distance
            LIMIT 3",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}
