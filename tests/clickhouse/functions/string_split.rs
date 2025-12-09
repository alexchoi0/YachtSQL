use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_split_by_char() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT splitByChar(',', 'a,b,c,d')")
        .unwrap();
    assert_table_eq!(result, [[["a", "b", "c", "d"]]]);
}

#[test]
fn test_split_by_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT splitByString('::', 'a::b::c')")
        .unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[test]
fn test_split_by_regexp() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT splitByRegexp('[,;]', 'a,b;c,d')")
        .unwrap();
    assert_table_eq!(result, [[["a", "b", "c", "d"]]]);
}

#[test]
fn test_split_by_whitespace() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT splitByWhitespace('hello   world  test')")
        .unwrap();
    assert_table_eq!(result, [[["hello", "world", "test"]]]);
}

#[test]
fn test_split_by_non_alpha() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT splitByNonAlpha('hello123world456test')")
        .unwrap();
    assert_table_eq!(result, [[["hello", "world", "test"]]]);
}

#[test]
fn test_array_string_concat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayStringConcat(['a', 'b', 'c'], '-')")
        .unwrap();
    assert_table_eq!(result, [["a-b-c"]]);
}

#[test]
fn test_array_string_concat_no_sep() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayStringConcat(['a', 'b', 'c'])")
        .unwrap();
    assert_table_eq!(result, [["abc"]]);
}

#[test]
fn test_alpha_tokens() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT alphaTokens('hello123world456')")
        .unwrap();
    assert_table_eq!(result, [[["hello", "world"]]]);
}

#[test]
fn test_extract_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT extractAll('hello123world456', '[0-9]+')")
        .unwrap();
    assert_table_eq!(result, [[["123", "456"]]]);
}

#[test]
fn test_extract_all_groups_horizontal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT extractAllGroupsHorizontal('abc=123 def=456', '([a-z]+)=([0-9]+)')")
        .unwrap();
    assert_table_eq!(result, [[[["abc", "def"], ["123", "456"]]]]);
}

#[test]
fn test_extract_all_groups_vertical() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT extractAllGroupsVertical('abc=123 def=456', '([a-z]+)=([0-9]+)')")
        .unwrap();
    assert_table_eq!(result, [[[["abc", "123"], ["def", "456"]]]]);
}

#[test]
fn test_ngrams() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ngrams('hello', 2)").unwrap();
    assert_table_eq!(result, [[["he", "el", "ll", "lo"]]]);
}

#[test]
fn test_tokens() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tokens('hello, world! how are you?')")
        .unwrap();
    assert_table_eq!(result, [[["hello", "world", "how", "are", "you"]]]);
}

#[test]
fn test_split_by_char_max_substrings() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT splitByChar(',', 'a,b,c,d,e', 3)")
        .unwrap();
    assert_table_eq!(result, [[["a", "b", "c,d,e"]]]);
}

#[test]
fn test_split_by_string_max_substrings() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT splitByString('::', 'a::b::c::d', 2)")
        .unwrap();
    assert_table_eq!(result, [[["a", "b::c::d"]]]);
}

#[test]
fn test_split_empty_string() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT splitByChar(',', '')").unwrap();
    assert_table_eq!(result, [[[""]]]);
}

#[test]
fn test_split_no_delimiter() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT splitByChar(',', 'no delimiter here')")
        .unwrap();
    assert_table_eq!(result, [[["no delimiter here"]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_split_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE csv_data (line String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO csv_data VALUES
            ('a,b,c'),
            ('1,2,3'),
            ('x,y,z')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT splitByChar(',', line) AS parts FROM csv_data")
        .unwrap();
    assert_table_eq!(
        result,
        [[["a", "b", "c"]], [["1", "2", "3"]], [["x", "y", "z"]]]
    );
}

#[ignore = "Implement me!"]
#[test]
fn test_split_and_access() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE paths (path String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO paths VALUES
            ('/home/user/documents'),
            ('/var/log/syslog'),
            ('/etc/config')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT path,
                splitByChar('/', path) AS parts,
                splitByChar('/', path)[-1] AS filename
            FROM paths",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [
                "/home/user/documents",
                ["", "home", "user", "documents"],
                "documents"
            ],
            ["/var/log/syslog", ["", "var", "log", "syslog"], "syslog"],
            ["/etc/config", ["", "etc", "config"], "config"]
        ]
    );
}

#[test]
fn test_concat_and_split_roundtrip() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayStringConcat(splitByChar(',', 'a,b,c'), ',')")
        .unwrap();
    assert_table_eq!(result, [["a,b,c"]]);
}
