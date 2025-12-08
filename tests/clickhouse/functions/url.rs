use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_protocol() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT protocol('https://example.com/path')")
        .unwrap();
    assert_table_eq!(result, [["https"]]);
}

#[test]
fn test_domain() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT domain('https://example.com/path')")
        .unwrap();
    assert_table_eq!(result, [["example.com"]]);
}

#[test]
fn test_domain_without_www() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT domainWithoutWWW('https://www.example.com/path')")
        .unwrap();
    assert_table_eq!(result, [["example.com"]]);
}

#[test]
fn test_top_level_domain() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT topLevelDomain('https://example.com/path')")
        .unwrap();
    assert_table_eq!(result, [["com"]]);
}

#[test]
fn test_first_significant_subdomain() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT firstSignificantSubdomain('https://news.example.com/path')")
        .unwrap();
    assert_table_eq!(result, [["example"]]);
}

#[test]
fn test_port() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT port('https://example.com:8080/path')")
        .unwrap();
    assert_table_eq!(result, [[8080]]);
}

#[test]
fn test_path() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT path('https://example.com/path/to/resource')")
        .unwrap();
    assert_table_eq!(result, [["/path/to/resource"]]);
}

#[test]
fn test_path_full() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT pathFull('https://example.com/path?query=1')")
        .unwrap();
    assert_table_eq!(result, [["/path?query=1"]]);
}

#[test]
fn test_query_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT queryString('https://example.com/path?foo=bar&baz=qux')")
        .unwrap();
    assert_table_eq!(result, [["foo=bar&baz=qux"]]);
}

#[test]
fn test_fragment() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT fragment('https://example.com/path#section')")
        .unwrap();
    assert_table_eq!(result, [["section"]]);
}

#[test]
fn test_query_string_and_fragment() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT queryStringAndFragment('https://example.com/path?foo=bar#section')")
        .unwrap();
    assert_table_eq!(result, [["foo=bar#section"]]);
}

#[test]
fn test_extract_url_parameter() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT extractURLParameter('https://example.com?foo=bar&baz=qux', 'foo')")
        .unwrap();
    assert_table_eq!(result, [["bar"]]);
}

#[test]
fn test_extract_url_parameters() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT extractURLParameters('https://example.com?foo=bar&baz=qux')")
        .unwrap();
    assert_table_eq!(result, [[["foo=bar", "baz=qux"]]]);
}

#[test]
fn test_extract_url_parameter_names() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT extractURLParameterNames('https://example.com?foo=bar&baz=qux')")
        .unwrap();
    assert_table_eq!(result, [[["foo", "baz"]]]);
}

#[test]
fn test_url_hierarchy() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT URLHierarchy('https://example.com/a/b/c')")
        .unwrap();
    assert_table_eq!(
        result,
        [[[
            "https://example.com/",
            "https://example.com/a/",
            "https://example.com/a/b/",
            "https://example.com/a/b/c"
        ]]]
    );
}

#[test]
fn test_url_path_hierarchy() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT URLPathHierarchy('https://example.com/a/b/c')")
        .unwrap();
    assert_table_eq!(result, [[["/a/", "/a/b/", "/a/b/c"]]]);
}

#[test]
fn test_decode_url_component() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT decodeURLComponent('hello%20world')")
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[test]
fn test_encode_url_component() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT encodeURLComponent('hello world')")
        .unwrap();
    assert_table_eq!(result, [["hello%20world"]]);
}

#[test]
fn test_encode_url_form_component() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT encodeURLFormComponent('hello world')")
        .unwrap();
    assert_table_eq!(result, [["hello+world"]]);
}

#[test]
fn test_decode_url_form_component() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT decodeURLFormComponent('hello+world')")
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[test]
fn test_netloc() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT netloc('https://user:pass@example.com:8080/path')")
        .unwrap();
    assert_table_eq!(result, [["user:pass@example.com:8080"]]);
}

#[test]
fn test_cut_to_first_significant_subdomain() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT cutToFirstSignificantSubdomain('https://news.example.com/path')")
        .unwrap();
    assert_table_eq!(result, [["example.com"]]);
}

#[test]
fn test_cut_www() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT cutWWW('https://www.example.com/path')")
        .unwrap();
    assert_table_eq!(result, [["https://example.com/path"]]);
}

#[test]
fn test_cut_query_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT cutQueryString('https://example.com/path?foo=bar')")
        .unwrap();
    assert_table_eq!(result, [["https://example.com/path"]]);
}

#[test]
fn test_cut_fragment() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT cutFragment('https://example.com/path#section')")
        .unwrap();
    assert_table_eq!(result, [["https://example.com/path"]]);
}

#[test]
fn test_cut_query_string_and_fragment() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT cutQueryStringAndFragment('https://example.com/path?foo=bar#section')")
        .unwrap();
    assert_table_eq!(result, [["https://example.com/path"]]);
}

#[test]
fn test_cut_url_parameter() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT cutURLParameter('https://example.com?foo=bar&baz=qux', 'foo')")
        .unwrap();
    assert_table_eq!(result, [["https://example.com/?baz=qux"]]);
}
