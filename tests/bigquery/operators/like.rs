use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_like_prefix() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE 'hel%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_suffix() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE '%llo'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_contains() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE '%ell%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_exact() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE 'hello'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_no_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE 'world%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_underscore_single_char() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'cat' LIKE 'c_t'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_multiple_underscores() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' LIKE 'h___o'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE names (name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO names VALUES ('alice'), ('bob'), ('alex'), ('anna')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM names WHERE name LIKE 'a%' ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["alex"], ["alice"], ["anna"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_like() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (item STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES ('apple'), ('banana'), ('apricot'), ('orange')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT item FROM items WHERE item NOT LIKE 'a%' ORDER BY item")
        .await
        .unwrap();
    assert_table_eq!(result, [["banana"], ["orange"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL LIKE 'test%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_empty_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT '' LIKE '%'").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_mixed_wildcards() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'testing' LIKE 't_st%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_case_sensitive() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'Hello' LIKE 'hello'")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_only_percent() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'anything' LIKE '%'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_any_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'apple' LIKE ANY ('app%', 'ban%', 'ora%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_any_no_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'grape' LIKE ANY ('app%', 'ban%', 'ora%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_any_multiple_matches() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'apple' LIKE ANY ('a%', '%pple', 'app%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_all_basic() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'apple' LIKE ALL ('a%', '%e', '%ppl%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_all_partial_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'apple' LIKE ALL ('a%', '%z', '%ppl%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_all_no_match() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'grape' LIKE ALL ('a%', 'b%', 'c%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_like_any() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'grape' NOT LIKE ANY ('app%', 'ban%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_like_all() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'grape' NOT LIKE ALL ('app%', 'ban%', 'ora%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_any_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE fruits (name STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO fruits VALUES ('apple'), ('banana'), ('cherry'), ('apricot'), ('orange')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM fruits WHERE name LIKE ANY ('a%', 'b%') ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["apple"], ["apricot"], ["banana"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_all_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE codes (code STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO codes VALUES ('ABC123'), ('ABC456'), ('DEF123'), ('ABCDEF')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT code FROM codes WHERE code LIKE ALL ('A%', '%C%') ORDER BY code")
        .await
        .unwrap();
    assert_table_eq!(result, [["ABC123"], ["ABC456"], ["ABCDEF"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_any_with_underscore() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'cat' LIKE ANY ('c_t', 'd_g', 'b_t')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_all_with_underscore() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'cat' LIKE ALL ('c__', '_at', 'c_t')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_any_empty_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT '' LIKE ANY ('%', 'a%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_any_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL LIKE ANY ('a%', 'b%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_all_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL LIKE ALL ('a%', 'b%')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}
