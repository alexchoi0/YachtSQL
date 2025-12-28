use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_integers() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 1 IS DISTINCT FROM 2")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_same_integers() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 1 IS DISTINCT FROM 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_null() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 1 IS DISTINCT FROM NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_both_null() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT NULL IS DISTINCT FROM NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_distinct_from_integers() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 1 IS NOT DISTINCT FROM 1")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_distinct_from_different() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 1 IS NOT DISTINCT FROM 2")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_distinct_from_null() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 1 IS NOT DISTINCT FROM NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_distinct_from_both_null() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT NULL IS NOT DISTINCT FROM NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_strings() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 'hello' IS DISTINCT FROM 'world'")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_same_strings() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 'hello' IS DISTINCT FROM 'hello'")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE test_distinct (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO test_distinct VALUES (1, 1), (1, 2), (NULL, 1), (NULL, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a IS DISTINCT FROM b FROM test_distinct ORDER BY a NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [[false], [true], [true], [false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_distinct_from_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE test_not_distinct (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO test_not_distinct VALUES (1, 1), (1, 2), (NULL, 1), (NULL, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a IS NOT DISTINCT FROM b FROM test_not_distinct ORDER BY a NULLS LAST")
        .await
        .unwrap();
    assert_table_eq!(result, [[true], [false], [false], [true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 'A'), (2, NULL), (3, 'B'), (4, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM products WHERE category IS DISTINCT FROM NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_distinct_from_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, NULL), (2, 10), (3, NULL), (4, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM items WHERE value IS NOT DISTINCT FROM NULL ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_in_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE left_t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE right_t (id INT64, val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO left_t VALUES (1, NULL), (2, 10)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO right_t VALUES (1, NULL), (2, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT l.id FROM left_t l JOIN right_t r ON l.id = r.id WHERE l.val IS NOT DISTINCT FROM r.val ORDER BY l.id").await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_case_expression() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT CASE WHEN 1 IS DISTINCT FROM 2 THEN 'different' ELSE 'same' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["different"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_booleans() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT TRUE IS DISTINCT FROM FALSE")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_same_booleans() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT TRUE IS DISTINCT FROM TRUE")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_floats() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 1.5 IS DISTINCT FROM 2.5")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_distinct_from_same_floats() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 1.5 IS DISTINCT FROM 1.5")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}
