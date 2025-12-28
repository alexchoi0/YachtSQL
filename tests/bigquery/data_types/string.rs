use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_string_basic() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('hello')")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_empty() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('')")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_with_spaces() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('hello world')")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_comparison() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('apple'), ('banana'), ('cherry')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t WHERE val > 'banana' ORDER BY val")
        .await
        .unwrap();

    assert_table_eq!(result, [["cherry"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_equality() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('apple'), ('banana'), ('apple')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t WHERE val = 'apple' ORDER BY val")
        .await
        .unwrap();

    assert_table_eq!(result, [["apple"], ["apple"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_like() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('apple'), ('application'), ('banana')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t WHERE val LIKE 'app%' ORDER BY val")
        .await
        .unwrap();

    assert_table_eq!(result, [["apple"], ["application"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_ordering() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('banana'), ('apple'), ('cherry')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t ORDER BY val ASC")
        .await
        .unwrap();

    assert_table_eq!(result, [["apple"], ["banana"], ["cherry"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_concat_operator() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 'hello' || ' ' || 'world'")
        .await
        .unwrap();

    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_in_list() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE t (val STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES ('a'), ('b'), ('c'), ('d')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM t WHERE val IN ('a', 'c') ORDER BY val")
        .await
        .unwrap();

    assert_table_eq!(result, [["a"], ["c"]]);
}
