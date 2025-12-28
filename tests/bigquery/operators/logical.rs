use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_and_true_true() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE AND TRUE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_and_true_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE AND FALSE").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_and_false_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT FALSE AND FALSE").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_or_true_true() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE OR TRUE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_or_true_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE OR FALSE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_or_false_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT FALSE OR FALSE").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_true() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT TRUE").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_false() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT FALSE").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_and_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE AND NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_and_false_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT FALSE AND NULL").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_or_with_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT FALSE OR NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_or_true_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT TRUE OR NULL").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT NOT NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_logical_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT (TRUE AND FALSE) OR TRUE")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_precedence() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRUE OR FALSE AND FALSE")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_with_comparison() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 > 3 AND 2 < 4").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_with_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE flags (a BOOL, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO flags VALUES (TRUE, TRUE), (TRUE, FALSE), (FALSE, TRUE), (FALSE, FALSE)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM flags WHERE a AND b")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_or_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE flags (a BOOL, b BOOL)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO flags VALUES (TRUE, TRUE), (TRUE, FALSE), (FALSE, TRUE), (FALSE, FALSE)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a, b FROM flags WHERE a OR b ORDER BY a DESC, b DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[true, true], [true, false], [false, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE flags (a BOOL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO flags VALUES (TRUE), (FALSE)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a FROM flags WHERE NOT a")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM nullable WHERE val IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM nullable WHERE val IS NOT NULL ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}
