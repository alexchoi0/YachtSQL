use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_case_simple() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["one"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_searched() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 5 > 3 THEN 'yes' ELSE 'no' END")
        .await
        .unwrap();
    assert_table_eq!(result, [["yes"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_multiple_conditions() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES ('A', 95), ('B', 75), ('C', 55)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END AS grade FROM scores ORDER BY name").await
        .unwrap();
    assert_table_eq!(result, [["A", "A"], ["B", "B"], ["C", "C"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_no_else() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN FALSE THEN 'yes' END")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_first_non_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, NULL, 'hello', 'world')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_all_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, NULL, NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_first_not_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE('first', 'second')")
        .await
        .unwrap();
    assert_table_eq!(result, [["first"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(5, 5)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_not_equal() {
    let session = create_session();
    let result = session.execute_sql("SELECT NULLIF(5, 3)").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_strings() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULLIF('hello', 'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if_true() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IF(TRUE, 'yes', 'no')")
        .await
        .unwrap();
    assert_table_eq!(result, [["yes"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if_false() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IF(FALSE, 'yes', 'no')")
        .await
        .unwrap();
    assert_table_eq!(result, [["no"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if_with_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT IF(5 > 3, 'greater', 'lesser')")
        .await
        .unwrap();
    assert_table_eq!(result, [["greater"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (name STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES ('A', 10), ('B', 20), ('C', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM items WHERE CASE WHEN value > 15 THEN TRUE ELSE FALSE END ORDER BY name").await
        .unwrap();
    assert_table_eq!(result, [["B"], ["C"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_case() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CASE WHEN 5 > 3 THEN CASE WHEN 2 > 1 THEN 'both' ELSE 'first' END ELSE 'neither' END").await
        .unwrap();
    assert_table_eq!(result, [["both"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (name STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES ('A', NULL), ('B', 10), ('C', NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM items WHERE COALESCE(value, 0) > 5 ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["B"]]);
}
