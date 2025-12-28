use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_in_integers() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 3 IN (1, 2, 3, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_integers() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 5 IN (1, 2, 3, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_strings() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'apple' IN ('apple', 'banana', 'cherry')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO products VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM products WHERE id IN (1, 3) ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["apple"], ["cherry"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM items WHERE id NOT IN (2, 4) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NULL IN (1, 2, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_list_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 1 IN (1, 2, NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_empty_result() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE id IN (10, 20)")
        .await
        .unwrap();
    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_single_value() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 IN (5)").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_with_expressions() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 6 IN (2 * 2, 2 * 3, 2 * 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
