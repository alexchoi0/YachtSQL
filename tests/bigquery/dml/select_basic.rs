#![allow(clippy::approx_constant)]

use yachtsql::YachtSQLSession;

use super::super::common::create_session;
use crate::assert_table_eq;

async fn setup_users_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
        )
        .await
        .unwrap();
}

async fn setup_products_table(session: &YachtSQLSession) {
    session
        .execute_sql(
            "CREATE TABLE products (id INT64, name STRING, price FLOAT64, in_stock BOOLEAN)",
        )
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 'Laptop', 999.99, true), (2, 'Mouse', 29.99, true), (3, 'Keyboard', 79.99, false)").await
        .unwrap();
}

async fn setup_nullable_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE nullable_data (id INT64, value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable_data VALUES (1, 'a'), (2, NULL), (3, 'c')")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_all_columns() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT * FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Alice", 30], [2, "Bob", 25], [3, "Charlie", 35],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_specific_columns() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT name, age FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice", 30], ["Bob", 25], ["Charlie", 35],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_single_column() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT name FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_column_alias() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT name AS user_name, age AS user_age FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice", 30], ["Bob", 25], ["Charlie", 35],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_equality_filter() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT id, name FROM users WHERE age = 30")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_greater_than_filter() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT id, name FROM users WHERE age > 25 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"], [3, "Charlie"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_less_than_filter() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT id, name FROM users WHERE age < 30 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_not_equal_filter() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM users WHERE age != 30 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_string_filter() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT id, age FROM users WHERE name = 'Bob'")
        .await
        .unwrap();

    assert_table_eq!(result, [[2, 25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_and_condition() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT id, name FROM users WHERE age > 25 AND age < 35")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_or_condition() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM users WHERE age = 25 OR age = 35 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_boolean_column() {
    let session = create_session();
    setup_products_table(&session).await;

    let result = session
        .execute_sql("SELECT name FROM products WHERE in_stock = true ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [["Laptop"], ["Mouse"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_float_comparison() {
    let session = create_session();
    setup_products_table(&session).await;

    let result = session
        .execute_sql("SELECT name FROM products WHERE price > 50.0 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [["Laptop"], ["Keyboard"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_null_check() {
    let session = create_session();
    setup_nullable_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM nullable_data WHERE value IS NULL")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_not_null_check() {
    let session = create_session();
    setup_nullable_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM nullable_data WHERE value IS NOT NULL ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_from_empty_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE empty_table (id INT64, name STRING)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM empty_table")
        .await
        .unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_no_matching_rows() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT * FROM users WHERE age > 100")
        .await
        .unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_literal_values() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 1, 'hello', CAST(3.14 AS FLOAT64), true")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "hello", 3.14, true,]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_null_literal() {
    let session = create_session();

    let result = session.execute_sql("SELECT NULL").await.unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_expression() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT id, age + 10 AS age_plus_ten FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 40], [2, 35], [3, 45],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_table_alias() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT u.id, u.name FROM users u ORDER BY u.id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"], [3, "Charlie"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_with_qualified_column() {
    let session = create_session();
    setup_users_table(&session).await;

    let result = session
        .execute_sql("SELECT users.id, users.name FROM users ORDER BY users.id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"], [3, "Charlie"],]);
}
