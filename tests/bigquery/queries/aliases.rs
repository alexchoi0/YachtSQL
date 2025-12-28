use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_column_alias() {
    let session = create_session();
    let result = session.execute_sql("SELECT 1 AS number").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_column_alias_without_as() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' greeting")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_expression_alias() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 2 + 3 AS sum_result")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT u.id, u.name FROM users AS u ORDER BY u.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "alice"], [2, "bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_alias_without_as() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT t.id, t.value FROM items t ORDER BY t.id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alias_in_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (30), (10), (20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT value AS v FROM data ORDER BY v")
        .await
        .unwrap();
    assert_table_eq!(result, [[10], [20], [30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (100), (200), (300)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(amount) AS total FROM sales")
        .await
        .unwrap();
    assert_table_eq!(result, [[600]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_aliases() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 'Widget', 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id AS product_id, name AS product_name, price * 2 AS double_price FROM products").await
        .unwrap();
    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_join_with_table_aliases() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, dept_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE departments (id INT64, dept_name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employees VALUES (1, 'alice', 10), (2, 'bob', 20)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO departments VALUES (10, 'Engineering'), (20, 'Sales')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.name",
        ).await
        .unwrap();
    assert_table_eq!(result, [["alice", "Engineering"], ["bob", "Sales"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_subquery_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE numbers (n INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT sub.n FROM (SELECT n FROM numbers) AS sub ORDER BY sub.n")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}
