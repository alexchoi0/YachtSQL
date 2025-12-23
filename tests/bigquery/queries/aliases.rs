use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_column_alias() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT 1 AS number").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_column_alias_without_as() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT 'hello' greeting").unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_expression_alias() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT 2 + 3 AS sum_result").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_table_alias() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    let result = session
        .execute_sql("SELECT u.id, u.name FROM users AS u ORDER BY u.id")
        .unwrap();
    assert_table_eq!(result, [[1, "alice"], [2, "bob"]]);
}

#[test]
fn test_table_alias_without_as() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = session
        .execute_sql("SELECT t.id, t.value FROM items t ORDER BY t.id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[test]
fn test_alias_in_order_by() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (30), (10), (20)")
        .unwrap();

    let result = session
        .execute_sql("SELECT value AS v FROM data ORDER BY v")
        .unwrap();
    assert_table_eq!(result, [[10], [20], [30]]);
}

#[test]
fn test_aggregate_alias() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE sales (amount INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (100), (200), (300)")
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(amount) AS total FROM sales")
        .unwrap();
    assert_table_eq!(result, [[600]]);
}

#[test]
fn test_multiple_aliases() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 'Widget', 50)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id AS product_id, name AS product_name, price * 2 AS double_price FROM products")
        .unwrap();
    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[test]
fn test_join_with_table_aliases() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, dept_id INT64)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE departments (id INT64, dept_name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO employees VALUES (1, 'alice', 10), (2, 'bob', 20)")
        .unwrap();
    session
        .execute_sql("INSERT INTO departments VALUES (10, 'Engineering'), (20, 'Sales')")
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.name",
        )
        .unwrap();
    assert_table_eq!(result, [["alice", "Engineering"], ["bob", "Sales"]]);
}

#[test]
fn test_subquery_alias() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE numbers (n INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3)")
        .unwrap();

    let result = session
        .execute_sql("SELECT sub.n FROM (SELECT n FROM numbers) AS sub ORDER BY sub.n")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}
