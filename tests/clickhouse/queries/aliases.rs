use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_column_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name AS user_name FROM users ORDER BY user_name")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
fn test_expression_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE products (name STRING, price INT64, quantity INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES ('Widget', 10, 5), ('Gadget', 20, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, price * quantity AS total_value FROM products ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Gadget", 60], ["Widget", 50],]);
}

#[test]
fn test_table_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, dept_id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 10), (2, 'Bob', 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT e.name FROM employees e ORDER BY e.name")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
fn test_alias_in_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES ('Alice', 85), ('Bob', 92), ('Charlie', 78)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, score AS points FROM scores ORDER BY points DESC")
        .unwrap();
    assert_table_eq!(result, [["Bob", 92], ["Alice", 85], ["Charlie", 78],]);
}

#[test]
fn test_alias_without_as() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name user_name FROM users")
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_multiple_aliases() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 2, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a AS x, b AS y, c AS z FROM data")
        .unwrap();
    assert_table_eq!(result, [[1, 2, 3]]);
}

#[test]
fn test_aggregate_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (product STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('A', 100), ('A', 200), ('B', 150)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT product, SUM(amount) AS total_sales FROM sales GROUP BY product ORDER BY total_sales DESC")
        .unwrap();
    assert_table_eq!(result, [["A", 300], ["B", 150]]);
}

#[test]
fn test_subquery_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sub.val FROM (SELECT val FROM nums) AS sub ORDER BY sub.val")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
fn test_alias_with_special_characters() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (42)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value AS \"my-value\" FROM data")
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[test]
fn test_table_alias_in_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64, total INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE customers (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 2, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT c.name, o.total FROM orders o, customers c WHERE o.customer_id = c.id ORDER BY c.name")
        .unwrap();
    assert_table_eq!(result, [["Alice", 100], ["Bob", 200]]);
}
