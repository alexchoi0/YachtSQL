use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, dept_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1), (4, 'Diana', NULL)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')",
        )
        .unwrap();
}

#[test]
fn test_inner_join() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT u.name, d.name FROM users u INNER JOIN departments d ON u.dept_id = d.id ORDER BY u.name")
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Bob", "Sales"],
            ["Charlie", "Engineering"],
        ]
    );
}

#[test]
fn test_left_join() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT u.name, d.name FROM users u LEFT JOIN departments d ON u.dept_id = d.id ORDER BY u.name")
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Bob", "Sales"],
            ["Charlie", "Engineering"],
            ["Diana", null],
        ]
    );
}

#[ignore = "Implement me!"]
#[test]
fn test_full_outer_join() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT u.name, d.name FROM users u FULL OUTER JOIN departments d ON u.dept_id = d.id ORDER BY u.name, d.name")
        .unwrap();

    assert_table_eq!(
        result,
        [
            [null, "Marketing"],
            ["Alice", "Engineering"],
            ["Bob", "Sales"],
            ["Charlie", "Engineering"],
            ["Diana", null],
        ]
    );
}

#[test]
fn test_cross_join() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE a (x INT64)").unwrap();
    executor.execute_sql("CREATE TABLE b (y INT64)").unwrap();
    executor
        .execute_sql("INSERT INTO a VALUES (1), (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO b VALUES (10), (20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT x, y FROM a CROSS JOIN b ORDER BY x, y")
        .unwrap();

    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20],]);
}

#[test]
fn test_self_join() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, manager_id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO employees VALUES (1, 'Alice', NULL), (2, 'Bob', 1), (3, 'Charlie', 1)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT e.name, m.name FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.name")
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice", null], ["Bob", "Alice"], ["Charlie", "Alice"],]
    );
}

#[test]
fn test_join_with_where_clause() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT u.name, d.name FROM users u INNER JOIN departments d ON u.dept_id = d.id WHERE d.name = 'Engineering' ORDER BY u.name")
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice", "Engineering"], ["Charlie", "Engineering"],]
    );
}

#[test]
fn test_join_multiple_conditions() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64, product_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 101), (3, 2, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT o.id, p.name FROM orders o INNER JOIN products p ON o.product_id = p.id WHERE o.user_id = 1 ORDER BY o.id")
        .unwrap();

    assert_table_eq!(result, [[1, "Widget"], [2, "Gadget"],]);
}

#[test]
fn test_join_three_tables() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE customers (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64, product_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 2, 101)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT c.name, p.name FROM customers c INNER JOIN orders o ON c.id = o.customer_id INNER JOIN products p ON o.product_id = p.id ORDER BY c.name")
        .unwrap();

    assert_table_eq!(result, [["Alice", "Widget"], ["Bob", "Gadget"],]);
}
