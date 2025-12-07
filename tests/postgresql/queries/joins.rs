use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, dept_id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1), (4, 'Diana', NULL)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')",
        )
        .unwrap();
}

#[test]
#[ignore = "Implement me!"]
fn test_inner_join() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT u.name, d.name FROM users u INNER JOIN departments d ON u.dept_id = d.id ORDER BY u.id",
        )
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
#[ignore = "Implement me!"]
fn test_left_join() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT u.name, d.name FROM users u LEFT JOIN departments d ON u.dept_id = d.id ORDER BY u.id",
        )
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

#[test]
#[ignore = "Implement me!"]
fn test_right_join() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT u.name, d.name FROM users u RIGHT JOIN departments d ON u.dept_id = d.id ORDER BY d.id, u.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Charlie", "Engineering"],
            ["Bob", "Sales"],
            [null, "Marketing"],
        ]
    );
}

#[test]
fn test_full_outer_join() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT u.name, d.name FROM users u FULL OUTER JOIN departments d ON u.dept_id = d.id ORDER BY u.name NULLS LAST, d.name NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Bob", "Sales"],
            ["Charlie", "Engineering"],
            ["Diana", null],
            [null, "Marketing"],
        ]
    );
}

#[test]
fn test_cross_join() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE a (x INT64)").unwrap();
    executor
        .execute_sql("INSERT INTO a VALUES (1), (2)")
        .unwrap();

    executor.execute_sql("CREATE TABLE b (y INT64)").unwrap();
    executor
        .execute_sql("INSERT INTO b VALUES (10), (20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT x, y FROM a CROSS JOIN b ORDER BY x, y")
        .unwrap();

    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20],]);
}

#[test]
#[ignore = "Implement me!"]
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
        .execute_sql(
            "SELECT e.name AS employee, m.name AS manager FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice", null], ["Bob", "Alice"], ["Charlie", "Alice"],]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_join_with_multiple_conditions() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, min_amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 150), (2, 'Bob', 100)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id AND o.amount >= u.min_amount ORDER BY o.id",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice", 200], ["Bob", 150],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_join_three_tables() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64, product_id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 1), (2, 1, 2), (3, 2, 1)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT u.name AS user_name, p.name AS product_name FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id ORDER BY o.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice", "Widget"], ["Alice", "Gadget"], ["Bob", "Widget"],]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_natural_join() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE t1 (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE t2 (id INT64, dept STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t2 VALUES (1, 'Engineering'), (2, 'Sales')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, dept FROM t1 NATURAL JOIN t2 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [["Alice", "Engineering"], ["Bob", "Sales"],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_join_using() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE t1 (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE t2 (id INT64, dept STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t2 VALUES (1, 'Engineering'), (2, 'Sales')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, dept FROM t1 JOIN t2 USING (id) ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [["Alice", "Engineering"], ["Bob", "Sales"],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_join_with_where_clause() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT u.name, d.name FROM users u JOIN departments d ON u.dept_id = d.id WHERE d.name = 'Engineering' ORDER BY u.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice", "Engineering"], ["Charlie", "Engineering"],]
    );
}

#[test]
fn test_left_join_with_is_null() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT u.name FROM users u LEFT JOIN departments d ON u.dept_id = d.id WHERE d.id IS NULL",
        )
        .unwrap();

    assert_table_eq!(result, [["Diana"]]);
}
