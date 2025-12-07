use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, dept_id INT64, salary INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 1, 50000), (2, 'Bob', 1, 60000), (3, 'Charlie', 2, 55000), (4, 'Diana', 2, 70000)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
        .unwrap();
}

#[test]
fn test_subquery_in_where_in() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering') ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"],]);
}

#[test]
fn test_subquery_in_where_not_in() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM employees WHERE dept_id NOT IN (SELECT id FROM departments WHERE name = 'Sales') ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"],]);
}

#[test]
fn test_subquery_in_from_clause() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT sub.name, sub.salary FROM (SELECT name, salary FROM employees WHERE salary > 55000) AS sub ORDER BY sub.name")
        .unwrap();

    assert_table_eq!(result, [["Bob", 60000], ["Diana", 70000],]);
}

#[test]
fn test_subquery_with_aggregation() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Bob"], ["Diana"],]);
}

#[test]
fn test_exists_subquery() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.salary > 55000) ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Engineering"], ["Sales"],]);
}

#[test]
fn test_not_exists_subquery() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, product_id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Gizmo')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1), (2, 1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM products p WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id) ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Gadget"], ["Gizmo"],]);
}

#[test]
fn test_nested_subquery() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE id IN (SELECT dept_id FROM employees WHERE salary > 65000)) ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Charlie"], ["Diana"],]);
}
