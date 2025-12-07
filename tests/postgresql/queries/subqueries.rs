use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, dept_id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
        .unwrap();
}

fn setup_employees_departments(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE departments (dept_id INT64, dept_name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')")
        .unwrap();

    executor
        .execute_sql(
            "CREATE TABLE employees (emp_id INT64, emp_name STRING, dept_id INT64, salary INT64)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 1, 70000), (2, 'Bob', 1, 80000), (3, 'Charlie', 2, 60000), (4, 'Diana', 2, 65000), (5, 'Eve', NULL, 50000)")
        .unwrap();
}

fn setup_products_categories(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE categories (cat_id INT64, cat_name STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Clothing'), (3, 'Books')",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE TABLE products (prod_id INT64, prod_name STRING, cat_id INT64, price FLOAT64)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Laptop', 1, 999.99), (2, 'Phone', 1, 599.99), (3, 'Shirt', 2, 29.99), (4, 'Novel', 3, 14.99)")
        .unwrap();
}

fn setup_nullable_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)")
        .unwrap();
}

#[test]
#[ignore = "Implement me!"]
fn test_scalar_subquery_in_select() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) AS order_count FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [["Alice", 2], ["Bob", 1], ["Charlie", 0],]);
}

#[test]
fn test_subquery_in_where_with_in() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
fn test_subquery_in_where_with_not_in() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders)")
        .unwrap();

    assert_table_eq!(result, [["Charlie"]]);
}

#[test]
fn test_subquery_with_exists() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) ORDER BY u.id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
fn test_subquery_with_not_exists() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)")
        .unwrap();

    assert_table_eq!(result, [["Charlie"]]);
}

#[test]
fn test_subquery_with_comparison() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT name FROM users WHERE id > (SELECT MIN(user_id) FROM orders) ORDER BY id",
        )
        .unwrap();

    assert_table_eq!(result, [["Bob"], ["Charlie"]]);
}

#[test]
fn test_subquery_with_all() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM users WHERE id <= ALL (SELECT user_id FROM orders)")
        .unwrap();

    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_subquery_with_any() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT name FROM users WHERE id = ANY (SELECT user_id FROM orders) ORDER BY id",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
fn test_derived_table() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT sub.name FROM (SELECT id, name FROM users WHERE dept_id = 1) sub ORDER BY sub.id",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Charlie"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_correlated_subquery() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT name, (SELECT SUM(amount) FROM orders WHERE user_id = users.id) AS total FROM users ORDER BY id",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice", 300], ["Bob", 150], ["Charlie", null],]);
}

#[test]
fn test_nested_subquery() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders))",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_subquery_in_from_with_join() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT u.name, o.total FROM users u JOIN (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id) o ON u.id = o.user_id ORDER BY u.id",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice", 300], ["Bob", 150],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_with_limit() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM users WHERE id = (SELECT user_id FROM orders ORDER BY amount DESC LIMIT 1)")
        .unwrap();

    assert_table_eq!(result, [["Alice"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_lateral_subquery() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT u.name, o.max_amount FROM users u, LATERAL (SELECT MAX(amount) AS max_amount FROM orders WHERE user_id = u.id) o WHERE o.max_amount IS NOT NULL ORDER BY u.id",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice", 200], ["Bob", 150],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_uncorrelated_scalar_subquery_avg() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Diana"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_uncorrelated_scalar_subquery_count() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT (SELECT COUNT(*) FROM employees) AS total_employees")
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_uncorrelated_scalar_subquery_max() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT emp_name FROM employees WHERE salary = (SELECT MAX(salary) FROM employees)",
        )
        .unwrap();

    assert_table_eq!(result, [["Bob"]]);
}

#[test]
fn test_uncorrelated_scalar_subquery_min() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT emp_name FROM employees WHERE salary = (SELECT MIN(salary) FROM employees)",
        )
        .unwrap();

    assert_table_eq!(result, [["Eve"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_uncorrelated_scalar_subquery_sum() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT (SELECT SUM(salary) FROM employees) AS total_salary")
        .unwrap();

    assert_table_eq!(result, [[325000]]);
}

#[test]
fn test_correlated_scalar_subquery_by_department() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT e.emp_name, (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept_id = e.dept_id) AS dept_avg
             FROM employees e
             WHERE e.dept_id IS NOT NULL
             ORDER BY e.emp_id"
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 75000.0],
            ["Bob", 75000.0],
            ["Charlie", 62500.0],
            ["Diana", 62500.0]
        ]
    );
}

#[test]
fn test_correlated_scalar_subquery_count_by_category() {
    let mut executor = create_executor();
    setup_products_categories(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT c.cat_name, (SELECT COUNT(*) FROM products p WHERE p.cat_id = c.cat_id) AS prod_count
             FROM categories c
             ORDER BY c.cat_id"
        )
        .unwrap();

    assert_table_eq!(result, [["Electronics", 2], ["Clothing", 1], ["Books", 1],]);
}

#[test]
fn test_correlated_scalar_subquery_max_by_category() {
    let mut executor = create_executor();
    setup_products_categories(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT c.cat_name, (SELECT MAX(p.price) FROM products p WHERE p.cat_id = c.cat_id) AS max_price
             FROM categories c
             ORDER BY c.cat_id"
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", 999.99],
            ["Clothing", 29.99],
            ["Books", 14.99],
        ]
    );
}

#[test]
fn test_in_subquery_with_multiple_values() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE dept_id IN (SELECT dept_id FROM departments WHERE dept_name IN ('Engineering', 'Sales')) ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"], ["Diana"],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_not_in_subquery_with_null() {
    let mut executor = create_executor();
    setup_nullable_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM items WHERE value NOT IN (SELECT value FROM items WHERE value IS NOT NULL) ORDER BY id")
        .unwrap();

    assert_table_eq!(result, []);
}

#[test]
#[ignore = "Implement me!"]
fn test_not_in_with_empty_subquery() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE dept_id NOT IN (SELECT dept_id FROM departments WHERE dept_name = 'NonExistent') ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"], ["Diana"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_in_with_empty_subquery() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE dept_id IN (SELECT dept_id FROM departments WHERE dept_name = 'NonExistent')")
        .unwrap();

    assert_table_eq!(result, []);
}

#[test]
#[ignore = "Implement me!"]
fn test_exists_with_true_condition() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT emp_name FROM employees WHERE EXISTS (SELECT 1 WHERE TRUE) ORDER BY emp_id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice"], ["Bob"], ["Charlie"], ["Diana"], ["Eve"]]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_exists_with_false_condition() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE EXISTS (SELECT 1 WHERE FALSE)")
        .unwrap();

    assert_table_eq!(result, []);
}

#[test]
fn test_not_exists_with_empty_result() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT dept_name FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.dept_id)")
        .unwrap();

    assert_table_eq!(result, [["HR"]]);
}

#[test]
fn test_exists_correlated() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT dept_name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.dept_id) ORDER BY d.dept_id")
        .unwrap();

    assert_table_eq!(result, [["Engineering"], ["Sales"]]);
}

#[test]
fn test_all_greater_than() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary > ALL (SELECT salary FROM employees WHERE dept_id = 2)")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
fn test_all_with_empty_subquery() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary > ALL (SELECT salary FROM employees WHERE dept_id = 999) ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice"], ["Bob"], ["Charlie"], ["Diana"], ["Eve"]]
    );
}

#[test]
fn test_any_equals() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary = ANY (SELECT salary FROM employees WHERE dept_id = 1) ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_any_with_empty_subquery() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary = ANY (SELECT salary FROM employees WHERE dept_id = 999)")
        .unwrap();

    assert_table_eq!(result, []);
}

#[test]
fn test_any_less_than() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary < ANY (SELECT salary FROM employees WHERE dept_id = 1) ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Charlie"], ["Diana"], ["Eve"],]);
}

#[test]
fn test_some_operator() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary = SOME (SELECT salary FROM employees WHERE dept_id = 2) ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Charlie"], ["Diana"]]);
}

#[test]
fn test_subquery_with_group_by() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT dept_id, (SELECT COUNT(*) FROM employees e2 WHERE e2.dept_id = e.dept_id) AS cnt
             FROM employees e
             WHERE e.dept_id IS NOT NULL
             GROUP BY dept_id
             ORDER BY dept_id"
        )
        .unwrap();

    assert_table_eq!(result, [[1, 2], [2, 2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_with_having() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT dept_id, COUNT(*) AS cnt
             FROM employees
             WHERE dept_id IS NOT NULL
             GROUP BY dept_id
             HAVING COUNT(*) >= (SELECT COUNT(*) FROM employees WHERE dept_id = 1)",
        )
        .unwrap();

    assert_table_eq!(result, [[1, 2], [2, 2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_multiple_subqueries_in_select() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                (SELECT COUNT(*) FROM employees) AS total_emp,
                (SELECT AVG(salary) FROM employees) AS avg_salary,
                (SELECT MAX(salary) FROM employees) AS max_salary",
        )
        .unwrap();

    assert_table_eq!(result, [[5, 65000.0, 80000]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_in_case_expression() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT emp_name,
                    CASE WHEN salary > (SELECT AVG(salary) FROM employees) THEN 'High' ELSE 'Low' END AS salary_level
             FROM employees
             ORDER BY emp_id"
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "High"],
            ["Bob", "High"],
            ["Charlie", "Low"],
            ["Diana", "High"],
            ["Eve", "Low"],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_deeply_nested_subquery() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT emp_name FROM employees
             WHERE salary > (
                SELECT AVG(salary) FROM employees
                WHERE dept_id IN (
                    SELECT dept_id FROM departments
                    WHERE dept_name = 'Sales'
                )
             )
             ORDER BY emp_id",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Diana"],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_with_distinct() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE dept_id IN (SELECT DISTINCT dept_id FROM employees WHERE dept_id IS NOT NULL) ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"], ["Diana"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_with_order_by_and_limit() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary >= (SELECT salary FROM employees ORDER BY salary DESC LIMIT 1 OFFSET 1)")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
fn test_subquery_comparison_not_equal() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary != (SELECT MAX(salary) FROM employees) ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Charlie"], ["Diana"], ["Eve"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_comparison_less_than_or_equal() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary <= (SELECT AVG(salary) FROM employees) ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Charlie"], ["Eve"]]);
}

#[test]
fn test_subquery_comparison_greater_than_or_equal() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary >= (SELECT AVG(salary) FROM employees) ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Diana"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_correlated_subquery_with_null_handling() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT e.emp_name,
                    (SELECT d.dept_name FROM departments d WHERE d.dept_id = e.dept_id) AS dept
             FROM employees e
             ORDER BY e.emp_id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Bob", "Engineering"],
            ["Charlie", "Sales"],
            ["Diana", "Sales"],
            ["Eve", null],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_with_union() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT emp_name FROM employees
             WHERE dept_id IN (
                SELECT dept_id FROM departments WHERE dept_name = 'Engineering'
                UNION
                SELECT dept_id FROM departments WHERE dept_name = 'Sales'
             )
             ORDER BY emp_id",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"], ["Diana"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_returning_null() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE salary = (SELECT salary FROM employees WHERE emp_name = 'NonExistent')")
        .unwrap();

    assert_table_eq!(result, []);
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_with_coalesce() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT emp_name, COALESCE((SELECT d.dept_name FROM departments d WHERE d.dept_id = e.dept_id), 'No Department') AS dept
             FROM employees e
             ORDER BY e.emp_id"
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Bob", "Engineering"],
            ["Charlie", "Sales"],
            ["Diana", "Sales"],
            ["Eve", "No Department"],
        ]
    );
}

#[test]
fn test_subquery_as_table_expression() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT * FROM (
                SELECT dept_id, AVG(salary) AS avg_salary
                FROM employees
                WHERE dept_id IS NOT NULL
                GROUP BY dept_id
             ) sub
             WHERE avg_salary > 60000
             ORDER BY dept_id",
        )
        .unwrap();

    assert_table_eq!(result, [[1, 75000.0], [2, 62500.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_multiple_correlated_subqueries() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT d.dept_name,
                    (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.dept_id) AS emp_count,
                    (SELECT AVG(e.salary) FROM employees e WHERE e.dept_id = d.dept_id) AS avg_salary
             FROM departments d
             ORDER BY d.dept_id"
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Engineering", 2, 75000.0],
            ["Sales", 2, 62500.0],
            ["HR", 0, null]
        ]
    );
}

#[test]
fn test_subquery_with_between() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT emp_name FROM employees
             WHERE salary BETWEEN
                (SELECT MIN(salary) FROM employees WHERE dept_id = 1) AND
                (SELECT MAX(salary) FROM employees WHERE dept_id = 1)
             ORDER BY emp_id",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_row_subquery() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE (dept_id, salary) = (SELECT dept_id, MAX(salary) FROM employees WHERE dept_id = 1 GROUP BY dept_id)")
        .unwrap();

    assert_table_eq!(result, [["Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_in_update() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    executor
        .execute_sql("UPDATE employees SET salary = salary + 1000 WHERE dept_id = (SELECT dept_id FROM departments WHERE dept_name = 'Engineering')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT emp_name, salary FROM employees WHERE dept_id = 1 ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Alice", 71000], ["Bob", 81000],]);
}

#[test]
fn test_subquery_in_delete() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    executor
        .execute_sql("DELETE FROM employees WHERE dept_id IN (SELECT dept_id FROM departments WHERE dept_name = 'HR')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM employees")
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_exists_with_null_in_subquery() {
    let mut executor = create_executor();
    setup_nullable_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM items i1 WHERE EXISTS (SELECT 1 FROM items i2 WHERE i2.value = i1.value) ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_all_with_null_in_subquery() {
    let mut executor = create_executor();
    setup_nullable_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM items WHERE value > ALL (SELECT value FROM items WHERE value IS NOT NULL) ORDER BY id")
        .unwrap();

    assert_table_eq!(result, []);
}

#[test]
#[ignore = "Implement me!"]
fn test_any_with_null_in_subquery() {
    let mut executor = create_executor();
    setup_nullable_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM items WHERE value = ANY (SELECT value FROM items WHERE value IS NOT NULL) ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_caching_uncorrelated() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT emp_name, salary - (SELECT AVG(salary) FROM employees) AS diff_from_avg
             FROM employees
             ORDER BY emp_id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 5000.0],
            ["Bob", 15000.0],
            ["Charlie", -5000.0],
            ["Diana", 0.0],
            ["Eve", -15000.0]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_with_aggregates_and_filter() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT emp_name FROM employees
             WHERE salary > (SELECT AVG(salary) FROM employees WHERE salary > 50000)
             ORDER BY emp_id",
        )
        .unwrap();

    assert_table_eq!(result, [["Bob"]]);
}

#[test]
fn test_lateral_cross_join() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT d.dept_name, e.emp_name
             FROM departments d
             CROSS JOIN LATERAL (SELECT emp_name FROM employees WHERE dept_id = d.dept_id ORDER BY emp_id LIMIT 1) e
             ORDER BY d.dept_id"
        )
        .unwrap();

    assert_table_eq!(result, [["Engineering", "Alice"], ["Sales", "Charlie"],]);
}

#[test]
fn test_lateral_left_join() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT d.dept_name, e.emp_name
             FROM departments d
             LEFT JOIN LATERAL (SELECT emp_name FROM employees WHERE dept_id = d.dept_id ORDER BY emp_id LIMIT 1) e ON TRUE
             ORDER BY d.dept_id"
        )
        .unwrap();

    assert_table_eq!(
        result,
        [["Engineering", "Alice"], ["Sales", "Charlie"], ["HR", null],]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_with_window_function() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT * FROM (
                SELECT emp_name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn
                FROM employees
             ) sub
             WHERE rn <= 2",
        )
        .unwrap();

    assert_table_eq!(result, [["Bob", 80000, 1], ["Alice", 70000, 2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_scalar_subquery_in_order_by() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT emp_name FROM employees
             ORDER BY (SELECT COUNT(*) FROM employees e2 WHERE e2.dept_id = employees.dept_id) DESC, emp_id"
        )
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice"], ["Bob"], ["Charlie"], ["Diana"], ["Eve"]]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_with_cte() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "WITH high_salary AS (SELECT emp_id, emp_name, salary FROM employees WHERE salary > 65000)
             SELECT emp_name FROM employees
             WHERE emp_id IN (SELECT emp_id FROM high_salary)
             ORDER BY emp_id"
        )
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_exists_with_count_zero() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT dept_name FROM departments d WHERE (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.dept_id) = 0")
        .unwrap();

    assert_table_eq!(result, [["HR"]]);
}

#[test]
fn test_subquery_equality_null_safe() {
    let mut executor = create_executor();
    setup_nullable_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM items WHERE value IS NOT DISTINCT FROM (SELECT value FROM items WHERE id = 2)")
        .unwrap();

    assert_table_eq!(result, [[2], [4]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_tuple_in_subquery() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql("SELECT emp_name FROM employees WHERE (dept_id, emp_id) IN (SELECT dept_id, emp_id FROM employees WHERE salary > 65000) ORDER BY emp_id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_subquery_with_self_join() {
    let mut executor = create_executor();
    setup_employees_departments(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT e1.emp_name FROM employees e1
             WHERE e1.salary > (
                SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept_id = e1.dept_id
             )
             ORDER BY e1.emp_id",
        )
        .unwrap();

    assert_table_eq!(result, [["Bob"], ["Diana"]]);
}
