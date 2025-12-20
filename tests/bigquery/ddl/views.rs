use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_base_table(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql(
            "CREATE TABLE employees (id INT64, name STRING, department STRING, salary INT64)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000), (2, 'Bob', 'Engineering', 90000), (3, 'Charlie', 'Sales', 80000), (4, 'Diana', 'Sales', 85000)")
        .unwrap();
}

#[test]
#[ignore = "Implement me!"]
fn test_create_view() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql(
            "CREATE VIEW engineers AS SELECT * FROM employees WHERE department = 'Engineering'",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM engineers ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_create_view_with_columns() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql("CREATE VIEW emp_names (emp_id, emp_name) AS SELECT id, name FROM employees")
        .unwrap();

    let result = executor
        .execute_sql("SELECT emp_name FROM emp_names ORDER BY emp_id")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"], ["Diana"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_create_or_replace_view() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql("CREATE VIEW dept_view AS SELECT * FROM employees WHERE department = 'Sales'")
        .unwrap();

    executor
        .execute_sql("CREATE OR REPLACE VIEW dept_view AS SELECT * FROM employees WHERE department = 'Engineering'")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM dept_view")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_create_view_if_not_exists() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql("CREATE VIEW my_view AS SELECT * FROM employees")
        .unwrap();

    executor
        .execute_sql("CREATE VIEW IF NOT EXISTS my_view AS SELECT id FROM employees")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM my_view")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_drop_view() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql("CREATE VIEW temp_view AS SELECT * FROM employees")
        .unwrap();

    executor.execute_sql("DROP VIEW temp_view").unwrap();

    let result = executor.execute_sql("SELECT * FROM temp_view");
    assert!(result.is_err());
}

#[test]
fn test_drop_view_if_exists() {
    let mut executor = create_executor();

    let result = executor.execute_sql("DROP VIEW IF EXISTS nonexistent_view");
    assert!(result.is_ok());
}

#[test]
#[ignore = "Implement me!"]
fn test_view_with_aggregation() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql("CREATE VIEW dept_stats AS SELECT department, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM employees GROUP BY department")
        .unwrap();

    let result = executor
        .execute_sql("SELECT department, cnt FROM dept_stats ORDER BY department")
        .unwrap();
    assert_table_eq!(result, [["Engineering", 2], ["Sales", 2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_view_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE staff (id INT64, name STRING, dept_id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO staff VALUES (1, 'Alice', 1), (2, 'Bob', 2)")
        .unwrap();

    executor
        .execute_sql("CREATE VIEW staff_with_dept AS SELECT s.id, s.name, d.name AS dept_name FROM staff s JOIN departments d ON s.dept_id = d.id")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, dept_name FROM staff_with_dept ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Alice", "Engineering"], ["Bob", "Sales"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_view_with_subquery() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql("CREATE VIEW high_earners AS SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM high_earners ORDER BY salary DESC")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_view_in_subquery() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql(
            "CREATE VIEW eng_view AS SELECT * FROM employees WHERE department = 'Engineering'",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM eng_view)")
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_view_with_cte() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql("CREATE VIEW ranked_employees AS WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn FROM employees) SELECT * FROM ranked WHERE rn = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM ranked_employees ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Diana"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_nested_views() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql("CREATE VIEW all_employees AS SELECT * FROM employees")
        .unwrap();

    executor
        .execute_sql("CREATE VIEW high_salary AS SELECT * FROM all_employees WHERE salary > 85000")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM high_salary ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_materialized_view() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql("CREATE MATERIALIZED VIEW dept_summary AS SELECT department, SUM(salary) AS total_salary FROM employees GROUP BY department")
        .unwrap();

    let result = executor
        .execute_sql("SELECT department, total_salary FROM dept_summary ORDER BY department")
        .unwrap();
    assert_table_eq!(result, [["Engineering", 190000], ["Sales", 165000]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_view_with_options() {
    let mut executor = create_executor();
    setup_base_table(&mut executor);

    executor
        .execute_sql("CREATE VIEW employees_view OPTIONS(description='Employee data view') AS SELECT * FROM employees")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM employees_view")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}
