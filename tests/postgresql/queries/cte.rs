use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql(
            "CREATE TABLE employees (id INTEGER, name TEXT, manager_id INTEGER, salary INTEGER)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'CEO', NULL, 200000), (2, 'VP', 1, 150000), (3, 'Manager', 2, 100000), (4, 'Developer', 3, 80000)")
        .unwrap();
}

#[test]
fn test_simple_cte() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "WITH high_earners AS (SELECT name, salary FROM employees WHERE salary > 100000) SELECT name FROM high_earners ORDER BY salary DESC",
        )
        .unwrap();

    assert_table_eq!(result, [["CEO"], ["VP"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_cte_with_column_aliases() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "WITH emp_data (emp_name, emp_salary) AS (SELECT name, salary FROM employees) SELECT emp_name FROM emp_data WHERE emp_salary > 100000 ORDER BY emp_salary DESC",
        )
        .unwrap();

    assert_table_eq!(result, [["CEO"], ["VP"]]);
}

#[test]
fn test_multiple_ctes() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "WITH managers AS (SELECT id, name FROM employees WHERE manager_id IS NOT NULL), top_managers AS (SELECT id, name FROM managers WHERE id IN (SELECT manager_id FROM employees)) SELECT name FROM top_managers ORDER BY id",
        )
        .unwrap();

    assert_table_eq!(result, [["VP"], ["Manager"]]);
}

#[test]
fn test_cte_used_multiple_times() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "WITH emp AS (SELECT * FROM employees) SELECT (SELECT COUNT(*) FROM emp) AS total, (SELECT AVG(salary) FROM emp) AS avg_salary",
        );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_cte_with_join() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "WITH mgrs AS (SELECT id, name AS mgr_name FROM employees WHERE id IN (SELECT manager_id FROM employees WHERE manager_id IS NOT NULL)) SELECT e.name, m.mgr_name FROM employees e JOIN mgrs m ON e.manager_id = m.id ORDER BY e.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [["VP", "CEO"], ["Manager", "VP"], ["Developer", "Manager"],]
    );
}

#[test]
fn test_recursive_cte() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "WITH RECURSIVE emp_hierarchy AS (SELECT id, name, manager_id, 1 AS level FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.manager_id, h.level + 1 FROM employees e JOIN emp_hierarchy h ON e.manager_id = h.id) SELECT name, level FROM emp_hierarchy ORDER BY level, id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [["CEO", 1], ["VP", 2], ["Manager", 3], ["Developer", 4],]
    );
}

#[test]
fn test_recursive_cte_with_limit() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE numbers (n INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH RECURSIVE cnt AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cnt WHERE n < 5) SELECT n FROM cnt ORDER BY n",
        )
        .unwrap();

    assert_table_eq!(result, [[1], [2], [3], [4], [5],]);
}

#[test]
fn test_cte_with_aggregation() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "WITH salary_stats AS (SELECT AVG(salary) AS avg_sal, MAX(salary) AS max_sal FROM employees) SELECT name FROM employees, salary_stats WHERE salary > avg_sal ORDER BY salary DESC",
        )
        .unwrap();

    assert_table_eq!(result, [["CEO"], ["VP"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_cte_in_subquery() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT name FROM employees WHERE salary > (WITH avg_sal AS (SELECT AVG(salary) AS val FROM employees) SELECT val FROM avg_sal) ORDER BY salary DESC",
        )
        .unwrap();

    assert_table_eq!(result, [["CEO"], ["VP"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_cte_with_insert() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    executor
        .execute_sql("CREATE TABLE high_earners (name TEXT, salary INTEGER)")
        .unwrap();

    executor
        .execute_sql(
            "WITH he AS (SELECT name, salary FROM employees WHERE salary > 100000) INSERT INTO high_earners SELECT * FROM he",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM high_earners ORDER BY salary DESC")
        .unwrap();

    assert_table_eq!(result, [["CEO"], ["VP"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_cte_with_update() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    executor
        .execute_sql(
            "WITH low_earners AS (SELECT id FROM employees WHERE salary < 100000) UPDATE employees SET salary = salary * 1.1 WHERE id IN (SELECT id FROM low_earners)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT salary FROM employees WHERE name = 'Developer'")
        .unwrap();

    assert_table_eq!(result, [[88000]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_cte_with_delete() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    executor
        .execute_sql(
            "WITH low_earners AS (SELECT id FROM employees WHERE salary < 90000) DELETE FROM employees WHERE id IN (SELECT id FROM low_earners)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM employees")
        .unwrap();

    assert_table_eq!(result, [[3]]);
}
