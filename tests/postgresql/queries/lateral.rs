use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_tables(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, dept_id INT64, salary INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 1, 100000), (2, 'Bob', 1, 90000), (3, 'Charlie', 2, 80000), (4, 'Diana', 2, 85000), (5, 'Eve', 3, 70000)")
        .unwrap();
}

#[test]
#[ignore = "Implement me!"]
fn test_lateral_basic() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT d.name, e.name AS emp_name
             FROM departments d,
             LATERAL (SELECT * FROM employees WHERE dept_id = d.id LIMIT 1) e
             ORDER BY d.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Engineering", "Alice"],
            ["Sales", "Charlie"],
            ["HR", "Eve"]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_lateral_top_n_per_group() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT d.name, e.name AS emp_name, e.salary
             FROM departments d
             CROSS JOIN LATERAL (
                 SELECT * FROM employees
                 WHERE dept_id = d.id
                 ORDER BY salary DESC
                 LIMIT 2
             ) e
             ORDER BY d.id, e.salary DESC",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Engineering", "Alice", 100000],
            ["Engineering", "Bob", 90000],
            ["Sales", "Diana", 85000],
            ["Sales", "Charlie", 80000],
            ["HR", "Eve", 70000]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_lateral_aggregate() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT d.name, stats.avg_salary, stats.max_salary
             FROM departments d
             CROSS JOIN LATERAL (
                 SELECT AVG(salary) AS avg_salary, MAX(salary) AS max_salary
                 FROM employees
                 WHERE dept_id = d.id
             ) stats
             ORDER BY d.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Engineering", 95000.0, 100000],
            ["Sales", 82500.0, 85000],
            ["HR", 70000.0, 70000]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_lateral_left_join() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    executor
        .execute_sql("INSERT INTO departments VALUES (4, 'Empty')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT d.name, e.name AS emp_name
             FROM departments d
             LEFT JOIN LATERAL (
                 SELECT * FROM employees WHERE dept_id = d.id LIMIT 1
             ) e ON TRUE
             ORDER BY d.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Engineering", "Alice"],
            ["Sales", "Charlie"],
            ["HR", "Eve"],
            ["Empty", null]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_lateral_with_values() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT * FROM
             (VALUES (1, 'a'), (2, 'b')) AS t(id, val),
             LATERAL (SELECT id * 2 AS doubled) AS l",
        )
        .unwrap();

    assert_table_eq!(result, [[1, "a", 2], [2, "b", 4]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_lateral_unnest() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE arrays (id INT64, arr INT64[])")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arrays VALUES (1, [10, 20, 30]), (2, [40, 50])")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT a.id, u.elem
             FROM arrays a,
             LATERAL UNNEST(a.arr) AS u(elem)
             ORDER BY a.id, u.elem",
        )
        .unwrap();

    assert_table_eq!(result, [[1, 10], [1, 20], [1, 30], [2, 40], [2, 50]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_lateral_generate_series() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT t.n, gs.val
             FROM (VALUES (3), (5)) AS t(n),
             LATERAL GENERATE_SERIES(1, t.n) AS gs(val)
             ORDER BY t.n, gs.val",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            [3, 1],
            [3, 2],
            [3, 3],
            [5, 1],
            [5, 2],
            [5, 3],
            [5, 4],
            [5, 5]
        ]
    );
}

#[test]
fn test_lateral_correlated() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT d.name,
                    (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS emp_count,
                    highest.salary AS top_salary
             FROM departments d
             CROSS JOIN LATERAL (
                 SELECT salary FROM employees WHERE dept_id = d.id ORDER BY salary DESC LIMIT 1
             ) highest
             ORDER BY d.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Engineering", 2, 100000],
            ["Sales", 2, 85000],
            ["HR", 1, 70000]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_lateral_multiple() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT d.name, top.name AS top_earner, bottom.name AS bottom_earner
             FROM departments d
             CROSS JOIN LATERAL (
                 SELECT name FROM employees WHERE dept_id = d.id ORDER BY salary DESC LIMIT 1
             ) top
             CROSS JOIN LATERAL (
                 SELECT name FROM employees WHERE dept_id = d.id ORDER BY salary ASC LIMIT 1
             ) bottom
             ORDER BY d.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Engineering", "Alice", "Bob"],
            ["Sales", "Diana", "Charlie"],
            ["HR", "Eve", "Eve"]
        ]
    );
}

#[test]
fn test_lateral_with_cte() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "WITH dept_stats AS (
                 SELECT dept_id, AVG(salary) AS avg_sal
                 FROM employees
                 GROUP BY dept_id
             )
             SELECT d.name, above.name AS above_avg
             FROM departments d
             JOIN dept_stats ds ON d.id = ds.dept_id
             CROSS JOIN LATERAL (
                 SELECT name FROM employees
                 WHERE dept_id = d.id AND salary > ds.avg_sal
                 ORDER BY salary DESC
                 LIMIT 1
             ) above
             ORDER BY d.id",
        )
        .unwrap();

    assert_table_eq!(result, [["Engineering", "Alice"], ["Sales", "Diana"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_lateral_json_each() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE json_data (id INT64, data JSON)")
        .unwrap();
    executor
        .execute_sql(r#"INSERT INTO json_data VALUES (1, '{"a": 1, "b": 2}')"#)
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT jd.id, je.key, je.value
             FROM json_data jd,
             LATERAL JSON_EACH(jd.data) AS je(key, value)",
        )
        .unwrap();

    assert_table_eq!(result, [[1, "a", "1"], [1, "b", "2"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_lateral_recursive_depth() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE tree (id INT64, parent_id INT64, name STRING)")
        .unwrap();
    executor.execute_sql("INSERT INTO tree VALUES (1, NULL, 'root'), (2, 1, 'child1'), (3, 1, 'child2'), (4, 2, 'grandchild')").unwrap();

    let result = executor
        .execute_sql(
            "SELECT t.name, children.cnt
             FROM tree t
             CROSS JOIN LATERAL (
                 SELECT COUNT(*) AS cnt FROM tree WHERE parent_id = t.id
             ) children
             ORDER BY t.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [["root", 2], ["child1", 1], ["child2", 0], ["grandchild", 0]]
    );
}
