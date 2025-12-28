use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::{create_session, d};

async fn setup_tables(session: &YachtSQLSession) {
    session
        .execute_sql(
            "CREATE TABLE employees (id INT64, name STRING, manager_id INT64, salary INT64)",
        )
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employees VALUES (1, 'CEO', NULL, 200000), (2, 'VP', 1, 150000), (3, 'Manager', 2, 100000), (4, 'Developer', 3, 80000)").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_simple_cte() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "WITH high_earners AS (SELECT name, salary FROM employees WHERE salary > 100000) SELECT name FROM high_earners ORDER BY salary DESC",
        ).await
        .unwrap();

    assert_table_eq!(result, [["CEO"], ["VP"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_with_column_aliases() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "WITH emp_data (emp_name, emp_salary) AS (SELECT name, salary FROM employees) SELECT emp_name FROM emp_data WHERE emp_salary > 100000 ORDER BY emp_salary DESC",
        ).await
        .unwrap();

    assert_table_eq!(result, [["CEO"], ["VP"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_ctes() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "WITH managers AS (SELECT id, name FROM employees WHERE manager_id IS NOT NULL), top_managers AS (SELECT id, name FROM managers WHERE id IN (SELECT manager_id FROM employees)) SELECT name FROM top_managers ORDER BY id",
        ).await
        .unwrap();

    assert_table_eq!(result, [["VP"], ["Manager"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_used_multiple_times() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "WITH emp AS (SELECT * FROM employees), stats AS (SELECT COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM emp) SELECT cnt AS total, CAST(avg_sal AS FLOAT64) AS avg_salary FROM stats",
        ).await
        .unwrap();

    assert_table_eq!(result, [[4, 132500.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_with_join() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "WITH mgrs AS (SELECT id, name AS mgr_name FROM employees WHERE id IN (SELECT manager_id FROM employees WHERE manager_id IS NOT NULL)) SELECT e.name, m.mgr_name FROM employees e JOIN mgrs m ON e.manager_id = m.id ORDER BY e.id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [["VP", "CEO"], ["Manager", "VP"], ["Developer", "Manager"],]
    );
}

#[tokio::test(flavor = "current_thread")]

async fn test_recursive_cte() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "WITH RECURSIVE emp_hierarchy AS (SELECT id, name, manager_id, 1 AS level FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.manager_id, h.level + 1 FROM employees e JOIN emp_hierarchy h ON e.manager_id = h.id) SELECT name, level FROM emp_hierarchy ORDER BY level, id",
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [["CEO", 1], ["VP", 2], ["Manager", 3], ["Developer", 4],]
    );
}

#[tokio::test(flavor = "current_thread")]

async fn test_recursive_cte_with_limit() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE numbers (n INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH RECURSIVE cnt AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cnt WHERE n < 5) SELECT n FROM cnt ORDER BY n",
        ).await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [3], [4], [5],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_with_aggregation() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "WITH salary_stats AS (SELECT AVG(salary) AS avg_sal, MAX(salary) AS max_sal FROM employees) SELECT name FROM employees, salary_stats WHERE salary > avg_sal ORDER BY salary DESC",
        ).await
        .unwrap();

    assert_table_eq!(result, [["CEO"], ["VP"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_in_subquery() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT name FROM employees WHERE salary > (WITH avg_sal AS (SELECT AVG(salary) AS val FROM employees) SELECT val FROM avg_sal) ORDER BY salary DESC",
        ).await
        .unwrap();

    assert_table_eq!(result, [["CEO"], ["VP"]]);
}

#[tokio::test(flavor = "current_thread")]

async fn test_cte_with_insert() {
    let session = create_session();
    setup_tables(&session).await;

    session
        .execute_sql("CREATE TABLE high_earners (name STRING, salary INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "WITH he AS (SELECT name, salary FROM employees WHERE salary > 100000) INSERT INTO high_earners SELECT * FROM he",
        ).await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM high_earners ORDER BY salary DESC")
        .await
        .unwrap();

    assert_table_eq!(result, [["CEO"], ["VP"]]);
}

#[tokio::test(flavor = "current_thread")]

async fn test_cte_with_update() {
    let session = create_session();
    setup_tables(&session).await;

    session
        .execute_sql(
            "WITH low_earners AS (SELECT id FROM employees WHERE salary < 100000) UPDATE employees SET salary = CAST(salary * 1.1 AS INT64) WHERE id IN (SELECT id FROM low_earners)",
        ).await
        .unwrap();

    let result = session
        .execute_sql("SELECT salary FROM employees WHERE name = 'Developer'")
        .await
        .unwrap();

    assert_table_eq!(result, [[88000]]);
}

#[tokio::test(flavor = "current_thread")]

async fn test_cte_with_delete() {
    let session = create_session();
    setup_tables(&session).await;

    session
        .execute_sql(
            "WITH low_earners AS (SELECT id FROM employees WHERE salary < 90000) DELETE FROM employees WHERE id IN (SELECT id FROM low_earners)",
        ).await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM employees")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]

async fn test_recursive_cte_fibonacci() {
    let session = create_session();

    let result = session
        .execute_sql(
            "WITH RECURSIVE fib AS (
                SELECT 1 AS n, 1 AS fib_n, 1 AS fib_n_minus_1
                UNION ALL
                SELECT n + 1, fib_n + fib_n_minus_1, fib_n
                FROM fib
                WHERE n < 10
            )
            SELECT n, fib_n FROM fib ORDER BY n",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, 1],
            [2, 2],
            [3, 3],
            [4, 5],
            [5, 8],
            [6, 13],
            [7, 21],
            [8, 34],
            [9, 55],
            [10, 89],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]

async fn test_recursive_cte_path_finding() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE edges (from_node STRING, to_node STRING, weight INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO edges VALUES
            ('A', 'B', 1), ('B', 'C', 2), ('C', 'D', 1),
            ('A', 'C', 4), ('B', 'D', 3)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH RECURSIVE paths AS (
                SELECT from_node AS start_node, to_node AS end_node,
                       weight AS total_weight, [from_node, to_node] AS path
                FROM edges
                WHERE from_node = 'A'
                UNION ALL
                SELECT p.start_node, e.to_node, p.total_weight + e.weight,
                       ARRAY_CONCAT(p.path, [e.to_node])
                FROM paths p
                JOIN edges e ON p.end_node = e.from_node
                WHERE NOT e.to_node IN UNNEST(p.path)
            )
            SELECT COUNT(*) >= 1
            FROM paths
            WHERE end_node = 'D'",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]

async fn test_recursive_cte_tree_aggregation() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE org (id INT64, name STRING, parent_id INT64, salary INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO org VALUES
            (1, 'CEO', NULL, 500000),
            (2, 'CTO', 1, 300000),
            (3, 'CFO', 1, 280000),
            (4, 'Dev Lead', 2, 150000),
            (5, 'Dev 1', 4, 100000),
            (6, 'Dev 2', 4, 95000),
            (7, 'Accountant', 3, 80000)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH RECURSIVE subtree AS (
                SELECT id, name, parent_id, salary, 0 AS depth
                FROM org WHERE parent_id IS NULL
                UNION ALL
                SELECT o.id, o.name, o.parent_id, o.salary, s.depth + 1
                FROM org o
                JOIN subtree s ON o.parent_id = s.id
            )
            SELECT name, depth, salary FROM subtree ORDER BY depth, name",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["CEO", 0, 500000],
            ["CFO", 1, 280000],
            ["CTO", 1, 300000],
            ["Accountant", 2, 80000],
            ["Dev Lead", 2, 150000],
            ["Dev 1", 3, 100000],
            ["Dev 2", 3, 95000],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]

async fn test_recursive_cte_generate_series() {
    let session = create_session();

    let result = session
        .execute_sql(
            "WITH RECURSIVE series AS (
                SELECT DATE '2024-01-01' AS dt
                UNION ALL
                SELECT DATE_ADD(dt, INTERVAL 1 DAY)
                FROM series
                WHERE dt < DATE '2024-01-07'
            )
            SELECT dt FROM series ORDER BY dt",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [d(2024, 1, 1)],
            [d(2024, 1, 2)],
            [d(2024, 1, 3)],
            [d(2024, 1, 4)],
            [d(2024, 1, 5)],
            [d(2024, 1, 6)],
            [d(2024, 1, 7)],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]

async fn test_recursive_cte_max_depth() {
    let session = create_session();

    let result = session
        .execute_sql(
            "WITH RECURSIVE deep AS (
                SELECT 1 AS level, 'start' AS data
                UNION ALL
                SELECT level + 1, CONCAT(data, '-', CAST(level + 1 AS STRING))
                FROM deep
                WHERE level < 100
            )
            SELECT MAX(level) AS max_level FROM deep",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[100]]);
}

#[tokio::test(flavor = "current_thread")]

async fn test_recursive_cte_with_aggregation() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE categories (id INT64, name STRING, parent_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO categories VALUES
            (1, 'Electronics', NULL),
            (2, 'Computers', 1),
            (3, 'Laptops', 2),
            (4, 'Phones', 1),
            (5, 'Smartphones', 4)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH RECURSIVE cat_tree AS (
                SELECT id, name, parent_id, 1 AS level, CAST(name AS STRING) AS path
                FROM categories WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, c.parent_id, ct.level + 1,
                       CONCAT(ct.path, ' > ', c.name)
                FROM categories c
                JOIN cat_tree ct ON c.parent_id = ct.id
            )
            SELECT id, name, level, path FROM cat_tree ORDER BY path",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "Electronics", 1, "Electronics"],
            [2, "Computers", 2, "Electronics > Computers"],
            [3, "Laptops", 3, "Electronics > Computers > Laptops"],
            [4, "Phones", 2, "Electronics > Phones"],
            [5, "Smartphones", 3, "Electronics > Phones > Smartphones"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]

async fn test_recursive_cte_multiple_anchors() {
    let session = create_session();

    let result = session
        .execute_sql(
            "WITH RECURSIVE multi AS (
                SELECT 1 AS n, 'A' AS source
                UNION ALL
                SELECT 100 AS n, 'B' AS source
                UNION ALL
                SELECT n + 1, source
                FROM multi
                WHERE n < 3 OR (n >= 100 AND n < 102)
            )
            SELECT n, source FROM multi ORDER BY source, n",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "A"],
            [2, "A"],
            [3, "A"],
            [100, "B"],
            [101, "B"],
            [102, "B"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]

async fn test_recursive_cte_string_accumulation() {
    let session = create_session();

    let result = session
        .execute_sql(
            "WITH RECURSIVE words AS (
                SELECT 1 AS pos, 'Hello' AS word, 'Hello' AS sentence
                UNION ALL
                SELECT pos + 1,
                       CASE pos + 1
                           WHEN 2 THEN 'World'
                           WHEN 3 THEN 'from'
                           WHEN 4 THEN 'SQL'
                       END,
                       CONCAT(sentence, ' ', CASE pos + 1
                           WHEN 2 THEN 'World'
                           WHEN 3 THEN 'from'
                           WHEN 4 THEN 'SQL'
                       END)
                FROM words
                WHERE pos < 4
            )
            SELECT sentence FROM words WHERE pos = 4",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Hello World from SQL"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_referencing_another_cte() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "WITH
                base AS (SELECT * FROM employees),
                filtered AS (SELECT * FROM base WHERE salary > 90000),
                ranked AS (SELECT name, RANK() OVER (ORDER BY salary DESC) AS rk FROM filtered)
            SELECT name FROM ranked WHERE rk = 1",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["CEO"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_with_window_functions() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "WITH windowed AS (
                SELECT
                    name,
                    salary,
                    SUM(salary) OVER (ORDER BY salary) AS running_total,
                    AVG(salary) OVER () AS avg_salary
                FROM employees
            )
            SELECT name, running_total FROM windowed WHERE salary > avg_salary ORDER BY running_total",
        ).await
        .unwrap();

    assert_table_eq!(result, [["VP", 330000], ["CEO", 530000]]);
}
