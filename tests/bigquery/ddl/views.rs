use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_base_table(session: &yachtsql::YachtSQLSession) {
    session
        .execute_sql(
            "CREATE TABLE employees (id INT64, name STRING, department STRING, salary INT64)",
        )
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000), (2, 'Bob', 'Engineering', 90000), (3, 'Charlie', 'Sales', 80000), (4, 'Diana', 'Sales', 85000)").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_view() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE VIEW engineers AS SELECT * FROM employees WHERE department = 'Engineering'",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM engineers ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_view_with_columns() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW emp_names (emp_id, emp_name) AS SELECT id, name FROM employees")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT emp_name FROM emp_names ORDER BY emp_id")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"], ["Diana"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_or_replace_view() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW dept_view AS SELECT * FROM employees WHERE department = 'Sales'")
        .await
        .unwrap();

    session
        .execute_sql("CREATE OR REPLACE VIEW dept_view AS SELECT * FROM employees WHERE department = 'Engineering'").await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM dept_view")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_view_if_not_exists() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW my_view AS SELECT * FROM employees")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW IF NOT EXISTS my_view AS SELECT id FROM employees")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM my_view")
        .await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_view() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW temp_view AS SELECT * FROM employees")
        .await
        .unwrap();

    session.execute_sql("DROP VIEW temp_view").await.unwrap();

    let result = session.execute_sql("SELECT * FROM temp_view").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_view_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("DROP VIEW IF EXISTS nonexistent_view")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_aggregation() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW dept_stats AS SELECT department, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM employees GROUP BY department").await
        .unwrap();

    let result = session
        .execute_sql("SELECT department, cnt FROM dept_stats ORDER BY department")
        .await
        .unwrap();
    assert_table_eq!(result, [["Engineering", 2], ["Sales", 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE staff (id INT64, name STRING, dept_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO staff VALUES (1, 'Alice', 1), (2, 'Bob', 2)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW staff_with_dept AS SELECT s.id, s.name, d.name AS dept_name FROM staff s JOIN departments d ON s.dept_id = d.id").await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, dept_name FROM staff_with_dept ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", "Engineering"], ["Bob", "Sales"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_subquery() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW high_earners AS SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)").await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM high_earners ORDER BY salary DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_in_subquery() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE VIEW eng_view AS SELECT * FROM employees WHERE department = 'Engineering'",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM eng_view)")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_cte() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW ranked_employees AS WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn FROM employees) SELECT * FROM ranked WHERE rn = 1").await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM ranked_employees ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Diana"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_views() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW all_employees AS SELECT * FROM employees")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW high_salary AS SELECT * FROM all_employees WHERE salary > 85000")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM high_salary ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_materialized_view() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE MATERIALIZED VIEW dept_summary AS SELECT department, SUM(salary) AS total_salary FROM employees GROUP BY department").await
        .unwrap();

    let result = session
        .execute_sql("SELECT department, total_salary FROM dept_summary ORDER BY department")
        .await
        .unwrap();
    assert_table_eq!(result, [["Engineering", 190000], ["Sales", 165000]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_options() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW employees_view OPTIONS(description='Employee data view') AS SELECT * FROM employees").await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM employees_view")
        .await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_multiple_options() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE VIEW emp_view_opts
            OPTIONS (
                description = 'Employee view with options',
                labels = [('env', 'prod'), ('team', 'hr')],
                expiration_timestamp = TIMESTAMP '2030-01-01 00:00:00 UTC'
            )
            AS SELECT * FROM employees",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM emp_view_opts")
        .await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_materialized_view_with_options() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_with_opts
            OPTIONS (
                enable_refresh = true,
                refresh_interval_minutes = 60,
                description = 'Materialized view with refresh'
            )
            AS SELECT department, AVG(salary) as avg_salary
            FROM employees
            GROUP BY department",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT department FROM mv_with_opts ORDER BY department")
        .await
        .unwrap();
    assert_table_eq!(result, [["Engineering"], ["Sales"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_materialized_view_with_partition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE time_data (id INT64, created DATE, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO time_data VALUES (1, DATE '2024-01-15', 100), (2, DATE '2024-02-20', 200)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_partitioned
            PARTITION BY created
            AS SELECT * FROM time_data",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_materialized_view_with_cluster() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_clustered
            CLUSTER BY department
            AS SELECT department, COUNT(*) as cnt
            FROM employees
            GROUP BY department",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT department, cnt FROM mv_clustered ORDER BY department")
        .await
        .unwrap();
    assert_table_eq!(result, [["Engineering", 2], ["Sales", 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_materialized_view_with_partition_and_cluster() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE sales_data (id INT64, sale_date DATE, region STRING, amount INT64)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO sales_data VALUES
            (1, DATE '2024-01-15', 'US', 1000),
            (2, DATE '2024-01-20', 'EU', 2000),
            (3, DATE '2024-02-10', 'US', 1500)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW sales_mv
            PARTITION BY sale_date
            CLUSTER BY region
            AS SELECT sale_date, region, SUM(amount) as total
            FROM sales_data
            GROUP BY sale_date, region",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_materialized_view_max_staleness() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_staleness
            OPTIONS (max_staleness = INTERVAL 4 HOUR)
            AS SELECT department, SUM(salary) as total
            FROM employees
            GROUP BY department",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_materialized_view_allow_non_incremental() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_non_incremental
            OPTIONS (allow_non_incremental_definition = true)
            AS SELECT DISTINCT department FROM employees",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_or_replace_materialized_view() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW replace_mv
            AS SELECT department, COUNT(*) as cnt FROM employees GROUP BY department",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE OR REPLACE MATERIALIZED VIEW replace_mv
            AS SELECT department, SUM(salary) as total FROM employees GROUP BY department",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT department, total FROM replace_mv ORDER BY department")
        .await
        .unwrap();
    assert_table_eq!(result, [["Engineering", 190000], ["Sales", 165000]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_materialized_view_if_not_exists() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_exists
            AS SELECT department, COUNT(*) as cnt FROM employees GROUP BY department",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_exists
            AS SELECT department, SUM(salary) as total FROM employees GROUP BY department",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT department, cnt FROM mv_exists ORDER BY department")
        .await
        .unwrap();
    assert_table_eq!(result, [["Engineering", 2], ["Sales", 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_materialized_view() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW to_drop_mv
            AS SELECT department, COUNT(*) as cnt FROM employees GROUP BY department",
        )
        .await
        .unwrap();

    session
        .execute_sql("DROP MATERIALIZED VIEW to_drop_mv")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM to_drop_mv").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_materialized_view_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("DROP MATERIALIZED VIEW IF EXISTS nonexistent_mv")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_view_set_options() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW alter_opts_view AS SELECT * FROM employees")
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER VIEW alter_opts_view SET OPTIONS (description = 'Updated view description')",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_view_set_multiple_options() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW multi_opts_view AS SELECT * FROM employees")
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER VIEW multi_opts_view SET OPTIONS (
                description = 'Updated description',
                labels = [('env', 'staging')]
            )",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_materialized_view_set_options() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW alter_mv
            OPTIONS (enable_refresh = false)
            AS SELECT department, SUM(salary) as total FROM employees GROUP BY department",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER MATERIALIZED VIEW alter_mv SET OPTIONS (
                enable_refresh = true,
                refresh_interval_minutes = 30
            )",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_qualified_name() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA view_schema")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE view_schema.base_table (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO view_schema.base_table VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE VIEW view_schema.qualified_view AS SELECT * FROM view_schema.base_table",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM view_schema.qualified_view")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_collation() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE collate_base (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO collate_base VALUES (1, 'Alice'), (2, 'bob'), (3, 'Charlie')")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE VIEW collate_view
            OPTIONS (default_collation = 'und:ci')
            AS SELECT * FROM collate_base",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_security_invoker() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE VIEW invoker_view
            SQL SECURITY INVOKER
            AS SELECT * FROM employees",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM invoker_view")
        .await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_security_definer() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE VIEW definer_view
            SQL SECURITY DEFINER
            AS SELECT * FROM employees",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM definer_view")
        .await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_order_by() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE VIEW ordered_view AS SELECT name, salary FROM employees ORDER BY salary DESC",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM ordered_view")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"], ["Diana"], ["Charlie"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_limit() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW limited_view AS SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 2").await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM limited_view")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_union() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE TABLE contractors (id INT64, name STRING, department STRING, rate INT64)",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO contractors VALUES (100, 'Eve', 'Engineering', 150)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE VIEW all_workers AS
            SELECT id, name, department FROM employees
            UNION ALL
            SELECT id, name, department FROM contractors",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM all_workers")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_window_function() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE VIEW ranked_by_dept AS
            SELECT
                name,
                department,
                salary,
                RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
            FROM employees",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, dept_rank FROM ranked_by_dept WHERE dept_rank = 1 ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 1], ["Diana", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_case_expression() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE VIEW salary_tier AS
            SELECT
                name,
                salary,
                CASE
                    WHEN salary >= 90000 THEN 'High'
                    WHEN salary >= 80000 THEN 'Medium'
                    ELSE 'Low'
                END as tier
            FROM employees",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, tier FROM salary_tier ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Alice", "High"],
            ["Bob", "High"],
            ["Charlie", "Medium"],
            ["Diana", "Medium"]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_distinct() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW distinct_depts AS SELECT DISTINCT department FROM employees")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT department FROM distinct_depts ORDER BY department")
        .await
        .unwrap();
    assert_table_eq!(result, [["Engineering"], ["Sales"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_update_underlying_table() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE VIEW dynamic_view AS SELECT * FROM employees WHERE department = 'Engineering'",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO employees VALUES (5, 'Frank', 'Engineering', 95000)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM dynamic_view")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_materialized_view_with_complex_aggregation() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW complex_mv AS
            SELECT
                department,
                COUNT(*) as employee_count,
                SUM(salary) as total_salary,
                AVG(salary) as avg_salary,
                MIN(salary) as min_salary,
                MAX(salary) as max_salary
            FROM employees
            GROUP BY department",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT department, employee_count, min_salary, max_salary FROM complex_mv ORDER BY department").await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Engineering", 2, 90000, 100000],
            ["Sales", 2, 80000, 85000]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_array_column() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE array_base (id INT64, tags ARRAY<STRING>)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO array_base VALUES (1, ['rust', 'sql']), (2, ['python', 'ml'])")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW array_view AS SELECT id, tags FROM array_base")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, ARRAY_LENGTH(tags) FROM array_view ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2], [2, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_view_with_struct_column() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE struct_base (id INT64, info STRUCT<name STRING, age INT64>)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO struct_base VALUES (1, STRUCT('Alice', 30)), (2, STRUCT('Bob', 25))",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW struct_view AS SELECT id, info.name, info.age FROM struct_base")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, age FROM struct_view ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 30], ["Bob", 25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_view_alter_column() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql("CREATE VIEW column_opts_view AS SELECT id, name, salary FROM employees")
        .await
        .unwrap();

    session
        .execute_sql("ALTER VIEW column_opts_view ALTER COLUMN name SET OPTIONS (description = 'Employee name')").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_materialized_view_alter_column() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_col_opts
            AS SELECT department, SUM(salary) as total FROM employees GROUP BY department",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER MATERIALIZED VIEW mv_col_opts ALTER COLUMN total SET OPTIONS (description = 'Total salary')",
        ).await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_materialized_view_replica() {
    let session = create_session();
    setup_base_table(&session).await;

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW base_mv
            AS SELECT department, SUM(salary) as total FROM employees GROUP BY department",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW REPLICA replica_mv
            AS REPLICA OF base_mv",
        )
        .await
        .unwrap();
}
