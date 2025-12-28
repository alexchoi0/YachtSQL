use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_tables(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, dept_id INT64, salary INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 1, 50000), (2, 'Bob', 1, 60000), (3, 'Charlie', 2, 55000), (4, 'Diana', 2, 70000)").await
        .unwrap();
    session
        .execute_sql("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_subquery_in_where_in() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering') ORDER BY name").await
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_subquery_in_where_not_in() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT name FROM employees WHERE dept_id NOT IN (SELECT id FROM departments WHERE name = 'Sales') ORDER BY name").await
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_subquery_in_from_clause() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT sub.name, sub.salary FROM (SELECT name, salary FROM employees WHERE salary > 55000) AS sub ORDER BY sub.name").await
        .unwrap();

    assert_table_eq!(result, [["Bob", 60000], ["Diana", 70000],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_subquery_literal_values() {
    let session = create_session();

    let result = session
        .execute_sql(r#"SELECT * FROM (SELECT "apple" AS fruit, "carrot" AS vegetable)"#)
        .await
        .unwrap();

    assert_table_eq!(result, [["apple", "carrot"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_with_qualified_star() {
    let session = create_session();

    let result = session
        .execute_sql(
            r#"WITH groceries AS
              (SELECT "milk" AS dairy,
               "eggs" AS protein,
               "bread" AS grain)
            SELECT g.*
            FROM groceries AS g"#,
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["milk", "eggs", "bread"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cte_with_star() {
    let session = create_session();

    let result = session
        .execute_sql(
            r#"WITH groceries AS
              (SELECT "milk" AS dairy,
               "eggs" AS protein,
               "bread" AS grain)
            SELECT *
            FROM groceries AS g"#,
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["milk", "eggs", "bread"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_struct_offset_star() {
    let session = create_session();

    let result = session
        .execute_sql(
            r#"WITH locations AS
              (SELECT ARRAY<STRUCT<city STRING, state STRING>>[("Seattle", "Washington"),
                ("Phoenix", "Arizona")] AS location)
            SELECT l.location[offset(0)].*
            FROM locations l"#,
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Seattle", "Washington"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_star_except() {
    let session = create_session();

    let result = session
        .execute_sql(
            r#"WITH orders AS
              (SELECT 5 as order_id,
              "sprocket" as item_name,
              200 as quantity)
            SELECT * EXCEPT (order_id)
            FROM orders"#,
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["sprocket", 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_select_star_replace() {
    let session = create_session();

    let result = session
        .execute_sql(
            r#"WITH orders AS
              (SELECT 5 as order_id,
              "sprocket" as item_name,
              200 as quantity)
            SELECT * REPLACE (quantity * 2 AS quantity)
            FROM orders"#,
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[5, "sprocket", 400]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_subquery_with_aggregation() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY name").await
        .unwrap();

    assert_table_eq!(result, [["Bob"], ["Diana"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exists_subquery() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.salary > 55000) ORDER BY name").await
        .unwrap();

    assert_table_eq!(result, [["Engineering"], ["Sales"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_exists_subquery() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE orders (id INT64, product_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Gizmo')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (1, 1), (2, 1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM products p WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id) ORDER BY name").await
        .unwrap();

    assert_table_eq!(result, [["Gadget"], ["Gizmo"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_subquery() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql("SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE id IN (SELECT dept_id FROM employees WHERE salary > 65000)) ORDER BY name").await
        .unwrap();

    assert_table_eq!(result, [["Charlie"], ["Diana"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_dedup_pattern_with_row_number() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE test_table (db_id INT64, db_basis_t INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO test_table VALUES (1, 100, 'a'), (1, 200, 'b'), (2, 100, 'c')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "WITH latest AS (
                SELECT db_id, MAX(db_basis_t) as latest_db_basis_t
                FROM test_table
                GROUP BY db_id
            ),
            dedupe AS (
                SELECT T.*,
                    ROW_NUMBER() OVER (PARTITION BY T.db_id) as row_number_id
                FROM test_table T
                INNER JOIN latest L
                ON T.db_id = L.db_id AND T.db_basis_t = L.latest_db_basis_t
            )
            SELECT * EXCEPT (row_number_id, db_basis_t)
            FROM dedupe
            WHERE row_number_id = 1
            ORDER BY db_id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "b"], [2, "c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_ignore_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE test_data (id INT64, val STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO test_data VALUES (1, 'a'), (1, NULL), (1, 'b'), (2, NULL), (2, 'c')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, ARRAY_AGG(val IGNORE NULLS) as vals FROM test_data GROUP BY id ORDER BY id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, ["a", "b"]], [2, ["c"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_backtick_quoted_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE `my-table` (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO `my-table` VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM `my-table`")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_backtick_bigquery_qualified_table() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA `my-dataset`")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE `my-dataset.my-table` (id INT64, value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO `my-dataset.my-table` VALUES (1, 'hello'), (2, 'world')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM `my-dataset.my-table` ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "hello"], [2, "world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_backtick_mixed_operations() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE `source-table` (id INT64, data STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO `source-table` VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    session
        .execute_sql("UPDATE `source-table` SET data = 'updated' WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM `source-table` ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "updated"], [2, "b"]]);

    session
        .execute_sql("DELETE FROM `source-table` WHERE id = 2")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM `source-table`")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "updated"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_correlated_scalar_subquery_order_by_non_projected() {
    let session = create_session();
    setup_tables(&session).await;

    let result = session
        .execute_sql(
            "SELECT
                d.id,
                d.name,
                (SELECT e.name
                 FROM employees e
                 WHERE e.dept_id = d.id
                 ORDER BY e.salary DESC
                 LIMIT 1) AS top_earner
            FROM departments d
            ORDER BY d.id",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Engineering", "Bob"], [2, "Sales", "Diana"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_with_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE events (order_id INT64, code STRING, ts INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO events VALUES (1, 'A', 100), (1, 'B', 300), (1, 'C', 200), (2, 'X', 50)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT order_id, ARRAY_AGG(code ORDER BY ts DESC) as codes FROM events GROUP BY order_id ORDER BY order_id").await
        .unwrap();

    assert_table_eq!(result, [[1, ["B", "C", "A"]], [2, ["X"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_ignore_nulls_with_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE events (order_id INT64, code STRING, ts INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO events VALUES (1, 'A', 100), (1, NULL, 300), (1, 'B', 200), (2, NULL, 50), (2, 'X', 100)").await
        .unwrap();

    let result = session
        .execute_sql("SELECT order_id, ARRAY_AGG(code IGNORE NULLS ORDER BY ts DESC) as codes FROM events GROUP BY order_id ORDER BY order_id").await
        .unwrap();

    assert_table_eq!(result, [[1, ["B", "A"]], [2, ["X"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_offset_on_array() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, vals ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql(r#"INSERT INTO data VALUES (1, ['a', 'b', 'c']), (2, ['x']), (3, [])"#)
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, vals[SAFE_OFFSET(0)] as first_val FROM data ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "a"], [2, "x"], [3, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exists_with_unnest() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (id INT64, status_codes ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql(r#"INSERT INTO orders VALUES (1, ['100', '129', '200']), (2, ['100', '200']), (3, ['219A', '300'])"#).await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, EXISTS (SELECT c FROM UNNEST(status_codes) c WHERE c = '129' LIMIT 1) AS has_129 FROM orders ORDER BY id").await
        .unwrap();

    assert_table_eq!(result, [[1, true], [2, false], [3, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exists_with_unnest_like_pattern() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (id INT64, status_codes ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql(r#"INSERT INTO orders VALUES (1, ['100', '129', '200']), (2, ['100', '200']), (3, ['219A', '300'])"#).await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, EXISTS (SELECT c FROM UNNEST(status_codes) c WHERE c LIKE '219%' LIMIT 1) AS has_219x FROM orders ORDER BY id").await
        .unwrap();

    assert_table_eq!(result, [[1, false], [2, false], [3, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exists_with_unnest_in_list() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (id INT64, status_codes ARRAY<STRING>)")
        .await
        .unwrap();
    session
        .execute_sql(r#"INSERT INTO orders VALUES (1, ['610', '200']), (2, ['100', '200']), (3, ['611', '612'])"#).await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, EXISTS (SELECT c FROM UNNEST(status_codes) c WHERE c IN ('610', '611', '612') LIMIT 1) AS has_complete FROM orders ORDER BY id").await
        .unwrap();

    assert_table_eq!(result, [[1, true], [2, false], [3, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_farm_fingerprint() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT FARM_FINGERPRINT('hello') AS hash")
        .await
        .unwrap();

    let records = result.to_records().unwrap();
    let hash = records[0].values()[0].as_i64().unwrap();
    assert_ne!(hash, 0);

    let result2 = session
        .execute_sql("SELECT FARM_FINGERPRINT('hello') = FARM_FINGERPRINT('hello') AS same, FARM_FINGERPRINT('hello') = FARM_FINGERPRINT('world') AS different").await
        .unwrap();

    assert_table_eq!(result2, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_timestamp_array() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-03 00:00:00', INTERVAL 1 DAY)) AS len").await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_date_array() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-05', INTERVAL 1 DAY) AS dates").await
        .unwrap();

    let records = result.to_records().unwrap();
    let arr = records[0].values()[0].as_array().unwrap();
    assert_eq!(arr.len(), 5);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_or() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE flags (grp INT64, flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO flags VALUES (1, true), (1, false), (2, false), (2, false), (3, NULL)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT grp, LOGICAL_OR(flag) AS any_true FROM flags GROUP BY grp ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, true], [2, false], [3, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_logical_and() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE flags (grp INT64, flag BOOL)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO flags VALUES (1, true), (1, true), (2, true), (2, false), (3, NULL)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT grp, LOGICAL_AND(flag) AS all_true FROM flags GROUP BY grp ORDER BY grp",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[1, true], [2, false], [3, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_select_as_struct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT ARRAY(SELECT AS STRUCT id, name FROM items WHERE id <= 2 ORDER BY id) AS arr",
        )
        .await
        .unwrap();

    eprintln!("Result: {:?}", result);
    let records = result.to_records().unwrap();
    eprintln!("Records: {:?}", records);
    eprintln!("First value: {:?}", records[0].values()[0]);
    let arr = records[0].values()[0].as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let s0 = arr[0].as_struct().unwrap();
    assert_eq!(s0[0].0, "id");
    assert_eq!(s0[0].1.as_i64().unwrap(), 1);
    assert_eq!(s0[1].0, "name");
    assert_eq!(s0[1].1.as_str().unwrap(), "Alice");
    let s1 = arr[1].as_struct().unwrap();
    assert_eq!(s1[0].1.as_i64().unwrap(), 2);
    assert_eq!(s1[1].1.as_str().unwrap(), "Bob");
}

#[tokio::test(flavor = "current_thread")]
async fn test_temp_function_basic() {
    let session = create_session();
    session
        .execute_sql("CREATE TEMP FUNCTION double_it(x INT64) AS (x * 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT double_it(5) AS doubled")
        .await
        .unwrap();

    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_temp_function_string() {
    let session = create_session();
    session
        .execute_sql("CREATE TEMP FUNCTION greet(name STRING) AS (CONCAT('Hello, ', name, '!'))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT greet('World') AS greeting")
        .await
        .unwrap();

    assert_table_eq!(result, [["Hello, World!"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_temp_function_complex() {
    let session = create_session();
    session
        .execute_sql(
            r#"CREATE TEMP FUNCTION to_uuid(s STRING)
            AS (
                CONCAT(SUBSTR(TO_HEX(SHA256(s)), 0, 8),
                       '-',
                       SUBSTR(TO_HEX(SHA256(s)), 8, 4),
                       '-',
                       SUBSTR(TO_HEX(SHA256(s)), 12, 4),
                       '-',
                       SUBSTR(TO_HEX(SHA256(s)), 16, 4),
                       '-',
                       SUBSTR(TO_HEX(SHA256(s)), 20, 12))
            )"#,
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT to_uuid('hello') = to_uuid('hello') AS same, to_uuid('hello') = to_uuid('world') AS diff").await
        .unwrap();

    assert_table_eq!(result, [[true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_temp_function_in_expression() {
    let session = create_session();
    session
        .execute_sql("CREATE TEMP FUNCTION add_one(x INT64) AS (x + 1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT add_one(add_one(5)) AS nested, add_one(10) + add_one(20) AS sum")
        .await
        .unwrap();

    assert_table_eq!(result, [[7, 32]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_union_with_nested_subqueries() {
    let session = create_session();

    session
        .execute_sql(
            r#"CREATE TABLE partner_data (
                user_id STRING,
                app_id STRING,
                app_type STRING,
                lddr_source STRING,
                premium FLOAT64,
                visit_time DATETIME,
                issued_time DATETIME
            )"#,
        )
        .await
        .unwrap();

    session
        .execute_sql(
            r#"INSERT INTO partner_data VALUES
                ('u1', 'a1', 'ladder', 'lincoln', 1000.0, DATETIME '2024-01-01 10:00:00', DATETIME '2024-01-15 10:00:00'),
                ('u2', 'a2', 'ladder', 'lincoln', 2000.0, DATETIME '2024-01-02 10:00:00', DATETIME '2024-01-20 10:00:00'),
                ('u3', 'a3', 'other', 'lincoln', 1500.0, DATETIME '2024-01-03 10:00:00', DATETIME '2024-01-25 10:00:00')
            "#,
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            r#"
            SELECT
              'type-a' AS rule_type,
              sub.user_id,
              sub.app_id,
              ROUND(CAST(1.2 * premium AS FLOAT64), 2) AS payment
            FROM (
                SELECT user_id, app_id, premium
                FROM partner_data
                WHERE visit_time <= issued_time
                  AND visit_time IS NOT NULL
                  AND IFNULL(lddr_source, '') = 'lincoln'
            ) sub
            WHERE app_id IN ('a1', 'a2')
            UNION ALL
            SELECT
              'type-b' AS rule_type,
              tbl.user_id,
              tbl.app_id,
              ROUND(CAST(300 AS FLOAT64), 2) AS payment
            FROM partner_data tbl
            INNER JOIN (
                SELECT user_id, ARRAY_AGG(app_id ORDER BY issued_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_app
                FROM partner_data
                WHERE issued_time IS NOT NULL
                GROUP BY user_id
            ) first_apps ON tbl.user_id = first_apps.user_id AND tbl.app_id = first_apps.first_app
            WHERE COALESCE(tbl.visit_time, tbl.issued_time) <= tbl.issued_time
            ORDER BY rule_type, user_id
            "#,
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["type-a", "u1", "a1", 1200.0],
            ["type-a", "u2", "a2", 2400.0],
            ["type-b", "u1", "a1", 300.0],
            ["type-b", "u2", "a2", 300.0],
            ["type-b", "u3", "a3", 300.0],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_struct_field_access() {
    let session = create_session();

    session
        .execute_sql(
            r#"CREATE TABLE attribution_data (
                user_id STRING,
                partner STRING,
                visit_time DATETIME,
                external_id STRING,
                medium STRING,
                source STRING,
                campaign STRING
            )"#,
        )
        .await
        .unwrap();

    session
        .execute_sql(
            r#"INSERT INTO attribution_data VALUES
                ('u1', 'partner_a', DATETIME '2024-01-01 10:00:00', 'ext1', 'web', 'google', 'campaign1'),
                ('u1', 'partner_a', DATETIME '2024-01-05 10:00:00', 'ext2', 'mobile', 'facebook', 'campaign2'),
                ('u2', 'partner_b', DATETIME '2024-01-02 10:00:00', 'ext3', 'web', 'bing', 'campaign3'),
                ('u3', 'partner_a', DATETIME '2024-01-03 10:00:00', 'ext4', 'app', 'direct', 'campaign4')
            "#,
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            r#"
            SELECT DISTINCT *
            FROM (
                SELECT
                    user_id,
                    partner,
                    external_id,
                    medium,
                    source,
                    campaign
                FROM attribution_data
                WHERE source IN ('google', 'direct')
                UNION ALL
                SELECT
                    user_id,
                    partner,
                    ARRAY_AGG(external_id IGNORE NULLS ORDER BY visit_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS external_id,
                    ARRAY_AGG(medium IGNORE NULLS ORDER BY visit_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS medium,
                    ARRAY_AGG(source IGNORE NULLS ORDER BY visit_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS source,
                    ARRAY_AGG(campaign IGNORE NULLS ORDER BY visit_time DESC LIMIT 1)[SAFE_OFFSET(0)] AS campaign
                FROM attribution_data
                WHERE source IN ('facebook', 'bing')
                GROUP BY user_id, partner
            )
            ORDER BY user_id, partner, external_id
            "#,
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["u1", "partner_a", "ext1", "web", "google", "campaign1"],
            ["u1", "partner_a", "ext2", "mobile", "facebook", "campaign2"],
            ["u2", "partner_b", "ext3", "web", "bing", "campaign3"],
            ["u3", "partner_a", "ext4", "app", "direct", "campaign4"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_ignore_nulls_limit_debug() {
    let session = create_session();

    session
        .execute_sql(
            r#"CREATE TABLE attr_debug (
                user_id STRING,
                partner STRING,
                visit_time DATETIME,
                external_id STRING,
                source STRING
            )"#,
        )
        .await
        .unwrap();

    session
        .execute_sql(
            r#"INSERT INTO attr_debug VALUES
                ('u1', 'partner_a', DATETIME '2024-01-05 10:00:00', 'ext2', 'facebook'),
                ('u2', 'partner_b', DATETIME '2024-01-02 10:00:00', 'ext3', 'bing')
            "#,
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"
            SELECT
                user_id,
                partner,
                ARRAY_AGG(external_id IGNORE NULLS ORDER BY visit_time ASC LIMIT 1) AS arr
            FROM attr_debug
            WHERE source IN ('facebook', 'bing')
            GROUP BY user_id, partner
            ORDER BY user_id
            "#,
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["u1", "partner_a", ["ext2"]], ["u2", "partner_b", ["ext3"]],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_ignore_nulls_limit_safe_offset() {
    let session = create_session();

    session
        .execute_sql(
            r#"CREATE TABLE attr_offset (
                user_id STRING,
                partner STRING,
                visit_time DATETIME,
                external_id STRING,
                source STRING
            )"#,
        )
        .await
        .unwrap();

    session
        .execute_sql(
            r#"INSERT INTO attr_offset VALUES
                ('u1', 'partner_a', DATETIME '2024-01-05 10:00:00', 'ext2', 'facebook'),
                ('u2', 'partner_b', DATETIME '2024-01-02 10:00:00', 'ext3', 'bing')
            "#,
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"
            SELECT
                user_id,
                partner,
                ARRAY_AGG(external_id IGNORE NULLS ORDER BY visit_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS external_id
            FROM attr_offset
            WHERE source IN ('facebook', 'bing')
            GROUP BY user_id, partner
            ORDER BY user_id
            "#,
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [["u1", "partner_a", "ext2"], ["u2", "partner_b", "ext3"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_agg_in_union_all() {
    let session = create_session();

    session
        .execute_sql(
            r#"CREATE TABLE attr_union (
                user_id STRING,
                partner STRING,
                visit_time DATETIME,
                external_id STRING,
                source STRING
            )"#,
        )
        .await
        .unwrap();

    session
        .execute_sql(
            r#"INSERT INTO attr_union VALUES
                ('u1', 'partner_a', DATETIME '2024-01-01 10:00:00', 'ext1', 'google'),
                ('u1', 'partner_a', DATETIME '2024-01-05 10:00:00', 'ext2', 'facebook'),
                ('u2', 'partner_b', DATETIME '2024-01-02 10:00:00', 'ext3', 'bing'),
                ('u3', 'partner_a', DATETIME '2024-01-03 10:00:00', 'ext4', 'direct')
            "#,
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"
            SELECT user_id, partner, external_id
            FROM attr_union
            WHERE source IN ('google', 'direct')
            UNION ALL
            SELECT
                user_id,
                partner,
                ARRAY_AGG(external_id IGNORE NULLS ORDER BY visit_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS external_id
            FROM attr_union
            WHERE source IN ('facebook', 'bing')
            GROUP BY user_id, partner
            ORDER BY user_id, external_id
            "#,
        ).await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["u1", "partner_a", "ext1"],
            ["u1", "partner_a", "ext2"],
            ["u2", "partner_b", "ext3"],
            ["u3", "partner_a", "ext4"],
        ]
    );
}

async fn setup_players_mascots(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE Players (username STRING, team STRING, level INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE Mascots (team STRING, mascot STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO Players VALUES ('gorbie', 'red', 29), ('junelyn', 'blue', 2), ('corba', 'green', 43)").await
        .unwrap();
    session
        .execute_sql("INSERT INTO Mascots VALUES ('red', 'cardinal'), ('blue', 'finch'), ('green', 'parrot')").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_subquery_in_select() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT
                username,
                (SELECT mascot FROM Mascots WHERE Players.team = Mascots.team) AS player_mascot
            FROM Players
            ORDER BY username",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["corba", "parrot"],
            ["gorbie", "cardinal"],
            ["junelyn", "finch"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_subquery_with_aggregate() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT
                username,
                level,
                CAST((SELECT AVG(level) FROM Players) AS FLOAT64) AS avg_level
            FROM Players
            ORDER BY username",
        )
        .await
        .unwrap();

    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 3);
    let avg = records[0].values()[2].as_f64().unwrap();
    assert!((avg - 24.666666).abs() < 0.01);
}

async fn setup_npcs(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE NPCs (username STRING, team STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO NPCs VALUES ('niles', 'red'), ('jujul', 'red'), ('kira', 'blue')")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_subquery_basic() {
    let session = create_session();
    setup_npcs(&session).await;

    let result = session
        .execute_sql(
            "SELECT ARRAY(SELECT username FROM NPCs WHERE team = 'red' ORDER BY username) AS red",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[["jujul", "niles"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_subquery_empty_result() {
    let session = create_session();
    setup_npcs(&session).await;

    let result = session
        .execute_sql("SELECT ARRAY(SELECT username FROM NPCs WHERE team = 'yellow') AS yellow")
        .await
        .unwrap();

    assert_table_eq!(result, [[[]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_subquery_with_filter() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE Products (id INT64, name STRING, category STRING, price FLOAT64)",
        )
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO Products VALUES (1, 'Laptop', 'Electronics', 999.99), (2, 'Phone', 'Electronics', 599.99), (3, 'Desk', 'Furniture', 299.99)").await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY(SELECT name FROM Products WHERE category = 'Electronics' ORDER BY price DESC) AS electronics").await
        .unwrap();

    assert_table_eq!(result, [[["Laptop", "Phone"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_subquery_returns_null_when_no_match() {
    let session = create_session();
    setup_players_mascots(&session).await;

    session
        .execute_sql("INSERT INTO Players VALUES ('nobody', 'yellow', 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
                username,
                (SELECT mascot FROM Mascots WHERE Players.team = Mascots.team) AS player_mascot
            FROM Players
            WHERE username = 'nobody'",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["nobody", null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_subquery_in_where_clause() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT username, level
            FROM Players
            WHERE level > (SELECT AVG(level) FROM Players)
            ORDER BY username",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["corba", 43], ["gorbie", 29],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_scalar_subquery_in_case() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT
                username,
                CASE
                    WHEN level > (SELECT AVG(level) FROM Players) THEN 'above average'
                    ELSE 'below average'
                END AS status
            FROM Players
            ORDER BY username",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["corba", "above average"],
            ["gorbie", "above average"],
            ["junelyn", "below average"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_array_subquery_with_limit() {
    let session = create_session();
    setup_npcs(&session).await;

    let result = session
        .execute_sql("SELECT ARRAY(SELECT username FROM NPCs ORDER BY username LIMIT 2) AS top2")
        .await
        .unwrap();

    assert_table_eq!(result, [[["jujul", "kira"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_subquery_exists() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql("SELECT 'corba' IN (SELECT username FROM Players) AS result")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_subquery_not_exists() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql("SELECT 'unknown' IN (SELECT username FROM Players) AS result")
        .await
        .unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_subquery() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql("SELECT 'unknown' NOT IN (SELECT username FROM Players) AS result")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_subquery_empty_result() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT 'corba' IN (SELECT username FROM Players WHERE team = 'yellow') AS result",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exists_subquery_true() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql("SELECT EXISTS(SELECT username FROM Players WHERE team = 'red') AS result")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exists_subquery_false() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql("SELECT EXISTS(SELECT username FROM Players WHERE team = 'yellow') AS result")
        .await
        .unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exists_subquery_multiple_columns() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT EXISTS(SELECT username, team, level FROM Players WHERE level > 20) AS result",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_subquery_basic() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql("SELECT results.username FROM (SELECT * FROM Players) AS results ORDER BY results.username").await
        .unwrap();

    assert_table_eq!(result, [["corba"], ["gorbie"], ["junelyn"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_subquery_with_filter() {
    let session = create_session();
    setup_npcs(&session).await;

    let result = session
        .execute_sql(
            "SELECT username
            FROM (SELECT * FROM NPCs WHERE team = 'red') AS red_team
            ORDER BY username",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["jujul"], ["niles"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_subquery_with_cte() {
    let session = create_session();
    setup_npcs(&session).await;

    let result = session
        .execute_sql(
            "SELECT username
            FROM (
                WITH red_team AS (SELECT * FROM NPCs WHERE team = 'red')
                SELECT * FROM red_team
            )
            ORDER BY username",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["jujul"], ["niles"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_subquery_with_column() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT username, team
            FROM Players
            WHERE team IN (SELECT team FROM Mascots WHERE mascot = 'cardinal')
            ORDER BY username",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["gorbie", "red"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_exists_subquery_in_select() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT NOT EXISTS(SELECT username FROM Players WHERE team = 'yellow') AS result",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_unnest_array_subquery() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql("SELECT 'corba' IN UNNEST(ARRAY(SELECT username FROM Players)) AS result")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_doc_example_expression_subquery_mascot() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT
              username,
              (SELECT mascot FROM Mascots WHERE Players.team = Mascots.team) AS player_mascot
            FROM
              Players
            ORDER BY username",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["corba", "parrot"],
            ["gorbie", "cardinal"],
            ["junelyn", "finch"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_doc_example_expression_subquery_avg() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT
              username,
              level,
              CAST((SELECT AVG(level) FROM Players) AS FLOAT64) AS avg_level
            FROM
              Players
            ORDER BY username",
        )
        .await
        .unwrap();

    let records = result.to_records().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].values()[0].as_str().unwrap(), "corba");
    assert_eq!(records[0].values()[1].as_i64().unwrap(), 43);
    let avg = records[0].values()[2].as_f64().unwrap();
    assert!((avg - 24.666666).abs() < 0.01);
}

#[tokio::test(flavor = "current_thread")]
async fn test_doc_example_array_subquery_red_team() {
    let session = create_session();
    setup_npcs(&session).await;

    let result = session
        .execute_sql(
            "SELECT
              ARRAY(SELECT username FROM NPCs WHERE team = 'red' ORDER BY username) AS red",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[["jujul", "niles"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_doc_example_in_subquery() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT
              'corba' IN (SELECT username FROM Players) AS result",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_doc_example_exists_subquery() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT
              EXISTS(SELECT username FROM Players WHERE team = 'yellow') AS result",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_doc_example_table_subquery() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT results.username
            FROM (SELECT * FROM Players) AS results
            ORDER BY results.username",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["corba"], ["gorbie"], ["junelyn"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_doc_example_table_subquery_with_cte() {
    let session = create_session();
    setup_npcs(&session).await;

    let result = session
        .execute_sql(
            "SELECT
              username
            FROM (
              WITH red_team AS (SELECT * FROM NPCs WHERE team = 'red')
              SELECT * FROM red_team
            )
            ORDER BY username",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["jujul"], ["niles"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_volatile_subquery_rand() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT
              results.username
            FROM
              (SELECT * FROM Players WHERE RAND() < 0.5) AS results
            ORDER BY results.username",
        )
        .await
        .unwrap();

    let records = result.to_records().unwrap();
    assert!(records.len() <= 3);
}

#[tokio::test(flavor = "current_thread")]
async fn test_volatile_subquery_rand_always_true() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT
              results.username
            FROM
              (SELECT * FROM Players WHERE RAND() < 1.0) AS results
            ORDER BY results.username",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["corba"], ["gorbie"], ["junelyn"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_volatile_subquery_rand_always_false() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT
              results.username
            FROM
              (SELECT * FROM Players WHERE RAND() < 0.0) AS results
            ORDER BY results.username",
        )
        .await
        .unwrap();

    let records = result.to_records().unwrap();
    assert!(records.is_empty());
}

async fn setup_players_mascots_with_sparrow(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE Players2 (username STRING, team STRING, level INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE Mascots2 (team STRING, mascot STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO Players2 VALUES ('gorbie', 'red', 29), ('junelyn', 'blue', 2), ('corba', 'green', 43)").await
        .unwrap();
    session
        .execute_sql("INSERT INTO Mascots2 VALUES ('red', 'cardinal'), ('blue', 'finch'), ('green', 'parrot'), ('yellow', 'sparrow')").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_correlated_subquery_not_exists_unassigned_mascot() {
    let session = create_session();
    setup_players_mascots_with_sparrow(&session).await;

    let result = session
        .execute_sql(
            "SELECT mascot
            FROM Mascots2
            WHERE
              NOT EXISTS(SELECT username FROM Players2 WHERE Mascots2.team = Players2.team)
            ORDER BY mascot",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["sparrow"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_correlated_scalar_subquery_player_mascot() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT
              username,
              (SELECT mascot FROM Mascots WHERE Players.team = Mascots.team) AS player_mascot
            FROM Players
            ORDER BY username",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["corba", "parrot"],
            ["gorbie", "cardinal"],
            ["junelyn", "finch"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_correlated_subquery_exists_with_condition() {
    let session = create_session();
    setup_players_mascots(&session).await;

    let result = session
        .execute_sql(
            "SELECT username
            FROM Players
            WHERE EXISTS(SELECT 1 FROM Mascots WHERE Players.team = Mascots.team AND mascot LIKE '%a%')
            ORDER BY username",
        ).await
        .unwrap();

    assert_table_eq!(result, [["corba"], ["gorbie"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_correlated_subquery_count() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE Orders (order_id INT64, customer_id INT64, amount FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE Customers (customer_id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO Customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO Orders VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
              name,
              (SELECT COUNT(*) FROM Orders WHERE Orders.customer_id = Customers.customer_id) AS order_count
            FROM Customers
            ORDER BY name",
        ).await
        .unwrap();

    assert_table_eq!(result, [["Alice", 2], ["Bob", 1], ["Charlie", 0],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_correlated_subquery_max() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE Sales (id INT64, product STRING, region STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE Regions (region STRING, manager STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO Regions VALUES ('East', 'Alice'), ('West', 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO Sales VALUES (1, 'Widget', 'East', 100), (2, 'Gadget', 'East', 200), (3, 'Widget', 'West', 150)").await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT
              manager,
              (SELECT MAX(amount) FROM Sales WHERE Sales.region = Regions.region) AS max_sale
            FROM Regions
            ORDER BY manager",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice", 200], ["Bob", 150],]);
}
