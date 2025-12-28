use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_simple_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING, quantity INT64)")
        .await
        .unwrap();
}

async fn setup_table_with_defaults(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE orders (id INT64, status STRING DEFAULT 'pending', amount INT64 DEFAULT 0)").await
        .unwrap();
}

async fn setup_nullable_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE contacts (id INT64, email STRING, phone STRING)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_single_row() {
    let session = create_session();
    setup_simple_table(&session).await;

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM items").await.unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_multiple_rows() {
    let session = create_session();
    setup_simple_table(&session).await;

    session
        .execute_sql(
            "INSERT INTO items VALUES (1, 'Widget', 100), (2, 'Gadget', 50), (3, 'Gizmo', 75)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM items ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Widget", 100], [2, "Gadget", 50], [3, "Gizmo", 75],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_column_list() {
    let session = create_session();
    setup_simple_table(&session).await;

    session
        .execute_sql("INSERT INTO items (id, name, quantity) VALUES (1, 'Widget', 100)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM items").await.unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_reordered_columns() {
    let session = create_session();
    setup_simple_table(&session).await;

    session
        .execute_sql("INSERT INTO items (quantity, name, id) VALUES (100, 'Widget', 1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, quantity FROM items")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_partial_columns() {
    let session = create_session();
    setup_nullable_table(&session).await;

    session
        .execute_sql("INSERT INTO contacts (id, email) VALUES (1, 'test@example.com')")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM contacts").await.unwrap();

    assert_table_eq!(result, [[1, "test@example.com", null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_null_value() {
    let session = create_session();
    setup_nullable_table(&session).await;

    session
        .execute_sql("INSERT INTO contacts VALUES (1, 'test@example.com', NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM contacts").await.unwrap();

    assert_table_eq!(result, [[1, "test@example.com", null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_default_keyword() {
    let session = create_session();
    setup_table_with_defaults(&session).await;

    session
        .execute_sql("INSERT INTO orders (id, status, amount) VALUES (1, DEFAULT, DEFAULT)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM orders").await.unwrap();

    assert_table_eq!(result, [[1, "pending", 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_default_values() {
    let session = create_session();
    setup_table_with_defaults(&session).await;

    session
        .execute_sql("INSERT INTO orders (id) VALUES (1)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM orders").await.unwrap();

    assert_table_eq!(result, [[1, "pending", 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_expression() {
    let session = create_session();
    setup_simple_table(&session).await;

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 50 + 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT quantity FROM items")
        .await
        .unwrap();

    assert_table_eq!(result, [[100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_multiple_statements() {
    let session = create_session();
    setup_simple_table(&session).await;

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (2, 'Gadget', 50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM items ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Widget", 100], [2, "Gadget", 50],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_into_empty_table() {
    let session = create_session();
    setup_simple_table(&session).await;

    let result_before = session
        .execute_sql("SELECT COUNT(*) FROM items")
        .await
        .unwrap();
    assert_table_eq!(result_before, [[0]]);

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100)")
        .await
        .unwrap();

    let result_after = session
        .execute_sql("SELECT COUNT(*) FROM items")
        .await
        .unwrap();
    assert_table_eq!(result_after, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_subquery() {
    let session = create_session();
    setup_simple_table(&session).await;

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100), (2, 'Gadget', 50)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE items_copy (id INT64, name STRING, quantity INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO items_copy SELECT * FROM items WHERE quantity > 60")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM items_copy")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_expression_default() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE events (id INT64, name STRING, created_date DATE DEFAULT CURRENT_DATE())",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO events (id, name) VALUES (1, 'login')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, created_date IS NOT NULL AS has_date FROM events")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "login", true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_struct_type() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE products (
                name STRING,
                specs STRUCT<color STRING, weight FLOAT64>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO products VALUES ('Widget', STRUCT('red', 1.5))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, specs.color FROM products")
        .await
        .unwrap();

    assert_table_eq!(result, [["Widget", "red"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_array_type() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE with_tags (id INT64, tags ARRAY<STRING>)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO with_tags (tags) VALUES (['tag1', 'tag2'])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(tags) FROM with_tags")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_select_with_unnest_simple() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE numbers (n INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO numbers SELECT * FROM UNNEST([1, 2, 3]) AS n")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM numbers ORDER BY n")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_select_with_unnest() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE warehouse (warehouse STRING, state STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO warehouse (warehouse, state)
            SELECT *
            FROM UNNEST([('warehouse #1', 'WA'),
                  ('warehouse #2', 'CA'),
                  ('warehouse #3', 'WA')])",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM warehouse ORDER BY warehouse")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["warehouse #1", "WA"],
            ["warehouse #2", "CA"],
            ["warehouse #3", "WA"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_select_with_cte_simple() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE results (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "WITH src AS (SELECT 1 AS id, 100 AS value UNION ALL SELECT 2, 200)
             INSERT INTO results SELECT * FROM src",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM results ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_select_with_cte() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE warehouse (warehouse STRING, state STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO warehouse (warehouse, state)
            WITH w AS (
                SELECT ARRAY<STRUCT<warehouse STRING, state STRING>>
                    [('warehouse #1', 'WA'),
                     ('warehouse #2', 'CA')] col
            )
            SELECT warehouse, state FROM w, UNNEST(w.col)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM warehouse ORDER BY warehouse")
        .await
        .unwrap();

    assert_table_eq!(result, [["warehouse #1", "WA"], ["warehouse #2", "CA"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_nested_struct_simple() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE orders (
                id INT64,
                customer STRUCT<name STRING, address STRUCT<city STRING, zip STRING>>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO orders VALUES (
                1,
                STRUCT('John Doe', STRUCT('New York', '10001'))
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, customer FROM orders")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, {"John Doe", {"New York", "10001"}}]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_nested_struct() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                quantity INT64,
                specifications STRUCT<
                    color STRING,
                    warranty STRING,
                    dimensions STRUCT<depth FLOAT64, height FLOAT64, width FLOAT64>
                >
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory
            VALUES('washer', 10, ('white', '1 year', (30.0, 40.0, 28.0)))",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product, specifications.color FROM detailed_inventory")
        .await
        .unwrap();

    assert_table_eq!(result, [["washer", "white"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_array_of_struct() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE with_comments (
                product STRING,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO with_comments
            VALUES('washer', [(DATE '2024-01-01', 'comment1'), (DATE '2024-01-02', 'comment2')])",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product, ARRAY_LENGTH(comments) FROM with_comments")
        .await
        .unwrap();

    assert_table_eq!(result, [["washer", 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_values_with_subquery_simple() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE config (key STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO config VALUES ('max', 100)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE computed (id INT64, max_value INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO computed VALUES (1, (SELECT value FROM config WHERE key = 'max'))",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM computed").await.unwrap();

    assert_table_eq!(result, [[1, 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_values_with_subquery() {
    let session = create_session();
    setup_simple_table(&session).await;

    session
        .execute_sql("INSERT INTO items VALUES (1, 'microwave', 20)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO items (id, name, quantity)
            VALUES(2, 'countertop microwave',
              (SELECT quantity FROM items WHERE name = 'microwave'))",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT quantity FROM items WHERE id = 2")
        .await
        .unwrap();

    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_into_keyword() {
    let session = create_session();
    setup_simple_table(&session).await;

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM items").await.unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_copy_table() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE inventory (product STRING, quantity INT64, supply_constrained BOOL)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dishwasher', 30, false),
            ('dryer', 30, false),
            ('washer', 20, false)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (product STRING, quantity INT64, supply_constrained BOOL)",
        ).await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory (product, quantity, supply_constrained)
            SELECT product, quantity, false
            FROM inventory",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM detailed_inventory")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_range_function() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_START(RANGE(DATE '2024-01-01', DATE '2024-12-31'))")
        .await
        .unwrap();

    assert_table_eq!(result, [["2024-01-01"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_insert_with_range_type() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE employee_schedule (
                emp_id INT64,
                dept_id INT64,
                duration RANGE<DATE>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO employee_schedule (emp_id, dept_id, duration)
            VALUES(10, 1000, RANGE<DATE> '[2010-01-10, 2010-03-10)'),
                  (10, 2000, RANGE<DATE> '[2010-03-10, 2010-07-15)'),
                  (20, 2000, RANGE<DATE> '[2010-03-10, 2010-07-20)'),
                  (20, 1000, RANGE<DATE> '[2020-05-10, 2020-09-20)')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT emp_id, dept_id FROM employee_schedule ORDER BY emp_id, dept_id")
        .await
        .unwrap();

    assert_table_eq!(result, [[10, 1000], [10, 2000], [20, 1000], [20, 2000]]);
}
