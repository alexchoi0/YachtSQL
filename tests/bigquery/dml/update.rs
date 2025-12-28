use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_users_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_single_column() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("UPDATE users SET age = 31 WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT age FROM users WHERE id = 1")
        .await
        .unwrap();

    assert_table_eq!(result, [[31]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_multiple_columns() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("UPDATE users SET name = 'Alicia', age = 31 WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, age FROM users WHERE id = 1")
        .await
        .unwrap();

    assert_table_eq!(result, [["Alicia", 31]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_all_rows() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("UPDATE users SET age = age + 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, age FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 31], [2, 26], [3, 36],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_expression() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("UPDATE users SET age = age * 2 WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT age FROM users WHERE id = 1")
        .await
        .unwrap();

    assert_table_eq!(result, [[60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_multiple_conditions() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("UPDATE users SET age = 40 WHERE age > 25 AND age < 35")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, age FROM users WHERE age = 40")
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice", 40]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_or_condition() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("UPDATE users SET age = 50 WHERE id = 1 OR id = 3")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, age FROM users WHERE age = 50 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 50], [3, 50],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_set_null() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("UPDATE users SET name = NULL WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM users WHERE id = 1")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_no_matching_rows() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("UPDATE users SET age = 100 WHERE id = 999")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM users WHERE age = 100")
        .await
        .unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_in_clause() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("UPDATE users SET age = 99 WHERE id IN (1, 3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, age FROM users WHERE age = 99 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 99], [3, 99],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_like_condition() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("UPDATE users SET age = 0 WHERE name LIKE 'A%'")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, age FROM users WHERE age = 0")
        .await
        .unwrap();

    assert_table_eq!(result, [["Alice", 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_from_clause() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE inventory (product STRING, quantity INT64, supply_constrained BOOL)",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, quantity INT64, warehouse STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dishwasher', 30, NULL),
            ('dryer', 30, NULL),
            ('washer', 20, NULL)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 200, 'warehouse #2'),
            ('washer', 100, 'warehouse #1')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE inventory i
            SET quantity = i.quantity + n.quantity,
                supply_constrained = false
            FROM new_arrivals n
            WHERE i.product = n.product",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product, quantity FROM inventory ORDER BY product")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["dishwasher", 30], ["dryer", 230], ["washer", 120]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_subquery_in_set() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE inventory (product STRING, quantity INT64, supply_constrained BOOL)",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, quantity INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dryer', 30, NULL),
            ('washer', 10, NULL)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 200),
            ('washer', 100)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE inventory
            SET quantity = quantity +
              (SELECT quantity FROM new_arrivals
               WHERE inventory.product = new_arrivals.product),
                supply_constrained = false
            WHERE product IN (SELECT product FROM new_arrivals)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product, quantity FROM inventory ORDER BY product")
        .await
        .unwrap();

    assert_table_eq!(result, [["dryer", 230], ["washer", 110]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_nested_struct_field() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                specifications STRUCT<color STRING, warranty STRING>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', STRUCT('blue', '6 months')),
            ('dryer', STRUCT('black', '6 months'))",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE detailed_inventory
            SET specifications.color = 'white',
                specifications.warranty = '1 year'
            WHERE product LIKE '%washer%'",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, specifications.color, specifications.warranty
            FROM detailed_inventory
            WHERE product = 'washer'",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["washer", "white", "1 year"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_entire_struct() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                specifications STRUCT<color STRING, warranty STRING>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', STRUCT('blue', '6 months'))",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE detailed_inventory
            SET specifications = STRUCT('white', '1 year')
            WHERE product = 'washer'",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT specifications.color FROM detailed_inventory")
        .await
        .unwrap();

    assert_table_eq!(result, [["white"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_default_keyword() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE inventory (
                product STRING,
                quantity INT64,
                supply_constrained BOOL DEFAULT TRUE
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('washer', 10, false),
            ('dryer', 20, false)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE inventory
            SET quantity = quantity - 10,
                supply_constrained = DEFAULT
            WHERE product LIKE '%washer%'",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, quantity, supply_constrained FROM inventory WHERE product = 'washer'",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["washer", 0, true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_array_append() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', []),
            ('dryer', [])",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE detailed_inventory
            SET comments = ARRAY_CONCAT(comments,
              [(DATE '2024-01-01', 'comment1')])
            WHERE product LIKE '%washer%'",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, ARRAY_LENGTH(comments) FROM detailed_inventory ORDER BY product",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["dryer", 0], ["washer", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_three_table_join() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE detailed_inventory (product STRING, supply_constrained BOOL)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, warehouse STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE warehouse (warehouse STRING, state STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('dryer', false),
            ('oven', false),
            ('washer', false)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 'warehouse #2'),
            ('oven', 'warehouse #3'),
            ('washer', 'warehouse #1')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO warehouse VALUES
            ('warehouse #1', 'WA'),
            ('warehouse #2', 'CA'),
            ('warehouse #3', 'WA')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE detailed_inventory
            SET supply_constrained = true
            FROM new_arrivals, warehouse
            WHERE detailed_inventory.product = new_arrivals.product AND
                  new_arrivals.warehouse = warehouse.warehouse AND
                  warehouse.state = 'WA'",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, supply_constrained
            FROM detailed_inventory
            WHERE supply_constrained = true
            ORDER BY product",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["oven", true], ["washer", true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_where_true() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("UPDATE users SET age = age + 1 WHERE true")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, age FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 31], [2, 26], [3, 36]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_join_on_clause() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE detailed_inventory (product STRING, supply_constrained BOOL)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, warehouse STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE warehouse (warehouse STRING, state STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('dryer', false),
            ('oven', false),
            ('washer', false)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('oven', 'warehouse #3'),
            ('washer', 'warehouse #1')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO warehouse VALUES
            ('warehouse #1', 'WA'),
            ('warehouse #3', 'WA')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE detailed_inventory
            SET supply_constrained = true
            FROM new_arrivals
            INNER JOIN warehouse
            ON new_arrivals.warehouse = warehouse.warehouse
            WHERE detailed_inventory.product = new_arrivals.product AND
                  warehouse.state = 'WA'",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product FROM detailed_inventory
            WHERE supply_constrained = true
            ORDER BY product",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["oven"], ["washer"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_repeated_records_with_array_subquery() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', []),
            ('dryer', [])",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE detailed_inventory
            SET comments = ARRAY(
              SELECT comment FROM UNNEST(comments) AS comment
              UNION ALL
              SELECT (DATE '2024-01-01', 'comment1')
            )
            WHERE product LIKE '%washer%'",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, ARRAY_LENGTH(comments) FROM detailed_inventory ORDER BY product",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["dryer", 0], ["washer", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_delete_from_repeated_records() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', [(DATE '2024-01-01', 'comment1'), (DATE '2024-01-02', 'comment2')]),
            ('dryer', [(DATE '2024-01-01', 'comment2')])",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE detailed_inventory
            SET comments = ARRAY(
              SELECT c FROM UNNEST(comments) AS c
              WHERE c.comment NOT LIKE '%comment2%'
            )
            WHERE true",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, ARRAY_LENGTH(comments) FROM detailed_inventory ORDER BY product",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["dryer", 0], ["washer", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_update_append_second_entry_to_repeated_records() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', [(DATE '2024-01-01', 'comment1')]),
            ('dryer', [])",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "UPDATE detailed_inventory
            SET comments = ARRAY(
              SELECT comment FROM UNNEST(comments) AS comment
              UNION ALL
              SELECT (DATE '2024-01-01', 'comment2')
            )
            WHERE true",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, ARRAY_LENGTH(comments) FROM detailed_inventory ORDER BY product",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["dryer", 1], ["washer", 2]]);
}
