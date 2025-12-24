use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

fn setup_users_table(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
        )
        .unwrap();
}

#[test]
fn test_update_single_column() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("UPDATE users SET age = 31 WHERE id = 1")
        .unwrap();

    let result = session
        .execute_sql("SELECT age FROM users WHERE id = 1")
        .unwrap();

    assert_table_eq!(result, [[31]]);
}

#[test]
fn test_update_multiple_columns() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("UPDATE users SET name = 'Alicia', age = 31 WHERE id = 1")
        .unwrap();

    let result = session
        .execute_sql("SELECT name, age FROM users WHERE id = 1")
        .unwrap();

    assert_table_eq!(result, [["Alicia", 31]]);
}

#[test]
fn test_update_all_rows() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("UPDATE users SET age = age + 1")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, age FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, 31], [2, 26], [3, 36],]);
}

#[test]
fn test_update_with_expression() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("UPDATE users SET age = age * 2 WHERE id = 1")
        .unwrap();

    let result = session
        .execute_sql("SELECT age FROM users WHERE id = 1")
        .unwrap();

    assert_table_eq!(result, [[60]]);
}

#[test]
fn test_update_with_multiple_conditions() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("UPDATE users SET age = 40 WHERE age > 25 AND age < 35")
        .unwrap();

    let result = session
        .execute_sql("SELECT name, age FROM users WHERE age = 40")
        .unwrap();

    assert_table_eq!(result, [["Alice", 40]]);
}

#[test]
fn test_update_with_or_condition() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("UPDATE users SET age = 50 WHERE id = 1 OR id = 3")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, age FROM users WHERE age = 50 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, 50], [3, 50],]);
}

#[test]
fn test_update_set_null() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("UPDATE users SET name = NULL WHERE id = 1")
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM users WHERE id = 1")
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_update_no_matching_rows() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("UPDATE users SET age = 100 WHERE id = 999")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM users WHERE age = 100")
        .unwrap();

    assert_table_eq!(result, []);
}

#[test]
fn test_update_with_in_clause() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("UPDATE users SET age = 99 WHERE id IN (1, 3)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, age FROM users WHERE age = 99 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, 99], [3, 99],]);
}

#[test]
fn test_update_with_like_condition() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("UPDATE users SET age = 0 WHERE name LIKE 'A%'")
        .unwrap();

    let result = session
        .execute_sql("SELECT name, age FROM users WHERE age = 0")
        .unwrap();

    assert_table_eq!(result, [["Alice", 0]]);
}

#[test]
fn test_update_with_from_clause() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE inventory (product STRING, quantity INT64, supply_constrained BOOL)",
        )
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, quantity INT64, warehouse STRING)")
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dishwasher', 30, NULL),
            ('dryer', 30, NULL),
            ('washer', 20, NULL)",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 200, 'warehouse #2'),
            ('washer', 100, 'warehouse #1')",
        )
        .unwrap();

    session
        .execute_sql(
            "UPDATE inventory i
            SET quantity = i.quantity + n.quantity,
                supply_constrained = false
            FROM new_arrivals n
            WHERE i.product = n.product",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT product, quantity FROM inventory ORDER BY product")
        .unwrap();

    assert_table_eq!(
        result,
        [["dishwasher", 30], ["dryer", 230], ["washer", 120]]
    );
}

#[test]
#[ignore]
fn test_update_with_subquery_in_set() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE inventory (product STRING, quantity INT64, supply_constrained BOOL)",
        )
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, quantity INT64)")
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dryer', 30, NULL),
            ('washer', 10, NULL)",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 200),
            ('washer', 100)",
        )
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
        .unwrap();

    let result = session
        .execute_sql("SELECT product, quantity FROM inventory ORDER BY product")
        .unwrap();

    assert_table_eq!(result, [["dryer", 230], ["washer", 110]]);
}

#[test]
#[ignore]
fn test_update_nested_struct_field() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                specifications STRUCT<color STRING, warranty STRING>
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', STRUCT('blue', '6 months')),
            ('dryer', STRUCT('black', '6 months'))",
        )
        .unwrap();

    session
        .execute_sql(
            "UPDATE detailed_inventory
            SET specifications.color = 'white',
                specifications.warranty = '1 year'
            WHERE product LIKE '%washer%'",
        )
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, specifications.color, specifications.warranty
            FROM detailed_inventory
            WHERE product = 'washer'",
        )
        .unwrap();

    assert_table_eq!(result, [["washer", "white", "1 year"]]);
}

#[test]
#[ignore]
fn test_update_entire_struct() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                specifications STRUCT<color STRING, warranty STRING>
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', STRUCT('blue', '6 months'))",
        )
        .unwrap();

    session
        .execute_sql(
            "UPDATE detailed_inventory
            SET specifications = STRUCT('white', '1 year')
            WHERE product = 'washer'",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT specifications.color FROM detailed_inventory")
        .unwrap();

    assert_table_eq!(result, [["white"]]);
}

#[test]
#[ignore]
fn test_update_with_default_keyword() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE inventory (
                product STRING,
                quantity INT64,
                supply_constrained BOOL DEFAULT TRUE
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('washer', 10, false),
            ('dryer', 20, false)",
        )
        .unwrap();

    session
        .execute_sql(
            "UPDATE inventory
            SET quantity = quantity - 10,
                supply_constrained = DEFAULT
            WHERE product LIKE '%washer%'",
        )
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, quantity, supply_constrained FROM inventory WHERE product = 'washer'",
        )
        .unwrap();

    assert_table_eq!(result, [["washer", 0, true]]);
}

#[test]
fn test_update_array_append() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', []),
            ('dryer', [])",
        )
        .unwrap();

    session
        .execute_sql(
            "UPDATE detailed_inventory
            SET comments = ARRAY_CONCAT(comments,
              [(DATE '2024-01-01', 'comment1')])
            WHERE product LIKE '%washer%'",
        )
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, ARRAY_LENGTH(comments) FROM detailed_inventory ORDER BY product",
        )
        .unwrap();

    assert_table_eq!(result, [["dryer", 0], ["washer", 1]]);
}

#[test]
fn test_update_with_three_table_join() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE detailed_inventory (product STRING, supply_constrained BOOL)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, warehouse STRING)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE warehouse (warehouse STRING, state STRING)")
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('dryer', false),
            ('oven', false),
            ('washer', false)",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 'warehouse #2'),
            ('oven', 'warehouse #3'),
            ('washer', 'warehouse #1')",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO warehouse VALUES
            ('warehouse #1', 'WA'),
            ('warehouse #2', 'CA'),
            ('warehouse #3', 'WA')",
        )
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
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, supply_constrained
            FROM detailed_inventory
            WHERE supply_constrained = true
            ORDER BY product",
        )
        .unwrap();

    assert_table_eq!(result, [["oven", true], ["washer", true]]);
}

#[test]
fn test_update_where_true() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("UPDATE users SET age = age + 1 WHERE true")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, age FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, 31], [2, 26], [3, 36]]);
}

#[test]
fn test_update_with_join_on_clause() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE detailed_inventory (product STRING, supply_constrained BOOL)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, warehouse STRING)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE warehouse (warehouse STRING, state STRING)")
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('dryer', false),
            ('oven', false),
            ('washer', false)",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('oven', 'warehouse #3'),
            ('washer', 'warehouse #1')",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO warehouse VALUES
            ('warehouse #1', 'WA'),
            ('warehouse #3', 'WA')",
        )
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
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product FROM detailed_inventory
            WHERE supply_constrained = true
            ORDER BY product",
        )
        .unwrap();

    assert_table_eq!(result, [["oven"], ["washer"]]);
}

#[test]
#[ignore]
fn test_update_repeated_records_with_array_subquery() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', []),
            ('dryer', [])",
        )
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
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, ARRAY_LENGTH(comments) FROM detailed_inventory ORDER BY product",
        )
        .unwrap();

    assert_table_eq!(result, [["dryer", 0], ["washer", 1]]);
}

#[test]
#[ignore]
fn test_update_delete_from_repeated_records() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', [(DATE '2024-01-01', 'comment1'), (DATE '2024-01-02', 'comment2')]),
            ('dryer', [(DATE '2024-01-01', 'comment2')])",
        )
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
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, ARRAY_LENGTH(comments) FROM detailed_inventory ORDER BY product",
        )
        .unwrap();

    assert_table_eq!(result, [["dryer", 0], ["washer", 1]]);
}

#[test]
#[ignore]
fn test_update_append_second_entry_to_repeated_records() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('washer', [(DATE '2024-01-01', 'comment1')]),
            ('dryer', [])",
        )
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
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, ARRAY_LENGTH(comments) FROM detailed_inventory ORDER BY product",
        )
        .unwrap();

    assert_table_eq!(result, [["dryer", 1], ["washer", 2]]);
}
