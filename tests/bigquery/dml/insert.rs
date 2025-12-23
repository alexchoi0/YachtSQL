use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

fn setup_simple_table(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING, quantity INT64)")
        .unwrap();
}

fn setup_table_with_defaults(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE orders (id INT64, status STRING DEFAULT 'pending', amount INT64 DEFAULT 0)")
        .unwrap();
}

fn setup_nullable_table(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE contacts (id INT64, email STRING, phone STRING)")
        .unwrap();
}

#[test]
fn test_insert_single_row() {
    let mut session = create_session();
    setup_simple_table(&mut session);

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM items").unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[test]
fn test_insert_multiple_rows() {
    let mut session = create_session();
    setup_simple_table(&mut session);

    session
        .execute_sql(
            "INSERT INTO items VALUES (1, 'Widget', 100), (2, 'Gadget', 50), (3, 'Gizmo', 75)",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM items ORDER BY id")
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Widget", 100], [2, "Gadget", 50], [3, "Gizmo", 75],]
    );
}

#[test]
fn test_insert_with_column_list() {
    let mut session = create_session();
    setup_simple_table(&mut session);

    session
        .execute_sql("INSERT INTO items (id, name, quantity) VALUES (1, 'Widget', 100)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM items").unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[test]
fn test_insert_with_reordered_columns() {
    let mut session = create_session();
    setup_simple_table(&mut session);

    session
        .execute_sql("INSERT INTO items (quantity, name, id) VALUES (100, 'Widget', 1)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, quantity FROM items")
        .unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[test]
fn test_insert_partial_columns() {
    let mut session = create_session();
    setup_nullable_table(&mut session);

    session
        .execute_sql("INSERT INTO contacts (id, email) VALUES (1, 'test@example.com')")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM contacts").unwrap();

    assert_table_eq!(result, [[1, "test@example.com", null]]);
}

#[test]
fn test_insert_with_null_value() {
    let mut session = create_session();
    setup_nullable_table(&mut session);

    session
        .execute_sql("INSERT INTO contacts VALUES (1, 'test@example.com', NULL)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM contacts").unwrap();

    assert_table_eq!(result, [[1, "test@example.com", null]]);
}

#[test]
fn test_insert_with_default_keyword() {
    let mut session = create_session();
    setup_table_with_defaults(&mut session);

    session
        .execute_sql("INSERT INTO orders (id, status, amount) VALUES (1, DEFAULT, DEFAULT)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM orders").unwrap();

    assert_table_eq!(result, [[1, "pending", 0]]);
}

#[test]
fn test_insert_default_values() {
    let mut session = create_session();
    setup_table_with_defaults(&mut session);

    session
        .execute_sql("INSERT INTO orders (id) VALUES (1)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM orders").unwrap();

    assert_table_eq!(result, [[1, "pending", 0]]);
}

#[test]
fn test_insert_with_expression() {
    let mut session = create_session();
    setup_simple_table(&mut session);

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 50 + 50)")
        .unwrap();

    let result = session.execute_sql("SELECT quantity FROM items").unwrap();

    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_insert_multiple_statements() {
    let mut session = create_session();
    setup_simple_table(&mut session);

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100)")
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (2, 'Gadget', 50)")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM items ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, "Widget", 100], [2, "Gadget", 50],]);
}

#[test]
fn test_insert_into_empty_table() {
    let mut session = create_session();
    setup_simple_table(&mut session);

    let result_before = session.execute_sql("SELECT COUNT(*) FROM items").unwrap();
    assert_table_eq!(result_before, [[0]]);

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100)")
        .unwrap();

    let result_after = session.execute_sql("SELECT COUNT(*) FROM items").unwrap();
    assert_table_eq!(result_after, [[1]]);
}

#[test]
fn test_insert_with_subquery() {
    let mut session = create_session();
    setup_simple_table(&mut session);

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100), (2, 'Gadget', 50)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE items_copy (id INT64, name STRING, quantity INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO items_copy SELECT * FROM items WHERE quantity > 60")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM items_copy").unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[test]
fn test_insert_with_expression_default() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE events (id INT64, name STRING, created_date DATE DEFAULT CURRENT_DATE())",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO events (id, name) VALUES (1, 'login')")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, created_date IS NOT NULL AS has_date FROM events")
        .unwrap();

    assert_table_eq!(result, [[1, "login", true]]);
}

#[test]
fn test_insert_with_struct_type() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE products (
                name STRING,
                specs STRUCT<color STRING, weight FLOAT64>
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO products VALUES ('Widget', STRUCT('red', 1.5))")
        .unwrap();

    let result = session
        .execute_sql("SELECT name, specs.color FROM products")
        .unwrap();

    assert_table_eq!(result, [["Widget", "red"]]);
}

#[test]
fn test_insert_with_array_type() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE with_tags (id INT64, tags ARRAY<STRING>)")
        .unwrap();

    session
        .execute_sql("INSERT INTO with_tags (tags) VALUES (['tag1', 'tag2'])")
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(tags) FROM with_tags")
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore]
fn test_insert_select_with_unnest() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE warehouse (warehouse STRING, state STRING)")
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO warehouse (warehouse, state)
            SELECT *
            FROM UNNEST([('warehouse #1', 'WA'),
                  ('warehouse #2', 'CA'),
                  ('warehouse #3', 'WA')])",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM warehouse ORDER BY warehouse")
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

#[test]
#[ignore]
fn test_insert_select_with_cte() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE warehouse (warehouse STRING, state STRING)")
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
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM warehouse ORDER BY warehouse")
        .unwrap();

    assert_table_eq!(result, [["warehouse #1", "WA"], ["warehouse #2", "CA"],]);
}

#[test]
#[ignore]
fn test_insert_with_nested_struct() {
    let mut session = create_session();

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
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory
            VALUES('washer', 10, ('white', '1 year', (30.0, 40.0, 28.0)))",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT product, specifications.color FROM detailed_inventory")
        .unwrap();

    assert_table_eq!(result, [["washer", "white"]]);
}

#[test]
fn test_insert_with_array_of_struct() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE with_comments (
                product STRING,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO with_comments
            VALUES('washer', [(DATE '2024-01-01', 'comment1'), (DATE '2024-01-02', 'comment2')])",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT product, ARRAY_LENGTH(comments) FROM with_comments")
        .unwrap();

    assert_table_eq!(result, [["washer", 2]]);
}

#[test]
#[ignore]
fn test_insert_values_with_subquery() {
    let mut session = create_session();
    setup_simple_table(&mut session);

    session
        .execute_sql("INSERT INTO items VALUES (1, 'microwave', 20)")
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO items (id, name, quantity)
            VALUES(2, 'countertop microwave',
              (SELECT quantity FROM items WHERE name = 'microwave'))",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT quantity FROM items WHERE id = 2")
        .unwrap();

    assert_table_eq!(result, [[20]]);
}

#[test]
fn test_insert_into_keyword() {
    let mut session = create_session();
    setup_simple_table(&mut session);

    session
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM items").unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[test]
fn test_insert_copy_table() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE inventory (product STRING, quantity INT64, supply_constrained BOOL)",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dishwasher', 30, false),
            ('dryer', 30, false),
            ('washer', 20, false)",
        )
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (product STRING, quantity INT64, supply_constrained BOOL)",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory (product, quantity, supply_constrained)
            SELECT product, quantity, false
            FROM inventory",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM detailed_inventory")
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore]
fn test_insert_with_range_type() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE employee_schedule (
                emp_id INT64,
                dept_id INT64,
                duration RANGE<DATE>
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO employee_schedule (emp_id, dept_id, duration)
            VALUES(10, 1000, RANGE<DATE> '[2010-01-10, 2010-03-10)'),
                  (10, 2000, RANGE<DATE> '[2010-03-10, 2010-07-15)'),
                  (20, 2000, RANGE<DATE> '[2010-03-10, 2010-07-20)'),
                  (20, 1000, RANGE<DATE> '[2020-05-10, 2020-09-20)')",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT emp_id, dept_id FROM employee_schedule ORDER BY emp_id, dept_id")
        .unwrap();

    assert_table_eq!(result, [[10, 1000], [10, 2000], [20, 1000], [20, 2000]]);
}
