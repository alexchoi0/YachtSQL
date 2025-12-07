use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_view_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, active BOOL)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO users VALUES (1, 'Alice', TRUE), (2, 'Bob', FALSE), (3, 'Charlie', TRUE)",
        )
        .unwrap();
    executor
        .execute_sql("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = TRUE")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM active_users ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Charlie"]]);
}

#[test]
fn test_create_view_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64, amount INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 2, 200)")
        .unwrap();
    executor.execute_sql("CREATE VIEW order_details AS SELECT o.id, u.name, o.amount FROM orders o JOIN users u ON o.user_id = u.id").unwrap();

    let result = executor
        .execute_sql("SELECT name, amount FROM order_details ORDER BY amount")
        .unwrap();
    assert_table_eq!(result, [["Alice", 100], ["Bob", 200]]);
}

#[test]
fn test_drop_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW items_view AS SELECT * FROM items")
        .unwrap();
    executor.execute_sql("DROP VIEW items_view").unwrap();

    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_drop_view_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP VIEW IF EXISTS nonexistent_view")
        .unwrap();

    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_or_replace_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, 'A'), (2, 'B')")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW items_view AS SELECT id FROM items")
        .unwrap();
    executor
        .execute_sql("CREATE OR REPLACE VIEW items_view AS SELECT id, name FROM items")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM items_view ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [["A"], ["B"]]);
}

#[test]
fn test_view_with_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (product STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('A', 100), ('B', 200), ('A', 150)")
        .unwrap();
    executor.execute_sql("CREATE VIEW sales_summary AS SELECT product, SUM(amount) AS total FROM sales GROUP BY product").unwrap();

    let result = executor
        .execute_sql("SELECT product, total FROM sales_summary ORDER BY product")
        .unwrap();
    assert_table_eq!(result, [["A", 250], ["B", 200]]);
}

#[test]
fn test_view_with_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (1), (2), (3), (4), (5)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE VIEW above_avg AS SELECT val FROM nums WHERE val > (SELECT AVG(val) FROM nums)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM above_avg ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[4], [5]]);
}

#[test]
fn test_nested_views() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id INT64, category STRING, price INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW items_a AS SELECT * FROM items WHERE category = 'A'")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW expensive_a AS SELECT * FROM items_a WHERE price > 100")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM expensive_a").unwrap();
    assert_table_eq!(result, [[3]]);
}
