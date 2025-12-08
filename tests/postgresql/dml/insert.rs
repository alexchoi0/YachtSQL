use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_simple_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE items (id INTEGER, name TEXT, quantity INTEGER)")
        .unwrap();
}

fn setup_table_with_defaults(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE orders (id INTEGER, status TEXT DEFAULT 'pending', amount INTEGER DEFAULT 0)")
        .unwrap();
}

fn setup_nullable_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE contacts (id INTEGER, email TEXT, phone TEXT)")
        .unwrap();
}

#[test]
fn test_insert_single_row() {
    let mut executor = create_executor();
    setup_simple_table(&mut executor);

    executor
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM items").unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[test]
fn test_insert_multiple_rows() {
    let mut executor = create_executor();
    setup_simple_table(&mut executor);

    executor
        .execute_sql(
            "INSERT INTO items VALUES (1, 'Widget', 100), (2, 'Gadget', 50), (3, 'Gizmo', 75)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id")
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Widget", 100], [2, "Gadget", 50], [3, "Gizmo", 75],]
    );
}

#[test]
fn test_insert_with_column_list() {
    let mut executor = create_executor();
    setup_simple_table(&mut executor);

    executor
        .execute_sql("INSERT INTO items (id, name, quantity) VALUES (1, 'Widget', 100)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM items").unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[test]
fn test_insert_with_reordered_columns() {
    let mut executor = create_executor();
    setup_simple_table(&mut executor);

    executor
        .execute_sql("INSERT INTO items (quantity, name, id) VALUES (100, 'Widget', 1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, quantity FROM items")
        .unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[test]
fn test_insert_partial_columns() {
    let mut executor = create_executor();
    setup_nullable_table(&mut executor);

    executor
        .execute_sql("INSERT INTO contacts (id, email) VALUES (1, 'test@example.com')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM contacts").unwrap();

    assert_table_eq!(result, [[1, "test@example.com", null]]);
}

#[test]
fn test_insert_with_null_value() {
    let mut executor = create_executor();
    setup_nullable_table(&mut executor);

    executor
        .execute_sql("INSERT INTO contacts VALUES (1, 'test@example.com', NULL)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM contacts").unwrap();

    assert_table_eq!(result, [[1, "test@example.com", null]]);
}

#[test]
fn test_insert_with_default_keyword() {
    let mut executor = create_executor();
    setup_table_with_defaults(&mut executor);

    executor
        .execute_sql("INSERT INTO orders (id, status, amount) VALUES (1, DEFAULT, DEFAULT)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM orders").unwrap();

    assert_table_eq!(result, [[1, "pending", 0]]);
}

#[test]
fn test_insert_default_values() {
    let mut executor = create_executor();
    setup_table_with_defaults(&mut executor);

    executor
        .execute_sql("INSERT INTO orders (id) VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM orders").unwrap();

    assert_table_eq!(result, [[1, "pending", 0]]);
}

#[test]
fn test_insert_with_expression() {
    let mut executor = create_executor();
    setup_simple_table(&mut executor);

    executor
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 50 + 50)")
        .unwrap();

    let result = executor.execute_sql("SELECT quantity FROM items").unwrap();

    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_insert_multiple_statements() {
    let mut executor = create_executor();
    setup_simple_table(&mut executor);

    executor
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (2, 'Gadget', 50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, "Widget", 100], [2, "Gadget", 50],]);
}

#[test]
fn test_insert_into_empty_table() {
    let mut executor = create_executor();
    setup_simple_table(&mut executor);

    let result_before = executor.execute_sql("SELECT COUNT(*) FROM items").unwrap();
    assert_table_eq!(result_before, [[0]]);

    executor
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100)")
        .unwrap();

    let result_after = executor.execute_sql("SELECT COUNT(*) FROM items").unwrap();
    assert_table_eq!(result_after, [[1]]);
}

#[test]
fn test_insert_returning() {
    let mut executor = create_executor();
    setup_simple_table(&mut executor);

    let result = executor
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100) RETURNING *")
        .unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[test]
fn test_insert_returning_specific_columns() {
    let mut executor = create_executor();
    setup_simple_table(&mut executor);

    let result = executor
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100) RETURNING id, name")
        .unwrap();

    assert_table_eq!(result, [[1, "Widget"]]);
}

#[test]
fn test_insert_returning_multiple_rows() {
    let mut executor = create_executor();
    setup_simple_table(&mut executor);

    let result = executor
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100), (2, 'Gadget', 50) RETURNING id")
        .unwrap();

    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_insert_with_subquery() {
    let mut executor = create_executor();
    setup_simple_table(&mut executor);

    executor
        .execute_sql("INSERT INTO items VALUES (1, 'Widget', 100), (2, 'Gadget', 50)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE items_copy (id INTEGER, name TEXT, quantity INTEGER)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO items_copy SELECT * FROM items WHERE quantity > 60")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM items_copy").unwrap();

    assert_table_eq!(result, [[1, "Widget", 100]]);
}

#[test]
fn test_insert_on_conflict_do_nothing() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE unique_items (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO unique_items VALUES (1, 'Widget')")
        .unwrap();

    executor
        .execute_sql("INSERT INTO unique_items VALUES (1, 'Different') ON CONFLICT DO NOTHING")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM unique_items WHERE id = 1")
        .unwrap();

    assert_table_eq!(result, [["Widget"]]);
}

#[test]
fn test_insert_on_conflict_do_update() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE upsert_items (id INTEGER PRIMARY KEY, name TEXT, count INTEGER)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO upsert_items VALUES (1, 'Widget', 10)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO upsert_items VALUES (1, 'Widget', 5) ON CONFLICT (id) DO UPDATE SET count = upsert_items.count + EXCLUDED.count")
        .unwrap();

    let result = executor
        .execute_sql("SELECT count FROM upsert_items WHERE id = 1")
        .unwrap();

    assert_table_eq!(result, [[15]]);
}
