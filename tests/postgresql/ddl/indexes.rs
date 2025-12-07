use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_index_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_users_name ON users (name)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM users ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
fn test_create_index_multiple_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64, status STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_orders_user_status ON orders (user_id, status)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 'pending'), (2, 1, 'complete')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM orders WHERE user_id = 1 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_create_unique_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, email STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX idx_users_email ON users (email)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'alice@example.com')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM users").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_drop_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_items_name ON items (name)")
        .unwrap();
    executor.execute_sql("DROP INDEX idx_items_name").unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, 'test')")
        .unwrap();

    let result = executor.execute_sql("SELECT name FROM items").unwrap();
    assert_table_eq!(result, [["test"]]);
}

#[test]
fn test_drop_index_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id INT64)")
        .unwrap();
    executor
        .execute_sql("DROP INDEX IF EXISTS nonexistent_index")
        .unwrap();

    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_index_if_not_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_name ON users (name)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX IF NOT EXISTS idx_name ON users (name)")
        .unwrap();

    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_index_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, status STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_pending ON orders (id) WHERE status = 'pending'")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 'pending'), (2, 'complete')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM orders WHERE status = 'pending'")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_index_desc() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_value_desc ON items (value DESC)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, 100), (2, 50), (3, 75)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM items ORDER BY value DESC")
        .unwrap();
    assert_table_eq!(result, [[1], [3], [2]]);
}

#[test]
fn test_create_index_nulls_first() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_value ON items (value NULLS FIRST)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, NULL), (2, 100), (3, 50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM items ORDER BY value NULLS FIRST")
        .unwrap();
    assert_table_eq!(result, [[1], [3], [2]]);
}
