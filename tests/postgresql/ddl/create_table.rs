use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_simple_table() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INTEGER, name TEXT)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_create_table_with_multiple_columns() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE products (id INTEGER, name TEXT, price DOUBLE PRECISION, in_stock BOOLEAN)",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Laptop', 999.99, true)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM products").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.num_columns(), 4);
}

#[test]
fn test_create_table_with_primary_key() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO items VALUES (1, 'Widget')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM items WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [[1, "Widget"]]);
}

#[test]
fn test_create_table_with_not_null() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE required (id INTEGER, name TEXT NOT NULL)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO required VALUES (1, 'Test')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM required").unwrap();
    assert_table_eq!(result, [[1, "Test"]]);
}

#[test]
fn test_create_table_with_default() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE defaults (id INTEGER, status TEXT DEFAULT 'pending')")
        .unwrap();

    executor
        .execute_sql("INSERT INTO defaults (id) VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM defaults").unwrap();
    assert_table_eq!(result, [[1, "pending"]]);
}

#[test]
fn test_create_table_with_unique() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE unique_emails (id INTEGER, email TEXT UNIQUE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO unique_emails VALUES (1, 'test@example.com')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM unique_emails").unwrap();
    assert_table_eq!(result, [[1, "test@example.com"]]);
}

#[test]
fn test_create_table_if_not_exists() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE test_table (id INTEGER)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE IF NOT EXISTS test_table (id INTEGER)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO test_table VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM test_table").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_with_check_constraint() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE positive (id INTEGER, value INTEGER CHECK (value > 0))")
        .unwrap();

    executor
        .execute_sql("INSERT INTO positive VALUES (1, 10)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM positive").unwrap();
    assert_table_eq!(result, [[1, 10]]);
}

#[test]
fn test_create_table_with_foreign_key() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO parent VALUES (1, 'Parent')")
        .unwrap();

    executor
        .execute_sql("INSERT INTO child VALUES (1, 1)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM child").unwrap();
    assert_table_eq!(result, [[1, 1]]);
}

#[test]
fn test_create_table_with_composite_primary_key() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE composite (a INTEGER, b INTEGER, c TEXT, PRIMARY KEY (a, b))")
        .unwrap();

    executor
        .execute_sql("INSERT INTO composite VALUES (1, 2, 'test')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM composite").unwrap();
    assert_table_eq!(result, [[1, 2, "test"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_create_table_as_select() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE source (id INTEGER, name TEXT)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO source VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE copy AS SELECT * FROM source WHERE id = 1")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM copy").unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}
