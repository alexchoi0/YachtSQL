use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_alter_table_add_column() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    executor
        .execute_sql("ALTER TABLE users ADD COLUMN age INT64")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, "Alice", null], [2, "Bob", 30],]);
}

#[test]
fn test_alter_table_drop_column() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        .unwrap();

    executor
        .execute_sql("ALTER TABLE users DROP COLUMN age")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_alter_table_rename_column() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    executor
        .execute_sql("ALTER TABLE users RENAME COLUMN name TO full_name")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, full_name FROM users")
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_alter_table_rename_table() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE old_name (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO old_name VALUES (1, 'Alice')")
        .unwrap();

    executor
        .execute_sql("ALTER TABLE old_name RENAME TO new_name")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM new_name").unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_alter_table_add_column_with_default() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    executor
        .execute_sql("ALTER TABLE users ADD COLUMN status STRING DEFAULT 'active'")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();

    assert_table_eq!(result, [[1, "Alice", "active"]]);
}

#[test]
fn test_alter_table_add_constraint() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INT64, email STRING)")
        .unwrap();

    executor
        .execute_sql("ALTER TABLE users ADD CONSTRAINT unique_email UNIQUE (email)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'test@example.com')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();

    assert_table_eq!(result, [[1, "test@example.com"]]);
}

#[test]
fn test_alter_table_set_not_null() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("ALTER TABLE users ALTER COLUMN name SET NOT NULL")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_alter_table_drop_not_null() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING NOT NULL)")
        .unwrap();

    executor
        .execute_sql("ALTER TABLE users ALTER COLUMN name DROP NOT NULL")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, NULL)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();
    assert_table_eq!(result, [[1, null]]);
}
