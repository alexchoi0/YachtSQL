use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_users_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
        )
        .unwrap();
}

#[test]
fn test_update_single_column() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("UPDATE users SET age = 31 WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT age FROM users WHERE id = 1")
        .unwrap();

    assert_table_eq!(result, [[31]]);
}

#[test]
fn test_update_multiple_columns() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("UPDATE users SET name = 'Alicia', age = 31 WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, age FROM users WHERE id = 1")
        .unwrap();

    assert_table_eq!(result, [["Alicia", 31]]);
}

#[test]
fn test_update_all_rows() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("UPDATE users SET age = age + 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, age FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, 31], [2, 26], [3, 36],]);
}

#[test]
fn test_update_with_expression() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("UPDATE users SET age = age * 2 WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT age FROM users WHERE id = 1")
        .unwrap();

    assert_table_eq!(result, [[60]]);
}

#[test]
fn test_update_with_multiple_conditions() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("UPDATE users SET age = 40 WHERE age > 25 AND age < 35")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, age FROM users WHERE age = 40")
        .unwrap();

    assert_table_eq!(result, [["Alice", 40]]);
}

#[test]
fn test_update_with_or_condition() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("UPDATE users SET age = 50 WHERE id = 1 OR id = 3")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, age FROM users WHERE age = 50 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, 50], [3, 50],]);
}

#[test]
fn test_update_set_null() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("UPDATE users SET name = NULL WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM users WHERE id = 1")
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_update_no_matching_rows() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("UPDATE users SET age = 100 WHERE id = 999")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM users WHERE age = 100")
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_update_returning() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("UPDATE users SET age = 31 WHERE id = 1 RETURNING *")
        .unwrap();

    assert_table_eq!(result, [[1, "Alice", 31]]);
}

#[test]
fn test_update_returning_specific_columns() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("UPDATE users SET age = 31 WHERE id = 1 RETURNING id, age")
        .unwrap();

    assert_table_eq!(result, [[1, 31]]);
}

#[test]
fn test_update_with_in_clause() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("UPDATE users SET age = 99 WHERE id IN (1, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, age FROM users WHERE age = 99 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, 99], [3, 99],]);
}

#[test]
fn test_update_with_like_condition() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("UPDATE users SET age = 0 WHERE name LIKE 'A%'")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, age FROM users WHERE age = 0")
        .unwrap();

    assert_table_eq!(result, [["Alice", 0]]);
}
