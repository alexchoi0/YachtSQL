use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_users_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
        )
        .unwrap();
}

#[test]
fn test_delete_single_row() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("DELETE FROM users WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_delete_multiple_rows() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("DELETE FROM users WHERE age > 25")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM users").unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_delete_all_rows() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor.execute_sql("DELETE FROM users").unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();

    assert_table_eq!(result, []);
}

#[test]
fn test_delete_with_and_condition() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("DELETE FROM users WHERE age > 25 AND age < 35")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_delete_with_or_condition() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("DELETE FROM users WHERE id = 1 OR id = 3")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM users").unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_delete_with_in_clause() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("DELETE FROM users WHERE id IN (1, 2)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM users").unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_delete_with_not_in_clause() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("DELETE FROM users WHERE id NOT IN (2)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM users").unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_delete_no_matching_rows() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("DELETE FROM users WHERE id = 999")
        .unwrap();

    let result = executor.execute_sql("SELECT COUNT(*) FROM users").unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_delete_with_subquery() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("CREATE TABLE to_delete (user_id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO to_delete VALUES (1), (3)")
        .unwrap();

    executor
        .execute_sql("DELETE FROM users WHERE id IN (SELECT user_id FROM to_delete)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM users").unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_delete_with_like_condition() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("DELETE FROM users WHERE name LIKE 'A%'")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_delete_with_is_null() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE nullable (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (1, 'a'), (2, NULL), (3, 'c')")
        .unwrap();

    executor
        .execute_sql("DELETE FROM nullable WHERE value IS NULL")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM nullable ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_delete_with_between() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor
        .execute_sql("DELETE FROM users WHERE age BETWEEN 26 AND 34")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_truncate_table() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    executor.execute_sql("TRUNCATE TABLE users").unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();

    assert_table_eq!(result, []);
}
