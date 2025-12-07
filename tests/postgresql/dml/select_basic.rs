use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::{create_executor, numeric};

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

fn setup_products_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql(
            "CREATE TABLE products (id INT64, name STRING, price FLOAT64, in_stock BOOLEAN)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Laptop', 999.99, true), (2, 'Mouse', 29.99, true), (3, 'Keyboard', 79.99, false)")
        .unwrap();
}

fn setup_nullable_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE nullable_data (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable_data VALUES (1, 'a'), (2, NULL), (3, 'c')")
        .unwrap();
}

#[test]
fn test_select_all_columns() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Alice", 30], [2, "Bob", 25], [3, "Charlie", 35],]
    );
}

#[test]
fn test_select_specific_columns() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT name, age FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [["Alice", 30], ["Bob", 25], ["Charlie", 35],]);
}

#[test]
fn test_select_single_column() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"]]);
}

#[test]
fn test_select_with_column_alias() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT name AS user_name, age AS user_age FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [["Alice", 30], ["Bob", 25], ["Charlie", 35],]);
}

#[test]
fn test_select_with_equality_filter() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, name FROM users WHERE age = 30")
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_select_with_greater_than_filter() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, name FROM users WHERE age > 25 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"], [3, "Charlie"],]);
}

#[test]
fn test_select_with_less_than_filter() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, name FROM users WHERE age < 30 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2, "Bob"]]);
}

#[test]
fn test_select_with_not_equal_filter() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM users WHERE age != 30 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_select_with_string_filter() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, age FROM users WHERE name = 'Bob'")
        .unwrap();

    assert_table_eq!(result, [[2, 25]]);
}

#[test]
fn test_select_with_and_condition() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, name FROM users WHERE age > 25 AND age < 35")
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_select_with_or_condition() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM users WHERE age = 25 OR age = 35 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_select_with_boolean_column() {
    let mut executor = create_executor();
    setup_products_table(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM products WHERE in_stock = true ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [["Laptop"], ["Mouse"]]);
}

#[test]
fn test_select_with_float_comparison() {
    let mut executor = create_executor();
    setup_products_table(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM products WHERE price > 50.0 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [["Laptop"], ["Keyboard"]]);
}

#[test]
fn test_select_with_null_check() {
    let mut executor = create_executor();
    setup_nullable_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM nullable_data WHERE value IS NULL")
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_select_with_not_null_check() {
    let mut executor = create_executor();
    setup_nullable_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM nullable_data WHERE value IS NOT NULL ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_select_from_empty_table() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, name STRING)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM empty_table").unwrap();

    assert_table_eq!(result, []);
}

#[test]
#[ignore = "Implement me!"]
fn test_select_with_no_matching_rows() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM users WHERE age > 100")
        .unwrap();

    assert_table_eq!(result, []);
}

#[test]
fn test_select_literal_values() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 1, 'hello', 3.14, true")
        .unwrap();

    assert_table_eq!(result, [[1, "hello", (numeric("3.14")), true]]);
}

#[test]
fn test_select_null_literal() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT NULL").unwrap();

    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_select_expression() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, age + 10 AS age_plus_ten FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, 40], [2, 35], [3, 45],]);
}

#[test]
fn test_select_with_table_alias() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT u.id, u.name FROM users u ORDER BY u.id")
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"], [3, "Charlie"],]);
}

#[test]
fn test_select_with_qualified_column() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT users.id, users.name FROM users ORDER BY users.id")
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"], [3, "Charlie"],]);
}
