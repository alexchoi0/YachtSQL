use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_column_alias_basic() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 1 + 2 AS result").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_column_alias_string() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'hello' AS greeting").unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_column_alias_without_as() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 1 + 2 result").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_multiple_column_aliases() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 1 AS a, 2 AS b, 3 AS c")
        .unwrap();
    assert_table_eq!(result, [[1, 2, 3]]);
}

#[test]
fn test_table_alias_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT u.name FROM users u ORDER BY u.id")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
fn test_table_alias_with_as() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT u.name FROM users AS u")
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_table_alias_in_join() {
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

    let result = executor.execute_sql("SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.name").unwrap();
    assert_table_eq!(result, [["Alice", 100], ["Bob", 200]]);
}

#[test]
fn test_alias_in_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (3), (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val AS v FROM items ORDER BY v")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_alias_in_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (category STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('A', 100), ('B', 200), ('A', 150)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category AS cat, SUM(amount) AS total FROM sales GROUP BY cat ORDER BY cat",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 250], ["B", 200]]);
}

#[test]
fn test_subquery_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sq.val FROM (SELECT val FROM nums) AS sq ORDER BY sq.val")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
fn test_expression_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (price INT64, qty INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (10, 5), (20, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT price * qty AS total FROM items ORDER BY total")
        .unwrap();
    assert_table_eq!(result, [[50], [60]]);
}

#[test]
fn test_quoted_alias() {
    let mut executor = create_executor();
    let result = executor.execute_sql(r#"SELECT 1 AS "My Result""#).unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_alias_with_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS average FROM nums")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.num_columns(), 3);
}
