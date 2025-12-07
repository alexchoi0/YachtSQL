use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_between_integers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 5 BETWEEN 1 AND 10").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_between_lower_bound() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 1 BETWEEN 1 AND 10").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_between_upper_bound() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 10 BETWEEN 1 AND 10").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_between_outside_range() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 15 BETWEEN 1 AND 10").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_not_between() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 15 NOT BETWEEN 1 AND 10")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_between_strings() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'cat' BETWEEN 'apple' AND 'dog'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_between_in_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE scores (id INT64, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES (1, 50), (2, 75), (3, 90), (4, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM scores WHERE score BETWEEN 40 AND 80 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_not_between_in_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 5), (2, 15), (3, 25), (4, 35)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM data WHERE value NOT BETWEEN 10 AND 30 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [4]]);
}

#[test]
fn test_between_with_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NULL BETWEEN 1 AND 10")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_between_negative_numbers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT -5 BETWEEN -10 AND 0").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_between_with_expressions() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 10 BETWEEN 2 * 2 AND 5 * 3")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
