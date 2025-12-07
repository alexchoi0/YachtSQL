use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_between_integers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 5 BETWEEN 1 AND 10").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_between_integers_false() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 15 BETWEEN 1 AND 10").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_between_inclusive_lower() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 1 BETWEEN 1 AND 10").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_between_inclusive_upper() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 10 BETWEEN 1 AND 10").unwrap();
    assert_table_eq!(result, [[true]]);
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
fn test_not_between_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 5 NOT BETWEEN 1 AND 10")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_between_strings() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'banana' BETWEEN 'apple' AND 'cherry'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_between_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO scores VALUES ('Alice', 85), ('Bob', 92), ('Charlie', 78), ('Diana', 95)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM scores WHERE score BETWEEN 80 AND 90 ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_not_between_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO scores VALUES ('Alice', 85), ('Bob', 92), ('Charlie', 78), ('Diana', 95)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM scores WHERE score NOT BETWEEN 80 AND 90 ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Bob"], ["Charlie"], ["Diana"]]);
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
    executor
        .execute_sql("CREATE TABLE nums (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (5, 10), (15, 20), (25, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a FROM nums WHERE a BETWEEN 10 AND 20 ORDER BY a")
        .unwrap();
    assert_table_eq!(result, [[15]]);
}
