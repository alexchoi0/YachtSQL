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
        .execute_sql("SELECT 'b' BETWEEN 'a' AND 'c'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_between_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (5), (10), (15), (20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM numbers WHERE val BETWEEN 5 AND 15 ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[5], [10], [15]]);
}

#[test]
fn test_in_list_integers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 5 IN (1, 3, 5, 7)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_in_list_not_found() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 4 IN (1, 3, 5, 7)").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_not_in_list() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 4 NOT IN (1, 3, 5, 7)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_in_list_strings() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'apple' IN ('apple', 'banana', 'cherry')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_in_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fruits (name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fruits VALUES ('apple'), ('banana'), ('cherry'), ('date')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM fruits WHERE name IN ('apple', 'cherry') ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["apple"], ["cherry"]]);
}

#[test]
fn test_in_single_value() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 5 IN (5)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_between_symmetric() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 5 BETWEEN SYMMETRIC 10 AND 1")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
