use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_is_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT isNull(NULL)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_is_not_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT isNotNull(5)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_coalesce() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT coalesce(NULL, NULL, 'default')")
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[test]
fn test_coalesce_first_non_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT coalesce('first', 'second')")
        .unwrap();
    assert_table_eq!(result, [["first"]]);
}

#[test]
fn test_if_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ifNull(NULL, 'default')")
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[test]
fn test_if_null_not_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ifNull('value', 'default')")
        .unwrap();
    assert_table_eq!(result, [["value"]]);
}

#[test]
fn test_null_if() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT nullIf(5, 5)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_null_if_not_equal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT nullIf(5, 10)").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_assume_not_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT assumeNotNull(5)").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_nullable() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toNullable(5)").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_null_in() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT nullIn(1, [1, 2, NULL]")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_not_null_in() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT notNullIn(5, [1, 2, NULL]")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_null_in_ignore_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT nullInIgnoreNull(NULL, [1, 2, NULL]")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_is_zero_or_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT isZeroOrNull(0)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_is_zero_or_null_with_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT isZeroOrNull(NULL)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_is_zero_or_null_non_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT isZeroOrNull(5)").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_nullable_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nullable_test (id INT64, value Nullable(INT64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable_test VALUES (1, 100), (2, NULL), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, ifNull(value, 0) AS val FROM nullable_test ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 0], [3, 300]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_null_safe_equals() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NULL <=> NULL").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_null_safe_not_equals() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NULL <=> 5").unwrap();
    assert_table_eq!(result, [[false]]);
}
