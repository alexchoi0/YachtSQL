use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_generate_uuid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GENERATE_UUID() IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_generate_uuid_uniqueness() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GENERATE_UUID() != GENERATE_UUID()")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_session_user() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SESSION_USER() IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_current_user() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CURRENT_USER() IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_format_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FORMAT('%s has %d items', 'cart', 5)")
        .unwrap();
    assert_table_eq!(result, [["cart has 5 items"]]);
}

#[test]
fn test_format_float() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FORMAT('%.2f', 3.12131)")
        .unwrap();
    assert_table_eq!(result, [["3.12"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_format_date() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FORMAT('%t', DATE '2024-01-15') IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_divide() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SAFE_DIVIDE(10, 2)").unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[test]
fn test_safe_divide_by_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SAFE_DIVIDE(10, 0)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_multiply() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_MULTIPLY(1000000000000, 1000000000000)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_negate() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_NEGATE(-9223372036854775808)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_add() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_ADD(9223372036854775807, 1)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_subtract() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_SUBTRACT(-9223372036854775808, 1)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_convert_bytes_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_error_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CASE WHEN 1 > 0 THEN 'ok' ELSE ERROR('should not happen') END")
        .unwrap();
    assert_table_eq!(result, [["ok"]]);
}

#[test]
fn test_ifnull() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IFNULL(NULL, 'default')")
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[test]
fn test_nullif() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NULLIF(5, 5)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_nullif_not_equal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NULLIF(5, 3)").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_uuid_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id STRING, name STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO items VALUES (GENERATE_UUID(), 'item1'), (GENERATE_UUID(), 'item2')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT id) FROM items")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}
