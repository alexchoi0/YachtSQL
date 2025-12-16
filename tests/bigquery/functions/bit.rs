use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_bit_count() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT BIT_COUNT(5)").unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_bit_count_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT BIT_COUNT(0)").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_bit_count_bytes() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT BIT_COUNT(b'\\xFF')").unwrap();
    assert_table_eq!(result, [[8]]);
}

#[test]
fn test_byte_length() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT BYTE_LENGTH('hello')").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_byte_length_unicode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT BYTE_LENGTH('日本語')")
        .unwrap();
    assert_table_eq!(result, [[9]]);
}

#[test]
fn test_byte_length_bytes() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT BYTE_LENGTH(b'\\x00\\x01\\x02')")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_bit_and_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bits (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bits VALUES (7), (3), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BIT_AND(val) FROM bits")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_bit_or_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bits (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bits VALUES (1), (2), (4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BIT_OR(val) FROM bits")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
fn test_bit_xor_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bits (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bits VALUES (5), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BIT_XOR(val) FROM bits")
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

#[test]
fn test_bit_operations_with_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE flags (category STRING, flag INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO flags VALUES ('A', 1), ('A', 2), ('A', 4), ('B', 3), ('B', 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, BIT_OR(flag) FROM flags GROUP BY category ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", 7], ["B", 7]]);
}

#[test]
fn test_bit_count_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT BIT_COUNT(NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}
