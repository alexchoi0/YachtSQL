use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_bitwise_and() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 91 & 15").unwrap();
    assert_table_eq!(result, [[11]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bitwise_or() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 32 | 3").unwrap();
    assert_table_eq!(result, [[35]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bitwise_xor() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 17 # 5").unwrap();
    assert_table_eq!(result, [[20]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bitwise_not() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ~1").unwrap();
    assert_table_eq!(result, [[-2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bitwise_shift_left() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 1 << 4").unwrap();
    assert_table_eq!(result, [[16]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bitwise_shift_right() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 16 >> 2").unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_bit_string_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT B'101010'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_bit_string_hex() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT X'1FF'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_bit_varying() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bit_test (id INTEGER, flags BIT VARYING(8))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bit_test VALUES (1, B'10101010')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM bit_test").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bit_fixed() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_bit (id INTEGER, code BIT(8))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_bit VALUES (1, B'11111111')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM fixed_bit").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bit_and_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bits (val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bits VALUES (5), (7), (15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BIT_AND(val) FROM bits")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bit_or_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bits_or (val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bits_or VALUES (1), (2), (4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BIT_OR(val) FROM bits_or")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bit_xor_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bits_xor (val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bits_xor VALUES (1), (3), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BIT_XOR(val) FROM bits_xor")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bit_count() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT BIT_COUNT(B'10101010')")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_get_bit() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GET_BIT(B'10101010', 6)")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_bit() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SET_BIT(B'10101010', 0, 0)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_length_bit() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LENGTH(B'10101010')").unwrap();
    assert_table_eq!(result, [[8]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_octet_length_bit() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT OCTET_LENGTH(B'10101010')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_bit_string_concat() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT B'1010' || B'0101'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_bit_string_overlay() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT OVERLAY(B'11111111' PLACING B'0000' FROM 3 FOR 4)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_bit_string_position() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POSITION(B'0101' IN B'110101010')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_bit_string_substring() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SUBSTRING(B'11110000' FROM 3 FOR 4)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_bitwise_in_expression() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT (15 & 7) | 8").unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bitwise_with_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE flags (id INTEGER, perms INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO flags VALUES (1, 7), (2, 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, perms & 4 AS read FROM flags ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 4], [2, 4]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bitwise_shift_chain() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT (1 << 4) >> 2").unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bitwise_complex() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ((255 >> 4) & 15) | 240")
        .unwrap();
    assert_table_eq!(result, [[255]]);
}
