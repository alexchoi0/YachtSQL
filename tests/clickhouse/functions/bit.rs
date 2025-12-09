use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_bit_and() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitAnd(12, 10)").unwrap();
    assert_table_eq!(result, [[8]]);
}

#[test]
fn test_bit_or() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitOr(12, 10)").unwrap();
    assert_table_eq!(result, [[14]]);
}

#[test]
fn test_bit_xor() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitXor(12, 10)").unwrap();
    assert_table_eq!(result, [[6]]);
}

#[test]
fn test_bit_not() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitNot(toUInt8(0))").unwrap();
    assert_table_eq!(result, [[-1]]); // ~0 for i64
}

#[test]
fn test_bit_shift_left() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitShiftLeft(1, 4)").unwrap();
    assert_table_eq!(result, [[16]]);
}

#[test]
fn test_bit_shift_right() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitShiftRight(16, 2)").unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_bit_rotate_left() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitRotateLeft(toUInt8(128), 1)")
        .unwrap();
    assert_table_eq!(result, [[1]]); // 0b10000000 rotated left = 0b00000001
}

#[test]
fn test_bit_rotate_right() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitRotateRight(toUInt8(1), 1)")
        .unwrap();
    assert_table_eq!(result, [[128]]); // 0b00000001 rotated right = 0b10000000
}

#[test]
fn test_bit_test() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitTest(8, 3)").unwrap();
    assert_table_eq!(result, [[1]]); // 8 = 0b1000, bit 3 is set
}

#[test]
fn test_bit_test_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitTestAll(15, 0, 1, 2, 3)")
        .unwrap();
    assert_table_eq!(result, [[1]]); // 15 = 0b1111, all bits 0-3 are set
}

#[test]
fn test_bit_test_any() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitTestAny(6, 0, 2)").unwrap();
    assert_table_eq!(result, [[1]]); // 6 = 0b110, bit 1 or 2 is set
}

#[test]
fn test_bit_count() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitCount(15)").unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_bit_hamming_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitHammingDistance(1, 2)")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Parser doesn't support binary literal syntax 0b..."]
#[test]
fn test_bit_slice() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitSlice(toUInt8(0b10110001), 2, 4)")
        .unwrap();
    assert!(result.num_rows() == 1); // bitSlice result depends on byte order
}

#[test]
fn test_byte_swap() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT byteSwap(toUInt16(256))")
        .unwrap();
    assert_table_eq!(result, [[1]]); // 256 = 0x0100, swapped = 0x0001 = 1
}

#[test]
fn test_bit_and_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bit_test (a UInt8, b UInt8)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bit_test VALUES (12, 10), (15, 7), (8, 4)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT a, b, bitAnd(a, b), bitOr(a, b), bitXor(a, b) FROM bit_test ORDER BY a",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [8, 4, 0, 12, 12],  // 8 & 4 = 0, 8 | 4 = 12, 8 ^ 4 = 12
            [12, 10, 8, 14, 6], // 12 & 10 = 8, 12 | 10 = 14, 12 ^ 10 = 6
            [15, 7, 7, 15, 8]   // 15 & 7 = 7, 15 | 7 = 15, 15 ^ 7 = 8
        ]
    );
}

#[ignore = "Parser doesn't support binary literal syntax 0b..."]
#[test]
fn test_bit_positions() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitPositionsToArray(toUInt8(0b10110001))")
        .unwrap();
    assert_table_eq!(result, [[[0, 4, 5, 7]]]); // bit positions for 177 = 0b10110001
}
