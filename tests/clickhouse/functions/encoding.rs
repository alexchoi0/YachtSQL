use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_hex() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT hex('hello')").unwrap();
    assert_table_eq!(result, [["68656C6C6F"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_unhex() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT unhex('68656C6C6F')").unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_base64_encode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT base64Encode('hello')")
        .unwrap();
    assert_table_eq!(result, [["aGVsbG8="]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_base64_decode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT base64Decode('aGVsbG8=')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_try_base64_decode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tryBase64Decode('aGVsbG8=')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_base58_encode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT base58Encode('hello')")
        .unwrap();
    assert_table_eq!(result, [["Cn8eVZg"]]); // Base58 encoding of 'hello'
}

#[ignore = "Implement me!"]
#[test]
fn test_base58_decode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT base58Decode(base58Encode('hello'))")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_bin() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bin(255)").unwrap();
    assert_table_eq!(result, [["11111111"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_unbin() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT unbin('11111111')").unwrap();
    assert_table_eq!(result, [[255]]); // Binary 11111111 = 255
}

#[ignore = "Implement me!"]
#[test]
fn test_bit_shift_left() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitShiftLeft(1, 4)").unwrap();
    assert_table_eq!(result, [[16]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_bit_shift_right() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitShiftRight(16, 4)").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_bit_and() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitAnd(5, 3)").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_bit_or() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitOr(5, 3)").unwrap();
    assert_table_eq!(result, [[7]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_bit_xor() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitXor(5, 3)").unwrap();
    assert_table_eq!(result, [[6]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_bit_not() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitNot(toUInt8(0))").unwrap();
    assert_table_eq!(result, [[255]]); // ~0 = 255 for UInt8
}

#[ignore = "Implement me!"]
#[test]
fn test_bit_count() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitCount(7)").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_bit_test() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitTest(15, 0)").unwrap();
    assert_table_eq!(result, [[1]]); // 15 = 0b1111, bit 0 is set
}

#[ignore = "Implement me!"]
#[test]
fn test_bit_test_all() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitTestAll(15, 0, 1)").unwrap();
    assert_table_eq!(result, [[1]]); // 15 = 0b1111, bits 0 and 1 are set
}

#[ignore = "Implement me!"]
#[test]
fn test_bit_test_any() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bitTestAny(15, 4, 5)").unwrap();
    assert_table_eq!(result, [[0]]); // 15 = 0b1111, bits 4 and 5 are not set
}

#[ignore = "Implement me!"]
#[test]
fn test_char() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT char(104, 101, 108, 108, 111)")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_ascii() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ascii('h')").unwrap();
    assert_table_eq!(result, [[104]]);
}
