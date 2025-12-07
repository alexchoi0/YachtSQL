use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_bytea_hex_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r"SELECT '\x48656c6c6f'::BYTEA")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytea_escape_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r"SELECT E'\\x48656c6c6f'::BYTEA")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_bytea_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE binary_data (id INT64, data BYTES)")
        .unwrap();
    executor
        .execute_sql(r"INSERT INTO binary_data VALUES (1, B'\x48656c6c6f')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM binary_data").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytea_length() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r"SELECT LENGTH(B'\x48656c6c6f')")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_bytea_concat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r"SELECT B'\x48656c' || B'\x6c6f'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytea_substring() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r"SELECT SUBSTRING(B'\x48656c6c6f', 1, 3)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytea_position() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r"SELECT POSITION(B'\x6c6c' IN B'\x48656c6c6f')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytea_octet_length() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r"SELECT OCTET_LENGTH(B'\x48656c6c6f')")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_bytea_comparison() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r"SELECT B'\x48656c6c6f' = B'\x48656c6c6f'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_bytea_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bin_null (id INT64, data BYTES)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bin_null VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT data FROM bin_null WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_encode_bytea_to_base64() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r"SELECT ENCODE(B'\x48656c6c6f', 'base64')")
        .unwrap();
    assert_table_eq!(result, [["SGVsbG8="]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_decode_base64_to_bytea() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DECODE('SGVsbG8=', 'base64')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_encode_bytea_to_hex() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(r"SELECT ENCODE(B'\x48656c6c6f', 'hex')")
        .unwrap();
    assert_table_eq!(result, [["48656c6c6f"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_decode_hex_to_bytea() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DECODE('48656c6c6f', 'hex')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}
