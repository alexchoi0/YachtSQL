use crate::assert_table_eq;
use crate::common::{create_executor, d, n, ts};

#[test]
fn test_safe_cast_string_to_int64_valid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('123' AS INT64)")
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[test]
fn test_safe_cast_string_to_int64_invalid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('abc' AS INT64)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_cast_string_to_float64_valid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('1.25' AS FLOAT64)")
        .unwrap();
    assert_table_eq!(result, [[1.25]]);
}

#[test]
fn test_safe_cast_string_to_float64_invalid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('not_a_number' AS FLOAT64)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_cast_string_to_date_valid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('2024-01-15' AS DATE)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 15)]]);
}

#[test]
fn test_safe_cast_string_to_date_invalid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('invalid-date' AS DATE)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_cast_string_to_timestamp_valid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('2024-01-15 10:30:00' AS TIMESTAMP)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 15, 10, 30, 0)]]);
}

#[test]
fn test_safe_cast_string_to_timestamp_invalid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('not-a-timestamp' AS TIMESTAMP)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_cast_int_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST(123 AS STRING)")
        .unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[test]
fn test_safe_cast_float_to_int64() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST(3.7 AS INT64)")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_safe_cast_overflow() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST(99999999999999999999 AS INT64)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_cast_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST(NULL AS INT64)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_cast_bool_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST(TRUE AS STRING)")
        .unwrap();
    assert_table_eq!(result, [["true"]]);
}

#[test]
fn test_safe_cast_string_to_bool_valid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('true' AS BOOL)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_safe_cast_string_to_bool_invalid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('maybe' AS BOOL)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_cast_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, '100'), (2, 'abc'), (3, '200')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM data WHERE SAFE_CAST(value AS INT64) > 50 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_safe_cast_coalesce_pattern() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT COALESCE(SAFE_CAST('abc' AS INT64), 0)")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_safe_cast_bytes_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST(b'hello' AS STRING)")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_cast_invalid_utf8_bytes() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST(b'\\xff\\xfe' AS STRING)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_divide_basic() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_DIVIDE(10.0, 2.0)")
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[test]
fn test_safe_divide_by_zero() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_DIVIDE(10.0, 0.0)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_divide_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_DIVIDE(NULL, 2.0)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_multiply_basic() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SAFE_MULTIPLY(5, 10)").unwrap();
    assert_table_eq!(result, [[50]]);
}

#[test]
fn test_safe_multiply_overflow() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_MULTIPLY(9223372036854775807, 2)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_add_basic() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SAFE_ADD(10, 20)").unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_safe_add_overflow() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_ADD(9223372036854775807, 1)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_subtract_basic() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_SUBTRACT(30, 10)")
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[test]
fn test_safe_subtract_overflow() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_SUBTRACT(-9223372036854775808, 1)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_negate_basic() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SAFE_NEGATE(10)").unwrap();
    assert_table_eq!(result, [[-10]]);
}

#[test]
fn test_safe_negate_min_int() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_NEGATE(-9223372036854775808)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_convert_bytes_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello world')")
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_convert_bytes_to_string_invalid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\\x80\\x81\\x82')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_in_aggregation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE values (val STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO values VALUES ('10'), ('20'), ('bad'), ('30')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(SAFE_CAST(val AS INT64)) FROM values")
        .unwrap();
    assert_table_eq!(result, [[60]]);
}

#[test]
fn test_safe_cast_numeric() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('123.456' AS NUMERIC)")
        .unwrap();
    assert_table_eq!(result, [[n("123.456")]]);
}

#[test]
fn test_safe_cast_bignumeric() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST('12345678901234567890.123456789' AS BIGNUMERIC)")
        .unwrap();
    assert_table_eq!(result, [[n("12345678901234567890.123456789")]]);
}

#[test]
fn test_safe_cast_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CAST(ARRAY[1, 2, 3] AS ARRAY<STRING>)")
        .unwrap();
    assert_table_eq!(result, [[["1", "2", "3"]]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_offset() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arr[SAFE_OFFSET(10)] FROM (SELECT ARRAY[1, 2, 3] AS arr)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_ordinal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arr[SAFE_ORDINAL(10)] FROM (SELECT ARRAY[1, 2, 3] AS arr)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}
