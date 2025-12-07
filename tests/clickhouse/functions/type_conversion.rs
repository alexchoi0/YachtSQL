#![allow(clippy::approx_constant)]

use crate::assert_table_eq;
use crate::common::{create_executor, d, n, ts};

#[ignore = "Implement me!"]
#[test]
fn test_to_int8() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toInt8(127)").unwrap();
    assert_table_eq!(result, [[127]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_int16() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toInt16(32767)").unwrap();
    assert_table_eq!(result, [[32767]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_int32() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toInt32(2147483647)").unwrap();
    assert_table_eq!(result, [[2147483647]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_int64() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toInt64('123')").unwrap();
    assert_table_eq!(result, [[123]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_uint8() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toUInt8(255)").unwrap();
    assert_table_eq!(result, [[255]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_uint16() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toUInt16(65535)").unwrap();
    assert_table_eq!(result, [[65535]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_uint32() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toUInt32(4294967295i64)")
        .unwrap();
    assert_table_eq!(result, [[4294967295i64]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_uint64() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toUInt64('123')").unwrap();
    assert_table_eq!(result, [[123]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_float32() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toFloat32('3.14')").unwrap();
    assert_table_eq!(result, [[3.14]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_float64() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toFloat64('3.12131')").unwrap();
    assert_table_eq!(result, [[3.12131]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_string() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toString(123)").unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_fixed_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toFixedString('hello', 10)")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_date() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toDate('2024-01-15')").unwrap();
    assert_table_eq!(result, [[d(2024, 1, 15)]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_ts() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toDateTime('2024-01-15 10:30:00')")
        .unwrap();
    assert_table_eq!(result, [[(ts(2024, 1, 15, 10, 30, 0))]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_datetime64() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toDateTime64('2024-01-15 10:30:00.123', 3)")
        .unwrap();
    assert_table_eq!(result, [[(ts(2024, 1, 15, 10, 30, 0))]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_decimal32() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toDecimal32('123.45', 2)")
        .unwrap();
    assert_table_eq!(result, [[n("123.45")]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_decimal64() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toDecimal64('123.456789', 6)")
        .unwrap();
    assert_table_eq!(result, [[n("123.456789")]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_decimal128() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toDecimal128('123.456789012345', 12)")
        .unwrap();
    assert_table_eq!(result, [[n("123.456789012345")]]);
}

#[test]
fn test_cast_as_int() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CAST('123' AS Int64)").unwrap();
    assert_table_eq!(result, [[123]]);
}

#[test]
fn test_cast_as_float() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CAST('3.14' AS Float64)")
        .unwrap();
    assert_table_eq!(result, [[3.14]]);
}

#[test]
fn test_cast_as_string() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CAST(123 AS String)").unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_int64_or_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toInt64OrNull('invalid')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_int64_or_zero() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toInt64OrZero('invalid')")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_float64_or_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toFloat64OrNull('invalid')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_float64_or_zero() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toFloat64OrZero('invalid')")
        .unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_date_or_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toDateOrNull('invalid')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_datetime_or_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toDateTimeOrNull('invalid')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_reinterpret_as_int64() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT reinterpretAsInt64('12345678')")
        .unwrap();
    assert_table_eq!(result, [[3978425819141910832i64]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_reinterpret_as_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT reinterpretAsString(toInt64(0x68656C6C6F))")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_type_name() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toTypeName(123)").unwrap();
    assert_table_eq!(result, [["Int64"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_accurate_cast() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT accurateCast(123.456, 'Int64')")
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_accurate_cast_or_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT accurateCastOrNull(999999999999999999999, 'Int32')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_parse_ts() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT parseDateTime('2024-01-15 10:30:00', '%Y-%m-%d %H:%M:%S')")
        .unwrap();
    assert_table_eq!(result, [[(ts(2024, 1, 15, 10, 30, 0))]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_parse_datetime_best_effort() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT parseDateTimeBestEffort('Jan 15, 2024 10:30 AM')")
        .unwrap();
    assert_table_eq!(result, [[(ts(2024, 1, 15, 10, 30, 0))]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_parse_datetime_best_effort_or_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT parseDateTimeBestEffortOrNull('invalid date')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}
