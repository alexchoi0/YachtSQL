#![allow(clippy::approx_constant)]

use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_round() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT round(3.12131)").unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[test]
fn test_round_precision() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT round(3.12131, 2)").unwrap();
    assert_table_eq!(result, [[3.12]]);
}

#[test]
fn test_round_negative_precision() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT round(12345, -2)").unwrap();
    assert_table_eq!(result, [[12300.0]]); // Round to nearest 100
}

#[ignore = "Implement me!"]
#[test]
fn test_round_bankers() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT roundBankers(2.5), roundBankers(3.5)")
        .unwrap();
    assert_table_eq!(result, [[2.0, 4.0]]); // Banker's rounding: 2.5->2, 3.5->4
}

#[ignore = "Implement me!"]
#[test]
fn test_round_bankers_precision() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT roundBankers(3.145, 2), roundBankers(3.155, 2)")
        .unwrap();
    assert_table_eq!(result, [[3.14, 3.16]]); // Banker's rounding to 2 decimal places
}

#[test]
fn test_floor() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT floor(3.7)").unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[test]
fn test_floor_negative() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT floor(-3.2)").unwrap();
    assert_table_eq!(result, [[-4.0]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_floor_precision() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT floor(3.12131, 2)").unwrap();
    assert_table_eq!(result, [[3.14]]);
}

#[test]
fn test_ceil() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ceil(3.2)").unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[test]
fn test_ceil_negative() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ceil(-3.7)").unwrap();
    assert_table_eq!(result, [[-3.0]]);
}

#[test]
fn test_ceiling_alias() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ceiling(3.2)").unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[test]
fn test_trunc() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT trunc(3.7)").unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[test]
fn test_trunc_negative() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT trunc(-3.7)").unwrap();
    assert_table_eq!(result, [[-3.0]]);
}

#[test]
fn test_truncate_alias() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT truncate(3.12131, 2)").unwrap();
    assert_table_eq!(result, [[3.12]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_round_down() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT roundDown(5.5, [0, 2, 4, 6, 8])")
        .unwrap();
    assert_table_eq!(result, [[4]]); // 5.5 rounds down to 4 in array
}

#[ignore = "Implement me!"]
#[test]
fn test_round_to_exp2() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT roundToExp2(17)").unwrap();
    assert_table_eq!(result, [[16]]); // 17 rounds down to 2^4 = 16
}

#[ignore = "Implement me!"]
#[test]
fn test_round_duration() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT roundDuration(100)").unwrap();
    assert_table_eq!(result, [[60]]); // 100 rounds to nearest duration boundary (60)
}

#[ignore = "Implement me!"]
#[test]
fn test_round_age() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT roundAge(25)").unwrap();
    assert_table_eq!(result, [[25]]); // 25 is already an age bracket boundary
}

#[test]
fn test_rounding_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE prices (item String, price Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO prices VALUES
            ('apple', 1.234),
            ('banana', 2.567),
            ('cherry', 3.891)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT item, price,
                round(price, 2) AS rounded,
                floor(price) AS floored,
                ceil(price) AS ceiled,
                trunc(price) AS truncated
            FROM prices
            ORDER BY item",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["apple", 1.234, 1.23, 1.0, 2.0, 1.0],
            ["banana", 2.567, 2.57, 2.0, 3.0, 2.0],
            ["cherry", 3.891, 3.89, 3.0, 4.0, 3.0]
        ]
    );
}

#[ignore = "Implement me!"]
#[test]
fn test_round_various_types() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                round(toDecimal64(3.12131, 5), 2) AS decimal_round,
                round(toFloat32(3.12131), 2) AS float32_round,
                round(toFloat64(3.12131), 2) AS float64_round",
        )
        .unwrap();
    assert_table_eq!(result, [[3.14, 3.14, 3.14]]);
}
