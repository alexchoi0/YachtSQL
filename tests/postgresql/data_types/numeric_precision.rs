use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_numeric_precision_scale() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 123.456::NUMERIC(6,3)")
        .unwrap();
    assert_table_eq!(result, [[123.456]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_no_precision() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 123.456789012345::NUMERIC")
        .unwrap();
    assert_table_eq!(result, [[123.456789012345]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_decimal_type() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 99.99::DECIMAL(4,2)").unwrap();
    assert_table_eq!(result, [[99.99]]);
}

#[test]
fn test_numeric_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE prices (id INTEGER, price NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO prices VALUES (1, 99.99)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM prices").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_high_precision() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 12345678901234567890.123456789::NUMERIC(30,9)")
        .unwrap();
    assert_table_eq!(result, [[12345678901234567890.123456789]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_arithmetic() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 10.5::NUMERIC + 20.3::NUMERIC")
        .unwrap();
    assert_table_eq!(result, [[30.8]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_division() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 10::NUMERIC / 3::NUMERIC")
        .unwrap();
    assert_table_eq!(result, [[3.3333333333333333]]);
}

#[test]
fn test_numeric_comparison() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 10.5::NUMERIC > 10.4::NUMERIC")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_smallint() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 32767::SMALLINT").unwrap();
    assert_table_eq!(result, [[32767]]);
}

#[test]
fn test_integer() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 2147483647::INTEGER").unwrap();
    assert_table_eq!(result, [[2147483647]]);
}

#[test]
fn test_bigint() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 9223372036854775807::BIGINT")
        .unwrap();
    assert_table_eq!(result, [[9223372036854775807i64]]);
}

#[test]
fn test_real() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 3.11::REAL").unwrap();
    assert_table_eq!(result, [[3.11]]);
}

#[test]
fn test_double_precision() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 3.111592653589793::DOUBLE PRECISION")
        .unwrap();
    assert_table_eq!(result, [[3.111592653589793]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_rounding() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROUND(123.456::NUMERIC, 2)")
        .unwrap();
    assert_table_eq!(result, [[123.46]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_truncate() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUNC(123.456::NUMERIC, 2)")
        .unwrap();
    assert_table_eq!(result, [[123.45]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_abs() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ABS(-123.45::NUMERIC)")
        .unwrap();
    assert_table_eq!(result, [[123.45]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_ceil() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CEIL(123.45::NUMERIC)")
        .unwrap();
    assert_table_eq!(result, [[124]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_floor() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FLOOR(123.45::NUMERIC)")
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_mod() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT MOD(10::NUMERIC, 3::NUMERIC)")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_power() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POWER(2::NUMERIC, 10::NUMERIC)")
        .unwrap();
    assert_table_eq!(result, [[1024]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_sqrt() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SQRT(144::NUMERIC)").unwrap();
    assert_table_eq!(result, [[12]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_aggregate_sum() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (val NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (10.50), (20.25), (30.75)")
        .unwrap();

    let result = executor.execute_sql("SELECT SUM(val) FROM nums").unwrap();
    assert_table_eq!(result, [[61.50]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_aggregate_avg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums_avg (val NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums_avg VALUES (10.00), (20.00), (30.00)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(val) FROM nums_avg")
        .unwrap();
    assert_table_eq!(result, [[20.00]]);
}

#[test]
fn test_numeric_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums_null (val NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums_null VALUES (NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val IS NULL FROM nums_null")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_negative() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT -123.456::NUMERIC(6,3)")
        .unwrap();
    assert_table_eq!(result, [[-123.456]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_numeric_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 0.00::NUMERIC(4,2)").unwrap();
    assert_table_eq!(result, [[0.00]]);
}

#[test]
fn test_numeric_sign() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SIGN(-123.45::NUMERIC)")
        .unwrap();
    assert_table_eq!(result, [[-1]]);
}
