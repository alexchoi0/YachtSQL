#![allow(clippy::approx_constant)]

use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_sin() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SIN(0)").unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_cos() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT COS(0)").unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_tan() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TAN(0)").unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_asin() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ASIN(0)").unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_acos() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ACOS(1)").unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_atan() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ATAN(0)").unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_atan2() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ATAN2(1, 1)").unwrap();
    assert_table_eq!(result, [[0.7853981633974483]]);
}

#[test]
fn test_sinh() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SINH(0)").unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_cosh() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT COSH(0)").unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_tanh() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TANH(0)").unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_asinh() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ASINH(0)").unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_acosh() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ACOSH(1)").unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_atanh() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ATANH(0)").unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_pi() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT PI()").unwrap();
    assert_table_eq!(result, [[3.141592653589793]]);
}

#[test]
fn test_radians() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT RADIANS(180)").unwrap();
    assert_table_eq!(result, [[3.141592653589793]]);
}

#[test]
fn test_degrees() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT DEGREES(PI())").unwrap();
    assert_table_eq!(result, [[180.0]]);
}

#[test]
fn test_cot() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT COT(1)").unwrap();
    assert_table_eq!(result, [[0.6420926159343306]]);
}

#[test]
fn test_sind() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SIND(90)").unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_cosd() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT COSD(0)").unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_tand() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TAND(45)").unwrap();
    assert_table_eq!(result, [[0.9999999999999999]]);
}

#[test]
fn test_asind() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ASIND(1)").unwrap();
    assert_table_eq!(result, [[90.0]]);
}

#[test]
fn test_acosd() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ACOSD(0)").unwrap();
    assert_table_eq!(result, [[90.0]]);
}

#[test]
fn test_atand() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ATAND(1)").unwrap();
    assert_table_eq!(result, [[45.0]]);
}

#[test]
fn test_atan2d() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ATAN2D(1, 1)").unwrap();
    assert_table_eq!(result, [[45.0]]);
}

#[test]
fn test_cotd() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT COTD(45)").unwrap();
    assert_table_eq!(result, [[1.0000000000000002]]);
}

#[test]
fn test_log_natural() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LN(2.718281828)").unwrap();
    assert_table_eq!(result, [[0.9999999998986399]]);
}

#[test]
fn test_log_base10() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LOG(100)").unwrap();
    assert_table_eq!(result, [[2.0]]);
}

#[test]
fn test_log_base() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LOG(2, 8)").unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[test]
fn test_exp() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT EXP(1)").unwrap();
    assert_table_eq!(result, [[2.718281828459045]]);
}

#[test]
fn test_cbrt() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CBRT(27)").unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[test]
fn test_factorial() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT FACTORIAL(5)").unwrap();
    assert_table_eq!(result, [[120]]);
}

#[test]
fn test_gcd() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT GCD(12, 8)").unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_lcm() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LCM(4, 6)").unwrap();
    assert_table_eq!(result, [[12]]);
}

#[test]
fn test_min_scale() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT MIN_SCALE(8.4100)").unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_scale() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SCALE(8.41)").unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_trim_scale() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TRIM_SCALE(8.4100)").unwrap();
    assert_table_eq!(result, [[8.41]]);
}

#[test]
fn test_width_bucket() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT WIDTH_BUCKET(5.35, 0.024, 10.06, 5)")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_random() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT RANDOM()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_setseed() {
    let mut executor = create_executor();
    executor.execute_sql("SELECT SETSEED(0.5)").unwrap();
    let result = executor.execute_sql("SELECT RANDOM()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_div() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT DIV(9, 4)").unwrap();
    assert_table_eq!(result, [[2]]);
}
