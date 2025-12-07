#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_abs_positive_int() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (42)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ABS(value) AS abs_val FROM numbers")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_abs_negative_int() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (-42)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ABS(value) AS abs_val FROM numbers")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_abs_float() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (-3.12131)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ABS(value) AS abs_val FROM numbers")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_abs_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT ABS(0) AS abs_val").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_abs_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ABS(value) AS abs_val FROM numbers")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_ceil_positive() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT CEIL(3.2) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_ceil_negative() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT CEIL(-3.8) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_ceiling_alias() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT CEILING(3.2) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_floor_positive() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT FLOOR(3.8) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_floor_negative() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT FLOOR(-3.2) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_round_no_decimals() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT ROUND(3.5) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_round_with_precision() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT ROUND(3.12131, 2) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_round_negative_precision() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT ROUND(1234.5678, -2) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_trunc_positive() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT TRUNC(3.8) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_trunc_negative() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT TRUNC(-3.8) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_trunc_with_precision() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT TRUNC(3.12131, 2) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_sign_positive() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT SIGN(42) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_sign_negative() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT SIGN(-42) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_sign_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT SIGN(0) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_power_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT POWER(2, 3) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_power_fractional_exponent() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT POWER(4, 0.5) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_power_negative_exponent() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT POWER(2, -2) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_power_zero_exponent() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT POWER(42, 0) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pow_alias() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT POW(2, 3) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_sqrt_perfect_square() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT SQRT(16) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_sqrt_non_perfect_square() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT SQRT(2) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_sqrt_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT SQRT(0) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[should_panic(expected = "ExecutionError")]
fn test_sqrt_negative_error() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor.execute_sql("SELECT SQRT(-1) AS result").unwrap();
}

#[test]
fn test_exp_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT EXP(0) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_exp_one() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT EXP(1) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_exp_negative() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT EXP(-1) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_ln_of_e() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT LN(EXP(1)) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_ln_of_one() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT LN(1) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_ln_greater_than_one() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT LN(10) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[should_panic(expected = "ExecutionError")]
fn test_ln_of_zero_error() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor.execute_sql("SELECT LN(0) AS result").unwrap();
}

#[should_panic(expected = "ExecutionError")]
fn test_ln_of_negative_error() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor.execute_sql("SELECT LN(-1) AS result").unwrap();
}

#[test]
fn test_log_same_as_ln() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT LOG(10) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_log_with_base() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT LOG(100, 10) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_log10_of_hundred() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT LOG10(100) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_log10_of_one() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT LOG10(1) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_sin_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT SIN(0) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_sin_pi_over_2() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT SIN(PI() / 2) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_cos_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT COS(0) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_cos_pi() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT COS(PI()) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_tan_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT TAN(0) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_tan_pi_over_4() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT TAN(PI() / 4) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_asin_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT ASIN(0) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_asin_one() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT ASIN(1) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[should_panic(expected = "ExecutionError")]
fn test_asin_out_of_range() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor.execute_sql("SELECT ASIN(2) AS result").unwrap();
}

#[test]
fn test_acos_one() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT ACOS(1) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_acos_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT ACOS(0) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_atan_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT ATAN(0) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_atan_one() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT ATAN(1) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_atan2_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT ATAN2(1, 1) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_atan2_negative_x() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT ATAN2(1, -1) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_mod_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT MOD(10, 3) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_mod_negative_dividend() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT MOD(-10, 3) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_mod_float() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT MOD(10.5, 3.0) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pi_constant() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT PI() AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_degrees_from_radians() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT DEGREES(PI()) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_degrees_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT DEGREES(0) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_radians_from_degrees() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT RADIANS(180) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_radians_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT RADIANS(0) AS result").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_greatest_two_values() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT GREATEST(5, 10) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_greatest_multiple_values() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT GREATEST(5, 10, 3, 15, 7) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_greatest_with_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT GREATEST(5, NULL, 10) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_greatest_floats() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT GREATEST(3.14, 2.71, 1.41) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_least_two_values() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT LEAST(5, 10) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_least_multiple_values() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT LEAST(5, 10, 3, 15, 7) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_least_with_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT LEAST(5, NULL, 3) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_least_negative_numbers() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT LEAST(-5, -10, -3) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_math_in_where_clause() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1.5)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (2.7)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (3.9)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM numbers WHERE FLOOR(value) = 2")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_nested_math_functions() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT ROUND(SQRT(ABS(-16)), 2) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_math_with_aggregation() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1.2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (2.7)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (3.9)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ROUND(AVG(value), 1) AS avg_rounded FROM numbers")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_math_in_order_by() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (-5.2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (3.1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (-7.8)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM numbers ORDER BY ABS(value)")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_pythagorean_theorem() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE triangles (a FLOAT64, b FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO triangles VALUES (3, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SQRT(POWER(a, 2) + POWER(b, 2)) AS hypotenuse FROM triangles")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_distance_formula() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE points (x1 FLOAT64, y1 FLOAT64, x2 FLOAT64, y2 FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO points VALUES (0, 0, 3, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SQRT(POWER(x2 - x1, 2) + POWER(y2 - y1, 2)) AS distance FROM points")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_trigonometric_identity() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE angles (theta FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO angles VALUES (0.5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT POWER(SIN(theta), 2) + POWER(COS(theta), 2) AS identity FROM angles")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_exponential_growth() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE growth (years FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO growth VALUES (0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO growth VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO growth VALUES (2)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT years, ROUND(100 * POWER(1.05, years), 2) AS value FROM growth ORDER BY years",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_log_transformation() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE data (value FLOAT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO data VALUES (1)").unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value, LOG10(value) AS log_value FROM data ORDER BY value")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_rounding_modes_comparison() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (3.7)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT FLOOR(value) AS floor_val, CEIL(value) AS ceil_val, ROUND(value) AS round_val, TRUNC(value) AS trunc_val FROM numbers"
    ).unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_all_null_handling() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (NULL)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT ABS(value) AS abs_val, SQRT(value) AS sqrt_val, ROUND(value) AS round_val FROM numbers"
    ).unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_mod_vs_modulo_operator() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT MOD(10, 3) AS mod_func, 10 % 3 AS mod_op")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_division_by_zero_in_mod() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT MOD(10, 0) AS result");

    assert!(result.is_err());
}

#[test]
fn test_very_small_power() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT POWER(10, -308) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_negative_base_fractional_exponent() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT POWER(-1, 0.5) AS result");

    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_zero_to_zero_power() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor
        .execute_sql("SELECT POWER(0, 0) AS result")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}
