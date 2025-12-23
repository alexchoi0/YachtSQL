use crate::assert_table_eq;
use crate::common::{create_session, n};

#[test]
fn test_abs() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ABS(-5)").unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_abs_float() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ABS(-3.14)").unwrap();

    assert_table_eq!(result, [[n("3.14")]]);
}

#[test]
fn test_ceil() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT CEIL(3.2)").unwrap();

    assert_table_eq!(result, [[n("4")]]);
}

#[test]
fn test_floor() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT FLOOR(3.8)").unwrap();

    assert_table_eq!(result, [[n("3")]]);
}

#[test]
fn test_round() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(3.456, 2)").unwrap();

    assert_table_eq!(result, [[n("3.46")]]);
}

#[test]
fn test_round_no_decimal() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(3.5)").unwrap();

    assert_table_eq!(result, [[n("4")]]);
}

#[test]
fn test_mod() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT MOD(10, 3)").unwrap();

    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_power() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT POWER(2, 3)").unwrap();

    assert_table_eq!(result, [[8.0]]);
}

#[test]
fn test_sqrt() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT SQRT(16)").unwrap();

    assert_table_eq!(result, [[4.0]]);
}

#[test]
fn test_cbrt() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT CBRT(27)").unwrap();

    assert_table_eq!(result, [[3.0]]);
}

#[test]
fn test_cbrt_negative() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT CBRT(-8)").unwrap();

    assert_table_eq!(result, [[-2.0]]);
}

#[test]
fn test_sign() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT SIGN(-5), SIGN(0), SIGN(5)")
        .unwrap();

    assert_table_eq!(result, [[-1, 0, 1]]);
}

#[test]
fn test_arithmetic_add() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT 1 + 2").unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_arithmetic_subtract() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT 5 - 3").unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_arithmetic_multiply() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT 4 * 3").unwrap();

    assert_table_eq!(result, [[12]]);
}

#[test]
fn test_arithmetic_divide() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT 10 / 4").unwrap();

    assert_table_eq!(result, [[2.5]]);
}

#[test]
fn test_arithmetic_negative() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT -5").unwrap();

    assert_table_eq!(result, [[-5]]);
}

#[test]
fn test_math_on_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (10), (-5), (0)")
        .unwrap();

    let result = session
        .execute_sql("SELECT value, ABS(value), SIGN(value) FROM numbers ORDER BY value")
        .unwrap();

    assert_table_eq!(result, [[-5, 5, -1], [0, 0, 0], [10, 10, 1]]);
}

#[test]
fn test_greatest() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT GREATEST(1, 5, 3)").unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_least() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT LEAST(1, 5, 3)").unwrap();

    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_sin() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT SIN(0)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_sin_pi_half() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(SIN(ACOS(-1) / 2), 5)")
        .unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_cos() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT COS(0)").unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_cos_pi() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(COS(ACOS(-1)), 5)")
        .unwrap();

    assert_table_eq!(result, [[-1.0]]);
}

#[test]
fn test_tan() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT TAN(0)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_asin() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ASIN(0)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_asin_one() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(ASIN(1), 5)").unwrap();

    assert_table_eq!(result, [[1.5708]]);
}

#[test]
fn test_acos() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ACOS(1)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_acos_zero() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(ACOS(0), 5)").unwrap();

    assert_table_eq!(result, [[1.5708]]);
}

#[test]
fn test_atan() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ATAN(0)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_atan2() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ATAN2(0, 1)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_atan2_quadrant() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(ATAN2(1, 1), 5)").unwrap();

    assert_table_eq!(result, [[0.7854]]);
}

#[test]
fn test_sinh() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT SINH(0)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_cosh() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT COSH(0)").unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_tanh() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT TANH(0)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
#[ignore]
fn test_asinh() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ASINH(0)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
#[ignore]
fn test_acosh() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ACOSH(1)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
#[ignore]
fn test_atanh() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ATANH(0)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_cot() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(COT(1), 5)").unwrap();

    assert_table_eq!(result, [[0.64209]]);
}

#[test]
#[ignore]
fn test_coth() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(COTH(1), 5)").unwrap();

    assert_table_eq!(result, [[1.31304]]);
}

#[test]
fn test_csc() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(CSC(1), 5)").unwrap();

    assert_table_eq!(result, [[1.18840]]);
}

#[test]
#[ignore]
fn test_csch() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(CSCH(1), 5)").unwrap();

    assert_table_eq!(result, [[0.85092]]);
}

#[test]
fn test_sec() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(SEC(0), 5)").unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[test]
#[ignore]
fn test_sech() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(SECH(0), 5)").unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_exp() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT EXP(0)").unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_exp_one() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(EXP(1), 5)").unwrap();

    assert_table_eq!(result, [[2.71828]]);
}

#[test]
fn test_ln() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT LN(1)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_ln_e() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(LN(EXP(1)), 5)").unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_log() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT LOG(1)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_log_with_base() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT LOG(100, 10)").unwrap();

    assert_table_eq!(result, [[2.0]]);
}

#[test]
fn test_log10() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT LOG10(100)").unwrap();

    assert_table_eq!(result, [[2.0]]);
}

#[test]
fn test_log10_one() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT LOG10(1)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_ceiling() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT CEILING(3.2)").unwrap();

    assert_table_eq!(result, [[n("4")]]);
}

#[test]
fn test_ceiling_negative() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT CEILING(-2.3)").unwrap();

    assert_table_eq!(result, [[n("-2")]]);
}

#[test]
fn test_trunc() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT TRUNC(2.8)").unwrap();

    assert_table_eq!(result, [[n("2")]]);
}

#[test]
fn test_trunc_negative() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT TRUNC(-2.8)").unwrap();

    assert_table_eq!(result, [[n("-2")]]);
}

#[test]
fn test_trunc_with_precision() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT TRUNC(123.456, 2)").unwrap();

    assert_table_eq!(result, [[n("123.45")]]);
}

#[test]
fn test_trunc_negative_precision() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT TRUNC(123.456, -1)").unwrap();

    assert_table_eq!(result, [[n("120")]]);
}

#[test]
fn test_pow() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT POW(2, 3)").unwrap();

    assert_table_eq!(result, [[8.0]]);
}

#[test]
fn test_pow_fractional() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT POW(4, 0.5)").unwrap();

    assert_table_eq!(result, [[2.0]]);
}

#[test]
fn test_div() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT DIV(20, 4)").unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_div_with_remainder() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT DIV(20, 3)").unwrap();

    assert_table_eq!(result, [[6]]);
}

#[test]
fn test_div_negative() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT DIV(12, -7)").unwrap();

    assert_table_eq!(result, [[-1]]);
}

#[test]
#[ignore]
fn test_ieee_divide() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT IEEE_DIVIDE(20.0, 4.0)")
        .unwrap();

    assert_table_eq!(result, [[5.0]]);
}

#[test]
#[ignore]
fn test_ieee_divide_by_zero() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT IEEE_DIVIDE(25.0, 0.0)")
        .unwrap();

    let row = result.get_row(0).unwrap();
    let value = row.get(0).unwrap().as_f64().unwrap();
    assert!(value.is_infinite() && value.is_sign_positive());
}

#[test]
#[ignore]
fn test_ieee_divide_negative_by_zero() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT IEEE_DIVIDE(-25.0, 0.0)")
        .unwrap();

    let row = result.get_row(0).unwrap();
    let value = row.get(0).unwrap().as_f64().unwrap();
    assert!(value.is_infinite() && value.is_sign_negative());
}

#[test]
#[ignore]
fn test_ieee_divide_zero_by_zero() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT IEEE_DIVIDE(0.0, 0.0)").unwrap();

    let row = result.get_row(0).unwrap();
    let value = row.get(0).unwrap().as_f64().unwrap();
    assert!(value.is_nan());
}

#[test]
#[ignore]
fn test_is_inf_positive() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT IS_INF(IEEE_DIVIDE(1.0, 0.0))")
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore]
fn test_is_inf_negative() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT IS_INF(IEEE_DIVIDE(-1.0, 0.0))")
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore]
fn test_is_inf_false() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT IS_INF(25.0)").unwrap();

    assert_table_eq!(result, [[false]]);
}

#[test]
#[ignore]
fn test_is_nan_true() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT IS_NAN(IEEE_DIVIDE(0.0, 0.0))")
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore]
fn test_is_nan_false() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT IS_NAN(25.0)").unwrap();

    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_rand() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RAND() >= 0 AND RAND() < 1")
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_safe_add() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT SAFE_ADD(5, 4)").unwrap();

    assert_table_eq!(result, [[9]]);
}

#[test]
fn test_safe_subtract() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT SAFE_SUBTRACT(5, 4)").unwrap();

    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_safe_multiply() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT SAFE_MULTIPLY(20, 4)").unwrap();

    assert_table_eq!(result, [[80]]);
}

#[test]
fn test_safe_divide() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(20.0, 4.0)")
        .unwrap();

    assert_table_eq!(result, [[5.0]]);
}

#[test]
fn test_safe_divide_by_zero() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(20.0, 0.0)")
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_negate() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT SAFE_NEGATE(5)").unwrap();

    assert_table_eq!(result, [[-5]]);
}

#[test]
fn test_safe_negate_negative() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT SAFE_NEGATE(-5)").unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore]
fn test_cosine_distance() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(COSINE_DISTANCE([1.0, 2.0], [3.0, 4.0]), 5)")
        .unwrap();

    assert_table_eq!(result, [[0.01613]]);
}

#[test]
#[ignore]
fn test_cosine_distance_identical() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT COSINE_DISTANCE([1.0, 0.0], [1.0, 0.0])")
        .unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
#[ignore]
fn test_euclidean_distance() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(EUCLIDEAN_DISTANCE([1.0, 2.0], [3.0, 4.0]), 3)")
        .unwrap();

    assert_table_eq!(result, [[2.828]]);
}

#[test]
#[ignore]
fn test_euclidean_distance_identical() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT EUCLIDEAN_DISTANCE([1.0, 2.0], [1.0, 2.0])")
        .unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
#[ignore]
fn test_range_bucket() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(20, [0, 10, 20, 30, 40])")
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore]
fn test_range_bucket_between() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(25, [0, 10, 20, 30, 40])")
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore]
fn test_range_bucket_smaller_than_first() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(-10, [5, 10, 20, 30, 40])")
        .unwrap();

    assert_table_eq!(result, [[0]]);
}

#[test]
#[ignore]
fn test_range_bucket_greater_than_last() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(80, [0, 10, 20, 30, 40])")
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore]
fn test_range_bucket_empty_array() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(80, CAST([] AS ARRAY<INT64>))")
        .unwrap();

    assert_table_eq!(result, [[0]]);
}

#[test]
#[ignore]
fn test_range_bucket_null_point() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(NULL, [0, 10, 20, 30, 40])")
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_round_negative_places() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(123.7, -1)").unwrap();

    assert_table_eq!(result, [[n("120")]]);
}

#[test]
fn test_round_halfway_away_from_zero() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(2.5)").unwrap();

    assert_table_eq!(result, [[n("3")]]);
}

#[test]
fn test_round_halfway_negative() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT ROUND(-2.5)").unwrap();

    assert_table_eq!(result, [[n("-3")]]);
}

#[test]
#[ignore]
fn test_greatest_with_null() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT GREATEST(1, NULL, 3)").unwrap();

    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore]
fn test_least_with_null() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT LEAST(1, NULL, 3)").unwrap();

    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_greatest_strings() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT GREATEST('apple', 'banana', 'cherry')")
        .unwrap();

    assert_table_eq!(result, [["cherry"]]);
}

#[test]
fn test_least_strings() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT LEAST('apple', 'banana', 'cherry')")
        .unwrap();

    assert_table_eq!(result, [["apple"]]);
}

#[test]
fn test_mod_negative() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT MOD(-25, 12)").unwrap();

    assert_table_eq!(result, [[-1]]);
}

#[test]
fn test_abs_numeric() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT ABS(CAST(-123.456 AS NUMERIC))")
        .unwrap();

    assert_table_eq!(result, [[n("123.456")]]);
}

#[test]
fn test_ceil_negative() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT CEIL(-2.3)").unwrap();

    assert_table_eq!(result, [[n("-2")]]);
}

#[test]
fn test_floor_negative() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT FLOOR(-2.3)").unwrap();

    assert_table_eq!(result, [[n("-3")]]);
}

#[test]
fn test_sqrt_zero() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT SQRT(0)").unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_power_zero_exponent() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT POWER(5, 0)").unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_power_negative_exponent() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT POWER(2, -1)").unwrap();

    assert_table_eq!(result, [[0.5]]);
}
