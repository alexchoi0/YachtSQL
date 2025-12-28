use crate::assert_table_eq;
use crate::common::{create_session, n};

#[tokio::test(flavor = "current_thread")]
async fn test_abs() {
    let session = create_session();

    let result = session.execute_sql("SELECT ABS(-5)").await.unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_abs_float() {
    let session = create_session();

    let result = session.execute_sql("SELECT ABS(-3.14)").await.unwrap();

    assert_table_eq!(result, [[n("3.14")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ceil() {
    let session = create_session();

    let result = session.execute_sql("SELECT CEIL(3.2)").await.unwrap();

    assert_table_eq!(result, [[n("4")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_floor() {
    let session = create_session();

    let result = session.execute_sql("SELECT FLOOR(3.8)").await.unwrap();

    assert_table_eq!(result, [[n("3")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_round() {
    let session = create_session();

    let result = session.execute_sql("SELECT ROUND(3.456, 2)").await.unwrap();

    assert_table_eq!(result, [[n("3.46")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_round_no_decimal() {
    let session = create_session();

    let result = session.execute_sql("SELECT ROUND(3.5)").await.unwrap();

    assert_table_eq!(result, [[n("4")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mod() {
    let session = create_session();

    let result = session.execute_sql("SELECT MOD(10, 3)").await.unwrap();

    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_power() {
    let session = create_session();

    let result = session.execute_sql("SELECT POWER(2, 3)").await.unwrap();

    assert_table_eq!(result, [[8.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sqrt() {
    let session = create_session();

    let result = session.execute_sql("SELECT SQRT(16)").await.unwrap();

    assert_table_eq!(result, [[4.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cbrt() {
    let session = create_session();

    let result = session.execute_sql("SELECT CBRT(27)").await.unwrap();

    assert_table_eq!(result, [[3.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cbrt_negative() {
    let session = create_session();

    let result = session.execute_sql("SELECT CBRT(-8)").await.unwrap();

    assert_table_eq!(result, [[-2.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sign() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SIGN(-5), SIGN(0), SIGN(5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[-1, 0, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_arithmetic_add() {
    let session = create_session();

    let result = session.execute_sql("SELECT 1 + 2").await.unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_arithmetic_subtract() {
    let session = create_session();

    let result = session.execute_sql("SELECT 5 - 3").await.unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_arithmetic_multiply() {
    let session = create_session();

    let result = session.execute_sql("SELECT 4 * 3").await.unwrap();

    assert_table_eq!(result, [[12]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_arithmetic_divide() {
    let session = create_session();

    let result = session.execute_sql("SELECT 10 / 4").await.unwrap();

    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_arithmetic_negative() {
    let session = create_session();

    let result = session.execute_sql("SELECT -5").await.unwrap();

    assert_table_eq!(result, [[-5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_math_on_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (10), (-5), (0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT value, ABS(value), SIGN(value) FROM numbers ORDER BY value")
        .await
        .unwrap();

    assert_table_eq!(result, [[-5, 5, -1], [0, 0, 0], [10, 10, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_greatest() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT GREATEST(1, 5, 3)")
        .await
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_least() {
    let session = create_session();

    let result = session.execute_sql("SELECT LEAST(1, 5, 3)").await.unwrap();

    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sin() {
    let session = create_session();

    let result = session.execute_sql("SELECT SIN(0)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sin_pi_half() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(SIN(ACOS(-1) / 2), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cos() {
    let session = create_session();

    let result = session.execute_sql("SELECT COS(0)").await.unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cos_pi() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(COS(ACOS(-1)), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[-1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_tan() {
    let session = create_session();

    let result = session.execute_sql("SELECT TAN(0)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_asin() {
    let session = create_session();

    let result = session.execute_sql("SELECT ASIN(0)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_asin_one() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(ASIN(1), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.5708]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_acos() {
    let session = create_session();

    let result = session.execute_sql("SELECT ACOS(1)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_acos_zero() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(ACOS(0), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.5708]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_atan() {
    let session = create_session();

    let result = session.execute_sql("SELECT ATAN(0)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_atan2() {
    let session = create_session();

    let result = session.execute_sql("SELECT ATAN2(0, 1)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_atan2_quadrant() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(ATAN2(1, 1), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[0.7854]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sinh() {
    let session = create_session();

    let result = session.execute_sql("SELECT SINH(0)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cosh() {
    let session = create_session();

    let result = session.execute_sql("SELECT COSH(0)").await.unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_tanh() {
    let session = create_session();

    let result = session.execute_sql("SELECT TANH(0)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_asinh() {
    let session = create_session();

    let result = session.execute_sql("SELECT ASINH(0)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_acosh() {
    let session = create_session();

    let result = session.execute_sql("SELECT ACOSH(1)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_atanh() {
    let session = create_session();

    let result = session.execute_sql("SELECT ATANH(0)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cot() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(COT(1), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[0.64209]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coth() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(COTH(1), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.31304]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_csc() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(CSC(1), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.18840]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_csch() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(CSCH(1), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[0.85092]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sec() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(SEC(0), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sech() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(SECH(0), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exp() {
    let session = create_session();

    let result = session.execute_sql("SELECT EXP(0)").await.unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exp_one() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(EXP(1), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[2.71828]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ln() {
    let session = create_session();

    let result = session.execute_sql("SELECT LN(1)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ln_e() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(LN(EXP(1)), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_log() {
    let session = create_session();

    let result = session.execute_sql("SELECT LOG(1)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_log_with_base() {
    let session = create_session();

    let result = session.execute_sql("SELECT LOG(100, 10)").await.unwrap();

    assert_table_eq!(result, [[2.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_log10() {
    let session = create_session();

    let result = session.execute_sql("SELECT LOG10(100)").await.unwrap();

    assert_table_eq!(result, [[2.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_log10_one() {
    let session = create_session();

    let result = session.execute_sql("SELECT LOG10(1)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ceiling() {
    let session = create_session();

    let result = session.execute_sql("SELECT CEILING(3.2)").await.unwrap();

    assert_table_eq!(result, [[n("4")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ceiling_negative() {
    let session = create_session();

    let result = session.execute_sql("SELECT CEILING(-2.3)").await.unwrap();

    assert_table_eq!(result, [[n("-2")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trunc() {
    let session = create_session();

    let result = session.execute_sql("SELECT TRUNC(2.8)").await.unwrap();

    assert_table_eq!(result, [[n("2")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trunc_negative() {
    let session = create_session();

    let result = session.execute_sql("SELECT TRUNC(-2.8)").await.unwrap();

    assert_table_eq!(result, [[n("-2")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trunc_with_precision() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT TRUNC(123.456, 2)")
        .await
        .unwrap();

    assert_table_eq!(result, [[n("123.45")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trunc_negative_precision() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT TRUNC(123.456, -1)")
        .await
        .unwrap();

    assert_table_eq!(result, [[n("120")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_pow() {
    let session = create_session();

    let result = session.execute_sql("SELECT POW(2, 3)").await.unwrap();

    assert_table_eq!(result, [[8.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_pow_fractional() {
    let session = create_session();

    let result = session.execute_sql("SELECT POW(4, 0.5)").await.unwrap();

    assert_table_eq!(result, [[2.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div() {
    let session = create_session();

    let result = session.execute_sql("SELECT DIV(20, 4)").await.unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_with_remainder() {
    let session = create_session();

    let result = session.execute_sql("SELECT DIV(20, 3)").await.unwrap();

    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_div_negative() {
    let session = create_session();

    let result = session.execute_sql("SELECT DIV(12, -7)").await.unwrap();

    assert_table_eq!(result, [[-1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ieee_divide() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT IEEE_DIVIDE(20.0, 4.0)")
        .await
        .unwrap();

    assert_table_eq!(result, [[5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ieee_divide_by_zero() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT IEEE_DIVIDE(25.0, 0.0)")
        .await
        .unwrap();

    let row = result.get_row(0).unwrap();
    let value = row.get(0).unwrap().as_f64().unwrap();
    assert!(value.is_infinite() && value.is_sign_positive());
}

#[tokio::test(flavor = "current_thread")]
async fn test_ieee_divide_negative_by_zero() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT IEEE_DIVIDE(-25.0, 0.0)")
        .await
        .unwrap();

    let row = result.get_row(0).unwrap();
    let value = row.get(0).unwrap().as_f64().unwrap();
    assert!(value.is_infinite() && value.is_sign_negative());
}

#[tokio::test(flavor = "current_thread")]
async fn test_ieee_divide_zero_by_zero() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT IEEE_DIVIDE(0.0, 0.0)")
        .await
        .unwrap();

    let row = result.get_row(0).unwrap();
    let value = row.get(0).unwrap().as_f64().unwrap();
    assert!(value.is_nan());
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_inf_positive() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT IS_INF(IEEE_DIVIDE(1.0, 0.0))")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_inf_negative() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT IS_INF(IEEE_DIVIDE(-1.0, 0.0))")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_inf_false() {
    let session = create_session();

    let result = session.execute_sql("SELECT IS_INF(25.0)").await.unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_nan_true() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT IS_NAN(IEEE_DIVIDE(0.0, 0.0))")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_nan_false() {
    let session = create_session();

    let result = session.execute_sql("SELECT IS_NAN(25.0)").await.unwrap();

    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rand() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RAND() >= 0 AND RAND() < 1")
        .await
        .unwrap();

    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_add() {
    let session = create_session();

    let result = session.execute_sql("SELECT SAFE_ADD(5, 4)").await.unwrap();

    assert_table_eq!(result, [[9]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_subtract() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_SUBTRACT(5, 4)")
        .await
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_multiply() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_MULTIPLY(20, 4)")
        .await
        .unwrap();

    assert_table_eq!(result, [[80]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(20.0, 4.0)")
        .await
        .unwrap();

    assert_table_eq!(result, [[5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide_by_zero() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(20.0, 0.0)")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_negate() {
    let session = create_session();

    let result = session.execute_sql("SELECT SAFE_NEGATE(5)").await.unwrap();

    assert_table_eq!(result, [[-5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_negate_negative() {
    let session = create_session();

    let result = session.execute_sql("SELECT SAFE_NEGATE(-5)").await.unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cosine_distance() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(COSINE_DISTANCE([1.0, 2.0], [3.0, 4.0]), 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [[0.01613]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cosine_distance_identical() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT COSINE_DISTANCE([1.0, 0.0], [1.0, 0.0])")
        .await
        .unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_euclidean_distance() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(EUCLIDEAN_DISTANCE([1.0, 2.0], [3.0, 4.0]), 3)")
        .await
        .unwrap();

    assert_table_eq!(result, [[2.828]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_euclidean_distance_identical() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT EUCLIDEAN_DISTANCE([1.0, 2.0], [1.0, 2.0])")
        .await
        .unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_bucket() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(20, [0, 10, 20, 30, 40])")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_bucket_between() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(25, [0, 10, 20, 30, 40])")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_bucket_smaller_than_first() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(-10, [5, 10, 20, 30, 40])")
        .await
        .unwrap();

    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_bucket_greater_than_last() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(80, [0, 10, 20, 30, 40])")
        .await
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_bucket_empty_array() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(80, CAST([] AS ARRAY<INT64>))")
        .await
        .unwrap();

    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_bucket_null_point() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_BUCKET(NULL, [0, 10, 20, 30, 40])")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_round_negative_places() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ROUND(123.7, -1)")
        .await
        .unwrap();

    assert_table_eq!(result, [[n("120")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_round_halfway_away_from_zero() {
    let session = create_session();

    let result = session.execute_sql("SELECT ROUND(2.5)").await.unwrap();

    assert_table_eq!(result, [[n("3")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_round_halfway_negative() {
    let session = create_session();

    let result = session.execute_sql("SELECT ROUND(-2.5)").await.unwrap();

    assert_table_eq!(result, [[n("-3")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_greatest_with_null() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT GREATEST(1, NULL, 3)")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_least_with_null() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT LEAST(1, NULL, 3)")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_greatest_strings() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT GREATEST('apple', 'banana', 'cherry')")
        .await
        .unwrap();

    assert_table_eq!(result, [["cherry"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_least_strings() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT LEAST('apple', 'banana', 'cherry')")
        .await
        .unwrap();

    assert_table_eq!(result, [["apple"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_mod_negative() {
    let session = create_session();

    let result = session.execute_sql("SELECT MOD(-25, 12)").await.unwrap();

    assert_table_eq!(result, [[-1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_abs_numeric() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ABS(CAST(-123.456 AS NUMERIC))")
        .await
        .unwrap();

    assert_table_eq!(result, [[n("123.456")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ceil_negative() {
    let session = create_session();

    let result = session.execute_sql("SELECT CEIL(-2.3)").await.unwrap();

    assert_table_eq!(result, [[n("-2")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_floor_negative() {
    let session = create_session();

    let result = session.execute_sql("SELECT FLOOR(-2.3)").await.unwrap();

    assert_table_eq!(result, [[n("-3")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sqrt_zero() {
    let session = create_session();

    let result = session.execute_sql("SELECT SQRT(0)").await.unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_power_zero_exponent() {
    let session = create_session();

    let result = session.execute_sql("SELECT POWER(5, 0)").await.unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_power_negative_exponent() {
    let session = create_session();

    let result = session.execute_sql("SELECT POWER(2, -1)").await.unwrap();

    assert_table_eq!(result, [[0.5]]);
}
