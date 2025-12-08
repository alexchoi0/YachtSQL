use crate::assert_table_eq;
use crate::common::{create_executor, n};

#[test]
fn test_abs() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT ABS(-5)").unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_abs_float() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT ABS(-3.14)").unwrap();

    assert_table_eq!(result, [[(n("3.14"))]]);
}

#[test]
fn test_ceil() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT CEIL(3.2)").unwrap();

    assert_table_eq!(result, [[(n("4"))]]);
}

#[test]
fn test_floor() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT FLOOR(3.8)").unwrap();

    assert_table_eq!(result, [[(n("3"))]]);
}

#[test]
fn test_round() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT ROUND(3.456, 2)").unwrap();

    assert_table_eq!(result, [[(n("3.46"))]]);
}

#[test]
fn test_round_no_decimal() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT ROUND(3.5)").unwrap();

    assert_table_eq!(result, [[(n("4"))]]);
}

#[test]
fn test_mod() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT MOD(10, 3)").unwrap();

    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_power() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT POWER(2, 3)").unwrap();

    assert_table_eq!(result, [[8.0]]);
}

#[test]
fn test_sqrt() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT SQRT(16)").unwrap();

    assert_table_eq!(result, [[4.0]]);
}

#[test]
fn test_sign() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT SIGN(-5), SIGN(0), SIGN(5)")
        .unwrap();

    assert_table_eq!(result, [[-1, 0, 1]]);
}

#[test]
fn test_arithmetic_add() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT 1 + 2").unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_arithmetic_subtract() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT 5 - 3").unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_arithmetic_multiply() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT 4 * 3").unwrap();

    assert_table_eq!(result, [[12]]);
}

#[test]
fn test_arithmetic_divide() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT 10 / 4").unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_arithmetic_negative() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT -5").unwrap();

    assert_table_eq!(result, [[-5]]);
}

#[test]
fn test_math_on_table() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE numbers (value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (-5), (0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value, ABS(value), SIGN(value) FROM numbers ORDER BY value")
        .unwrap();

    assert_table_eq!(result, [[-5, 5, -1], [0, 0, 0], [10, 10, 1],]);
}

#[test]
fn test_greatest() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT GREATEST(1, 5, 3)").unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_least() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT LEAST(1, 5, 3)").unwrap();

    assert_table_eq!(result, [[1]]);
}
