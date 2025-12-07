use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_addition_integers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 1 + 2").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_subtraction_integers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 5 - 3").unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_multiplication_integers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 4 * 3").unwrap();
    assert_table_eq!(result, [[12]]);
}

#[test]
fn test_division_integers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 10 / 3").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_modulo() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 10 % 3").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_negative_numbers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT -5 + 3").unwrap();
    assert_table_eq!(result, [[-2]]);
}

#[test]
fn test_negative_result() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 3 - 10").unwrap();
    assert_table_eq!(result, [[-7]]);
}

#[test]
fn test_multiplication_negative() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT -4 * 3").unwrap();
    assert_table_eq!(result, [[-12]]);
}

#[test]
fn test_operator_precedence_mul_add() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 2 + 3 * 4").unwrap();
    assert_table_eq!(result, [[14]]);
}

#[test]
fn test_operator_precedence_parentheses() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT (2 + 3) * 4").unwrap();
    assert_table_eq!(result, [[20]]);
}

#[test]
fn test_complex_expression() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT (10 + 5) / 3 * 2").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_arithmetic_with_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (10, 3), (20, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b, a - b, a * b, a / b FROM nums ORDER BY a")
        .unwrap();

    assert_table_eq!(result, [[13, 7, 30, 3], [24, 16, 80, 5],]);
}

#[test]
fn test_arithmetic_null_propagation() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 5 + NULL").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_unary_minus() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT -(-5)").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_unary_plus() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT +5").unwrap();
    assert_table_eq!(result, [[5]]);
}
