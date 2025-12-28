use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_addition_integers() {
    let session = create_session();
    let result = session.execute_sql("SELECT 1 + 2").await.unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_subtraction_integers() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 - 3").await.unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiplication_integers() {
    let session = create_session();
    let result = session.execute_sql("SELECT 4 * 3").await.unwrap();
    assert_table_eq!(result, [[12]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_division_integers() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CAST(10 / 3 AS FLOAT64) > 3.3")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_modulo() {
    let session = create_session();
    let result = session.execute_sql("SELECT 10 % 3").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_negative_numbers() {
    let session = create_session();
    let result = session.execute_sql("SELECT -5 + 3").await.unwrap();
    assert_table_eq!(result, [[-2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_negative_result() {
    let session = create_session();
    let result = session.execute_sql("SELECT 3 - 10").await.unwrap();
    assert_table_eq!(result, [[-7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiplication_negative() {
    let session = create_session();
    let result = session.execute_sql("SELECT -4 * 3").await.unwrap();
    assert_table_eq!(result, [[-12]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_operator_precedence_mul_add() {
    let session = create_session();
    let result = session.execute_sql("SELECT 2 + 3 * 4").await.unwrap();
    assert_table_eq!(result, [[14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_operator_precedence_parentheses() {
    let session = create_session();
    let result = session.execute_sql("SELECT (2 + 3) * 4").await.unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_expression() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT (10 + 5) / 3 * 2")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_arithmetic_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nums (a INT64, b INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (10, 3), (20, 4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT a + b, a - b, a * b, ROUND(a / b, 2) FROM nums ORDER BY a")
        .await
        .unwrap();

    assert_table_eq!(result, [[13, 7, 30, 3.33], [24, 16, 80, 5.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_arithmetic_null_propagation() {
    let session = create_session();
    let result = session.execute_sql("SELECT 5 + NULL").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_minus() {
    let session = create_session();
    let result = session.execute_sql("SELECT -(-5)").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unary_plus() {
    let session = create_session();
    let result = session.execute_sql("SELECT +5").await.unwrap();
    assert_table_eq!(result, [[5]]);
}
