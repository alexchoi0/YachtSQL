use yachtsql::YachtSQLSession;

use super::super::common::create_session;
use crate::assert_table_eq;

async fn setup_numbers_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .await
        .unwrap();
}

async fn setup_strings_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE strings (id INT64, text STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO strings VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry')")
        .await
        .unwrap();
}

async fn setup_nullable_numbers(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE nullable_numbers (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable_numbers VALUES (1, 10), (2, NULL), (3, 30)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_equals_integer() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value = 20")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_equals_string() {
    let session = create_session();
    setup_strings_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text = 'banana'")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_equals_integer() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value != 20 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_equals_alternate_syntax() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value <> 20 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3], [4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_less_than() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value < 25 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_less_than_or_equal() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value <= 20 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_greater_than() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value > 30 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_greater_than_or_equal() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value >= 40 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[4], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_comparison_with_null_equals() {
    let session = create_session();
    setup_nullable_numbers(&session).await;

    let result = session
        .execute_sql("SELECT id FROM nullable_numbers WHERE value = NULL")
        .await
        .unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_null() {
    let session = create_session();
    setup_nullable_numbers(&session).await;

    let result = session
        .execute_sql("SELECT id FROM nullable_numbers WHERE value IS NULL")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_is_not_null() {
    let session = create_session();
    setup_nullable_numbers(&session).await;

    let result = session
        .execute_sql("SELECT id FROM nullable_numbers WHERE value IS NOT NULL ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_between() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value BETWEEN 20 AND 40 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [3], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_between() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value NOT BETWEEN 20 AND 40 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_list() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value IN (10, 30, 50) ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_in_list() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value NOT IN (10, 30, 50) ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_in_string_list() {
    let session = create_session();
    setup_strings_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text IN ('apple', 'cherry') ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_prefix() {
    let session = create_session();
    setup_strings_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text LIKE 'a%'")
        .await
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_suffix() {
    let session = create_session();
    setup_strings_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text LIKE '%a'")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_contains() {
    let session = create_session();
    setup_strings_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text LIKE '%an%'")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_like_single_char() {
    let session = create_session();
    setup_strings_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text LIKE '_____'")
        .await
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_not_like() {
    let session = create_session();
    setup_strings_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text NOT LIKE '%rry' ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_comparison_expression_result() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT 10 > 5, 10 < 5, 10 = 10, 10 != 10")
        .await
        .unwrap();

    assert_table_eq!(result, [[true, false, true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_comparison_expression() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT NULL = NULL, NULL IS NULL, NULL IS NOT NULL")
        .await
        .unwrap();

    assert_table_eq!(result, [[null, true, false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_comparison_operators() {
    let session = create_session();
    setup_strings_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text < 'banana' ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_string_greater_than() {
    let session = create_session();
    setup_strings_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text > 'banana' ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_chained_comparison() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value > 10 AND value < 40 ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_comparison_with_expression() {
    let session = create_session();
    setup_numbers_table(&session).await;

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value * 2 = 40")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}
