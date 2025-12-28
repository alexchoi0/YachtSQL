use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (1, 10, 'A'), (2, 20, 'A'), (3, 30, 'B'), (4, 40, 'B'), (5, 50, 'B')").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_star() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT(*) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_column() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_with_nulls() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nullable (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (3), (NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*), COUNT(value) FROM nullable")
        .await
        .unwrap();

    assert_table_eq!(result, [[4, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_avg() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT AVG(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[30.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_min() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT MIN(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_max() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT MAX(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_aggregates() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT(*), SUM(value), MIN(value), MAX(value) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[5, 150, 10, 50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_with_where() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value) FROM numbers WHERE value > 20")
        .await
        .unwrap();

    assert_table_eq!(result, [[120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_aggregate_empty_result() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value) FROM numbers WHERE value > 100")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_empty_result() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT(*) FROM numbers WHERE value > 100")
        .await
        .unwrap();

    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_with_expression() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM(value * 2) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[300]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT category) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_if() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNT_IF(value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_countif() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT COUNTIF(value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sum_if() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUM_IF(value, value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_sumif() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT SUMIF(value, value > 20) FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [[120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_listagg() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT LISTAGG(category, ',') FROM numbers")
        .await
        .unwrap();

    assert_table_eq!(result, [["A,A,B,B,B"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_xmlagg() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE xml_data (content STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO xml_data VALUES ('<item>1</item>'), ('<item>2</item>'), ('<item>3</item>')").await
        .unwrap();

    let result = session
        .execute_sql("SELECT XMLAGG(content) FROM xml_data")
        .await
        .unwrap();

    assert_table_eq!(result, [["<item>1</item><item>2</item><item>3</item>"]]);
}
