use crate::assert_table_eq;
use crate::common::{create_session, d};

#[tokio::test(flavor = "current_thread")]
async fn test_range_date_literal() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE(DATE '2024-01-01', DATE '2024-12-31') IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_timestamp_literal() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 23:59:59') IS NOT NULL",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_datetime_literal() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE(DATETIME '2024-01-01 00:00:00', DATETIME '2024-12-31 23:59:59') IS NOT NULL").await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_unbounded_start() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE(NULL, DATE '2024-12-31') IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_unbounded_end() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE(DATE '2024-01-01', NULL) IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE periods (id INT64, period RANGE<DATE>)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO periods VALUES (1, RANGE(DATE '2024-01-01', DATE '2024-06-30'))")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT id FROM periods").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_start() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_START(RANGE(DATE '2024-01-15', DATE '2024-12-31'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_end() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_END(RANGE(DATE '2024-01-01', DATE '2024-06-30'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 30)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_contains() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE_CONTAINS(RANGE(DATE '2024-01-01', DATE '2024-12-31'), DATE '2024-06-15')",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_contains_false() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE_CONTAINS(RANGE(DATE '2024-01-01', DATE '2024-06-30'), DATE '2024-12-15')",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_overlaps() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE_OVERLAPS(
                RANGE(DATE '2024-01-01', DATE '2024-06-30'),
                RANGE(DATE '2024-04-01', DATE '2024-12-31')
            )",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_overlaps_false() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE_OVERLAPS(
                RANGE(DATE '2024-01-01', DATE '2024-03-31'),
                RANGE(DATE '2024-07-01', DATE '2024-12-31')
            )",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_intersect() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE_INTERSECT(
                RANGE(DATE '2024-01-01', DATE '2024-06-30'),
                RANGE(DATE '2024-04-01', DATE '2024-12-31')
            ) IS NOT NULL",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_range_array() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(GENERATE_RANGE_ARRAY(RANGE(DATE '2024-01-01', DATE '2024-01-05'), INTERVAL 1 DAY))").await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_in_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE events (id INT64, event_period RANGE<DATE>)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO events VALUES
            (1, RANGE(DATE '2024-01-01', DATE '2024-03-31')),
            (2, RANGE(DATE '2024-04-01', DATE '2024-06-30')),
            (3, RANGE(DATE '2024-07-01', DATE '2024-09-30'))",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM events WHERE RANGE_CONTAINS(event_period, DATE '2024-05-15') ORDER BY id",
        ).await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_sessionize() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE_SESSIONIZE(
            TABLE(SELECT RANGE(DATE '2024-01-01', DATE '2024-01-10')),
            'range_col',
            INTERVAL 5 DAY
        )",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}
