use crate::assert_table_eq;
use crate::common::{create_session, d};

#[tokio::test(flavor = "current_thread")]
async fn test_interval_day() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM INTERVAL 5 DAY)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_month() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MONTH FROM INTERVAL 3 MONTH)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_year() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(YEAR FROM INTERVAL 2 YEAR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_hour() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(HOUR FROM INTERVAL 12 HOUR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[12]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_minute() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MINUTE FROM INTERVAL 30 MINUTE)")
        .await
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_second() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(SECOND FROM INTERVAL 45 SECOND)")
        .await
        .unwrap();
    assert_table_eq!(result, [[45]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_addition_to_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 10 DAY")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 25)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_subtraction_from_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' - INTERVAL 10 DAY")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 5)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_month_addition() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 2 MONTH")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 3, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_year_addition() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 1 YEAR")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2025, 1, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_timestamp_addition() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-01-15 10:00:00' + INTERVAL 2 HOUR)")
        .await
        .unwrap();
    assert_table_eq!(result, [[12]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_negative() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL -5 DAY")
        .await
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 10)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_multiplication() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM INTERVAL 1 DAY * 5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_in_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE durations (id INT64, duration INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO durations VALUES (1, INTERVAL 5 DAY), (2, INTERVAL 10 DAY)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM durations ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_with_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE events (id INT64, start_date DATE, duration_days INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO events VALUES (1, '2024-01-01', 5), (2, '2024-01-15', 10)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, start_date + MAKE_INTERVAL(day => duration_days) AS end_date FROM events ORDER BY id").await
        .unwrap();
    assert_table_eq!(result, [[1, d(2024, 1, 6)], [2, d(2024, 1, 25)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM MAKE_INTERVAL(year => 1, month => 2, day => 3))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_make_interval_time() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT EXTRACT(MINUTE FROM MAKE_INTERVAL(hour => 1, minute => 30, second => 45))",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_from_interval() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM INTERVAL 5 DAY)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_extract_month_from_interval() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MONTH FROM INTERVAL 3 MONTH)")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_comparison() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INTERVAL 5 DAY > INTERVAL 3 DAY")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_interval_() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE durations (id INT64, duration INTERVAL)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO durations VALUES (1, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM durations WHERE duration IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_justify_days() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MONTH FROM JUSTIFY_DAYS(INTERVAL 35 DAY))")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_justify_hours() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM JUSTIFY_HOURS(INTERVAL 30 HOUR))")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_justify_interval() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT EXTRACT(MONTH FROM JUSTIFY_INTERVAL(MAKE_INTERVAL(month => 1, day => 35, hour => 30)))",
        ).await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}
