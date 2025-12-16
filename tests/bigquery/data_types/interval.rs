use crate::assert_table_eq;
use crate::common::{create_executor, d};

#[test]
#[ignore = "Implement me!"]
fn test_interval_day() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(DAY FROM INTERVAL 5 DAY)")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_month() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(MONTH FROM INTERVAL 3 MONTH)")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_year() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(YEAR FROM INTERVAL 2 YEAR)")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_hour() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(HOUR FROM INTERVAL 12 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[12]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_minute() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(MINUTE FROM INTERVAL 30 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_second() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(SECOND FROM INTERVAL 45 SECOND)")
        .unwrap();
    assert_table_eq!(result, [[45]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_addition_to_date() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 10 DAY")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 25)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_subtraction_from_date() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE '2024-01-15' - INTERVAL 10 DAY")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 5)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_month_addition() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 2 MONTH")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 3, 15)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_year_addition() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL 1 YEAR")
        .unwrap();
    assert_table_eq!(result, [[d(2025, 1, 15)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_timestamp_addition() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-01-15 10:00:00' + INTERVAL 2 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[12]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_negative() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE '2024-01-15' + INTERVAL -5 DAY")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 10)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_multiplication() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(DAY FROM INTERVAL 1 DAY * 5)")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE durations (id INT64, duration INTERVAL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO durations VALUES (1, INTERVAL 5 DAY), (2, INTERVAL 10 DAY)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM durations ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
#[ignore = "MAKE_INTERVAL not yet available in BigQuery dialect"]
fn test_interval_with_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (id INT64, start_date DATE, duration_days INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES (1, '2024-01-01', 5), (2, '2024-01-15', 10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, start_date + MAKE_INTERVAL(day => duration_days) AS end_date FROM events ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, d(2024, 1, 6)], [2, d(2024, 1, 25)]]);
}

#[test]
#[ignore = "MAKE_INTERVAL not yet available in BigQuery dialect"]
fn test_make_interval() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(DAY FROM MAKE_INTERVAL(year => 1, month => 2, day => 3))")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "MAKE_INTERVAL not yet available in BigQuery dialect"]
fn test_make_interval_time() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT EXTRACT(MINUTE FROM MAKE_INTERVAL(hour => 1, minute => 30, second => 45))",
        )
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_extract_from_interval() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(DAY FROM INTERVAL 5 DAY)")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_extract_month_from_interval() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(MONTH FROM INTERVAL 3 MONTH)")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_comparison() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INTERVAL 5 DAY > INTERVAL 3 DAY")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE durations (id INT64, duration INTERVAL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO durations VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM durations WHERE duration IS NULL")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_justify_days() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(MONTH FROM JUSTIFY_DAYS(INTERVAL 35 DAY))")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_justify_hours() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(DAY FROM JUSTIFY_HOURS(INTERVAL 30 HOUR))")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_justify_interval() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT EXTRACT(MONTH FROM JUSTIFY_INTERVAL(INTERVAL '1 month 35 days 30 hours'))",
        )
        .unwrap();
    assert_table_eq!(result, [[2]]);
}
