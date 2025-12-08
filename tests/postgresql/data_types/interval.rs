use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_interval_literal_days() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT INTERVAL '5 days'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_interval_literal_hours() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT INTERVAL '3 hours'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_interval_literal_minutes() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INTERVAL '30 minutes'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_interval_literal_seconds() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INTERVAL '45 seconds'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_interval_literal_combined() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INTERVAL '1 day 2 hours 30 minutes'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_interval_literal_months() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT INTERVAL '3 months'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_interval_literal_years() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT INTERVAL '2 years'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_interval_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tasks (id INTEGER, duration INTERVAL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tasks VALUES (1, INTERVAL '2 hours')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM tasks").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_interval_addition() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INTERVAL '1 day' + INTERVAL '2 hours'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_interval_subtraction() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INTERVAL '5 days' - INTERVAL '2 days'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_interval_multiplication() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INTERVAL '1 hour' * 3")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_interval_division() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INTERVAL '6 hours' / 2")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_date_plus_interval() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE '2024-01-01' + INTERVAL '5 days'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_timestamp_plus_interval() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIMESTAMP '2024-01-01 10:00:00' + INTERVAL '3 hours'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_timestamp_minus_interval() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIMESTAMP '2024-01-01 10:00:00' - INTERVAL '2 hours'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_extract_days() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(DAY FROM INTERVAL '5 days 3 hours')")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_extract_hours() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(HOUR FROM INTERVAL '5 days 3 hours')")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_interval_comparison() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INTERVAL '2 days' > INTERVAL '1 day'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_interval_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE interval_null (id INTEGER, dur INTERVAL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO interval_null VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT dur IS NULL FROM interval_null")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_interval_negative() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT INTERVAL '-3 days'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_interval_iso_format() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT INTERVAL 'P1Y2M3D'").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_age_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT AGE(TIMESTAMP '2024-06-15', TIMESTAMP '2024-01-01')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_justify_days() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JUSTIFY_DAYS(INTERVAL '35 days')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_justify_hours() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JUSTIFY_HOURS(INTERVAL '27 hours')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_justify_interval() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JUSTIFY_INTERVAL(INTERVAL '1 month -1 day')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}
