use crate::assert_table_eq;
use crate::common::{create_executor, d, dt, tm, ts};

#[test]
fn test_current_date() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CURRENT_DATE IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_current_timestamp() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CURRENT_TIMESTAMP IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_date_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT DATE '2024-01-15'").unwrap();
    assert_table_eq!(result, [[d(2024, 1, 15)]]);
}

#[test]
fn test_extract_year() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(YEAR FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[2024]]);
}

#[test]
fn test_extract_month() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(MONTH FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

#[test]
fn test_extract_day() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(DAY FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
fn test_date_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (name STRING, event_date DATE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES ('A', '2024-01-01'), ('B', '2024-06-15'), ('C', '2024-12-31')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM events WHERE event_date > DATE '2024-06-01' ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["B"], ["C"]]);
}

#[test]
fn test_date_ordering() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (name STRING, event_date DATE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES ('C', '2024-12-31'), ('A', '2024-01-01'), ('B', '2024-06-15')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM events ORDER BY event_date")
        .unwrap();
    assert_table_eq!(result, [["A"], ["B"], ["C"]]);
}

#[test]
fn test_timestamp_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIMESTAMP '2024-06-15 10:30:00'")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 10, 30, 0)]]);
}

#[test]
fn test_extract_hour() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 14:30:45')")
        .unwrap();
    assert_table_eq!(result, [[14]]);
}

#[test]
fn test_extract_minute() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(MINUTE FROM TIMESTAMP '2024-06-15 14:30:45')")
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_extract_second() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(SECOND FROM TIMESTAMP '2024-06-15 14:30:45')")
        .unwrap();
    assert_table_eq!(result, [[45]]);
}

#[test]
fn test_date_with_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (name STRING, event_date DATE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES ('A', '2024-01-01'), ('B', NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM events WHERE event_date IS NULL")
        .unwrap();
    assert_table_eq!(result, [["B"]]);
}

#[test]
fn test_extract_dayofweek() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
fn test_date_in_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (product STRING, sale_date DATE, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('A', '2024-01-01', 100), ('B', '2024-01-01', 200), ('C', '2024-01-02', 150)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT sale_date, SUM(amount) FROM sales GROUP BY sale_date ORDER BY sale_date",
        )
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1), 300], [d(2024, 1, 2), 150]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_date_add() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 10 DAY)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 25)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_date_sub() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 10 DAY)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 5)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_date_diff() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE_DIFF(DATE '2024-01-20', DATE '2024-01-10', DAY)")
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_date_trunc() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE_TRUNC(DATE '2024-06-15', MONTH)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 1)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_date_from_unix_date() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE_FROM_UNIX_DATE(19723)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_unix_date() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT UNIX_DATE(DATE '2024-01-01')")
        .unwrap();
    assert_table_eq!(result, [[19723]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_parse_date() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PARSE_DATE('%Y-%m-%d', '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 15)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_format_date() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FORMAT_DATE('%Y/%m/%d', DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [["2024/06/15"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_last_day() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LAST_DAY(DATE '2024-02-15')")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 2, 29)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_timestamp_add() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:00:00', INTERVAL 1 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 15, 11, 0, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_timestamp_sub() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIMESTAMP_SUB(TIMESTAMP '2024-01-15 10:00:00', INTERVAL 1 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 15, 9, 0, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_timestamp_diff() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 12:00:00', TIMESTAMP '2024-01-15 10:00:00', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_timestamp_trunc() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:45', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 14, 0, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_parse_timestamp() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-06-15 14:30:00')")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 14, 30, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_format_timestamp() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FORMAT_TIMESTAMP('%Y/%m/%d %H:%M', TIMESTAMP '2024-06-15 14:30:00')")
        .unwrap();
    assert_table_eq!(result, [["2024/06/15 14:30"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_unix_seconds() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT UNIX_SECONDS(TIMESTAMP '2024-01-01 00:00:00 UTC')")
        .unwrap();
    assert_table_eq!(result, [[1704067200]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_timestamp_seconds() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIMESTAMP_SECONDS(1704067200)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 1, 0, 0, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_unix_millis() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT UNIX_MILLIS(TIMESTAMP '2024-01-01 00:00:00 UTC')")
        .unwrap();
    assert_table_eq!(result, [[1704067200000i64]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_timestamp_millis() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIMESTAMP_MILLIS(1704067200000)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 1, 0, 0, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_unix_micros() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT UNIX_MICROS(TIMESTAMP '2024-01-01 00:00:00 UTC')")
        .unwrap();
    assert_table_eq!(result, [[1704067200000000i64]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_timestamp_micros() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIMESTAMP_MICROS(1704067200000000)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 1, 0, 0, 0)]]);
}

#[test]
fn test_time_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TIME '14:30:00'").unwrap();
    assert_table_eq!(result, [[tm(14, 30, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_time_add() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIME_ADD(TIME '10:00:00', INTERVAL 30 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[tm(10, 30, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_time_sub() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIME_SUB(TIME '10:00:00', INTERVAL 30 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[tm(9, 30, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_time_diff() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIME_DIFF(TIME '14:30:00', TIME '10:00:00', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_time_trunc() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TIME_TRUNC(TIME '14:30:45', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[tm(14, 0, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_datetime_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATETIME '2024-06-15 14:30:00'")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 14, 30, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_datetime_add() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATETIME_ADD(DATETIME '2024-01-15 10:00:00', INTERVAL 1 DAY)")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 1, 16, 10, 0, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_datetime_sub() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATETIME_SUB(DATETIME '2024-01-15 10:00:00', INTERVAL 1 DAY)")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 1, 14, 10, 0, 0)]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_datetime_diff() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATETIME_DIFF(DATETIME '2024-01-20 10:00:00', DATETIME '2024-01-15 10:00:00', DAY)")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_datetime_trunc() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:45', MONTH)")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 1, 0, 0, 0)]]);
}

#[test]
fn test_extract_week() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(WEEK FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[24]]);
}

#[test]
fn test_extract_quarter() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(QUARTER FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_extract_dayofyear() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[167]]);
}
