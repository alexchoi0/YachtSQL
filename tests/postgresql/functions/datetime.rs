use crate::assert_table_eq;
use crate::common::{create_executor, date, timestamp};

#[test]
fn test_current_date() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CURRENT_DATE").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.num_columns(), 1);
}

#[test]
fn test_current_timestamp() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CURRENT_TIMESTAMP").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.num_columns(), 1);
}

#[test]
fn test_date_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT DATE '2024-01-15'").unwrap();
    assert_table_eq!(result, [[(date(2024, 1, 15))]]);
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
        .execute_sql("CREATE TABLE events (name TEXT, event_date DATE)")
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
        .execute_sql("CREATE TABLE events (name TEXT, event_date DATE)")
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
    assert_table_eq!(result, [[timestamp(2024, 6, 15, 10, 30, 0)]]);
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
        .execute_sql("CREATE TABLE events (name TEXT, event_date DATE)")
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
#[ignore = "Implement me!"]
fn test_extract_dayofweek() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXTRACT(DOW FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

#[test]
fn test_date_in_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (product TEXT, sale_date DATE, amount INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('A', '2024-01-01', 100), ('B', '2024-01-01', 200), ('C', '2024-01-02', 150)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT sale_date, SUM(amount) FROM sales GROUP BY sale_date ORDER BY sale_date",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [[(date(2024, 1, 1)), 300], [(date(2024, 1, 2)), 150]]
    );
}
