use crate::assert_table_eq;
use crate::common::{create_executor, d};

#[test]
fn test_date_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (id INT64, event_date DATE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES (1, '2024-01-15'), (2, '2024-06-30')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM events WHERE event_date = DATE '2024-01-15'")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_date_range_query() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (name STRING, event_date DATE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES ('A', '2024-01-01'), ('B', '2024-03-15'), ('C', '2024-06-30'), ('D', '2024-12-31')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM events WHERE event_date >= DATE '2024-03-01' AND event_date <= DATE '2024-07-01' ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["B"], ["C"]]);
}

#[test]
fn test_timestamp_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE logs (id INT64, created_at TIMESTAMP)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO logs VALUES (1, '2024-01-15 10:30:00'), (2, '2024-01-15 14:45:30')",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT COUNT(*) FROM logs").unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_date_null_handling() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (name STRING, event_date DATE)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO events VALUES ('A', '2024-01-01'), ('B', NULL), ('C', '2024-06-30')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM events WHERE event_date IS NOT NULL ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["A"], ["C"]]);
}

#[test]
fn test_date_sorting() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (name STRING, event_date DATE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES ('C', '2024-12-01'), ('A', '2024-01-01'), ('B', '2024-06-15')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM events ORDER BY event_date DESC")
        .unwrap();
    assert_table_eq!(result, [["C"], ["B"], ["A"]]);
}

#[test]
fn test_date_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (product STRING, sale_date DATE, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('A', '2024-01-01', 100), ('B', '2024-01-01', 150), ('C', '2024-01-02', 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sale_date, SUM(amount) AS total FROM sales GROUP BY sale_date ORDER BY sale_date")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1), 250], [d(2024, 1, 2), 200]]);
}

#[test]
fn test_timestamp_with_timezone_string() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE logs (message STRING, created_at TIMESTAMP)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO logs VALUES ('test', '2024-06-15 10:30:00')")
        .unwrap();

    let result = executor.execute_sql("SELECT message FROM logs").unwrap();
    assert_table_eq!(result, [["test"]]);
}

#[test]
fn test_date_between() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (name STRING, event_date DATE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES ('A', '2024-01-01'), ('B', '2024-03-15'), ('C', '2024-06-30'), ('D', '2024-12-31')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM events WHERE event_date BETWEEN DATE '2024-03-01' AND DATE '2024-07-01' ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["B"], ["C"]]);
}

#[test]
fn test_date_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (name STRING, event_date DATE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES ('A', '2024-01-01'), ('B', '2024-01-01'), ('C', '2024-06-30')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT event_date FROM events ORDER BY event_date")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1)], [d(2024, 6, 30)]]);
}
