use crate::assert_table_eq;
use crate::common::{create_executor, d, ts};

#[test]
fn test_gap_fill_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE metrics (
                ts TIMESTAMP,
                value INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO metrics VALUES
                (TIMESTAMP '2024-01-01 00:00:00', 10),
                (TIMESTAMP '2024-01-01 02:00:00', 30)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM GAP_FILL(
                (SELECT * FROM metrics),
                ts_column => ts,
                bucket_width => INTERVAL 1 HOUR
            ) ORDER BY ts",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            [ts(2024, 1, 1, 0, 0, 0), 10],
            [ts(2024, 1, 1, 1, 0, 0), null],
            [ts(2024, 1, 1, 2, 0, 0), 30]
        ]
    );
}

#[test]
fn test_gap_fill_with_partitions() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE sensor_data (
                sensor_id STRING,
                ts TIMESTAMP,
                reading FLOAT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO sensor_data VALUES
                ('A', TIMESTAMP '2024-01-01 00:00:00', 1.0),
                ('A', TIMESTAMP '2024-01-01 02:00:00', 3.0),
                ('B', TIMESTAMP '2024-01-01 00:00:00', 10.0),
                ('B', TIMESTAMP '2024-01-01 01:00:00', 20.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM GAP_FILL(
                (SELECT * FROM sensor_data),
                ts_column => ts,
                bucket_width => INTERVAL 1 HOUR,
                partitioning => (sensor_id)
            ) ORDER BY sensor_id, ts",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["A", ts(2024, 1, 1, 0, 0, 0), 1.0],
            ["A", ts(2024, 1, 1, 1, 0, 0), null],
            ["A", ts(2024, 1, 1, 2, 0, 0), 3.0],
            ["B", ts(2024, 1, 1, 0, 0, 0), 10.0],
            ["B", ts(2024, 1, 1, 1, 0, 0), 20.0]
        ]
    );
}

#[test]
fn test_gap_fill_multiple_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE multi_metrics (
                ts TIMESTAMP,
                temp FLOAT64,
                humidity FLOAT64,
                pressure FLOAT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO multi_metrics VALUES
                (TIMESTAMP '2024-01-01 00:00:00', 20.0, 50.0, 1013.0),
                (TIMESTAMP '2024-01-01 02:00:00', 22.0, 55.0, 1015.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM GAP_FILL(
                (SELECT * FROM multi_metrics),
                ts_column => ts,
                bucket_width => INTERVAL 1 HOUR,
                value_columns => (temp, humidity, pressure)
            ) ORDER BY ts",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            [ts(2024, 1, 1, 0, 0, 0), 20.0, 50.0, 1013.0],
            [ts(2024, 1, 1, 1, 0, 0), null, null, null],
            [ts(2024, 1, 1, 2, 0, 0), 22.0, 55.0, 1015.0]
        ]
    );
}

#[test]
fn test_gap_fill_with_origin() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE data_with_origin (
                ts TIMESTAMP,
                value INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO data_with_origin VALUES
                (TIMESTAMP '2024-01-01 01:30:00', 10),
                (TIMESTAMP '2024-01-01 03:30:00', 30)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM GAP_FILL(
                (SELECT * FROM data_with_origin),
                ts_column => ts,
                bucket_width => INTERVAL 1 HOUR,
                origin => TIMESTAMP '2024-01-01 00:30:00'
            ) ORDER BY ts",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            [ts(2024, 1, 1, 1, 30, 0), 10],
            [ts(2024, 1, 1, 2, 30, 0), null],
            [ts(2024, 1, 1, 3, 30, 0), 30]
        ]
    );
}

#[test]
fn test_gap_fill_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE raw_data (
                ts TIMESTAMP,
                category STRING,
                value INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO raw_data VALUES
                (TIMESTAMP '2024-01-01 00:00:00', 'X', 100),
                (TIMESTAMP '2024-01-01 00:00:00', 'Y', 200),
                (TIMESTAMP '2024-01-01 02:00:00', 'X', 150),
                (TIMESTAMP '2024-01-01 02:00:00', 'Y', 250)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM GAP_FILL(
                (SELECT ts, SUM(value) as total FROM raw_data GROUP BY ts),
                ts_column => ts,
                bucket_width => INTERVAL 1 HOUR
            ) ORDER BY ts",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            [ts(2024, 1, 1, 0, 0, 0), 300],
            [ts(2024, 1, 1, 1, 0, 0), null],
            [ts(2024, 1, 1, 2, 0, 0), 400]
        ]
    );
}

#[test]
fn test_gap_fill_with_date() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE daily_data (
                day DATE,
                count INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO daily_data VALUES
                (DATE '2024-01-01', 10),
                (DATE '2024-01-04', 40)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM GAP_FILL(
                (SELECT * FROM daily_data),
                ts_column => day,
                bucket_width => INTERVAL 1 DAY
            ) ORDER BY day",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            [d(2024, 1, 1), 10],
            [d(2024, 1, 2), null],
            [d(2024, 1, 3), null],
            [d(2024, 1, 4), 40]
        ]
    );
}
