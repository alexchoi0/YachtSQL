use crate::assert_table_eq;
use crate::common::{create_executor, datetime};

#[ignore = "Implement me!"]
#[test]
fn test_tumble() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tumble_test (ts DateTime, value Int64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO tumble_test VALUES
            ('2023-01-01 10:05:00', 10),
            ('2023-01-01 10:15:00', 20),
            ('2023-01-01 10:25:00', 30),
            ('2023-01-01 10:35:00', 40)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT tumble(ts, INTERVAL 20 MINUTE), sum(value)
            FROM tumble_test
            GROUP BY tumble(ts, INTERVAL 20 MINUTE)
            ORDER BY tumble(ts, INTERVAL 20 MINUTE)",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [datetime(2023, 1, 1, 10, 0, 0), 10_i64],
            [datetime(2023, 1, 1, 10, 20, 0), 50_i64],
        ]
    );
}

#[ignore = "Implement me!"]
#[test]
fn test_tumble_start() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tumbleStart(toDateTime('2023-01-01 10:15:00'), INTERVAL 1 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[datetime(2023, 1, 1, 10, 0, 0)]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_tumble_end() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tumbleEnd(toDateTime('2023-01-01 10:15:00'), INTERVAL 1 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[datetime(2023, 1, 1, 11, 0, 0)]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_hop() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE hop_test (ts DateTime, metric Int64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO hop_test VALUES
            ('2023-01-01 10:00:00', 100),
            ('2023-01-01 10:10:00', 110),
            ('2023-01-01 10:20:00', 120),
            ('2023-01-01 10:30:00', 130)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT hop(ts, INTERVAL 10 MINUTE, INTERVAL 20 MINUTE), avg(metric)
            FROM hop_test
            GROUP BY hop(ts, INTERVAL 10 MINUTE, INTERVAL 20 MINUTE)
            ORDER BY 1",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [datetime(2023, 1, 1, 9, 50, 0), 100.0],
            [datetime(2023, 1, 1, 10, 0, 0), 105.0],
            [datetime(2023, 1, 1, 10, 10, 0), 115.0],
            [datetime(2023, 1, 1, 10, 20, 0), 125.0],
        ]
    );
}

#[ignore = "Implement me!"]
#[test]
fn test_hop_start() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT hopStart(toDateTime('2023-01-01 10:15:00'), INTERVAL 10 MINUTE, INTERVAL 30 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[datetime(2023, 1, 1, 10, 0, 0)]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_hop_end() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT hopEnd(toDateTime('2023-01-01 10:15:00'), INTERVAL 10 MINUTE, INTERVAL 30 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[datetime(2023, 1, 1, 10, 30, 0)]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_time_slot() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT timeSlot(toDateTime('2023-01-01 10:47:00'))")
        .unwrap();
    assert_table_eq!(result, [[datetime(2023, 1, 1, 10, 30, 0)]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_time_slots() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT timeSlots(toDateTime('2023-01-01 10:00:00'), toUInt32(7200))")
        .unwrap();
    assert_table_eq!(
        result,
        [[[
            datetime(2023, 1, 1, 10, 0, 0),
            datetime(2023, 1, 1, 10, 30, 0),
            datetime(2023, 1, 1, 11, 0, 0),
            datetime(2023, 1, 1, 11, 30, 0)
        ]]]
    );
}

#[ignore = "Implement me!"]
#[test]
fn test_time_slots_with_size() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT timeSlots(toDateTime('2023-01-01 10:00:00'), toUInt32(7200), 1800)")
        .unwrap();
    assert_table_eq!(
        result,
        [[[
            datetime(2023, 1, 1, 10, 0, 0),
            datetime(2023, 1, 1, 10, 30, 0),
            datetime(2023, 1, 1, 11, 0, 0),
            datetime(2023, 1, 1, 11, 30, 0)
        ]]]
    );
}

#[ignore = "Implement me!"]
#[test]
fn test_tumble_aggregation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (event_time DateTime, user_id UInt32, event_type String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO events VALUES
            ('2023-01-01 10:00:00', 1, 'click'),
            ('2023-01-01 10:05:00', 2, 'view'),
            ('2023-01-01 10:10:00', 1, 'click'),
            ('2023-01-01 10:20:00', 3, 'purchase'),
            ('2023-01-01 10:25:00', 1, 'view'),
            ('2023-01-01 10:35:00', 2, 'click')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                tumbleStart(event_time, INTERVAL 15 MINUTE) AS window_start,
                tumbleEnd(event_time, INTERVAL 15 MINUTE) AS window_end,
                count() AS event_count,
                uniq(user_id) AS unique_users
            FROM events
            GROUP BY tumble(event_time, INTERVAL 15 MINUTE)
            ORDER BY window_start",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [
                datetime(2023, 1, 1, 10, 0, 0),
                datetime(2023, 1, 1, 10, 15, 0),
                3_i64,
                2_u64
            ],
            [
                datetime(2023, 1, 1, 10, 15, 0),
                datetime(2023, 1, 1, 10, 30, 0),
                2_i64,
                2_u64
            ],
            [
                datetime(2023, 1, 1, 10, 30, 0),
                datetime(2023, 1, 1, 10, 45, 0),
                1_i64,
                1_u64
            ],
        ]
    );
}

#[ignore = "Implement me!"]
#[test]
fn test_hop_sliding_window() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE metrics (ts DateTime, cpu Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO metrics VALUES
            ('2023-01-01 10:00:00', 50.0),
            ('2023-01-01 10:05:00', 55.0),
            ('2023-01-01 10:10:00', 60.0),
            ('2023-01-01 10:15:00', 65.0),
            ('2023-01-01 10:20:00', 70.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                hopStart(ts, INTERVAL 5 MINUTE, INTERVAL 15 MINUTE) AS window_start,
                avg(cpu) AS avg_cpu
            FROM metrics
            GROUP BY hop(ts, INTERVAL 5 MINUTE, INTERVAL 15 MINUTE)
            ORDER BY window_start",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [datetime(2023, 1, 1, 9, 50, 0), 50.0],
            [datetime(2023, 1, 1, 9, 55, 0), 52.5],
            [datetime(2023, 1, 1, 10, 0, 0), 55.0],
            [datetime(2023, 1, 1, 10, 5, 0), 60.0],
            [datetime(2023, 1, 1, 10, 10, 0), 65.0],
        ]
    );
}

#[ignore = "Implement me!"]
#[test]
fn test_date_bin() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT date_bin(INTERVAL 15 MINUTE, toDateTime('2023-01-01 10:47:00'))")
        .unwrap();
    assert_table_eq!(result, [[datetime(2023, 1, 1, 10, 45, 0)]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_date_bin_with_origin() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT date_bin(INTERVAL 15 MINUTE, toDateTime('2023-01-01 10:47:00'), toDateTime('2023-01-01 00:00:00'))"
        )
        .unwrap();
    assert_table_eq!(result, [[datetime(2023, 1, 1, 10, 45, 0)]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_window_id() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE window_id_test (ts DateTime, val Int64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO window_id_test VALUES
            ('2023-01-01 10:00:00', 1),
            ('2023-01-01 11:00:00', 2),
            ('2023-01-01 12:00:00', 3)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT ts, val, windowID(tumble(ts, INTERVAL 1 HOUR))
            FROM window_id_test
            ORDER BY ts",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [datetime(2023, 1, 1, 10, 0, 0), 1_i64, 1_u64],
            [datetime(2023, 1, 1, 11, 0, 0), 2_i64, 2_u64],
            [datetime(2023, 1, 1, 12, 0, 0), 3_i64, 3_u64],
        ]
    );
}
