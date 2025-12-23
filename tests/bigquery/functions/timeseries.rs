use crate::assert_table_eq;
use crate::common::{create_session, d, dt, null, ts};

#[test]
fn test_date_bucket_basic() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-06-15', INTERVAL 7 DAY)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 13)]]);
}

#[test]
#[ignore]
fn test_date_bucket_2_day() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "WITH some_dates AS (
                SELECT DATE '1949-12-29' AS my_date UNION ALL
                SELECT DATE '1949-12-30' UNION ALL
                SELECT DATE '1949-12-31' UNION ALL
                SELECT DATE '1950-01-01' UNION ALL
                SELECT DATE '1950-01-02' UNION ALL
                SELECT DATE '1950-01-03'
            )
            SELECT DATE_BUCKET(my_date, INTERVAL 2 DAY) AS bucket_lower_bound
            FROM some_dates
            ORDER BY my_date",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(1949, 12, 28)],
            [d(1949, 12, 30)],
            [d(1949, 12, 30)],
            [d(1950, 1, 1)],
            [d(1950, 1, 1)],
            [d(1950, 1, 3)]
        ]
    );
}

#[test]
#[ignore]
fn test_date_bucket_with_origin() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "WITH some_dates AS (
                SELECT DATE '2000-12-20' AS my_date UNION ALL
                SELECT DATE '2000-12-21' UNION ALL
                SELECT DATE '2000-12-22' UNION ALL
                SELECT DATE '2000-12-23' UNION ALL
                SELECT DATE '2000-12-24' UNION ALL
                SELECT DATE '2000-12-25'
            )
            SELECT DATE_BUCKET(my_date, INTERVAL 7 DAY, DATE '2000-12-24') AS bucket_lower_bound
            FROM some_dates
            ORDER BY my_date",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(2000, 12, 17)],
            [d(2000, 12, 17)],
            [d(2000, 12, 17)],
            [d(2000, 12, 17)],
            [d(2000, 12, 24)],
            [d(2000, 12, 24)]
        ]
    );
}

#[test]
fn test_date_bucket_with_null() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATE_BUCKET(NULL, INTERVAL 7 DAY)")
        .unwrap();
    assert!(result.get_row(0).unwrap().values()[0].is_null());
}

#[test]
#[ignore]
fn test_date_bucket_month_interval() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-06-15', INTERVAL 1 MONTH)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 5, 31)]]);
}

#[test]
fn test_datetime_bucket_basic() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_BUCKET(DATETIME '2024-06-15 14:30:00', INTERVAL 12 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 12, 0, 0)]]);
}

#[test]
#[ignore]
fn test_datetime_bucket_12_hour() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "WITH some_datetimes AS (
                SELECT DATETIME '1949-12-30 13:00:00' AS my_datetime UNION ALL
                SELECT DATETIME '1949-12-31 00:00:00' UNION ALL
                SELECT DATETIME '1949-12-31 13:00:00' UNION ALL
                SELECT DATETIME '1950-01-01 00:00:00' UNION ALL
                SELECT DATETIME '1950-01-01 13:00:00' UNION ALL
                SELECT DATETIME '1950-01-02 00:00:00'
            )
            SELECT DATETIME_BUCKET(my_datetime, INTERVAL 12 HOUR) AS bucket_lower_bound
            FROM some_datetimes
            ORDER BY my_datetime",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(1949, 12, 30, 12, 0, 0)],
            [dt(1949, 12, 31, 0, 0, 0)],
            [dt(1949, 12, 31, 12, 0, 0)],
            [dt(1950, 1, 1, 0, 0, 0)],
            [dt(1950, 1, 1, 12, 0, 0)],
            [dt(1950, 1, 2, 0, 0, 0)]
        ]
    );
}

#[test]
#[ignore]
fn test_datetime_bucket_with_origin() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "WITH some_datetimes AS (
                SELECT DATETIME '2000-12-20 00:00:00' AS my_datetime UNION ALL
                SELECT DATETIME '2000-12-21 00:00:00' UNION ALL
                SELECT DATETIME '2000-12-22 00:00:00' UNION ALL
                SELECT DATETIME '2000-12-23 00:00:00' UNION ALL
                SELECT DATETIME '2000-12-24 00:00:00' UNION ALL
                SELECT DATETIME '2000-12-25 00:00:00'
            )
            SELECT DATETIME_BUCKET(my_datetime, INTERVAL 7 DAY, DATETIME '2000-12-22 12:00:00') AS bucket_lower_bound
            FROM some_datetimes
            ORDER BY my_datetime",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2000, 12, 15, 12, 0, 0)],
            [dt(2000, 12, 15, 12, 0, 0)],
            [dt(2000, 12, 15, 12, 0, 0)],
            [dt(2000, 12, 22, 12, 0, 0)],
            [dt(2000, 12, 22, 12, 0, 0)],
            [dt(2000, 12, 22, 12, 0, 0)]
        ]
    );
}

#[test]
fn test_datetime_bucket_with_null() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_BUCKET(NULL, INTERVAL 12 HOUR)")
        .unwrap();
    assert!(result.get_row(0).unwrap().values()[0].is_null());
}

#[test]
fn test_datetime_bucket_minute_interval() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_BUCKET(DATETIME '2024-06-15 14:37:00', INTERVAL 15 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 14, 30, 0)]]);
}

#[test]
fn test_timestamp_bucket_basic() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_BUCKET(TIMESTAMP '2024-06-15 14:30:00', INTERVAL 12 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 12, 0, 0)]]);
}

#[test]
#[ignore]
fn test_timestamp_bucket_12_hour() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "WITH some_timestamps AS (
                SELECT TIMESTAMP '1949-12-30 13:00:00' AS my_timestamp UNION ALL
                SELECT TIMESTAMP '1949-12-31 00:00:00' UNION ALL
                SELECT TIMESTAMP '1949-12-31 13:00:00' UNION ALL
                SELECT TIMESTAMP '1950-01-01 00:00:00' UNION ALL
                SELECT TIMESTAMP '1950-01-01 13:00:00' UNION ALL
                SELECT TIMESTAMP '1950-01-02 00:00:00'
            )
            SELECT TIMESTAMP_BUCKET(my_timestamp, INTERVAL 12 HOUR) AS bucket_lower_bound
            FROM some_timestamps
            ORDER BY my_timestamp",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [ts(1949, 12, 30, 12, 0, 0)],
            [ts(1949, 12, 31, 0, 0, 0)],
            [ts(1949, 12, 31, 12, 0, 0)],
            [ts(1950, 1, 1, 0, 0, 0)],
            [ts(1950, 1, 1, 12, 0, 0)],
            [ts(1950, 1, 2, 0, 0, 0)]
        ]
    );
}

#[test]
#[ignore]
fn test_timestamp_bucket_with_origin() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "WITH some_timestamps AS (
                SELECT TIMESTAMP '2000-12-20 00:00:00' AS my_timestamp UNION ALL
                SELECT TIMESTAMP '2000-12-21 00:00:00' UNION ALL
                SELECT TIMESTAMP '2000-12-22 00:00:00' UNION ALL
                SELECT TIMESTAMP '2000-12-23 00:00:00' UNION ALL
                SELECT TIMESTAMP '2000-12-24 00:00:00' UNION ALL
                SELECT TIMESTAMP '2000-12-25 00:00:00'
            )
            SELECT TIMESTAMP_BUCKET(my_timestamp, INTERVAL 7 DAY, TIMESTAMP '2000-12-22 12:00:00') AS bucket_lower_bound
            FROM some_timestamps
            ORDER BY my_timestamp",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [ts(2000, 12, 15, 12, 0, 0)],
            [ts(2000, 12, 15, 12, 0, 0)],
            [ts(2000, 12, 15, 12, 0, 0)],
            [ts(2000, 12, 22, 12, 0, 0)],
            [ts(2000, 12, 22, 12, 0, 0)],
            [ts(2000, 12, 22, 12, 0, 0)]
        ]
    );
}

#[test]
fn test_timestamp_bucket_with_null() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_BUCKET(NULL, INTERVAL 12 HOUR)")
        .unwrap();
    assert!(result.get_row(0).unwrap().values()[0].is_null());
}

#[test]
fn test_timestamp_bucket_minute_interval() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_BUCKET(TIMESTAMP '2024-06-15 14:37:00', INTERVAL 15 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 14, 30, 0)]]);
}

#[test]
#[ignore]
fn test_gap_fill_locf() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE device_data AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<device_id INT64, time DATETIME, signal INT64, state STRING>>[
                    STRUCT(1, DATETIME '2023-11-01 09:34:01', 74, 'INACTIVE'),
                    STRUCT(2, DATETIME '2023-11-01 09:36:00', 77, 'ACTIVE'),
                    STRUCT(3, DATETIME '2023-11-01 09:37:00', 78, 'ACTIVE'),
                    STRUCT(4, DATETIME '2023-11-01 09:38:01', 80, 'ACTIVE')
                ]
            )",
        )
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE device_data,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [
                    ('signal', 'locf')
                ]
            )
            ORDER BY time",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 35, 0), 74],
            [dt(2023, 11, 1, 9, 36, 0), 77],
            [dt(2023, 11, 1, 9, 37, 0), 78],
            [dt(2023, 11, 1, 9, 38, 0), 78]
        ]
    );
}

#[test]
#[ignore]
fn test_gap_fill_linear() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE device_data AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<device_id INT64, time DATETIME, signal INT64, state STRING>>[
                    STRUCT(1, DATETIME '2023-11-01 09:34:01', 74, 'INACTIVE'),
                    STRUCT(2, DATETIME '2023-11-01 09:36:00', 77, 'ACTIVE'),
                    STRUCT(3, DATETIME '2023-11-01 09:37:00', 78, 'ACTIVE'),
                    STRUCT(4, DATETIME '2023-11-01 09:38:01', 80, 'ACTIVE')
                ]
            )",
        )
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE device_data,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [
                    ('signal', 'linear')
                ]
            )
            ORDER BY time",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 35, 0), 75],
            [dt(2023, 11, 1, 9, 36, 0), 77],
            [dt(2023, 11, 1, 9, 37, 0), 78],
            [dt(2023, 11, 1, 9, 38, 0), 80]
        ]
    );
}

#[test]
#[ignore]
fn test_gap_fill_null() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE device_data AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<device_id INT64, time DATETIME, signal INT64, state STRING>>[
                    STRUCT(1, DATETIME '2023-11-01 09:34:01', 74, 'INACTIVE'),
                    STRUCT(2, DATETIME '2023-11-01 09:36:00', 77, 'ACTIVE'),
                    STRUCT(3, DATETIME '2023-11-01 09:37:00', 78, 'ACTIVE'),
                    STRUCT(4, DATETIME '2023-11-01 09:38:01', 80, 'ACTIVE')
                ]
            )",
        )
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT time, signal
            FROM GAP_FILL(
                TABLE device_data,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [
                    ('signal', 'null')
                ]
            )
            ORDER BY time",
        )
        .unwrap();
    assert!(result.get_row(0).unwrap().values()[1].is_null());
    assert_eq!(result.get_row(1).unwrap().values()[1].as_i64().unwrap(), 77);
    assert_eq!(result.get_row(2).unwrap().values()[1].as_i64().unwrap(), 78);
    assert!(result.get_row(3).unwrap().values()[1].is_null());
}

#[test]
#[ignore]
fn test_gap_fill_with_partitions() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE device_data AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<device_id INT64, time DATETIME, signal INT64, state STRING>>[
                    STRUCT(2, DATETIME '2023-11-01 09:35:07', 87, 'ACTIVE'),
                    STRUCT(1, DATETIME '2023-11-01 09:35:26', 82, 'ACTIVE'),
                    STRUCT(3, DATETIME '2023-11-01 09:35:39', 74, 'INACTIVE'),
                    STRUCT(2, DATETIME '2023-11-01 09:36:07', 88, 'ACTIVE'),
                    STRUCT(1, DATETIME '2023-11-01 09:36:26', 82, 'ACTIVE'),
                    STRUCT(2, DATETIME '2023-11-01 09:37:07', 88, 'ACTIVE'),
                    STRUCT(1, DATETIME '2023-11-01 09:37:28', 80, 'ACTIVE'),
                    STRUCT(3, DATETIME '2023-11-01 09:37:39', 77, 'ACTIVE'),
                    STRUCT(2, DATETIME '2023-11-01 09:38:07', 86, 'ACTIVE'),
                    STRUCT(1, DATETIME '2023-11-01 09:38:26', 81, 'ACTIVE'),
                    STRUCT(3, DATETIME '2023-11-01 09:38:39', 77, 'ACTIVE')
                ]
            )",
        )
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE device_data,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                partitioning_columns => ['device_id'],
                value_columns => [
                    ('signal', 'locf')
                ]
            )
            ORDER BY device_id, time",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 36, 0), 1, 82],
            [dt(2023, 11, 1, 9, 37, 0), 1, 82],
            [dt(2023, 11, 1, 9, 38, 0), 1, 80],
            [dt(2023, 11, 1, 9, 36, 0), 2, 87],
            [dt(2023, 11, 1, 9, 37, 0), 2, 88],
            [dt(2023, 11, 1, 9, 38, 0), 2, 88],
            [dt(2023, 11, 1, 9, 36, 0), 3, 74],
            [dt(2023, 11, 1, 9, 37, 0), 3, 74],
            [dt(2023, 11, 1, 9, 38, 0), 3, 77]
        ]
    );
}

#[test]
#[ignore]
fn test_gap_fill_multiple_columns() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE device_data AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<device_id INT64, time DATETIME, signal INT64, state STRING>>[
                    STRUCT(1, DATETIME '2023-11-01 09:34:01', 74, 'ACTIVE'),
                    STRUCT(2, DATETIME '2023-11-01 09:36:00', 77, 'INACTIVE'),
                    STRUCT(3, DATETIME '2023-11-01 09:38:00', 78, 'ACTIVE'),
                    STRUCT(4, DATETIME '2023-11-01 09:39:01', 80, 'ACTIVE')
                ]
            )",
        )
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                TABLE device_data,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [
                    ('signal', 'linear'),
                    ('state', 'locf')
                ]
            )
            ORDER BY time",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 35, 0), 75, "ACTIVE"],
            [dt(2023, 11, 1, 9, 36, 0), 77, "INACTIVE"],
            [dt(2023, 11, 1, 9, 37, 0), 78, "INACTIVE"],
            [dt(2023, 11, 1, 9, 38, 0), 78, "ACTIVE"],
            [dt(2023, 11, 1, 9, 39, 0), 80, "ACTIVE"]
        ]
    );
}

#[test]
#[ignore]
fn test_gap_fill_with_origin() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TEMP TABLE device_data AS
            SELECT * FROM UNNEST(
                ARRAY<STRUCT<device_id INT64, time DATETIME, signal INT64, state STRING>>[
                    STRUCT(1, DATETIME '2023-11-01 09:34:01', 74, 'ACTIVE'),
                    STRUCT(2, DATETIME '2023-11-01 09:36:00', 77, 'INACTIVE'),
                    STRUCT(3, DATETIME '2023-11-01 09:38:00', 78, 'ACTIVE'),
                    STRUCT(4, DATETIME '2023-11-01 09:39:01', 80, 'ACTIVE')
                ]
            )",
        )
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT time, signal
            FROM GAP_FILL(
                TABLE device_data,
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [
                    ('signal', 'null')
                ],
                origin => DATETIME '2023-11-01 09:30:01'
            )
            ORDER BY time",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 34, 1), 74],
            [dt(2023, 11, 1, 9, 35, 1), null()],
            [dt(2023, 11, 1, 9, 36, 1), null()],
            [dt(2023, 11, 1, 9, 37, 1), null()],
            [dt(2023, 11, 1, 9, 38, 1), null()],
            [dt(2023, 11, 1, 9, 39, 1), 80]
        ]
    );
}

#[test]
#[ignore]
fn test_gap_fill_subquery() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "SELECT *
            FROM GAP_FILL(
                (
                    SELECT * FROM UNNEST(
                    ARRAY<STRUCT<device_id INT64, time DATETIME, signal INT64, state STRING>>[
                        STRUCT(1, DATETIME '2023-11-01 09:34:01', 74, 'INACTIVE'),
                        STRUCT(2, DATETIME '2023-11-01 09:36:00', 77, 'ACTIVE'),
                        STRUCT(3, DATETIME '2023-11-01 09:37:00', 78, 'ACTIVE'),
                        STRUCT(4, DATETIME '2023-11-01 09:38:01', 80, 'ACTIVE')
                    ])
                ),
                ts_column => 'time',
                bucket_width => INTERVAL 1 MINUTE,
                value_columns => [
                    ('signal', 'linear')
                ]
            )
            ORDER BY time",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [dt(2023, 11, 1, 9, 35, 0), 75],
            [dt(2023, 11, 1, 9, 36, 0), 77],
            [dt(2023, 11, 1, 9, 37, 0), 78],
            [dt(2023, 11, 1, 9, 38, 0), 80]
        ]
    );
}
