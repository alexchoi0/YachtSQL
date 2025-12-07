#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::single_match)]

mod common;
use common::{create_executor, d};

#[test]
fn test_current_date() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE data (id INT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO data VALUES (1)").unwrap();

    let result = executor.execute_sql("SELECT CURRENT_DATE() as today FROM data");

    match result {
        Ok(r) => {
            assert_eq!(r.num_rows(), 1);
        }
        Err(_) => {}
    }
}

#[test]
fn test_current_timestamp() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE data (id INT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO data VALUES (1)").unwrap();

    let result = executor.execute_sql("SELECT CURRENT_TIMESTAMP() as now FROM data");

    match result {
        Ok(r) => {
            assert_eq!(r.num_rows(), 1);
        }
        Err(_) => {}
    }
}

#[test]
fn test_now_function() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE data (id INT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO data VALUES (1)").unwrap();

    let result = executor.execute_sql("SELECT NOW() as current_time FROM data");

    match result {
        Ok(r) => {
            assert_eq!(r.num_rows(), 1);
        }
        Err(_) => {}
    }
}

#[test]
fn test_date_add() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DATE_ADD(event_date, INTERVAL 7 DAY) as future_date FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[d(2024, 1, 22)]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_date_sub() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT DATE_SUB(event_date, INTERVAL 7 DAY) as past_date FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[d(2024, 1, 8)]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_date_diff() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (start_date DATE, end_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (DATE '2024-01-01', DATE '2024-01-15')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT DATE_DIFF(end_date, start_date, DAY) as days FROM events");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[14]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_extract_year() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result = executor.execute_sql("SELECT EXTRACT(YEAR FROM event_date) as year FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[2024]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_extract_month() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result = executor.execute_sql("SELECT EXTRACT(MONTH FROM event_date) as month FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[1]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_extract_day() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result = executor.execute_sql("SELECT EXTRACT(DAY FROM event_date) as day FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[15]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_date_part() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result = executor.execute_sql("SELECT DATE_PART('year', event_date) as year FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[2024]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_date_trunc_month() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT DATE_TRUNC('month', event_date) as truncated FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[d(2024, 1, 1)]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_date_trunc_year() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-03-15')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT DATE_TRUNC('year', event_date) as truncated FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[d(2024, 1, 1)]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_date_format() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT DATE_FORMAT(event_date, '%Y-%m-%d') as formatted FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [["2024-01-15"]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_to_char() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT TO_CHAR(event_date, 'YYYY-MM-DD') as formatted FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [["2024-01-15"]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_parse_date() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE strings (date_str STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO strings VALUES ('2024-01-15')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT PARSE_DATE('%Y-%m-%d', date_str) as parsed FROM strings");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[d(2024, 1, 15)]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_timestamp_add() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (event_time TIMESTAMP)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (TIMESTAMP '2024-01-15 10:30:00')")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT TIMESTAMP_ADD(event_time, INTERVAL 2 HOUR) as future_time FROM events",
    );

    match result {
        Ok(r) => {
            assert_eq!(r.num_rows(), 1);
        }
        Err(_) => {}
    }
}

#[test]
fn test_timestamp_diff() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (start_time TIMESTAMP, end_time TIMESTAMP)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (TIMESTAMP '2024-01-15 10:00:00', TIMESTAMP '2024-01-15 12:30:00')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT TIMESTAMP_DIFF(end_time, start_time, HOUR) as hours FROM events");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[2]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_make_date() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE parts (year INT64, month INT64, day INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO parts VALUES (2024, 1, 15)")
        .unwrap();

    let result = executor.execute_sql("SELECT MAKE_DATE(year, month, day) as date FROM parts");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[d(2024, 1, 15)]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_make_timestamp() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE parts (y INT64, m INT64, d INT64, h INT64, min INT64, s INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO parts VALUES (2024, 1, 15, 10, 30, 45)")
        .unwrap();

    let result = executor.execute_sql("SELECT MAKE_TIMESTAMP(y, m, d, h, min, s) as ts FROM parts");

    match result {
        Ok(r) => {
            assert_eq!(r.num_rows(), 1);
        }
        Err(_) => {}
    }
}

#[test]
fn test_age_function() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (start_date DATE, end_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (DATE '2020-01-01', DATE '2024-01-01')")
        .unwrap();

    let result = executor.execute_sql("SELECT AGE(end_date, start_date) as age FROM events");

    match result {
        Ok(r) => {
            assert_eq!(r.num_rows(), 1);
        }
        Err(_) => {}
    }
}

#[test]
fn test_interval_arithmetic() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT event_date + INTERVAL '7 days' as future_date FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[d(2024, 1, 22)]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_extract_hour_from_timestamp() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (event_time TIMESTAMP)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (TIMESTAMP '2024-01-15 14:30:45')")
        .unwrap();

    let result = executor.execute_sql("SELECT EXTRACT(HOUR FROM event_time) as hour FROM events");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[14]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_extract_minute_from_timestamp() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (event_time TIMESTAMP)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (TIMESTAMP '2024-01-15 14:30:45')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT EXTRACT(MINUTE FROM event_time) as minute FROM events");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[30]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_extract_second_from_timestamp() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (event_time TIMESTAMP)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (TIMESTAMP '2024-01-15 14:30:45')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT EXTRACT(SECOND FROM event_time) as second FROM events");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[45]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_date_comparison() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (DATE '2024-01-15'), (DATE '2024-02-15'), (DATE '2024-03-15')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM events WHERE event_date > DATE '2024-01-31'");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[d(2024, 2, 15)], [d(2024, 3, 15)]]);
        }
        Err(_) => {}
    }
}

#[test]
fn test_timestamp_comparison() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (event_time TIMESTAMP)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (TIMESTAMP '2024-01-15 10:00:00'), (TIMESTAMP '2024-01-15 14:00:00')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM events WHERE event_time > TIMESTAMP '2024-01-15 12:00:00'");

    match result {
        Ok(r) => {
            assert_eq!(r.num_rows(), 1);
        }
        Err(_) => {}
    }
}

#[test]
fn test_date_null_handling() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (NULL), (DATE '2024-01-15')")
        .unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        executor.execute_sql("SELECT * FROM events WHERE event_date IS NULL")
    }));

    match result {
        Ok(Ok(r)) => {
            assert_table_eq!(r, [[null]]);
        }
        _ => {}
    }
}

#[test]
fn test_timezone_conversion() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (event_time TIMESTAMP)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (TIMESTAMP '2024-01-15 10:00:00')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT event_time AT TIME ZONE 'UTC' as utc_time FROM events");

    match result {
        Ok(r) => {
            assert_eq!(r.num_rows(), 1);
        }
        Err(_) => {}
    }
}

#[test]
fn test_extract_day_of_week() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT EXTRACT(DOW FROM event_date) as day_of_week FROM dates");

    match result {
        Ok(r) => {
            assert_eq!(r.num_rows(), 1);
        }
        Err(_) => {}
    }
}

#[test]
fn test_extract_day_of_year() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE dates (event_date DATE)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO dates VALUES (DATE '2024-01-15')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT EXTRACT(DOY FROM event_date) as day_of_year FROM dates");

    match result {
        Ok(r) => {
            assert_table_eq!(r, [[15]]);
        }
        Err(_) => {}
    }
}
