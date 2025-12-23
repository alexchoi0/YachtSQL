use crate::assert_table_eq;
use crate::common::{create_session, d, dt, null, tm, ts, ts_ms};

#[test]
fn test_current_date() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CURRENT_DATE IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_current_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CURRENT_TIMESTAMP IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_date_literal() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT DATE '2024-01-15'").unwrap();
    assert_table_eq!(result, [[d(2024, 1, 15)]]);
}

#[test]
fn test_extract_year() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(YEAR FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[2024]]);
}

#[test]
fn test_extract_month() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MONTH FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

#[test]
fn test_extract_day() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAY FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
fn test_date_comparison() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE events (name STRING, event_date DATE)")
        .unwrap();
    session
        .execute_sql("INSERT INTO events VALUES ('A', '2024-01-01'), ('B', '2024-06-15'), ('C', '2024-12-31')")
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM events WHERE event_date > DATE '2024-06-01' ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["B"], ["C"]]);
}

#[test]
fn test_date_ordering() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE events (name STRING, event_date DATE)")
        .unwrap();
    session
        .execute_sql("INSERT INTO events VALUES ('C', '2024-12-31'), ('A', '2024-01-01'), ('B', '2024-06-15')")
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM events ORDER BY event_date")
        .unwrap();
    assert_table_eq!(result, [["A"], ["B"], ["C"]]);
}

#[test]
fn test_timestamp_literal() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP '2024-06-15 10:30:00'")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 10, 30, 0)]]);
}

#[test]
fn test_extract_hour() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 14:30:45')")
        .unwrap();
    assert_table_eq!(result, [[14]]);
}

#[test]
fn test_extract_minute() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MINUTE FROM TIMESTAMP '2024-06-15 14:30:45')")
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_extract_second() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(SECOND FROM TIMESTAMP '2024-06-15 14:30:45')")
        .unwrap();
    assert_table_eq!(result, [[45]]);
}

#[test]
fn test_date_with_null() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE events (name STRING, event_date DATE)")
        .unwrap();
    session
        .execute_sql("INSERT INTO events VALUES ('A', '2024-01-01'), ('B', NULL)")
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM events WHERE event_date IS NULL")
        .unwrap();
    assert_table_eq!(result, [["B"]]);
}

#[test]
fn test_extract_dayofweek() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAYOFWEEK FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
fn test_date_in_group_by() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE sales (product STRING, sale_date DATE, amount INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES ('A', '2024-01-01', 100), ('B', '2024-01-01', 200), ('C', '2024-01-02', 150)")
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT sale_date, SUM(amount) FROM sales GROUP BY sale_date ORDER BY sale_date",
        )
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1), 300], [d(2024, 1, 2), 150]]);
}

#[test]
fn test_date_add() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 10 DAY)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 25)]]);
}

#[test]
fn test_date_sub() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATE_SUB(DATE '2024-01-15', INTERVAL 10 DAY)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 5)]]);
}

#[test]
fn test_date_diff() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATE_DIFF(DATE '2024-01-20', DATE '2024-01-10', DAY)")
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_date_trunc() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATE_TRUNC(DATE '2024-06-15', MONTH)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 1)]]);
}

#[test]
fn test_date_from_unix_date() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATE_FROM_UNIX_DATE(19723)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1)]]);
}

#[test]
fn test_unix_date() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT UNIX_DATE(DATE '2024-01-01')")
        .unwrap();
    assert_table_eq!(result, [[19723]]);
}

#[test]
fn test_parse_date() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT PARSE_DATE('%Y-%m-%d', '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 15)]]);
}

#[test]
fn test_format_date() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT_DATE('%Y/%m/%d', DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [["2024/06/15"]]);
}

#[test]
fn test_last_day() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT LAST_DAY(DATE '2024-02-15')")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 2, 29)]]);
}

#[test]
fn test_timestamp_add() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:00:00', INTERVAL 1 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 15, 11, 0, 0)]]);
}

#[test]
fn test_timestamp_sub() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_SUB(TIMESTAMP '2024-01-15 10:00:00', INTERVAL 1 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 15, 9, 0, 0)]]);
}

#[test]
fn test_timestamp_diff() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 12:00:00', TIMESTAMP '2024-01-15 10:00:00', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_timestamp_trunc() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:45', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 14, 0, 0)]]);
}

#[test]
fn test_parse_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-06-15 14:30:00')")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 14, 30, 0)]]);
}

#[test]
fn test_format_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT_TIMESTAMP('%Y/%m/%d %H:%M', TIMESTAMP '2024-06-15 14:30:00')")
        .unwrap();
    assert_table_eq!(result, [["2024/06/15 14:30"]]);
}

#[test]
fn test_unix_seconds() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT UNIX_SECONDS(TIMESTAMP '2024-01-01 00:00:00 UTC')")
        .unwrap();
    assert_table_eq!(result, [[1704067200]]);
}

#[test]
fn test_timestamp_seconds() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_SECONDS(1704067200)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 1, 0, 0, 0)]]);
}

#[test]
fn test_unix_millis() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT UNIX_MILLIS(TIMESTAMP '2024-01-01 00:00:00 UTC')")
        .unwrap();
    assert_table_eq!(result, [[1704067200000i64]]);
}

#[test]
fn test_timestamp_millis() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_MILLIS(1704067200000)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 1, 0, 0, 0)]]);
}

#[test]
fn test_unix_micros() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT UNIX_MICROS(TIMESTAMP '2024-01-01 00:00:00 UTC')")
        .unwrap();
    assert_table_eq!(result, [[1704067200000000i64]]);
}

#[test]
fn test_timestamp_micros() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_MICROS(1704067200000000)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 1, 0, 0, 0)]]);
}

#[test]
fn test_time_literal() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TIME '14:30:00'").unwrap();
    assert_table_eq!(result, [[tm(14, 30, 0)]]);
}

#[test]
fn test_time_add() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_ADD(TIME '10:00:00', INTERVAL 30 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[tm(10, 30, 0)]]);
}

#[test]
fn test_time_sub() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_SUB(TIME '10:00:00', INTERVAL 30 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[tm(9, 30, 0)]]);
}

#[test]
fn test_time_diff() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_DIFF(TIME '14:30:00', TIME '10:00:00', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_time_trunc() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_TRUNC(TIME '14:30:45', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[tm(14, 0, 0)]]);
}

#[test]
fn test_datetime_literal() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME '2024-06-15 14:30:00'")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 14, 30, 0)]]);
}

#[test]
fn test_datetime_add() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_ADD(DATETIME '2024-01-15 10:00:00', INTERVAL 1 DAY)")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 1, 16, 10, 0, 0)]]);
}

#[test]
fn test_datetime_sub() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_SUB(DATETIME '2024-01-15 10:00:00', INTERVAL 1 DAY)")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 1, 14, 10, 0, 0)]]);
}

#[test]
fn test_datetime_diff() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_DIFF(DATETIME '2024-01-20 10:00:00', DATETIME '2024-01-15 10:00:00', DAY)")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_datetime_trunc() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME_TRUNC(DATETIME '2024-06-15 14:30:45', MONTH)")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 1, 0, 0, 0)]]);
}

#[test]
#[ignore]
fn test_extract_week() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(WEEK FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[24]]);
}

#[test]
fn test_extract_quarter() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(QUARTER FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_extract_dayofyear() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAYOFYEAR FROM DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[167]]);
}

#[test]
fn test_datetime_constructor_from_parts() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME(2024, 6, 15, 14, 30, 45)")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 14, 30, 45)]]);
}

#[test]
fn test_datetime_constructor_from_date_and_time() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME(DATE '2024-06-15', TIME '14:30:00')")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 14, 30, 0)]]);
}

#[test]
fn test_datetime_constructor_from_date() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME(DATE '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 0, 0, 0)]]);
}

#[test]
fn test_datetime_constructor_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME(TIMESTAMP '2024-06-15 14:30:00')")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 14, 30, 0)]]);
}

#[test]
fn test_datetime_constructor_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT DATETIME(NULL)").unwrap();
    assert!(result.num_rows() == 1);
    assert!(result.get_row(0).unwrap().values()[0].is_null());
}

#[test]
fn test_datetime_constructor_from_string() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT DATETIME('2024-06-15 14:30:00')")
        .unwrap();
    assert_table_eq!(result, [[dt(2024, 6, 15, 14, 30, 0)]]);
}

#[test]
fn test_current_time() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CURRENT_TIME() IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_current_time_no_parens() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CURRENT_TIME IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_extract_hour_from_time() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(HOUR FROM TIME '15:30:00')")
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
fn test_extract_minute_from_time() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MINUTE FROM TIME '15:30:45')")
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_extract_second_from_time() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(SECOND FROM TIME '15:30:45')")
        .unwrap();
    assert_table_eq!(result, [[45]]);
}

#[test]
fn test_format_time_basic() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT_TIME('%R', TIME '15:30:00')")
        .unwrap();
    assert_table_eq!(result, [["15:30"]]);
}

#[test]
fn test_format_time_full() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT_TIME('%H:%M:%S', TIME '15:30:45')")
        .unwrap();
    assert_table_eq!(result, [["15:30:45"]]);
}

#[test]
fn test_format_time_12_hour() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT_TIME('%I:%M %p', TIME '15:30:00')")
        .unwrap();
    assert_table_eq!(result, [["03:30 PM"]]);
}

#[test]
#[ignore]
fn test_parse_time_hour_only() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT PARSE_TIME('%H', '15')")
        .unwrap();
    assert_table_eq!(result, [[tm(15, 0, 0)]]);
}

#[test]
fn test_parse_time_full() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT PARSE_TIME('%H:%M:%S', '14:30:45')")
        .unwrap();
    assert_table_eq!(result, [[tm(14, 30, 45)]]);
}

#[test]
fn test_parse_time_12_hour() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT PARSE_TIME('%I:%M:%S %p', '2:23:38 pm')")
        .unwrap();
    assert_table_eq!(result, [[tm(14, 23, 38)]]);
}

#[test]
fn test_parse_time_t_format() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT PARSE_TIME('%T', '07:30:00')")
        .unwrap();
    assert_table_eq!(result, [[tm(7, 30, 0)]]);
}

#[test]
fn test_time_constructor_from_parts() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TIME(15, 30, 0)").unwrap();
    assert_table_eq!(result, [[tm(15, 30, 0)]]);
}

#[test]
fn test_time_constructor_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME(TIMESTAMP '2008-12-25 15:30:00')")
        .unwrap();
    assert_table_eq!(result, [[tm(15, 30, 0)]]);
}

#[test]
fn test_time_constructor_from_datetime() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME(DATETIME '2008-12-25 15:30:00')")
        .unwrap();
    assert_table_eq!(result, [[tm(15, 30, 0)]]);
}

#[test]
fn test_time_add_hour() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_ADD(TIME '15:30:00', INTERVAL 10 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[tm(15, 40, 0)]]);
}

#[test]
fn test_time_add_second() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_ADD(TIME '15:30:00', INTERVAL 45 SECOND)")
        .unwrap();
    assert_table_eq!(result, [[tm(15, 30, 45)]]);
}

#[test]
fn test_time_add_wrap_around() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_ADD(TIME '23:30:00', INTERVAL 1 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[tm(0, 30, 0)]]);
}

#[test]
fn test_time_diff_minute() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_DIFF(TIME '15:30:00', TIME '14:35:00', MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[55]]);
}

#[test]
fn test_time_diff_second() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_DIFF(TIME '15:30:30', TIME '15:30:00', SECOND)")
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_time_diff_negative() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_DIFF(TIME '10:00:00', TIME '14:00:00', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[-4]]);
}

#[test]
fn test_time_sub_hour() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_SUB(TIME '15:30:00', INTERVAL 1 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[tm(14, 30, 0)]]);
}

#[test]
fn test_time_sub_second() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_SUB(TIME '15:30:30', INTERVAL 30 SECOND)")
        .unwrap();
    assert_table_eq!(result, [[tm(15, 30, 0)]]);
}

#[test]
fn test_time_sub_wrap_around() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_SUB(TIME '00:30:00', INTERVAL 1 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[tm(23, 30, 0)]]);
}

#[test]
fn test_time_trunc_minute() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_TRUNC(TIME '15:30:45', MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[tm(15, 30, 0)]]);
}

#[test]
fn test_time_trunc_second() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIME_TRUNC(TIME '15:30:45', SECOND)")
        .unwrap();
    assert_table_eq!(result, [[tm(15, 30, 45)]]);
}

#[test]
fn test_extract_microsecond_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MICROSECOND FROM TIMESTAMP '2024-06-15 14:30:45.123456')")
        .unwrap();
    assert_table_eq!(result, [[123456]]);
}

#[test]
fn test_extract_millisecond_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(MILLISECOND FROM TIMESTAMP '2024-06-15 14:30:45.123456')")
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[test]
fn test_extract_dayofweek_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAYOFWEEK FROM TIMESTAMP '2008-12-25 05:30:00+00')")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_extract_dayofyear_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DAYOFYEAR FROM TIMESTAMP '2024-06-15 14:30:45')")
        .unwrap();
    assert_table_eq!(result, [[167]]);
}

#[test]
fn test_extract_week_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(WEEK FROM TIMESTAMP '2005-01-03 12:34:56+00')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_extract_isoweek_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(ISOWEEK FROM TIMESTAMP '2005-01-03 12:34:56+00')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_extract_isoyear_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(ISOYEAR FROM TIMESTAMP '2005-01-03 12:34:56+00')")
        .unwrap();
    assert_table_eq!(result, [[2005]]);
}

#[test]
fn test_extract_isoyear_boundary() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(ISOYEAR FROM TIMESTAMP '2007-12-31 12:00:00+00'), EXTRACT(YEAR FROM TIMESTAMP '2007-12-31 12:00:00+00')")
        .unwrap();
    assert_table_eq!(result, [[2008, 2007]]);
}

#[test]
fn test_extract_date_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(DATE FROM TIMESTAMP '2024-06-15 14:30:45')")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 15)]]);
}

#[test]
fn test_extract_time_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(TIME FROM TIMESTAMP '2024-06-15 14:30:45')")
        .unwrap();
    assert_table_eq!(result, [[tm(14, 30, 45)]]);
}

#[test]
#[ignore]
fn test_extract_week_sunday_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(WEEK(SUNDAY) FROM TIMESTAMP '2017-11-06 00:00:00+00')")
        .unwrap();
    assert_table_eq!(result, [[45]]);
}

#[test]
fn test_extract_week_monday_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EXTRACT(WEEK(MONDAY) FROM TIMESTAMP '2017-11-06 00:00:00+00')")
        .unwrap();
    assert_table_eq!(result, [[44]]);
}

#[test]
fn test_format_timestamp_custom() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT_TIMESTAMP('%c', TIMESTAMP '2050-12-25 15:30:55+00', 'UTC')")
        .unwrap();
    assert_table_eq!(result, [["Sun Dec 25 15:30:55 2050"]]);
}

#[test]
fn test_format_timestamp_date_only() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT_TIMESTAMP('%b-%d-%Y', TIMESTAMP '2050-12-25 15:30:55+00')")
        .unwrap();
    assert_table_eq!(result, [["Dec-25-2050"]]);
}

#[test]
fn test_format_timestamp_month_year() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT_TIMESTAMP('%b %Y', TIMESTAMP '2050-12-25 15:30:55+00')")
        .unwrap();
    assert_table_eq!(result, [["Dec 2050"]]);
}

#[test]
fn test_format_timestamp_iso() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', TIMESTAMP '2050-12-25 15:30:55', 'UTC')",
        )
        .unwrap();
    assert_table_eq!(result, [["2050-12-25T15:30:55Z"]]);
}

#[test]
fn test_parse_timestamp_c_format() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT PARSE_TIMESTAMP('%c', 'Thu Dec 25 07:30:00 2008')")
        .unwrap();
    assert_table_eq!(result, [[ts(2008, 12, 25, 7, 30, 0)]]);
}

#[test]
#[ignore]
fn test_parse_timestamp_date_only() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT PARSE_TIMESTAMP('%Y-%m-%d', '2024-06-15')")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 0, 0, 0)]]);
}

#[test]
fn test_parse_timestamp_with_am_pm() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT PARSE_TIMESTAMP('%Y-%m-%d %I:%M:%S %p', '2024-06-15 02:30:00 PM')")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 14, 30, 0)]]);
}

#[test]
#[ignore]
fn test_timestamp_constructor_from_string() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP('2008-12-25 15:30:00+00')")
        .unwrap();
    assert_table_eq!(result, [[ts(2008, 12, 25, 15, 30, 0)]]);
}

#[test]
#[ignore]
fn test_timestamp_constructor_from_string_with_timezone() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP('2008-12-25 15:30:00', 'America/Los_Angeles')")
        .unwrap();
    assert_table_eq!(result, [[ts(2008, 12, 25, 23, 30, 0)]]);
}

#[test]
#[ignore]
fn test_timestamp_constructor_from_date() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP(DATE '2008-12-25')")
        .unwrap();
    assert_table_eq!(result, [[ts(2008, 12, 25, 0, 0, 0)]]);
}

#[test]
fn test_timestamp_constructor_from_datetime() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP(DATETIME '2008-12-25 15:30:00')")
        .unwrap();
    assert_table_eq!(result, [[ts(2008, 12, 25, 15, 30, 0)]]);
}

#[test]
#[ignore]
fn test_string_from_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT STRING(TIMESTAMP '2008-12-25 15:30:00+00', 'UTC')")
        .unwrap();
    assert_table_eq!(result, [["2008-12-25 15:30:00+00"]]);
}

#[test]
fn test_timestamp_add_minute() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_ADD(TIMESTAMP '2008-12-25 15:30:00+00', INTERVAL 10 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[ts(2008, 12, 25, 15, 40, 0)]]);
}

#[test]
fn test_timestamp_add_second() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:00:00', INTERVAL 30 SECOND)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 15, 10, 0, 30)]]);
}

#[test]
fn test_timestamp_add_day() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:00:00', INTERVAL 5 DAY)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 20, 10, 0, 0)]]);
}

#[test]
#[ignore]
fn test_timestamp_add_millisecond() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "SELECT TIMESTAMP_ADD(TIMESTAMP '2024-01-15 10:00:00', INTERVAL 500 MILLISECOND)",
        )
        .unwrap();
    assert_table_eq!(result, [[ts_ms(2024, 1, 15, 10, 0, 0, 500)]]);
}

#[test]
fn test_timestamp_sub_minute() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_SUB(TIMESTAMP '2008-12-25 15:30:00+00', INTERVAL 10 MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[ts(2008, 12, 25, 15, 20, 0)]]);
}

#[test]
fn test_timestamp_sub_second() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_SUB(TIMESTAMP '2024-01-15 10:00:30', INTERVAL 30 SECOND)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 15, 10, 0, 0)]]);
}

#[test]
fn test_timestamp_sub_day() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_SUB(TIMESTAMP '2024-01-20 10:00:00', INTERVAL 5 DAY)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 15, 10, 0, 0)]]);
}

#[test]
fn test_timestamp_diff_minute() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 12:30:00', TIMESTAMP '2024-01-15 12:00:00', MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_timestamp_diff_second() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-15 12:00:30', TIMESTAMP '2024-01-15 12:00:00', SECOND)")
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_timestamp_diff_day() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_DIFF(TIMESTAMP '2024-01-20 10:00:00', TIMESTAMP '2024-01-15 10:00:00', DAY)")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore]
fn test_timestamp_diff_negative() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_DIFF(TIMESTAMP '2018-08-14', TIMESTAMP '2018-10-14', DAY)")
        .unwrap();
    assert_table_eq!(result, [[-61]]);
}

#[test]
fn test_timestamp_diff_large() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_DIFF(TIMESTAMP '2010-07-07 10:20:00+00', TIMESTAMP '2008-12-25 15:30:00+00', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[13410]]);
}

#[test]
fn test_timestamp_diff_partial_hour() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_DIFF(TIMESTAMP '2001-02-01 01:00:00', TIMESTAMP '2001-02-01 00:00:01', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_timestamp_trunc_day() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:45', DAY)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 0, 0, 0)]]);
}

#[test]
fn test_timestamp_trunc_minute() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:45', MINUTE)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 14, 30, 0)]]);
}

#[test]
#[ignore]
fn test_timestamp_trunc_second() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:45.123', SECOND)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 15, 14, 30, 45)]]);
}

#[test]
fn test_timestamp_trunc_month() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:45', MONTH)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 1, 0, 0, 0)]]);
}

#[test]
fn test_timestamp_trunc_year() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:45', YEAR)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 1, 1, 0, 0, 0)]]);
}

#[test]
fn test_timestamp_trunc_quarter() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:45', QUARTER)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 4, 1, 0, 0, 0)]]);
}

#[test]
#[ignore]
fn test_timestamp_trunc_week() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:45', WEEK)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 9, 0, 0, 0)]]);
}

#[test]
#[ignore]
fn test_timestamp_trunc_week_monday() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2017-11-06 00:00:00+12', WEEK(MONDAY), 'UTC')",
        )
        .unwrap();
    assert_table_eq!(result, [[ts(2017, 10, 30, 0, 0, 0)]]);
}

#[test]
#[ignore]
fn test_timestamp_trunc_isoweek() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2024-06-15 14:30:45', ISOWEEK)")
        .unwrap();
    assert_table_eq!(result, [[ts(2024, 6, 10, 0, 0, 0)]]);
}

#[test]
#[ignore]
fn test_timestamp_trunc_isoyear() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(TIMESTAMP '2015-06-15 00:00:00+00', ISOYEAR)")
        .unwrap();
    assert_table_eq!(result, [[ts(2014, 12, 29, 0, 0, 0)]]);
}

#[test]
fn test_unix_seconds_specific() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT UNIX_SECONDS(TIMESTAMP '2008-12-25 15:30:00+00')")
        .unwrap();
    assert_table_eq!(result, [[1230219000]]);
}

#[test]
fn test_unix_millis_specific() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT UNIX_MILLIS(TIMESTAMP '2008-12-25 15:30:00+00')")
        .unwrap();
    assert_table_eq!(result, [[1230219000000i64]]);
}

#[test]
fn test_unix_micros_specific() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT UNIX_MICROS(TIMESTAMP '2008-12-25 15:30:00+00')")
        .unwrap();
    assert_table_eq!(result, [[1230219000000000i64]]);
}

#[test]
fn test_timestamp_seconds_specific() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_SECONDS(1230219000)")
        .unwrap();
    assert_table_eq!(result, [[ts(2008, 12, 25, 15, 30, 0)]]);
}

#[test]
fn test_timestamp_millis_specific() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_MILLIS(1230219000000)")
        .unwrap();
    assert_table_eq!(result, [[ts(2008, 12, 25, 15, 30, 0)]]);
}

#[test]
fn test_timestamp_micros_specific() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_MICROS(1230219000000000)")
        .unwrap();
    assert_table_eq!(result, [[ts(2008, 12, 25, 15, 30, 0)]]);
}

#[test]
fn test_unix_millis_with_fractional() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT UNIX_MILLIS(TIMESTAMP '1970-01-01 00:00:00.0018+00')")
        .unwrap();
    assert_table_eq!(result, [[1i64]]);
}

#[test]
fn test_unix_seconds_with_fractional() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT UNIX_SECONDS(TIMESTAMP '1970-01-01 00:00:01.8+00')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_timestamp_with_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TIMESTAMP(NULL)").unwrap();
    assert_table_eq!(result, [[null()]]);
}

#[test]
fn test_timestamp_add_with_null() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_ADD(NULL, INTERVAL 1 HOUR)")
        .unwrap();
    assert_table_eq!(result, [[null()]]);
}

#[test]
fn test_timestamp_diff_with_null() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_DIFF(NULL, TIMESTAMP '2024-01-01 00:00:00', HOUR)")
        .unwrap();
    assert_table_eq!(result, [[null()]]);
}

#[test]
fn test_timestamp_trunc_with_null() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TIMESTAMP_TRUNC(NULL, HOUR)")
        .unwrap();
    assert_table_eq!(result, [[null()]]);
}

#[test]
fn test_string_from_timestamp_basic() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT STRING(TIMESTAMP '2024-06-15 14:30:00')")
        .unwrap();
    assert_table_eq!(result, [["2024-06-15 14:30:00.000000 UTC"]]);
}

#[test]
fn test_string_from_timestamp_null() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT STRING(CAST(NULL AS TIMESTAMP))")
        .unwrap();
    assert_table_eq!(result, [[null()]]);
}
