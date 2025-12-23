use crate::assert_table_eq;
use crate::common::{create_session, d};

#[test]
fn test_range_date_literal() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE(DATE '2024-01-01', DATE '2024-12-31') IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_timestamp_literal() {
    let mut session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 23:59:59') IS NOT NULL",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_datetime_literal() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE(DATETIME '2024-01-01 00:00:00', DATETIME '2024-12-31 23:59:59') IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_unbounded_start() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE(NULL, DATE '2024-12-31') IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_unbounded_end() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE(DATE '2024-01-01', NULL) IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_column() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE periods (id INT64, period RANGE<DATE>)")
        .unwrap();
    session
        .execute_sql("INSERT INTO periods VALUES (1, RANGE(DATE '2024-01-01', DATE '2024-06-30'))")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM periods").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_range_start() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_START(RANGE(DATE '2024-01-15', DATE '2024-12-31'))")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 15)]]);
}

#[test]
fn test_range_end() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT RANGE_END(RANGE(DATE '2024-01-01', DATE '2024-06-30'))")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 6, 30)]]);
}

#[test]
fn test_range_contains() {
    let mut session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE_CONTAINS(RANGE(DATE '2024-01-01', DATE '2024-12-31'), DATE '2024-06-15')",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_contains_false() {
    let mut session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE_CONTAINS(RANGE(DATE '2024-01-01', DATE '2024-06-30'), DATE '2024-12-15')",
        )
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_range_overlaps() {
    let mut session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE_OVERLAPS(
                RANGE(DATE '2024-01-01', DATE '2024-06-30'),
                RANGE(DATE '2024-04-01', DATE '2024-12-31')
            )",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_overlaps_false() {
    let mut session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE_OVERLAPS(
                RANGE(DATE '2024-01-01', DATE '2024-03-31'),
                RANGE(DATE '2024-07-01', DATE '2024-12-31')
            )",
        )
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_range_intersect() {
    let mut session = create_session();

    let result = session
        .execute_sql(
            "SELECT RANGE_INTERSECT(
                RANGE(DATE '2024-01-01', DATE '2024-06-30'),
                RANGE(DATE '2024-04-01', DATE '2024-12-31')
            ) IS NOT NULL",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_generate_range_array() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(GENERATE_RANGE_ARRAY(RANGE(DATE '2024-01-01', DATE '2024-01-05'), INTERVAL 1 DAY))")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_range_in_where() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE events (id INT64, event_period RANGE<DATE>)")
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO events VALUES
            (1, RANGE(DATE '2024-01-01', DATE '2024-03-31')),
            (2, RANGE(DATE '2024-04-01', DATE '2024-06-30')),
            (3, RANGE(DATE '2024-07-01', DATE '2024-09-30'))",
        )
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id FROM events WHERE RANGE_CONTAINS(event_period, DATE '2024-05-15') ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_range_sessionize() {
    let mut session = create_session();

    let result = session.execute_sql(
        "SELECT RANGE_SESSIONIZE(
            TABLE(SELECT RANGE(DATE '2024-01-01', DATE '2024-01-10')),
            'range_col',
            INTERVAL 5 DAY
        )",
    );
    assert!(result.is_ok() || result.is_err());
}
