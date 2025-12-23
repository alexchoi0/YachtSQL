use crate::assert_table_eq;
use crate::common::{create_session, date};

#[test]
fn test_cast_int_to_string() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CAST(123 AS STRING)").unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[test]
fn test_cast_string_to_int() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CAST('456' AS INT64)").unwrap();
    assert_table_eq!(result, [[456]]);
}

#[test]
fn test_cast_float_to_int() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CAST(3.7 AS INT64)").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_cast_int_to_float() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CAST(5 AS FLOAT64)").unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[test]
fn test_cast_string_to_bool() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CAST('true' AS BOOL)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_cast_bool_to_string() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CAST(TRUE AS STRING)").unwrap();
    assert_table_eq!(result, [["true"]]);
}

#[test]
fn test_safe_cast_valid() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('123' AS INT64)")
        .unwrap();
    assert_table_eq!(result, [[123]]);
}

#[test]
fn test_safe_cast_invalid() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CAST('abc' AS INT64)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_cast_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CAST(NULL AS STRING)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_cast_with_column() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (100), (200), (300)")
        .unwrap();

    let result = session
        .execute_sql("SELECT CAST(val AS STRING) FROM data ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [["100"], ["200"], ["300"]]);
}

#[test]
fn test_cast_in_where() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (val STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES ('10'), ('20'), ('30')")
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM data WHERE CAST(val AS INT64) > 15 ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [["20"], ["30"]]);
}

#[test]
fn test_cast_date_to_string() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CAST(DATE '2024-01-15' AS STRING)")
        .unwrap();
    assert_table_eq!(result, [["2024-01-15"]]);
}

#[test]
fn test_cast_string_to_date() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CAST('2024-06-15' AS DATE)")
        .unwrap();
    assert_table_eq!(result, [[(date(2024, 6, 15))]]);
}
