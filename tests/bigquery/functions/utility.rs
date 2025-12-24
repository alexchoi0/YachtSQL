use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_generate_uuid() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT GENERATE_UUID() IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_generate_uuid_format() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(GENERATE_UUID()) = 36")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_generate_uuid_uniqueness() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT GENERATE_UUID() != GENERATE_UUID()")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_generate_uuid_lowercase() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE uuid_test AS SELECT GENERATE_UUID() AS uuid")
        .unwrap();
    let result = session
        .execute_sql("SELECT uuid = LOWER(uuid) FROM uuid_test")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_generate_uuid_hyphen_positions() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE uuid_test AS SELECT GENERATE_UUID() AS uuid")
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT
                SUBSTR(uuid, 9, 1) = '-'
                AND SUBSTR(uuid, 14, 1) = '-'
                AND SUBSTR(uuid, 19, 1) = '-'
                AND SUBSTR(uuid, 24, 1) = '-'
            FROM uuid_test",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_typeof_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TYPEOF(NULL)").unwrap();
    assert_table_eq!(result, [["NULL"]]);
}

#[test]
fn test_typeof_string() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TYPEOF('hello')").unwrap();
    assert_table_eq!(result, [["STRING"]]);
}

#[test]
fn test_typeof_int64() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TYPEOF(12 + 1)").unwrap();
    assert_table_eq!(result, [["INT64"]]);
}

#[test]
fn test_typeof_float64() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TYPEOF(4.7)").unwrap();
    assert_table_eq!(result, [["FLOAT64"]]);
}

#[test]
fn test_typeof_bool() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TYPEOF(TRUE)").unwrap();
    assert_table_eq!(result, [["BOOL"]]);
}

#[test]
fn test_typeof_date() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(DATE '2024-01-15')")
        .unwrap();
    assert_table_eq!(result, [["DATE"]]);
}

#[test]
fn test_typeof_timestamp() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(TIMESTAMP '2024-01-15 10:30:00')")
        .unwrap();
    assert_table_eq!(result, [["TIMESTAMP"]]);
}

#[test]
fn test_typeof_bytes() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TYPEOF(b'hello')").unwrap();
    assert_table_eq!(result, [["BYTES"]]);
}

#[test]
fn test_typeof_array() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TYPEOF([1, 2, 3])").unwrap();
    assert_table_eq!(result, [["ARRAY"]]);
}

#[test]
fn test_typeof_struct() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(STRUCT(25 AS x, 'apples' AS y))")
        .unwrap();
    assert_table_eq!(result, [["STRUCT"]]);
}

#[test]
fn test_typeof_struct_field() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(STRUCT(25 AS x, 'apples' AS y).y)")
        .unwrap();
    assert_table_eq!(result, [["STRING"]]);
}

#[test]
fn test_typeof_array_element() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF([25, 32][OFFSET(0)])")
        .unwrap();
    assert_table_eq!(result, [["INT64"]]);
}

#[test]
fn test_typeof_json() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(JSON '{\"a\": 1}')")
        .unwrap();
    assert_table_eq!(result, [["JSON"]]);
}

#[test]
fn test_typeof_numeric() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(NUMERIC '123.45')")
        .unwrap();
    assert_table_eq!(result, [["NUMERIC"]]);
}

#[test]
fn test_typeof_time() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(TIME '10:30:00')")
        .unwrap();
    assert_table_eq!(result, [["TIME"]]);
}

#[test]
fn test_typeof_datetime() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(DATETIME '2024-01-15 10:30:00')")
        .unwrap();
    assert_table_eq!(result, [["DATETIME"]]);
}

#[test]
fn test_typeof_interval() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(INTERVAL 1 DAY)")
        .unwrap();
    assert_table_eq!(result, [["INTERVAL"]]);
}

#[test]
fn test_typeof_multiple_columns() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "SELECT TYPEOF(NULL) AS a, TYPEOF('hello') AS b, TYPEOF(12+1) AS c, TYPEOF(4.7) AS d",
        )
        .unwrap();
    assert_table_eq!(result, [["NULL", "STRING", "INT64", "FLOAT64"]]);
}
