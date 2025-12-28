use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_generate_uuid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT GENERATE_UUID() IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_uuid_format() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(GENERATE_UUID()) = 36")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_uuid_uniqueness() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT GENERATE_UUID() != GENERATE_UUID()")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_uuid_lowercase() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE uuid_test AS SELECT GENERATE_UUID() AS uuid")
        .await
        .unwrap();
    let result = session
        .execute_sql("SELECT uuid = LOWER(uuid) FROM uuid_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_generate_uuid_hyphen_positions() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE uuid_test AS SELECT GENERATE_UUID() AS uuid")
        .await
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
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT TYPEOF(NULL)").await.unwrap();
    assert_table_eq!(result, [["NULL"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT TYPEOF('hello')").await.unwrap();
    assert_table_eq!(result, [["STRING"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_int64() {
    let session = create_session();
    let result = session.execute_sql("SELECT TYPEOF(12 + 1)").await.unwrap();
    assert_table_eq!(result, [["INT64"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_float64() {
    let session = create_session();
    let result = session.execute_sql("SELECT TYPEOF(4.7)").await.unwrap();
    assert_table_eq!(result, [["FLOAT64"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_bool() {
    let session = create_session();
    let result = session.execute_sql("SELECT TYPEOF(TRUE)").await.unwrap();
    assert_table_eq!(result, [["BOOL"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_date() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(DATE '2024-01-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [["DATE"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_timestamp() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(TIMESTAMP '2024-01-15 10:30:00')")
        .await
        .unwrap();
    assert_table_eq!(result, [["TIMESTAMP"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["BYTES"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_array() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF([1, 2, 3])")
        .await
        .unwrap();
    assert_table_eq!(result, [["ARRAY"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_struct() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(STRUCT(25 AS x, 'apples' AS y))")
        .await
        .unwrap();
    assert_table_eq!(result, [["STRUCT"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_struct_field() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(STRUCT(25 AS x, 'apples' AS y).y)")
        .await
        .unwrap();
    assert_table_eq!(result, [["STRING"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_array_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF([25, 32][OFFSET(0)])")
        .await
        .unwrap();
    assert_table_eq!(result, [["INT64"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_json() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(JSON '{\"a\": 1}')")
        .await
        .unwrap();
    assert_table_eq!(result, [["JSON"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_numeric() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(NUMERIC '123.45')")
        .await
        .unwrap();
    assert_table_eq!(result, [["NUMERIC"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_time() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(TIME '10:30:00')")
        .await
        .unwrap();
    assert_table_eq!(result, [["TIME"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_datetime() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(DATETIME '2024-01-15 10:30:00')")
        .await
        .unwrap();
    assert_table_eq!(result, [["DATETIME"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_interval() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TYPEOF(INTERVAL 1 DAY)")
        .await
        .unwrap();
    assert_table_eq!(result, [["INTERVAL"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_typeof_multiple_columns() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT TYPEOF(NULL) AS a, TYPEOF('hello') AS b, TYPEOF(12+1) AS c, TYPEOF(4.7) AS d",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["NULL", "STRING", "INT64", "FLOAT64"]]);
}
