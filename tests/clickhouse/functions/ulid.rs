use crate::common::create_executor;

#[test]
fn test_generate_ulid() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT generateULID()").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_generate_ulid_multiple() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT generateULID(), generateULID(), generateULID()")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_ulid_string_to_datetime() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ULIDStringToDateTime(generateULID())")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_ulid_string_to_datetime_with_timezone() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ULIDStringToDateTime(generateULID(), 'UTC')")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[ignore = "DEFAULT expression function not supported"]
#[test]
fn test_ulid_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ulid_test (id String DEFAULT generateULID(), data String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ulid_test (data) VALUES ('test1'), ('test2'), ('test3')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, data, ULIDStringToDateTime(id) AS created FROM ulid_test")
        .unwrap();
    assert!(result.num_rows() == 3);
}

#[test]
fn test_ulid_ordering() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ulid_order (ulid String, value Int64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO ulid_order
            SELECT generateULID(), number
            FROM numbers(5)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM ulid_order ORDER BY ulid")
        .unwrap();
    assert!(result.num_rows() == 5);
}

#[ignore = "Subquery in FROM clause must have an alias"]
#[test]
fn test_ulid_uniqueness() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT uniqExact(ulid) AS unique_count
            FROM (SELECT generateULID() AS ulid FROM numbers(1000))",
        )
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[ignore = "toUnixTimestamp not available in ClickHouse dialect"]
#[test]
fn test_ulid_extract_timestamp() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                generateULID() AS ulid,
                ULIDStringToDateTime(generateULID()) AS ts,
                toUnixTimestamp(ULIDStringToDateTime(generateULID())) AS unix_ts",
        )
        .unwrap();
    assert!(result.num_rows() == 1);
}
