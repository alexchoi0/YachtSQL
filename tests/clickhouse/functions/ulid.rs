use crate::common::create_executor;

#[ignore = "Type inference issue with return type"]
#[test]
fn test_generate_ulid() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT generateULID()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Type inference issue with return type"]
#[test]
fn test_generate_ulid_multiple() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT generateULID(), generateULID(), generateULID()")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Type inference issue with nested function return type"]
#[test]
fn test_ulid_string_to_datetime() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ULIDStringToDateTime(generateULID())")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Type inference issue with nested function return type"]
#[test]
fn test_ulid_string_to_datetime_with_timezone() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ULIDStringToDateTime(generateULID(), 'UTC')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
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
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
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
    assert!(result.num_rows() == 5); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_ulid_uniqueness() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT uniqExact(ulid) AS unique_count
            FROM (SELECT generateULID() AS ulid FROM numbers(1000))",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
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
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
