use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_generate_uuid_v4() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT generateUUIDv4()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_empty_uuid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT empty(toUUID('00000000-0000-0000-0000-000000000000'))")
        .unwrap();
    assert_table_eq!(result, [[1]]); // Zero UUID is empty
}

#[ignore = "Implement me!"]
#[test]
fn test_not_empty_uuid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT notEmpty(generateUUIDv4())")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_to_uuid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')")
        .unwrap();
    assert_table_eq!(result, [["61f0c404-5cb3-11e7-907b-a6006ad3dba0"]]);
}

#[test]
fn test_to_uuid_or_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toUUIDOrNull('invalid-uuid')")
        .unwrap();
    assert_table_eq!(result, [[null]]); // Invalid UUID returns NULL
}

#[ignore = "Type inference issue with return type"]
#[test]
fn test_to_uuid_or_zero() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toUUIDOrZero('invalid-uuid')")
        .unwrap();
    assert_table_eq!(result, [["00000000-0000-0000-0000-000000000000"]]); // Invalid returns zero UUID
}

#[ignore = "Type inference issue with BYTES return type"]
#[test]
fn test_uuid_string_to_num() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT UUIDStringToNum('61f0c404-5cb3-11e7-907b-a6006ad3dba0')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Type inference issue with BYTES return type from nested UUIDStringToNum"]
#[test]
fn test_uuid_num_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT UUIDNumToString(UUIDStringToNum('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))",
        )
        .unwrap();
    assert_table_eq!(result, [["61f0c404-5cb3-11e7-907b-a6006ad3dba0"]]); // Round-trip
}

#[ignore = "Implement me!"]
#[test]
fn test_uuid_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_test (id UUID, name STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO uuid_test VALUES (generateUUIDv4(), 'alice'), (generateUUIDv4(), 'bob')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM uuid_test ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["alice"], ["bob"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_uuid_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_cmp (id UUID, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_cmp VALUES ('00000000-0000-0000-0000-000000000001', 1), ('00000000-0000-0000-0000-000000000002', 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM uuid_cmp WHERE id = '00000000-0000-0000-0000-000000000001'")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_server_uuid() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT serverUUID()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
