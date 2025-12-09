use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_json_extract_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONExtractString('{\"name\": \"alice\"}', 'name')")
        .unwrap();
    assert_table_eq!(result, [["alice"]]);
}

#[test]
fn test_json_extract_int() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONExtractInt('{\"age\": 30}', 'age')")
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_json_extract_float() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONExtractFloat('{\"price\": 19.99}', 'price')")
        .unwrap();
    assert_table_eq!(result, [[19.99]]);
}

#[test]
fn test_json_extract_bool() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONExtractBool('{\"active\": true}', 'active')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_json_extract_raw() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONExtractRaw('{\"data\": {\"x\": 1}}', 'data')")
        .unwrap();
    assert_table_eq!(result, [["{\"x\":1}"]]);
}

#[test]
fn test_json_extract_array_raw() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONExtractArrayRaw('{\"items\": [1, 2, 3]}', 'items')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_json_extract_keys_and_values() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONExtractKeysAndValues('{\"a\": 1, \"b\": 2}', 'Int64')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_json_has() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONHas('{\"name\": \"alice\"}', 'name')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_json_length() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONLength('{\"items\": [1, 2, 3]}', 'items')")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_json_type() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONType('{\"name\": \"alice\"}')")
        .unwrap();
    assert_table_eq!(result, [["object"]]);
}

#[test]
fn test_json_extract_nested() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONExtractString('{\"user\": {\"name\": \"bob\"}}', 'user', 'name')")
        .unwrap();
    assert_table_eq!(result, [["bob"]]);
}

#[test]
fn test_json_extract_with_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE json_data (id INT64, data STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO json_data VALUES (1, '{\"name\": \"alice\"}'), (2, '{\"name\": \"bob\"}')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, JSONExtractString(data, 'name') AS name FROM json_data ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "alice"], [2, "bob"]]);
}

#[test]
fn test_to_json_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toJSONString(tuple(1, 'hello'))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_json_extract_keys() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT JSONExtractKeys('{\"a\": 1, \"b\": 2, \"c\": 3}')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
