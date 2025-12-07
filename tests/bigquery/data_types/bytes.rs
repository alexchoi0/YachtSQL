use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_bytes_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_hex_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\\x48\\x65\\x6c\\x6c\\x6f')")
        .unwrap();
    assert_table_eq!(result, [["Hello"]]);
}

#[test]
fn test_bytes_empty() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LENGTH(b'')").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_bytes_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bindata (id INT64, data BYTES)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bindata VALUES (1, b'hello'), (2, b'world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM bindata ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_bytes_length() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LENGTH(b'hello')").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_concat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(CONCAT(b'hello', b' ', b'world'))")
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_substr() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(SUBSTR(b'hello', 2, 3))")
        .unwrap();
    assert_table_eq!(result, [["ell"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_string_to_bytes() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(CAST('hello' AS BYTES))")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_bytes_comparison() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT b'abc' < b'abd'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_bytes_equality() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT b'hello' = b'hello'").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_bytes_() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bindata (id INT64, data BYTES)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bindata VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM bindata WHERE data IS NULL")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_from_base64() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64('aGVsbG8='))")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_to_base64() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TO_BASE64(b'hello')").unwrap();
    assert_table_eq!(result, [["aGVsbG8="]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_from_hex() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX('48656c6c6f'))")
        .unwrap();
    assert_table_eq!(result, [["Hello"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_to_hex() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TO_HEX(b'Hello')").unwrap();
    assert_table_eq!(result, [["48656c6c6f"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_left() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(LEFT(b'hello', 3))")
        .unwrap();
    assert_table_eq!(result, [["hel"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_right() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(RIGHT(b'hello', 3))")
        .unwrap();
    assert_table_eq!(result, [["llo"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_reverse() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(REVERSE(b'hello'))")
        .unwrap();
    assert_table_eq!(result, [["olleh"]]);
}

#[test]
fn test_bytes_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bindata (id INT64, data BYTES)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bindata VALUES (1, b'hello'), (2, b'world'), (3, b'hello')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM bindata WHERE data = b'hello' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bytes_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bindata (id INT64, data BYTES)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bindata VALUES (1, b'c'), (2, b'a'), (3, b'b')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM bindata ORDER BY data")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}
