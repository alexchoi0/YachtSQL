use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_array_literal_integers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ARRAY[1, 2, 3]").unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[test]
fn test_array_literal_strings() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ARRAY['a', 'b', 'c']").unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[test]
fn test_empty_array() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ARRAY[]::INTEGER[]").unwrap();
    assert_table_eq!(result, [[[]]]);
}

#[test]
fn test_array_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INTEGER, values INTEGER[])")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, ARRAY[10, 20, 30])")
        .unwrap();

    let result = executor.execute_sql("SELECT values FROM data").unwrap();
    assert_table_eq!(result, [[[10, 20, 30]]]);
}

#[test]
fn test_array_subscript() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT (ARRAY[10, 20, 30])[1]")
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_array_with_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INTEGER, values INTEGER[])")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, ARRAY[1, NULL, 3])")
        .unwrap();

    let result = executor.execute_sql("SELECT values FROM data").unwrap();
    assert_table_eq!(result, [[[1, null, 3]]]);
}

#[test]
fn test_array_null_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INTEGER, values INTEGER[])")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM data WHERE values IS NULL")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_nested_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]]")
        .unwrap();
    assert_table_eq!(result, [[[[1, 2], [3, 4]]]]);
}
