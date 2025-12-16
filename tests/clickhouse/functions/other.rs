use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_transform() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT transform(1, [1, 2, 3], ['one', 'two', 'three'], 'unknown')")
        .unwrap();
    assert_table_eq!(result, [["one"]]);
}

#[test]
fn test_transform_not_found() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT transform(5, [1, 2, 3], ['one', 'two', 'three'], 'unknown')")
        .unwrap();
    assert_table_eq!(result, [["unknown"]]);
}

#[test]
fn test_array_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE array_data (id INT64, arr ARRAY<INT64>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO array_data VALUES (1, [1, 2, 3])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, arrayJoin(arr) AS val FROM array_data ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[1, 1], [1, 2], [1, 3]]);
}

#[test]
fn test_running_accumulate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT runningAccumulate(sumState(val)) OVER (ORDER BY val) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[1], [3], [6]]);
}
