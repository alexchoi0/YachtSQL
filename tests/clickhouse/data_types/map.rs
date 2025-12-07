use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_map_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT map('key1', 1, 'key2', 2)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_access() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT map('key1', 1, 'key2', 2)['key1']")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_map_keys() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapKeys(map('a', 1, 'b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_values() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapValues(map('a', 1, 'b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_contains() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapContains(map('a', 1, 'b', 2), 'a')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_map_contains_key_missing() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapContains(map('a', 1, 'b', 2), 'c')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_map_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE map_table (id INT64, data Map(String, Int64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO map_table VALUES (1, map('x', 10, 'y', 20)), (2, map('x', 30, 'y', 40))",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, data['x'] FROM map_table ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, 30]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_map_add() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapAdd(map('a', 1), map('b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_subtract() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapSubtract(map('a', 5, 'b', 10), map('a', 2, 'b', 3))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_populate_series() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapPopulateSeries(map(1, 10, 5, 50), 1, 5)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_filter() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapFilter((k, v) -> v > 1, map('a', 1, 'b', 2, 'c', 3))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_apply() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapApply((k, v) -> (k, v * 2), map('a', 1, 'b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_from_arrays() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapFromArrays(['a', 'b', 'c'], [1, 2, 3])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_update() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapUpdate(map('a', 1, 'b', 2), map('b', 20, 'c', 30))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_concat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapConcat(map('a', 1), map('b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_exists() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapExists((k, v) -> v > 1, map('a', 1, 'b', 2))")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_map_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapAll((k, v) -> v > 0, map('a', 1, 'b', 2))")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_map_sort() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapSort(map('c', 3, 'a', 1, 'b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_reverse_sort() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapReverseSort(map('c', 3, 'a', 1, 'b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_empty_map() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT map()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_map_length() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT length(map('a', 1, 'b', 2, 'c', 3))")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}
