use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_array_length() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT array_length([1, 2, 3, 4, 5], 1)")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_array_length_empty() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT array_length([], 1)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_cardinality() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT cardinality([1, 2, 3])")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_array_cat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT array_cat([1, 2], [3, 4])")
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3, 4]]]);
}

#[test]
fn test_array_append() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT array_append([1, 2, 3], 4)")
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3, 4]]]);
}

#[test]
fn test_array_prepend() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT array_prepend(0, [1, 2, 3])")
        .unwrap();
    assert_table_eq!(result, [[[0, 1, 2, 3]]]);
}

#[test]
fn test_array_remove() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT array_remove([1, 2, 3, 2, 4], 2)")
        .unwrap();
    assert_table_eq!(result, [[[1, 3, 4]]]);
}

#[test]
fn test_array_replace() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT array_replace([1, 2, 3, 2], 2, 10)")
        .unwrap();
    assert_table_eq!(result, [[[1, 10, 3, 10]]]);
}

#[test]
fn test_array_position() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT array_position([10, 20, 30, 40], 30)")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_array_position_not_found() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT array_position([10, 20, 30], 50)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_array_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (category STRING, name STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO items VALUES ('fruit', 'apple'), ('fruit', 'banana'), ('veg', 'carrot')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, array_agg(name) FROM items GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["fruit", ["apple", "banana"]], ["veg", ["carrot"]],]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_unnest() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT unnest([1, 2, 3])").unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_array_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT array_to_string(['a', 'b', 'c'], ', ')")
        .unwrap();
    assert_table_eq!(result, [["a, b, c"]]);
}

#[test]
fn test_string_to_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT string_to_array('a,b,c', ',')")
        .unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_array_dims() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT array_dims([[1,2], [3,4]])")
        .unwrap();
    assert_table_eq!(result, [["[1:2][1:2]"]]);
}
