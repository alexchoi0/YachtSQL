use crate::assert_table_eq;
use crate::common::{create_executor, d};

#[test]
fn test_array_length() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH([1, 2, 3])")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_array_length_empty() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ARRAY_LENGTH([])").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_array_concat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_CONCAT([1, 2], [3, 4])")
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3, 4]]]);
}

#[test]
fn test_array_reverse() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_REVERSE([1, 2, 3])")
        .unwrap();
    assert_table_eq!(result, [[[3, 2, 1]]]);
}

#[test]
fn test_generate_array() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT GENERATE_ARRAY(1, 5)").unwrap();
    assert_table_eq!(result, [[[1, 2, 3, 4, 5]]]);
}

#[test]
fn test_generate_array_with_step() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GENERATE_ARRAY(0, 10, 2)")
        .unwrap();
    assert_table_eq!(result, [[[0, 2, 4, 6, 8, 10]]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_array_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], ',')")
        .unwrap();
    assert_table_eq!(result, [["a,b,c"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_array_contains() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, tags ARRAY<STRING>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, ['red', 'blue']), (2, ['green', 'yellow'])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM data WHERE 'red' IN UNNEST(tags)")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_array_length_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ARRAY_LENGTH(NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_array_slice() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_SLICE([1, 2, 3, 4, 5], 2, 4)")
        .unwrap();
    assert_table_eq!(result, [[[2, 3, 4]]]);
}

#[test]
fn test_array_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (category STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES ('A', 1), ('A', 2), ('B', 3)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, ARRAY_AGG(value ORDER BY value) FROM data GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", [1, 2]], ["B", [3]]]);
}

#[test]
fn test_unnest_from() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM UNNEST([1, 2, 3]) AS num ORDER BY num")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_unnest_with_offset() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT num, offset FROM UNNEST(['a', 'b', 'c']) AS num WITH OFFSET AS offset ORDER BY offset")
        .unwrap();
    assert_table_eq!(result, [["a", 0], ["b", 1], ["c", 2]]);
}

#[test]
fn test_array_concat_multiple() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY_CONCAT([1], [2], [3])")
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_array_first() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT [1, 2, 3][OFFSET(0)]").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_array_safe_offset() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT [1, 2, 3][SAFE_OFFSET(10)]")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_array_ordinal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT [1, 2, 3][ORDINAL(1)]")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_array_safe_ordinal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT [1, 2, 3][SAFE_ORDINAL(10)]")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_generate_date_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-05')")
        .unwrap();
    assert_table_eq!(
        result,
        [[[
            d(2024, 1, 1),
            d(2024, 1, 2),
            d(2024, 1, 3),
            d(2024, 1, 4),
            d(2024, 1, 5)
        ]]]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_array_filter() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT ARRAY(SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x WHERE x > 2 ORDER BY x)",
        )
        .unwrap();
    assert_table_eq!(result, [[[3, 4, 5]]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_array_transform() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ARRAY(SELECT x * 2 FROM UNNEST([1, 2, 3]) AS x ORDER BY x)")
        .unwrap();
    assert_table_eq!(result, [[[2, 4, 6]]]);
}
