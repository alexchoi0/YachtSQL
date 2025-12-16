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
fn test_unnest_with_offset() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT num, offset FROM UNNEST(['a', 'b', 'c']) AS num WITH OFFSET AS offset ORDER BY offset")
        .unwrap();
    assert_table_eq!(result, [["a", 0], ["b", 1], ["c", 2]]);
}

#[test]
fn test_unnest_struct_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT s.a, s.b FROM UNNEST([STRUCT(1 AS a, 'x' AS b), STRUCT(2 AS a, 'y' AS b)]) AS s ORDER BY s.a")
        .unwrap();
    assert_table_eq!(result, [[1, "x"], [2, "y"]]);
}

#[test]
fn test_unnest_struct_from_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id INT64, things ARRAY<STRUCT<name STRING, qty INT64>>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, [STRUCT('apple' AS name, 5 AS qty), STRUCT('banana' AS name, 3 AS qty)])")
        .unwrap();
    let result = executor
        .execute_sql("SELECT id, thing.name, thing.qty FROM items, UNNEST(things) AS thing ORDER BY thing.name")
        .unwrap();
    assert_table_eq!(result, [[1, "apple", 5], [1, "banana", 3]]);
}

#[test]
fn test_unnest_struct_from_table_with_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items2 (id INT64, things ARRAY<STRUCT<name STRING, qty INT64>>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items2 VALUES (1, [STRUCT('apple' AS name, 5 AS qty), STRUCT('banana' AS name, 3 AS qty)])")
        .unwrap();
    let result = executor
        .execute_sql("SELECT i.id, thing.name, thing.qty FROM items2 i, UNNEST(i.things) AS thing ORDER BY thing.name")
        .unwrap();
    assert_table_eq!(result, [[1, "apple", 5], [1, "banana", 3]]);
}

#[test]
fn test_unnest_struct_in_cte() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items3 (id INT64, things ARRAY<STRUCT<name STRING, qty INT64>>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items3 VALUES (1, [STRUCT('apple' AS name, 5 AS qty), STRUCT('banana' AS name, 3 AS qty)])")
        .unwrap();
    let result = executor
        .execute_sql(
            "WITH flattened AS (
            SELECT i.id, thing.name, thing.qty
            FROM items3 i, UNNEST(i.things) AS thing
        )
        SELECT * FROM flattened ORDER BY name",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "apple", 5], [1, "banana", 3]]);
}

#[test]
fn test_unnest_struct_positional_fields() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items4 (id INT64, things ARRAY<STRUCT<product_name STRING, quantity INT64>>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items4 VALUES (1, [STRUCT('Laptop', 1), STRUCT('Mouse', 2)])")
        .unwrap();
    let result = executor
        .execute_sql("SELECT i.id, thing.product_name, thing.quantity FROM items4 i, UNNEST(i.things) AS thing ORDER BY thing.quantity")
        .unwrap();
    assert_table_eq!(result, [[1, "Laptop", 1], [1, "Mouse", 2]]);
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
