use crate::assert_table_eq;
use crate::common::create_clickhouse_executor;

#[test]
fn test_array_join_basic() {
    let mut executor = create_clickhouse_executor();
    executor
        .execute_sql("CREATE TABLE arr_test (id Int64, items Array(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_test VALUES (1, ['a', 'b', 'c']), (2, ['x', 'y'])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, item FROM arr_test ARRAY JOIN items AS item ORDER BY id, item")
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [1, "b"], [1, "c"], [2, "x"], [2, "y"]]);
}

#[test]
fn test_array_join_left() {
    let mut executor = create_clickhouse_executor();
    executor
        .execute_sql("CREATE TABLE arr_test (id Int64, items Array(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_test VALUES (1, ['a', 'b']), (2, [])")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, item FROM arr_test LEFT ARRAY JOIN items AS item ORDER BY id, item",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [1, "b"], [2, null]]);
}

#[test]
#[ignore = "Requires function expressions in ARRAY JOIN"]
fn test_array_join_with_array_enumerate() {
    let mut executor = create_clickhouse_executor();
    executor
        .execute_sql("CREATE TABLE enum_arr_test (id Int64, items Array(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO enum_arr_test VALUES (1, ['first', 'second', 'third'])")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, item, idx FROM enum_arr_test ARRAY JOIN items AS item, arrayEnumerate(items) AS idx ORDER BY idx",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "first", 1], [1, "second", 2], [1, "third", 3]]);
}

#[test]
#[ignore = "Requires Nested type support"]
fn test_array_join_with_nested_type() {
    let mut executor = create_clickhouse_executor();
    executor
        .execute_sql(
            "CREATE TABLE nested_arr_test (
                id Int64,
                nested Nested(key String, value Int64)
            )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO nested_arr_test VALUES (1, ['a', 'b'], [10, 20])")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, nested.key, nested.value FROM nested_arr_test ARRAY JOIN nested ORDER BY nested.key",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "a", 10], [1, "b", 20]]);
}

#[test]
#[ignore = "Requires function expressions in ARRAY JOIN"]
fn test_array_join_with_map() {
    let mut executor = create_clickhouse_executor();
    executor
        .execute_sql("CREATE TABLE arr_map_test (id Int64, data Map(String, Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_map_test VALUES (1, map('a', 1, 'b', 2)), (2, map('x', 10))")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, key, value FROM arr_map_test ARRAY JOIN mapKeys(data) AS key, mapValues(data) AS value ORDER BY id, key",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "a", 1], [1, "b", 2], [2, "x", 10]]);
}

#[test]
#[ignore = "Requires subquery support in ARRAY JOIN"]
fn test_array_join_in_subquery() {
    let mut executor = create_clickhouse_executor();

    let result = executor
        .execute_sql(
            "SELECT n, arr_element
             FROM (SELECT number AS n, [1, 2, 3] AS arr FROM numbers(3))
             ARRAY JOIN arr AS arr_element
             ORDER BY n, arr_element",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [0, 1],
            [0, 2],
            [0, 3],
            [1, 1],
            [1, 2],
            [1, 3],
            [2, 1],
            [2, 2],
            [2, 3]
        ]
    );
}
