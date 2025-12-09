use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_array_join_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_join_test (id Int64, tags Array(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_join_test VALUES (1, ['a', 'b', 'c']), (2, ['x', 'y'])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, tags FROM arr_join_test ARRAY JOIN tags ORDER BY id, tags")
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [1, "b"], [1, "c"], [2, "x"], [2, "y"]]);
}

#[test]
fn test_array_join_with_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_alias_test (id Int64, nums Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_alias_test VALUES (1, [10, 20, 30])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, n FROM arr_alias_test ARRAY JOIN nums AS n ORDER BY n")
        .unwrap();
    assert_table_eq!(result, [[1, 10], [1, 20], [1, 30]]);
}

#[test]
fn test_left_array_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE left_arr_test (id Int64, items Array(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO left_arr_test VALUES (1, ['a', 'b']), (2, []), (3, ['x'])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, items FROM left_arr_test LEFT ARRAY JOIN items ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 4); // TODO: use table![[expected_values]]
}

#[test]
fn test_array_join_multiple_arrays() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE multi_arr_test (id Int64, keys Array(String), values Array(Int64))",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_arr_test VALUES (1, ['a', 'b'], [10, 20])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, k, v FROM multi_arr_test ARRAY JOIN keys AS k, values AS v")
        .unwrap();
    assert_table_eq!(result, [[1, "a", 10], [1, "b", 20]]);
}

#[ignore = "Requires function expressions in ARRAY JOIN"]
#[test]
fn test_array_join_with_array_enumerate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE enum_arr_test (id Int64, items Array(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO enum_arr_test VALUES (1, ['first', 'second', 'third'])")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, item, idx
            FROM enum_arr_test
            ARRAY JOIN items AS item, arrayEnumerate(items) AS idx
            ORDER BY idx",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "first", 1], [1, "second", 2], [1, "third", 3]]);
}

#[ignore = "Requires Nested type support"]
#[test]
fn test_array_join_nested() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE nested_arr_test (
                id Int64,
                nested Nested(key String, value Int64)
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO nested_arr_test VALUES
            (1, ['a', 'b'], [10, 20]),
            (2, ['x', 'y', 'z'], [100, 200, 300])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, nested.key, nested.value
            FROM nested_arr_test
            ARRAY JOIN nested
            ORDER BY id, nested.key",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "a", 10],
            [1, "b", 20],
            [2, "x", 100],
            [2, "y", 200],
            [2, "z", 300]
        ]
    );
}

#[test]
fn test_array_join_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_where_test (id Int64, scores Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_where_test VALUES (1, [10, 50, 90]), (2, [20, 80])")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, score
            FROM arr_where_test
            ARRAY JOIN scores AS score
            WHERE score > 30
            ORDER BY score",
        )
        .unwrap();
    assert_table_eq!(result, [[1, 50], [2, 80], [1, 90]]);
}

#[test]
fn test_array_join_aggregation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_agg_test (category String, values Array(Int64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO arr_agg_test VALUES
            ('A', [1, 2, 3]),
            ('B', [10, 20]),
            ('A', [4, 5])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, sum(v) AS total
            FROM arr_agg_test
            ARRAY JOIN values AS v
            GROUP BY category
            ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 15], ["B", 30]]);
}

#[test]
fn test_array_join_with_original_array() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_orig_test (id Int64, arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_orig_test VALUES (1, [10, 20, 30])")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, arr, element, length(arr) AS arr_len
            FROM arr_orig_test
            ARRAY JOIN arr AS element",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[test]
fn test_array_join_empty_handling() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_empty_test (id Int64, items Array(String))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO arr_empty_test VALUES
            (1, ['a']),
            (2, []),
            (3, ['b', 'c'])",
        )
        .unwrap();

    let inner_result = executor
        .execute_sql("SELECT id, item FROM arr_empty_test ARRAY JOIN items AS item ORDER BY id")
        .unwrap();
    assert_eq!(inner_result.num_rows(), 3);

    let left_result = executor
        .execute_sql(
            "SELECT id, item FROM arr_empty_test LEFT ARRAY JOIN items AS item ORDER BY id",
        )
        .unwrap();
    assert_eq!(left_result.num_rows(), 4);
}

#[ignore = "Requires function expressions in ARRAY JOIN"]
#[test]
fn test_array_join_with_map() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_map_test (id Int64, data Map(String, Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_map_test VALUES (1, map('a', 1, 'b', 2)), (2, map('x', 10))")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, key, value
            FROM arr_map_test
            ARRAY JOIN mapKeys(data) AS key, mapValues(data) AS value
            ORDER BY id, key",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "a", 1], [1, "b", 2], [2, "x", 10]]);
}

#[ignore = "Requires subquery support in ARRAY JOIN"]
#[test]
fn test_array_join_subquery() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT n, arr_element
            FROM (SELECT number AS n, [1, 2, 3] AS arr FROM numbers(3))
            ARRAY JOIN arr AS arr_element
            ORDER BY n, arr_element",
        )
        .unwrap();
    assert!(result.num_rows() == 9); // TODO: use table![[expected_values]]
}

#[ignore = "Requires tuple field access support"]
#[test]
fn test_array_join_with_tuple_array() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_arr_test (id Int64, pairs Array(Tuple(String, Int64)))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_arr_test VALUES (1, [('a', 1), ('b', 2)])")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, pair.1 AS key, pair.2 AS value
            FROM tuple_arr_test
            ARRAY JOIN pairs AS pair",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "a", 1], [1, "b", 2]]);
}

#[test]
fn test_array_join_grouping_set() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_group_test (region String, products Array(String), sales Array(Int64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO arr_group_test VALUES
            ('North', ['A', 'B'], [100, 200]),
            ('South', ['A', 'C'], [150, 250])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, product, sum(sale) AS total
            FROM arr_group_test
            ARRAY JOIN products AS product, sales AS sale
            GROUP BY region, product
            ORDER BY region, product",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["North", "A", 100],
            ["North", "B", 200],
            ["South", "A", 150],
            ["South", "C", 250]
        ]
    );
}
