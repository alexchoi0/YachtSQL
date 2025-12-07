use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_array_subscript() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_sub (arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_sub VALUES ([1, 2, 3])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT arr[1], arr[2], arr[3] FROM arr_sub")
        .unwrap();
    assert_table_eq!(result, [[1, 2, 3]]);
}

#[test]
fn test_array_subscript_negative() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_neg (arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_neg VALUES ([1, 2, 3])")
        .unwrap();

    let result = executor.execute_sql("SELECT arr[-1] FROM arr_neg").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_array_concat_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_concat_op (a Array(Int64), b Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_concat_op VALUES ([1, 2], [3, 4])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT arrayConcat(a, b) FROM arr_concat_op")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_array_has_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_has_op (id INT64, arr Array(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_has_op VALUES (1, ['a', 'b', 'c']), (2, ['x', 'y'])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM arr_has_op WHERE has(arr, 'b')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_array_hasall() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_hasall (id INT64, arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_hasall VALUES (1, [1, 2, 3, 4]), (2, [1, 2])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM arr_hasall WHERE hasAll(arr, [1, 2, 3])")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_array_hasany() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_hasany (id INT64, arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_hasany VALUES (1, [1, 2]), (2, [3, 4]), (3, [5, 6])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM arr_hasany WHERE hasAny(arr, [2, 3]) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_array_length_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_len_op (arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_len_op VALUES ([1, 2, 3, 4, 5])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT length(arr) FROM arr_len_op")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_array_empty_check() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_empty_op (id INT64, arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_empty_op VALUES (1, []), (2, [1])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM arr_empty_op WHERE empty(arr)")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_array_notempty_check() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_notempty (id INT64, arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_notempty VALUES (1, []), (2, [1])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM arr_notempty WHERE notEmpty(arr)")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_array_slice() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_slice (arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_slice VALUES ([1, 2, 3, 4, 5])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT arraySlice(arr, 2, 3) FROM arr_slice")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_array_first() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_first (arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_first VALUES ([10, 20, 30])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT arrayFirst(x -> x > 15, arr) FROM arr_first")
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[test]
fn test_array_last() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_last (arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_last VALUES ([10, 20, 30])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT arrayLast(x -> x < 25, arr) FROM arr_last")
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[test]
fn test_array_filter() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_filter (arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_filter VALUES ([1, 2, 3, 4, 5])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT arrayFilter(x -> x % 2 = 0, arr) FROM arr_filter")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_array_map() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_map (arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_map VALUES ([1, 2, 3])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT arrayMap(x -> x * 2, arr) FROM arr_map")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_array_reduce() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_reduce (arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_reduce VALUES ([1, 2, 3, 4])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT arrayReduce('sum', arr) FROM arr_reduce")
        .unwrap();
    assert_table_eq!(result, [[10]]);
}
