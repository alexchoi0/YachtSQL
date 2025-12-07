use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_array_map() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayMap(x -> x * 2, [1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[[2, 4, 6, 8, 10]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_map_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayMap(x -> upper(x), ['hello', 'world'])")
        .unwrap();
    assert_table_eq!(result, [[["HELLO", "WORLD"]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_filter() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayFilter(x -> x > 3, [1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[[4, 5]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_filter_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayFilter(x -> length(x) > 3, ['hi', 'hello', 'world', 'a'])")
        .unwrap();
    assert_table_eq!(result, [[["hello", "world"]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_exists() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayExists(x -> x > 5, [1, 2, 3, 4, 5, 6])")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayAll(x -> x > 0, [1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_first() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayFirst(x -> x > 3, [1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_first_index() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayFirstIndex(x -> x > 3, [1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_last() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayLast(x -> x < 4, [1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_last_index() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayLastIndex(x -> x < 4, [1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_count_matching() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayCount(x -> x % 2 = 0, [1, 2, 3, 4, 5, 6])")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_sum_lambda() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arraySum(x -> x * x, [1, 2, 3, 4])")
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_avg_lambda() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayAvg(x -> x * 2, [1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[6.0]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_min_lambda() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayMin(x -> abs(x), [-5, -2, 1, 3])")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_max_lambda() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayMax(x -> -x, [1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[-1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_sort_lambda() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arraySort(x -> -x, [3, 1, 4, 1, 5])")
        .unwrap();
    assert_table_eq!(result, [[[5, 4, 3, 1, 1]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_reverse_sort_lambda() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayReverseSort(x -> x, [3, 1, 4, 1, 5])")
        .unwrap();
    assert_table_eq!(result, [[[5, 4, 3, 1, 1]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_split() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_array_reduce() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayReduce('sum', [1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_reduce_max() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayReduce('max', [10, 5, 20, 15])")
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_reduce_avg() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayReduce('avg', [1.0, 2.0, 3.0, 4.0, 5.0])")
        .unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_reduce_in_rows() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayReduceInRanges('sum', [(1, 3), (2, 4)], [1, 2, 3, 4, 5])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_array_fold() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayFold((acc, x) -> acc + x, [1, 2, 3, 4], toInt64(0))")
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_cumulative_sum() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayCumSum([1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[[1, 3, 6, 10, 15]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_cumulative_sum_non_negative() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayCumSumNonNegative([1, -2, 3, -4, 5])")
        .unwrap();
    assert_table_eq!(result, [[[1, 0, 3, 0, 5]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_difference() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayDifference([1, 3, 6, 10, 15])")
        .unwrap();
    assert_table_eq!(result, [[[0, 2, 3, 4, 5]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_multi_array_map() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayMap((x, y) -> x + y, [1, 2, 3], [10, 20, 30])")
        .unwrap();
    assert_table_eq!(result, [[[11, 22, 33]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_lambda_with_tuple() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayMap(x -> (x, x * x), [1, 2, 3, 4])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_nested_lambda() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayMap(x -> arrayMap(y -> y * x, [1, 2, 3]), [10, 20])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_lambda_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lambda_test (id UInt32, values Array(Int64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO lambda_test VALUES
            (1, [1, 2, 3]),
            (2, [10, 20, 30]),
            (3, [-1, -2, -3])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id,
                arrayMap(x -> x * 2, values) AS doubled,
                arrayFilter(x -> x > 0, values) AS positive,
                arraySum(x -> x, values) AS total
            FROM lambda_test
            ORDER BY id",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_array_compact() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayCompact([1, 1, 2, 2, 2, 3, 3, 1])")
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3, 1]]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_array_zip() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayZip([1, 2, 3], ['a', 'b', 'c'])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_array_auc() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
