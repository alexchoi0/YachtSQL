use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_lambda_basic() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayMap(x -> x * 2, [1, 2, 3])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_lambda_filter() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayFilter(x -> x > 2, [1, 2, 3, 4, 5])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_lambda_two_args() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayMap((x, y) -> x + y, [1, 2, 3], [10, 20, 30])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_lambda_arrayfirst() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayFirst(x -> x > 5, [1, 3, 7, 10])")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
fn test_lambda_arraylast() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayLast(x -> x < 10, [1, 3, 7, 12])")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
fn test_lambda_arrayfirstindex() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayFirstIndex(x -> x > 5, [1, 3, 7, 10])")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_lambda_arraylastindex() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayLastIndex(x -> x < 10, [1, 3, 7, 12])")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_lambda_arrayexists() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayExists(x -> x > 5, [1, 2, 3])")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_lambda_arrayall() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayAll(x -> x > 0, [1, 2, 3])")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_lambda_arraycount() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayCount(x -> x % 2 = 0, [1, 2, 3, 4, 5])")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_lambda_arraysum() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arraySum(x -> x * 2, [1, 2, 3])")
        .unwrap();
    assert_table_eq!(result, [[12]]);
}

#[test]
fn test_lambda_arraycumsum() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayCumSum(x -> x, [1, 2, 3, 4])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_lambda_arraysort() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arraySort(x -> -x, [3, 1, 2])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_lambda_arrayreversesort() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayReverseSort(x -> x, [3, 1, 2])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_lambda_complex_expression() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayMap(x -> if(x > 2, x * 10, x), [1, 2, 3, 4])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_lambda_with_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lambda_table (arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lambda_table VALUES ([1, 2, 3]), ([4, 5, 6])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT arrayMap(x -> x + 10, arr) FROM lambda_table")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_lambda_nested() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayMap(x -> arrayMap(y -> y * 2, x), [[1, 2], [3, 4]]")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_lambda_arrayfill() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayFill(x -> x > 0, [0, 1, 0, 0, 1])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_lambda_arrayreversefill() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arrayReverseFill(x -> x > 0, [0, 0, 1, 0, 0])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_lambda_arraysplit() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [0, 0, 1, 0, 1])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
