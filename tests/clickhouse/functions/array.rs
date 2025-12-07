use crate::assert_table_eq;
use crate::common::create_executor;

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
