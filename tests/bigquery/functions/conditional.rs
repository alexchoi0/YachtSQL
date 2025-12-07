use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_case_simple() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
        .unwrap();
    assert_table_eq!(result, [["one"]]);
}

#[test]
fn test_case_searched() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CASE WHEN 5 > 3 THEN 'yes' ELSE 'no' END")
        .unwrap();
    assert_table_eq!(result, [["yes"]]);
}

#[test]
fn test_case_multiple_conditions() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES ('A', 95), ('B', 75), ('C', 55)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END AS grade FROM scores ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["A", "A"], ["B", "B"], ["C", "C"]]);
}

#[test]
fn test_case_no_else() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CASE WHEN FALSE THEN 'yes' END")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_coalesce_first_non_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT COALESCE(NULL, NULL, 'hello', 'world')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_coalesce_all_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT COALESCE(NULL, NULL, NULL)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_coalesce_first_not_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT COALESCE('first', 'second')")
        .unwrap();
    assert_table_eq!(result, [["first"]]);
}

#[test]
fn test_nullif_equal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NULLIF(5, 5)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_nullif_not_equal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NULLIF(5, 3)").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_nullif_strings() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NULLIF('hello', 'hello')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_if_true() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IF(TRUE, 'yes', 'no')")
        .unwrap();
    assert_table_eq!(result, [["yes"]]);
}

#[test]
fn test_if_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IF(FALSE, 'yes', 'no')")
        .unwrap();
    assert_table_eq!(result, [["no"]]);
}

#[test]
fn test_if_with_expression() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IF(5 > 3, 'greater', 'lesser')")
        .unwrap();
    assert_table_eq!(result, [["greater"]]);
}

#[test]
fn test_case_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('A', 10), ('B', 20), ('C', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM items WHERE CASE WHEN value > 15 THEN TRUE ELSE FALSE END ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["B"], ["C"]]);
}

#[test]
fn test_nested_case() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CASE WHEN 5 > 3 THEN CASE WHEN 2 > 1 THEN 'both' ELSE 'first' END ELSE 'neither' END")
        .unwrap();
    assert_table_eq!(result, [["both"]]);
}

#[test]
fn test_coalesce_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('A', NULL), ('B', 10), ('C', NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM items WHERE COALESCE(value, 0) > 5 ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["B"]]);
}
