use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_null_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NULL").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_is_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NULL IS NULL").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_not_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 5 IS NOT NULL").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_null_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nullable (id INTEGER, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (1, 100), (2, NULL), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM nullable WHERE value IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_null_not_equal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NULL = NULL").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_null_arithmetic() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 5 + NULL").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_coalesce_with_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT COALESCE(NULL, NULL, 'default')")
        .unwrap();
    assert_table_eq!(result, [["default"]]);
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
    let result = executor.execute_sql("SELECT NULLIF(5, 10)").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_null_in_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INTEGER, status TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 'active'), (2, NULL), (3, 'inactive'), (4, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM data WHERE status IS NOT NULL ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_null_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INTEGER, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM data ORDER BY value NULLS FIRST")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[test]
fn test_null_order_by_last() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INTEGER, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM data ORDER BY value NULLS LAST")
        .unwrap();
    assert_table_eq!(result, [[3], [1], [2]]);
}

#[test]
fn test_count_ignores_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INTEGER, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(value) FROM data")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_count_star_includes_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INTEGER, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .unwrap();

    let result = executor.execute_sql("SELECT COUNT(*) FROM data").unwrap();
    assert_table_eq!(result, [[3]]);
}
