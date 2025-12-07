use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_is_distinct_from_integers() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT 1 IS DISTINCT FROM 2").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_distinct_from_same_integers() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT 1 IS DISTINCT FROM 1").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_is_distinct_from_null() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 1 IS DISTINCT FROM NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_distinct_from_both_null() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT NULL IS DISTINCT FROM NULL")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_is_not_distinct_from_integers() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 1 IS NOT DISTINCT FROM 1")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_not_distinct_from_different() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 1 IS NOT DISTINCT FROM 2")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_is_not_distinct_from_null() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 1 IS NOT DISTINCT FROM NULL")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_is_not_distinct_from_both_null() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT NULL IS NOT DISTINCT FROM NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_distinct_from_strings() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 'hello' IS DISTINCT FROM 'world'")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_distinct_from_same_strings() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 'hello' IS DISTINCT FROM 'hello'")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_is_distinct_from_with_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test_distinct (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_distinct VALUES (1, 1), (1, 2), (NULL, 1), (NULL, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a IS DISTINCT FROM b FROM test_distinct ORDER BY a NULLS LAST")
        .unwrap();
    assert_table_eq!(result, [[false], [true], [true], [false]]);
}

#[test]
fn test_is_not_distinct_from_with_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test_not_distinct (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_not_distinct VALUES (1, 1), (1, 2), (NULL, 1), (NULL, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a IS NOT DISTINCT FROM b FROM test_not_distinct ORDER BY a NULLS LAST")
        .unwrap();
    assert_table_eq!(result, [[true], [false], [false], [true]]);
}

#[test]
fn test_is_distinct_from_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE products (id INT64, category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'A'), (2, NULL), (3, 'B'), (4, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM products WHERE category IS DISTINCT FROM NULL ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_is_not_distinct_from_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, NULL), (2, 10), (3, NULL), (4, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM items WHERE value IS NOT DISTINCT FROM NULL ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_is_distinct_from_in_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE left_t (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE right_t (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO left_t VALUES (1, NULL), (2, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO right_t VALUES (1, NULL), (2, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT l.id FROM left_t l JOIN right_t r ON l.id = r.id WHERE l.val IS NOT DISTINCT FROM r.val ORDER BY l.id")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_is_distinct_from_case_expression() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT CASE WHEN 1 IS DISTINCT FROM 2 THEN 'different' ELSE 'same' END")
        .unwrap();
    assert_table_eq!(result, [["different"]]);
}

#[test]
fn test_is_distinct_from_booleans() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT TRUE IS DISTINCT FROM FALSE")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_distinct_from_same_booleans() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT TRUE IS DISTINCT FROM TRUE")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_is_distinct_from_floats() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 1.5 IS DISTINCT FROM 2.5")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_distinct_from_same_floats() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 1.5 IS DISTINCT FROM 1.5")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}
