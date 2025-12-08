#![allow(clippy::approx_constant)]

use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_values_basic() {
    let mut executor = create_executor();
    let result = executor.execute_sql("VALUES (1), (2), (3)").unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_multiple_columns() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, "b"], [3, "c"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_as_subquery() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_with_order_by() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (3), (1), (2) ORDER BY 1")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_with_limit() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (1), (2), (3), (4), (5) LIMIT 3")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_with_offset() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (1), (2), (3), (4), (5) OFFSET 2")
        .unwrap();
    assert_table_eq!(result, [[3], [4], [5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_union() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (1), (2) UNION VALUES (3), (4)")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_union_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (1), (2) UNION ALL VALUES (1), (3)")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [1], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_intersect() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (1), (2), (3) INTERSECT VALUES (2), (3), (4)")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_except() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (1), (2), (3) EXCEPT VALUES (2)")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_in_cte() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "WITH data AS (VALUES (1, 'a'), (2, 'b'), (3, 'c'))
             SELECT * FROM data",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, "b"], [3, "c"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_join() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT t1.column1, t2.column1
             FROM (VALUES (1), (2)) AS t1
             CROSS JOIN (VALUES ('a'), ('b')) AS t2",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [1, "b"], [2, "a"], [2, "b"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_with_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (1, 'a'), (2, NULL), (3, 'c')")
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, null], [3, "c"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_mixed_types() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (1, 'text', TRUE, 3.14)")
        .unwrap();
    assert_table_eq!(result, [[1, "text", true, 3.14]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_expressions() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (1 + 2), (3 * 4), (10 / 2)")
        .unwrap();
    assert_table_eq!(result, [[3], [12], [5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_insert_from() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE values_insert (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO values_insert SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM values_insert ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_single_row() {
    let mut executor = create_executor();
    let result = executor.execute_sql("VALUES (42)").unwrap();
    assert_table_eq!(result, [[42]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_many_columns() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)")
        .unwrap();
    assert_table_eq!(result, [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_where_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE exists_test (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO exists_test VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM exists_test e
             WHERE EXISTS (SELECT 1 FROM (VALUES (1), (2)) AS v WHERE v.column1 = e.id)",
        )
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_values_in_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE in_test (id INTEGER, val TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO in_test VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM in_test WHERE id IN (SELECT column1 FROM (VALUES (1), (3)) AS v)",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [3, "c"]]);
}
