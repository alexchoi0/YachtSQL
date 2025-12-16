use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE table_a (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE table_b (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO table_a VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO table_b VALUES (2, 'Bob'), (3, 'Charlie'), (4, 'Diana')")
        .unwrap();
}

#[test]
#[ignore = "Implement me!"]
fn test_union_all() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM table_a UNION ALL SELECT name FROM table_b ORDER BY name")
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice"],
            ["Bob"],
            ["Bob"],
            ["Charlie"],
            ["Charlie"],
            ["Diana"],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_union_distinct() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM table_a UNION SELECT name FROM table_b ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"], ["Diana"],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_intersect() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM table_a INTERSECT SELECT name FROM table_b ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Bob"], ["Charlie"],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_except() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM table_a EXCEPT SELECT name FROM table_b ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Alice"],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_union_with_multiple_columns() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT id, name FROM table_a UNION SELECT id, name FROM table_b ORDER BY id")
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Alice"], [2, "Bob"], [3, "Charlie"], [4, "Diana"],]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_union_three_tables() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE t1 (x INT64)").unwrap();
    executor.execute_sql("CREATE TABLE t2 (x INT64)").unwrap();
    executor.execute_sql("CREATE TABLE t3 (x INT64)").unwrap();
    executor
        .execute_sql("INSERT INTO t1 VALUES (1), (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t2 VALUES (2), (3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t3 VALUES (3), (4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT x FROM t1 UNION SELECT x FROM t2 UNION SELECT x FROM t3 ORDER BY x")
        .unwrap();

    assert_table_eq!(result, [[1], [2], [3], [4],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_union_with_where_clause() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM table_a WHERE id > 1 UNION SELECT name FROM table_b WHERE id < 4 ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Bob"], ["Charlie"],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_except_reverse() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM table_b EXCEPT SELECT name FROM table_a ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Diana"],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_union_distinct_explicit() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT name FROM table_a UNION DISTINCT SELECT name FROM table_b ORDER BY name",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"], ["Diana"],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_intersect_distinct() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT name FROM table_a INTERSECT DISTINCT SELECT name FROM table_b ORDER BY name",
        )
        .unwrap();

    assert_table_eq!(result, [["Bob"], ["Charlie"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_intersect_all() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dup_a (name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE dup_b (name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dup_a VALUES ('Alice'), ('Alice'), ('Bob'), ('Bob'), ('Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dup_b VALUES ('Alice'), ('Bob'), ('Bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM dup_a INTERSECT ALL SELECT name FROM dup_b ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_except_distinct() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT name FROM table_a EXCEPT DISTINCT SELECT name FROM table_b ORDER BY name",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_except_all() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE except_a (name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE except_b (name STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO except_a VALUES ('Alice'), ('Alice'), ('Alice'), ('Bob'), ('Bob')",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO except_b VALUES ('Alice'), ('Bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM except_a EXCEPT ALL SELECT name FROM except_b ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Alice"], ["Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_mixed_set_operations() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE s1 (x INT64)").unwrap();
    executor.execute_sql("CREATE TABLE s2 (x INT64)").unwrap();
    executor.execute_sql("CREATE TABLE s3 (x INT64)").unwrap();
    executor
        .execute_sql("INSERT INTO s1 VALUES (1), (2), (3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO s2 VALUES (2), (3), (4)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO s3 VALUES (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql(
            "(SELECT x FROM s1 UNION SELECT x FROM s2) INTERSECT SELECT x FROM s3 ORDER BY x",
        )
        .unwrap();

    assert_table_eq!(result, [[3], [4]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_operations_with_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE null_a (x INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_b (x INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_a VALUES (1), (NULL), (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_b VALUES (2), (NULL), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT x FROM null_a INTERSECT SELECT x FROM null_b ORDER BY x NULLS FIRST")
        .unwrap();

    assert_table_eq!(result, [[null], [2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_except_with_subquery() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT name FROM table_a
            EXCEPT
            (SELECT name FROM table_b WHERE id > 3)
            ORDER BY name",
        )
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_union_intersect_precedence() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE p1 (x INT64)").unwrap();
    executor.execute_sql("CREATE TABLE p2 (x INT64)").unwrap();
    executor.execute_sql("CREATE TABLE p3 (x INT64)").unwrap();
    executor
        .execute_sql("INSERT INTO p1 VALUES (1), (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO p2 VALUES (2), (3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO p3 VALUES (1), (3)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT x FROM p1 UNION ALL SELECT x FROM p2 INTERSECT SELECT x FROM p3 ORDER BY x",
        )
        .unwrap();

    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_operation_with_cte() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "WITH cte_a AS (SELECT name FROM table_a),
                  cte_b AS (SELECT name FROM table_b)
            SELECT name FROM cte_a
            INTERSECT
            SELECT name FROM cte_b
            ORDER BY name",
        )
        .unwrap();

    assert_table_eq!(result, [["Bob"], ["Charlie"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_operation_corresponding() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_a (id INT64, name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE corr_b (name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_a VALUES (1, 'Alice', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_b VALUES ('Alice', 100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, value FROM corr_a INTERSECT SELECT name, value FROM corr_b")
        .unwrap();

    assert_table_eq!(result, [["Alice", 100]]);
}
