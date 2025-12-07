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
fn test_union_distinct() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM table_a UNION SELECT name FROM table_b ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Alice"], ["Bob"], ["Charlie"], ["Diana"],]);
}

#[test]
fn test_intersect() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM table_a INTERSECT SELECT name FROM table_b ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Bob"], ["Charlie"]]);
}

#[test]
fn test_except() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM table_a EXCEPT SELECT name FROM table_b ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Alice"]]);
}

#[test]
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
fn test_union_with_where_clause() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM table_a WHERE id > 1 UNION SELECT name FROM table_b WHERE id < 4 ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Bob"], ["Charlie"]]);
}

#[test]
fn test_except_reverse() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT name FROM table_b EXCEPT SELECT name FROM table_a ORDER BY name")
        .unwrap();

    assert_table_eq!(result, [["Diana"]]);
}
