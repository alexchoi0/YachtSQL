use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_string_basic() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE t (val STRING)").unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES ('hello')")
        .unwrap();

    let result = executor.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_string_empty() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE t (val STRING)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('')").unwrap();

    let result = executor.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [[""]]);
}

#[test]
fn test_string_with_spaces() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE t (val STRING)").unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES ('hello world')")
        .unwrap();

    let result = executor.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

#[test]
fn test_string_null() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE t (val STRING)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (NULL)").unwrap();

    let result = executor.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_string_comparison() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE t (val STRING)").unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES ('apple'), ('banana'), ('cherry')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM t WHERE val > 'banana' ORDER BY val")
        .unwrap();

    assert_table_eq!(result, [["cherry"]]);
}

#[test]
fn test_string_equality() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE t (val STRING)").unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES ('apple'), ('banana'), ('apple')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM t WHERE val = 'apple'")
        .unwrap();

    assert_table_eq!(result, [["apple"], ["apple"]]);
}

#[test]
fn test_string_like() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE t (val STRING)").unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES ('apple'), ('application'), ('banana')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM t WHERE val LIKE 'app%' ORDER BY val")
        .unwrap();

    assert_table_eq!(result, [["apple"], ["application"]]);
}

#[test]
fn test_string_ordering() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE t (val STRING)").unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES ('banana'), ('apple'), ('cherry')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM t ORDER BY val ASC")
        .unwrap();

    assert_table_eq!(result, [["apple"], ["banana"], ["cherry"]]);
}

#[test]
fn test_string_concat_operator() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT 'hello' || ' ' || 'world'")
        .unwrap();

    assert_table_eq!(result, [["hello world"]]);
}

#[test]
fn test_string_in_list() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE TABLE t (val STRING)").unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES ('a'), ('b'), ('c'), ('d')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM t WHERE val IN ('a', 'c') ORDER BY val")
        .unwrap();

    assert_table_eq!(result, [["a"], ["c"]]);
}
