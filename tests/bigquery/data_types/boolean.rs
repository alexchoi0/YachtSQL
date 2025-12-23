use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_boolean_true() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE t (val BOOLEAN)").unwrap();
    session.execute_sql("INSERT INTO t VALUES (true)").unwrap();

    let result = session.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_boolean_false() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE t (val BOOLEAN)").unwrap();
    session.execute_sql("INSERT INTO t VALUES (false)").unwrap();

    let result = session.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_boolean_() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE t (val BOOLEAN)").unwrap();
    session.execute_sql("INSERT INTO t VALUES (NULL)").unwrap();

    let result = session.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_boolean_and() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT true AND true, true AND false, false AND false")
        .unwrap();

    assert_table_eq!(result, [[true, false, false]]);
}

#[test]
fn test_boolean_or() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT true OR true, true OR false, false OR false")
        .unwrap();

    assert_table_eq!(result, [[true, true, false]]);
}

#[test]
fn test_boolean_not() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT NOT true, NOT false").unwrap();

    assert_table_eq!(result, [[false, true]]);
}

#[test]
fn test_boolean_filter() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE t (id INT64, active BOOLEAN)")
        .unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1, true), (2, false), (3, true)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM t WHERE active ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_boolean_comparison_result() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT 1 = 1, 1 = 2, 1 < 2, 1 > 2")
        .unwrap();

    assert_table_eq!(result, [[true, false, true, false]]);
}

#[test]
fn test_boolean_case_when() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE t (val INT64)").unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1), (2), (3)")
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT val, CASE WHEN val > 1 THEN true ELSE false END AS gt1 FROM t ORDER BY val",
        )
        .unwrap();

    assert_table_eq!(result, [[1, false], [2, true], [3, true]]);
}

#[test]
fn test_boolean_null_and() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT NULL AND true, NULL AND false")
        .unwrap();

    assert_table_eq!(result, [[null, false]]);
}

#[test]
fn test_boolean_null_or() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT NULL OR true, NULL OR false")
        .unwrap();

    assert_table_eq!(result, [[true, null]]);
}
