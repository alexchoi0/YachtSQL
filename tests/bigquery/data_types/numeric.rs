use crate::assert_table_eq;
use crate::common::{create_session, n};

#[test]
fn test_int64_basic() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE t (val INT64)").unwrap();
    session.execute_sql("INSERT INTO t VALUES (42)").unwrap();

    let result = session.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [[42]]);
}

#[test]
fn test_int64_negative() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE t (val INT64)").unwrap();
    session.execute_sql("INSERT INTO t VALUES (-100)").unwrap();

    let result = session.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [[-100]]);
}

#[test]
fn test_int64_zero() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE t (val INT64)").unwrap();
    session.execute_sql("INSERT INTO t VALUES (0)").unwrap();

    let result = session.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_float64_basic() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE t (val FLOAT64)").unwrap();
    session.execute_sql("INSERT INTO t VALUES (3.11)").unwrap();

    let result = session.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [[3.11]]);
}

#[test]
fn test_float64_scientific() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE t (val FLOAT64)").unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (1.5e10)")
        .unwrap();

    let result = session.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [[1.5e10]]);
}

#[test]
fn test_numeric_basic() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT 123.456").unwrap();

    assert_table_eq!(result, [[n("123.456")]]);
}

#[test]
fn test_numeric_arithmetic() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT 10.5 + 2.3").unwrap();

    assert_table_eq!(result, [[n("12.8")]]);
}

#[test]
fn test_int64_() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE t (val INT64)").unwrap();
    session.execute_sql("INSERT INTO t VALUES (NULL)").unwrap();

    let result = session.execute_sql("SELECT val FROM t").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_numeric_comparison() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE nums (a INT64, b INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (10, 20), (30, 30), (50, 40)")
        .unwrap();

    let result = session
        .execute_sql("SELECT a FROM nums WHERE a >= b ORDER BY a")
        .unwrap();

    assert_table_eq!(result, [[30], [50]]);
}

#[test]
fn test_numeric_in_expression() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE t (val INT64)").unwrap();
    session
        .execute_sql("INSERT INTO t VALUES (10), (20), (30)")
        .unwrap();

    let result = session
        .execute_sql("SELECT val * 2, val + 5 FROM t ORDER BY val")
        .unwrap();

    assert_table_eq!(result, [[20, 15], [40, 25], [60, 35],]);
}
