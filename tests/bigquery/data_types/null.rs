use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_null_literal() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT NULL").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_is_() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT NULL IS NULL").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_not_() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT 5 IS NOT NULL").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_null_in_table() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 100), (2, NULL), (3, 300)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable WHERE value IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_null_not_equal() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT NULL = NULL").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_null_arithmetic() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT 5 + NULL").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_coalesce_with_() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE(NULL, NULL, 'default')")
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[test]
fn test_coalesce_first_not_() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT COALESCE('first', 'second')")
        .unwrap();
    assert_table_eq!(result, [["first"]]);
}

#[test]
fn test_if() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT IFNULL(NULL, 'default')")
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[test]
fn test_ifnull_not_() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT IFNULL('value', 'default')")
        .unwrap();
    assert_table_eq!(result, [["value"]]);
}

#[test]
fn test_nullif_equal() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT NULLIF(5, 5)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_nullif_not_equal() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT NULLIF(5, 10)").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_null_in_where_clause() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, status STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 'active'), (2, NULL), (3, 'inactive'), (4, NULL)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE status IS NOT NULL ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_null_order_by() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data ORDER BY value NULLS FIRST")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[test]
fn test_null_order_by_last() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data ORDER BY value NULLS LAST")
        .unwrap();
    assert_table_eq!(result, [[3], [1], [2]]);
}

#[test]
fn test_count_ignores_() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(value) FROM data")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_count_star_includes_() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, NULL), (3, 50)")
        .unwrap();

    let result = session.execute_sql("SELECT COUNT(*) FROM data").unwrap();
    assert_table_eq!(result, [[3]]);
}
