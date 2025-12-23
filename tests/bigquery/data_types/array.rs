use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_array_literal_integers() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT [1, 2, 3]").unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[test]
fn test_array_literal_strings() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT ['a', 'b', 'c']").unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[test]
fn test_empty_array() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT []").unwrap();
    assert_table_eq!(result, [[[]]]); // empty array
}

#[test]
fn test_array_column() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, values ARRAY<INT64>)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, [10, 20, 30])")
        .unwrap();

    let result = session.execute_sql("SELECT values FROM data").unwrap();
    assert_table_eq!(result, [[[10, 20, 30]]]);
}

#[test]
fn test_array_subscript() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT [10, 20, 30][1]").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_array_with_() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, values ARRAY<INT64>)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, [1, NULL, 3])")
        .unwrap();

    let result = session.execute_sql("SELECT values FROM data").unwrap();
    assert_table_eq!(result, [[[1, null, 3]]]);
}

#[test]
fn test_array_null_column() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, values ARRAY<INT64>)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, NULL)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE values IS NULL")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_nested_array() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT [[1, 2], [3, 4]]").unwrap();
    assert_table_eq!(result, [[[[1, 2], [3, 4]]]]);
}
