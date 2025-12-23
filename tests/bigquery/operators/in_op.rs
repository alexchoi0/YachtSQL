use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_in_integers() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT 3 IN (1, 2, 3, 4)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_not_in_integers() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT 5 IN (1, 2, 3, 4)").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_in_strings() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT 'apple' IN ('apple', 'banana', 'cherry')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_in_where_clause() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO products VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date')",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM products WHERE id IN (1, 3) ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["apple"], ["cherry"]]);
}

#[test]
fn test_not_in_where_clause() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM items WHERE id NOT IN (2, 4) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_in_with_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT NULL IN (1, 2, 3)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_in_list_with_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT 1 IN (1, 2, NULL)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_in_empty_result() {
    let mut session = create_session();
    session.execute_sql("CREATE TABLE data (id INT64)").unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE id IN (10, 20)")
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_in_single_value() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT 5 IN (5)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_in_with_expressions() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT 6 IN (2 * 2, 2 * 3, 2 * 4)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
