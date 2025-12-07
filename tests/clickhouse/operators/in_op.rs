use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_in_integers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 5 IN (1, 3, 5, 7, 9)").unwrap();
    assert_table_eq!(result, [[crate::common::true]]);
}

#[test]
fn test_in_integers_false() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 4 IN (1, 3, 5, 7, 9)").unwrap();
    assert_table_eq!(result, [[crate::common::false]]);
}

#[test]
fn test_not_in_integers() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 4 NOT IN (1, 3, 5, 7, 9)")
        .unwrap();
    assert_table_eq!(result, [[crate::common::true]]);
}

#[test]
fn test_in_strings() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'apple' IN ('apple', 'banana', 'cherry')")
        .unwrap();
    assert_table_eq!(result, [[crate::common::true]]);
}

#[test]
fn test_in_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING, category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Apple', 'fruit'), (2, 'Carrot', 'vegetable'), (3, 'Banana', 'fruit'), (4, 'Broccoli', 'vegetable')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM products WHERE category IN ('fruit') ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Apple"], ["Banana"]]);
}

#[test]
fn test_not_in_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING, category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Apple', 'fruit'), (2, 'Carrot', 'vegetable'), (3, 'Banana', 'fruit'), (4, 'Broccoli', 'vegetable')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM products WHERE category NOT IN ('fruit') ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["Broccoli"], ["Carrot"]]);
}

#[test]
fn test_in_multiple_values() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM nums WHERE val IN (2, 4, 6, 8) ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[2], [4], [6], [8]]);
}

#[test]
fn test_in_single_value() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 5 IN (5)").unwrap();
    assert_table_eq!(result, [[crate::common::true]]);
}

#[test]
fn test_in_with_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nullable (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM nullable WHERE val IN (1, 3) ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}
