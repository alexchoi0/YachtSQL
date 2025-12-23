use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

fn setup_table(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING, price INT64, category STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'Apple', 100, 'Fruit'), (2, 'Banana', 50, 'Fruit'), (3, 'Carrot', 75, 'Vegetable'), (4, 'Date', 200, 'Fruit'), (5, 'Eggplant', 125, 'Vegetable')")
        .unwrap();
}

#[test]
fn test_order_by_single_column_asc() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY name ASC")
        .unwrap();

    assert_table_eq!(
        result,
        [["Apple"], ["Banana"], ["Carrot"], ["Date"], ["Eggplant"],]
    );
}

#[test]
fn test_order_by_single_column_desc() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY price DESC")
        .unwrap();

    assert_table_eq!(
        result,
        [["Date"], ["Eggplant"], ["Apple"], ["Carrot"], ["Banana"],]
    );
}

#[test]
fn test_order_by_multiple_columns() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT name, category FROM items ORDER BY category ASC, price DESC")
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Date", "Fruit"],
            ["Apple", "Fruit"],
            ["Banana", "Fruit"],
            ["Eggplant", "Vegetable"],
            ["Carrot", "Vegetable"],
        ]
    );
}

#[test]
fn test_limit() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY id LIMIT 3")
        .unwrap();

    assert_table_eq!(result, [["Apple"], ["Banana"], ["Carrot"],]);
}

#[test]
fn test_limit_offset() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY id LIMIT 2 OFFSET 2")
        .unwrap();

    assert_table_eq!(result, [["Carrot"], ["Date"],]);
}

#[test]
fn test_limit_larger_than_result() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY id LIMIT 100")
        .unwrap();

    assert_table_eq!(
        result,
        [["Apple"], ["Banana"], ["Carrot"], ["Date"], ["Eggplant"]]
    );
}

#[test]
fn test_offset_larger_than_result() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY id LIMIT 10 OFFSET 100")
        .unwrap();

    assert_table_eq!(result, []);
}

#[test]
fn test_order_by_with_null_values() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE nullable (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 30), (2, NULL), (3, 10), (4, NULL), (5, 20)")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable ORDER BY value ASC NULLS LAST")
        .unwrap();

    assert_table_eq!(result, [[3], [5], [1], [2], [4]]);
}

#[test]
fn test_order_by_expression() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql(
            "SELECT name, price * 2 AS double_price FROM items ORDER BY price * 2 DESC LIMIT 3",
        )
        .unwrap();

    assert_table_eq!(result, [["Date", 400], ["Eggplant", 250], ["Apple", 200],]);
}

#[test]
fn test_order_by_alias() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT name, price AS p FROM items ORDER BY p ASC LIMIT 3")
        .unwrap();

    assert_table_eq!(result, [["Banana", 50], ["Carrot", 75], ["Apple", 100],]);
}
