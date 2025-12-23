use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

fn setup_users_table(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
        )
        .unwrap();
}

#[test]
fn test_delete_single_row() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("DELETE FROM users WHERE id = 1")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_delete_multiple_rows() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("DELETE FROM users WHERE age > 25")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM users").unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_delete_all_rows() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session.execute_sql("DELETE FROM users").unwrap();

    let result = session.execute_sql("SELECT * FROM users").unwrap();

    assert_table_eq!(result, []);
}

#[test]
fn test_delete_with_and_condition() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("DELETE FROM users WHERE age > 25 AND age < 35")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_delete_with_or_condition() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("DELETE FROM users WHERE id = 1 OR id = 3")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM users").unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_delete_with_in_clause() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("DELETE FROM users WHERE id IN (1, 2)")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM users").unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_delete_with_not_in_clause() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("DELETE FROM users WHERE id NOT IN (2)")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM users").unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_delete_no_matching_rows() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("DELETE FROM users WHERE id = 999")
        .unwrap();

    let result = session.execute_sql("SELECT COUNT(*) FROM users").unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_delete_with_subquery() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("CREATE TABLE to_delete (user_id INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO to_delete VALUES (1), (3)")
        .unwrap();

    session
        .execute_sql("DELETE FROM users WHERE id IN (SELECT user_id FROM to_delete)")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM users").unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_delete_with_like_condition() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("DELETE FROM users WHERE name LIKE 'A%'")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_delete_with_is_null() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE nullable (id INT64, value STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 'a'), (2, NULL), (3, 'c')")
        .unwrap();

    session
        .execute_sql("DELETE FROM nullable WHERE value IS NULL")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_delete_with_between() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session
        .execute_sql("DELETE FROM users WHERE age BETWEEN 26 AND 34")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_truncate_table() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session.execute_sql("TRUNCATE TABLE users").unwrap();

    let result = session.execute_sql("SELECT * FROM users").unwrap();

    assert_table_eq!(result, []);
}

#[test]
#[ignore]
fn test_delete_with_alias() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE inventory (product STRING, quantity INT64)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, quantity INT64)")
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dishwasher', 30),
            ('dryer', 30),
            ('washer', 20),
            ('microwave', 20),
            ('oven', 5)",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 200),
            ('oven', 300),
            ('washer', 100)",
        )
        .unwrap();

    session
        .execute_sql(
            "DELETE FROM inventory i WHERE i.product NOT IN (SELECT product FROM new_arrivals)",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT product FROM inventory ORDER BY product")
        .unwrap();

    assert_table_eq!(result, [["dryer"], ["oven"], ["washer"]]);
}

#[test]
#[ignore]
fn test_delete_with_exists() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE inventory (product STRING, quantity INT64)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, quantity INT64)")
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dishwasher', 30),
            ('dryer', 30),
            ('washer', 20)",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 200),
            ('washer', 100)",
        )
        .unwrap();

    session
        .execute_sql(
            "DELETE FROM inventory
            WHERE NOT EXISTS
              (SELECT * FROM new_arrivals
               WHERE inventory.product = new_arrivals.product)",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT product FROM inventory ORDER BY product")
        .unwrap();

    assert_table_eq!(result, [["dryer"], ["washer"]]);
}

#[test]
fn test_delete_where_quantity_zero() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE inventory (product STRING, quantity INT64)")
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dishwasher', 20),
            ('dryer', 30),
            ('washer', 0),
            ('microwave', 20),
            ('oven', 0)",
        )
        .unwrap();

    session
        .execute_sql("DELETE FROM inventory WHERE quantity = 0")
        .unwrap();

    let result = session
        .execute_sql("SELECT product FROM inventory ORDER BY product")
        .unwrap();

    assert_table_eq!(result, [["dishwasher"], ["dryer"], ["microwave"]]);
}

#[test]
fn test_delete_without_from_keyword() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session.execute_sql("DELETE users WHERE id = 1").unwrap();

    let result = session
        .execute_sql("SELECT id FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_delete_where_true() {
    let mut session = create_session();
    setup_users_table(&mut session);

    session.execute_sql("DELETE FROM users WHERE true").unwrap();

    let result = session.execute_sql("SELECT * FROM users").unwrap();

    assert_table_eq!(result, []);
}
