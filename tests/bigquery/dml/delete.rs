use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_users_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_single_row() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("DELETE FROM users WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_multiple_rows() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("DELETE FROM users WHERE age > 25")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT id FROM users").await.unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_all_rows() {
    let session = create_session();
    setup_users_table(&session).await;

    session.execute_sql("DELETE FROM users").await.unwrap();

    let result = session.execute_sql("SELECT * FROM users").await.unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_and_condition() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("DELETE FROM users WHERE age > 25 AND age < 35")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_or_condition() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("DELETE FROM users WHERE id = 1 OR id = 3")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT id FROM users").await.unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_in_clause() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("DELETE FROM users WHERE id IN (1, 2)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT id FROM users").await.unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_not_in_clause() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("DELETE FROM users WHERE id NOT IN (2)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT id FROM users").await.unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_no_matching_rows() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("DELETE FROM users WHERE id = 999")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM users")
        .await
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_subquery() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("CREATE TABLE to_delete (user_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO to_delete VALUES (1), (3)")
        .await
        .unwrap();

    session
        .execute_sql("DELETE FROM users WHERE id IN (SELECT user_id FROM to_delete)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT id FROM users").await.unwrap();

    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_like_condition() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("DELETE FROM users WHERE name LIKE 'A%'")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_is_null() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nullable (id INT64, value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 'a'), (2, NULL), (3, 'c')")
        .await
        .unwrap();

    session
        .execute_sql("DELETE FROM nullable WHERE value IS NULL")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_between() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("DELETE FROM users WHERE age BETWEEN 26 AND 34")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_table() {
    let session = create_session();
    setup_users_table(&session).await;

    session.execute_sql("TRUNCATE TABLE users").await.unwrap();

    let result = session.execute_sql("SELECT * FROM users").await.unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_alias() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE inventory (product STRING, quantity INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, quantity INT64)")
        .await
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
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 200),
            ('oven', 300),
            ('washer', 100)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "DELETE FROM inventory i WHERE i.product NOT IN (SELECT product FROM new_arrivals)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product FROM inventory ORDER BY product")
        .await
        .unwrap();

    assert_table_eq!(result, [["dryer"], ["oven"], ["washer"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_with_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE inventory (product STRING, quantity INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, quantity INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dishwasher', 30),
            ('dryer', 30),
            ('washer', 20)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 200),
            ('washer', 100)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "DELETE FROM inventory
            WHERE NOT EXISTS
              (SELECT * FROM new_arrivals
               WHERE inventory.product = new_arrivals.product)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product FROM inventory ORDER BY product")
        .await
        .unwrap();

    assert_table_eq!(result, [["dryer"], ["washer"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_where_quantity_zero() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE inventory (product STRING, quantity INT64)")
        .await
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
        .await
        .unwrap();

    session
        .execute_sql("DELETE FROM inventory WHERE quantity = 0")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product FROM inventory ORDER BY product")
        .await
        .unwrap();

    assert_table_eq!(result, [["dishwasher"], ["dryer"], ["microwave"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_without_from_keyword() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("DELETE users WHERE id = 1")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_delete_where_true() {
    let session = create_session();
    setup_users_table(&session).await;

    session
        .execute_sql("DELETE FROM users WHERE true")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").await.unwrap();

    assert_table_eq!(result, []);
}
