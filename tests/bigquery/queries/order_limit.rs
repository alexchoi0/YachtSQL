use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING, price INT64, category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'Apple', 100, 'Fruit'), (2, 'Banana', 50, 'Fruit'), (3, 'Carrot', 75, 'Vegetable'), (4, 'Date', 200, 'Fruit'), (5, 'Eggplant', 125, 'Vegetable')").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_single_column_asc() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY name ASC")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Apple"], ["Banana"], ["Carrot"], ["Date"], ["Eggplant"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_single_column_desc() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY price DESC")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Date"], ["Eggplant"], ["Apple"], ["Carrot"], ["Banana"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_multiple_columns() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT name, category FROM items ORDER BY category ASC, price DESC")
        .await
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

#[tokio::test(flavor = "current_thread")]
async fn test_limit() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY id LIMIT 3")
        .await
        .unwrap();

    assert_table_eq!(result, [["Apple"], ["Banana"], ["Carrot"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_limit_offset() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY id LIMIT 2 OFFSET 2")
        .await
        .unwrap();

    assert_table_eq!(result, [["Carrot"], ["Date"],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_limit_larger_than_result() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY id LIMIT 100")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Apple"], ["Banana"], ["Carrot"], ["Date"], ["Eggplant"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_offset_larger_than_result() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT name FROM items ORDER BY id LIMIT 10 OFFSET 100")
        .await
        .unwrap();

    assert_table_eq!(result, []);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_with_null_values() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nullable (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1, 30), (2, NULL), (3, 10), (4, NULL), (5, 20)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM nullable ORDER BY value ASC NULLS LAST")
        .await
        .unwrap();

    assert_table_eq!(result, [[3], [5], [1], [2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_expression() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT name, price * 2 AS double_price FROM items ORDER BY price * 2 DESC LIMIT 3",
        )
        .await
        .unwrap();

    assert_table_eq!(result, [["Date", 400], ["Eggplant", 250], ["Apple", 200],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_order_by_alias() {
    let session = create_session();
    setup_table(&session).await;

    let result = session
        .execute_sql("SELECT name, price AS p FROM items ORDER BY p ASC LIMIT 3")
        .await
        .unwrap();

    assert_table_eq!(result, [["Banana", 50], ["Carrot", 75], ["Apple", 100],]);
}
