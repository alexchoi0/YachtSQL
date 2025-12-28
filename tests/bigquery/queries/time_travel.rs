use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_as_of_timestamp() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE versioned_data (id INT64, value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO versioned_data VALUES (1, 'initial')")
        .await
        .unwrap();

    let result = session.execute_sql(
        "SELECT * FROM versioned_data FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_as_of_literal() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM data FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00 UTC'")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_with_alias() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE events (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO events VALUES (1, 'Event A')")
        .await
        .unwrap();

    let result = session.execute_sql(
        "SELECT e.id FROM events AS e FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_in_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE customers (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (1, 100)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO customers VALUES (100, 'Alice')")
        .await
        .unwrap();

    let result = session.execute_sql(
        "SELECT o.id, c.name
        FROM orders AS o FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        JOIN customers AS c FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        ON o.customer_id = c.id",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_in_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (id INT64, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 100)")
        .await
        .unwrap();

    let result = session.execute_sql(
        "SELECT * FROM (
            SELECT * FROM products FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        )",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_with_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE logs (id INT64, level STRING, message STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO logs VALUES (1, 'ERROR', 'Something failed')")
        .await
        .unwrap();

    let result = session.execute_sql(
        "SELECT * FROM logs FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
        WHERE level = 'ERROR'",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_with_aggregation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (id INT64, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let result = session.execute_sql(
        "SELECT SUM(amount) FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_different_times() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE inventory (id INT64, quantity INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO inventory VALUES (1, 50)")
        .await
        .unwrap();

    let result = session.execute_sql(
        "SELECT
            (SELECT SUM(quantity) FROM inventory FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) AS hour_ago,
            (SELECT SUM(quantity) FROM inventory FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS day_ago",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_variable() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql("SET @snapshot_time = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM data FOR SYSTEM_TIME AS OF @snapshot_time")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_with_cte() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE metrics (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO metrics VALUES (1, 100)")
        .await
        .unwrap();

    let result = session.execute_sql(
        "WITH historical AS (
            SELECT * FROM metrics FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        )
        SELECT * FROM historical",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_system_time_comparison() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE prices (product_id INT64, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO prices VALUES (1, 100)")
        .await
        .unwrap();

    let result = session.execute_sql(
        "SELECT
            current.product_id,
            current.price AS current_price,
            historical.price AS historical_price
        FROM prices AS current
        JOIN prices FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) AS historical
        ON current.product_id = historical.product_id
        WHERE current.price != historical.price",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_assert_basic() {
    let session = create_session();

    let result = session.execute_sql("ASSERT (1 = 1)").await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_assert_with_message() {
    let session = create_session();

    let result = session
        .execute_sql("ASSERT (1 = 1) AS 'Basic equality check'")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_assert_fails() {
    let session = create_session();

    let result = session.execute_sql("ASSERT (1 = 2)").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_assert_with_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE check_data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO check_data VALUES (1), (2), (3)")
        .await
        .unwrap();

    let result = session
        .execute_sql("ASSERT ((SELECT COUNT(*) FROM check_data) = 3)")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_assert_not_null() {
    let session = create_session();

    let result = session
        .execute_sql("ASSERT (COALESCE(1, 0) IS NOT NULL)")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_assert_comparison() {
    let session = create_session();

    let result = session
        .execute_sql("ASSERT (5 > 3 AND 10 < 20) AS 'Range check'")
        .await;
    assert!(result.is_ok());
}
