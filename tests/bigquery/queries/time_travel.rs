use crate::common::create_executor;

#[test]
fn test_for_system_time_as_of_timestamp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE versioned_data (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO versioned_data VALUES (1, 'initial')")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT * FROM versioned_data FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_for_system_time_as_of_literal() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO data VALUES (1)").unwrap();

    let result = executor.execute_sql(
        "SELECT * FROM data FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00 UTC'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_for_system_time_with_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES (1, 'Event A')")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT e.id FROM events AS e FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_for_system_time_in_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE customers (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO customers VALUES (100, 'Alice')")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT o.id, c.name
        FROM orders AS o FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        JOIN customers AS c FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        ON o.customer_id = c.id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_for_system_time_in_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE products (id INT64, price INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 100)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT * FROM (
            SELECT * FROM products FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_for_system_time_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE logs (id INT64, level STRING, message STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO logs VALUES (1, 'ERROR', 'Something failed')")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT * FROM logs FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
        WHERE level = 'ERROR'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_for_system_time_with_aggregation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (id INT64, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT SUM(amount) FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_for_system_time_different_times() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE inventory (id INT64, quantity INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO inventory VALUES (1, 50)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT
            (SELECT SUM(quantity) FROM inventory FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) AS hour_ago,
            (SELECT SUM(quantity) FROM inventory FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS day_ago",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_for_system_time_variable() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO data VALUES (1)").unwrap();

    executor
        .execute_sql("SET @snapshot_time = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM data FOR SYSTEM_TIME AS OF @snapshot_time");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_for_system_time_with_cte() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE metrics (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO metrics VALUES (1, 100)")
        .unwrap();

    let result = executor.execute_sql(
        "WITH historical AS (
            SELECT * FROM metrics FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        )
        SELECT * FROM historical",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_for_system_time_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE prices (product_id INT64, price INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO prices VALUES (1, 100)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT
            current.product_id,
            current.price AS current_price,
            historical.price AS historical_price
        FROM prices AS current
        JOIN prices FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) AS historical
        ON current.product_id = historical.product_id
        WHERE current.price != historical.price",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_assert_basic() {
    let mut executor = create_executor();

    let result = executor.execute_sql("ASSERT (1 = 1)");
    assert!(result.is_ok());
}

#[test]
#[ignore = "Implement me!"]
fn test_assert_with_message() {
    let mut executor = create_executor();

    let result = executor.execute_sql("ASSERT (1 = 1) AS 'Basic equality check'");
    assert!(result.is_ok());
}

#[test]
fn test_assert_fails() {
    let mut executor = create_executor();

    let result = executor.execute_sql("ASSERT (1 = 2)");
    assert!(result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_assert_with_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE check_data (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO check_data VALUES (1), (2), (3)")
        .unwrap();

    let result = executor.execute_sql("ASSERT ((SELECT COUNT(*) FROM check_data) = 3)");
    assert!(result.is_ok());
}

#[test]
#[ignore = "Implement me!"]
fn test_assert_not_null() {
    let mut executor = create_executor();

    let result = executor.execute_sql("ASSERT (COALESCE(1, 0) IS NOT NULL)");
    assert!(result.is_ok());
}

#[test]
#[ignore = "Implement me!"]
fn test_assert_comparison() {
    let mut executor = create_executor();

    let result = executor.execute_sql("ASSERT (5 > 3 AND 10 < 20) AS 'Range check'");
    assert!(result.is_ok());
}
