use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_count_star_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE products (id INT64, category STRING, price FLOAT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Electronics', 100.0)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES (2, 'Electronics', 200.0)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES (3, 'Books', 20.0)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT category, COUNT(*) FROM products GROUP BY category");
    assert!(
        result.is_ok(),
        "COUNT(*) with GROUP BY failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 groups");
}

#[test]
fn test_sum_aggregate() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sales (id INT64, product STRING, amount INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO sales VALUES (1, 'A', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO sales VALUES (2, 'A', 150)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO sales VALUES (3, 'B', 200)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT product, SUM(amount) FROM sales GROUP BY product");
    assert!(result.is_ok(), "SUM with GROUP BY failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_avg_aggregate() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE scores (student STRING, score INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO scores VALUES ('Alice', 90)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Alice', 80)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Bob', 70)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT student, AVG(score) FROM scores GROUP BY student");
    assert!(result.is_ok(), "AVG with GROUP BY failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_min_aggregate() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE temps (city STRING, temperature INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO temps VALUES ('NYC', 30)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO temps VALUES ('NYC', 25)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO temps VALUES ('LA', 60)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT city, MIN(temperature) FROM temps GROUP BY city");
    assert!(result.is_ok(), "MIN with GROUP BY failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_max_aggregate() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE prices (item STRING, price INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO prices VALUES ('Apple', 5)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO prices VALUES ('Apple', 8)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO prices VALUES ('Orange', 6)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT item, MAX(price) FROM prices GROUP BY item");
    assert!(result.is_ok(), "MAX with GROUP BY failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_multiple_aggregates() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE orders (customer STRING, amount INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO orders VALUES ('Alice', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO orders VALUES ('Alice', 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO orders VALUES ('Bob', 150)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT customer, COUNT(*), SUM(amount), AVG(amount) FROM orders GROUP BY customer",
    );
    assert!(result.is_ok(), "Multiple aggregates failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_group_by_with_where() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE transactions (type STRING, amount INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO transactions VALUES ('credit', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO transactions VALUES ('credit', 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO transactions VALUES ('debit', 50)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO transactions VALUES ('debit', 30)")
        .expect("INSERT failed");

    let result = executor
        .execute_sql("SELECT type, SUM(amount) FROM transactions WHERE amount > 40 GROUP BY type");
    assert!(result.is_ok(), "GROUP BY with WHERE failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_count_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (category STRING, value INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO data VALUES ('A', 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES ('A', NULL)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES ('B', 20)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT category, COUNT(*) FROM data GROUP BY category");
    assert!(result.is_ok(), "COUNT(*) with NULLs failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_group_by_multiple_columns() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE metrics (region STRING, product STRING, sales INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO metrics VALUES ('US', 'A', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO metrics VALUES ('US', 'A', 150)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO metrics VALUES ('US', 'B', 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO metrics VALUES ('EU', 'A', 80)")
        .expect("INSERT failed");

    let result = executor
        .execute_sql("SELECT region, product, SUM(sales) FROM metrics GROUP BY region, product");
    assert!(
        result.is_ok(),
        "GROUP BY multiple columns failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3, "Expected 3 groups (US-A, US-B, EU-A)");
}

#[test]
fn test_aggregate_with_alias() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE inventory (item STRING, quantity INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO inventory VALUES ('Widget', 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO inventory VALUES ('Widget', 20)")
        .expect("INSERT failed");

    let result =
        executor.execute_sql("SELECT item, SUM(quantity) AS total FROM inventory GROUP BY item");
    assert!(result.is_ok(), "Aggregate with alias failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_group_by_empty_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE empty (category STRING, value INT64)")
        .expect("CREATE failed");

    let result = executor.execute_sql("SELECT category, COUNT(*) FROM empty GROUP BY category");
    assert!(
        result.is_ok(),
        "GROUP BY on empty table failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0, "Empty table should return 0 groups");
}

#[test]
fn test_group_by_single_group() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE single (category STRING, value INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO single VALUES ('A', 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO single VALUES ('A', 20)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO single VALUES ('A', 30)")
        .expect("INSERT failed");

    let result = executor.execute_sql("SELECT category, SUM(value) FROM single GROUP BY category");
    assert!(result.is_ok(), "GROUP BY single group failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Should have exactly 1 group");
}
