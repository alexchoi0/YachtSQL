use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_having_with_count() {
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
        .execute_sql("INSERT INTO orders VALUES ('Bob', 50)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO orders VALUES ('Charlie', 300)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO orders VALUES ('Charlie', 400)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO orders VALUES ('Charlie', 500)")
        .expect("INSERT failed");

    let result = executor
        .execute_sql("SELECT customer, COUNT(*) FROM orders GROUP BY customer HAVING COUNT(*) > 1");
    assert!(result.is_ok(), "HAVING with COUNT failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 customers with > 1 order");
}

#[test]
fn test_having_with_sum() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sales (product STRING, revenue INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 1000)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 2000)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO sales VALUES ('Gadget', 500)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO sales VALUES ('Gadget', 600)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO sales VALUES ('Tool', 100)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT product, SUM(revenue) FROM sales GROUP BY product HAVING SUM(revenue) > 1000",
    );
    assert!(result.is_ok(), "HAVING with SUM failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(
        batch.num_rows(),
        2,
        "Expected 2 products with revenue > 1000"
    );
}

#[test]
fn test_having_with_avg() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE grades (student STRING, score INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO grades VALUES ('Alice', 90)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO grades VALUES ('Alice', 95)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO grades VALUES ('Bob', 70)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO grades VALUES ('Bob', 75)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO grades VALUES ('Charlie', 60)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO grades VALUES ('Charlie', 65)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT student, AVG(score) FROM grades GROUP BY student HAVING AVG(score) >= 80",
    );
    assert!(result.is_ok(), "HAVING with AVG failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 student with avg >= 80");
}

#[test]
fn test_having_with_min_max() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE temperatures (city STRING, temp INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO temperatures VALUES ('NYC', 30)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO temperatures VALUES ('NYC', 35)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO temperatures VALUES ('NYC', 40)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO temperatures VALUES ('LA', 60)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO temperatures VALUES ('LA', 65)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO temperatures VALUES ('LA', 70)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT city, MAX(temp) FROM temperatures GROUP BY city HAVING MAX(temp) > 50",
    );
    assert!(result.is_ok(), "HAVING with MAX failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 city with max temp > 50");
}

#[test]
fn test_having_not_equals() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (group_id INT64, value INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (1, 20)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (2, 15)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (2, 15)")
        .expect("INSERT failed");

    let result = executor
        .execute_sql("SELECT group_id, COUNT(*) FROM data GROUP BY group_id HAVING COUNT(*) != 3");
    assert!(result.is_ok(), "HAVING with != failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 groups with count != 3");
}

#[test]
fn test_having_less_than() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE prices (product STRING, price INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO prices VALUES ('A', 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO prices VALUES ('A', 20)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO prices VALUES ('B', 50)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO prices VALUES ('B', 60)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT product, AVG(price) FROM prices GROUP BY product HAVING AVG(price) < 40",
    );
    assert!(result.is_ok(), "HAVING with < failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(
        batch.num_rows(),
        1,
        "Expected 1 product with avg price < 40"
    );
}

#[test]
fn test_having_greater_than_or_equal() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE inventory (warehouse STRING, qty INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO inventory VALUES ('East', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO inventory VALUES ('East', 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO inventory VALUES ('West', 50)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO inventory VALUES ('West', 75)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT warehouse, SUM(qty) FROM inventory GROUP BY warehouse HAVING SUM(qty) >= 200",
    );
    assert!(result.is_ok(), "HAVING with >= failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 warehouse with qty >= 200");
}

#[test]
fn test_having_less_than_or_equal() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE scores (team STRING, points INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO scores VALUES ('Red', 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Red', 15)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Blue', 20)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Blue', 25)")
        .expect("INSERT failed");

    let result = executor
        .execute_sql("SELECT team, MAX(points) FROM scores GROUP BY team HAVING MAX(points) <= 20");
    assert!(result.is_ok(), "HAVING with <= failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 team with max <= 20");
}

#[test]
fn test_having_filters_all_groups() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (category STRING, value INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO numbers VALUES ('A', 1)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES ('A', 2)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES ('B', 3)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO numbers VALUES ('B', 4)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT category, SUM(value) FROM numbers GROUP BY category HAVING SUM(value) > 1000",
    );
    assert!(
        result.is_ok(),
        "HAVING filtering all groups failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0, "Expected 0 rows after HAVING");
}

#[test]
fn test_having_filters_no_groups() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE values (type STRING, amount INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO values VALUES ('X', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO values VALUES ('X', 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO values VALUES ('Y', 150)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO values VALUES ('Y', 250)")
        .expect("INSERT failed");

    let result = executor
        .execute_sql("SELECT type, SUM(amount) FROM values GROUP BY type HAVING SUM(amount) > 0");
    assert!(
        result.is_ok(),
        "HAVING filtering no groups failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows after HAVING");
}

#[test]
fn test_having_with_group_by_column_reference() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE employees (dept STRING, salary INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO employees VALUES ('Engineering', 80000)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO employees VALUES ('Engineering', 90000)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO employees VALUES ('Sales', 60000)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO employees VALUES ('Sales', 70000)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT dept, AVG(salary) FROM employees GROUP BY dept HAVING AVG(salary) > 70000",
    );
    assert!(
        result.is_ok(),
        "HAVING with GROUP BY column failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(
        batch.num_rows(),
        1,
        "Expected 1 dept with avg salary > 70000"
    );
}

#[test]
fn test_having_with_count_star() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE events (user_id INT64, event_type STRING)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO events VALUES (1, 'click')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO events VALUES (1, 'view')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO events VALUES (1, 'purchase')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO events VALUES (2, 'click')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO events VALUES (2, 'view')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO events VALUES (3, 'view')")
        .expect("INSERT failed");

    let result = executor
        .execute_sql("SELECT user_id, COUNT(*) FROM events GROUP BY user_id HAVING COUNT(*) >= 3");
    assert!(
        result.is_ok(),
        "HAVING with COUNT(*) >= 3 failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Expected 1 user with >= 3 events");
}

#[test]
fn test_having_multiple_aggregates() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE transactions (merchant STRING, amount INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO transactions VALUES ('Store A', 50)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO transactions VALUES ('Store A', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO transactions VALUES ('Store A', 150)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO transactions VALUES ('Store B', 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO transactions VALUES ('Store B', 250)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT merchant, COUNT(*), SUM(amount) FROM transactions GROUP BY merchant HAVING COUNT(*) > 2",
    );
    assert!(
        result.is_ok(),
        "HAVING with multiple aggregates failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(
        batch.num_rows(),
        1,
        "Expected 1 merchant with > 2 transactions"
    );
}

#[test]
fn test_having_with_min() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE products (category STRING, price INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO products VALUES ('Electronics', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES ('Electronics', 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES ('Clothing', 20)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES ('Clothing', 30)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT category, MIN(price) FROM products GROUP BY category HAVING MIN(price) >= 50",
    );
    assert!(result.is_ok(), "HAVING with MIN failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(
        batch.num_rows(),
        1,
        "Expected 1 category with min price >= 50"
    );
}
