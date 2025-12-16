#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_count_all() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (3, 'Charlie')")
        .unwrap();

    let result = executor.execute_sql("SELECT COUNT(*) FROM users").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_count_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (3, 'Charlie')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(name) FROM users")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_sum() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sales (id INT64, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (2, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (3, 150)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(amount) FROM sales")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_avg() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE scores (id INT64, score FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO scores VALUES (1, 85.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES (2, 90.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES (3, 95.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(score) FROM scores")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_min() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (42)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (17)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (99)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT MIN(value) FROM numbers")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_max() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (42)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (17)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (99)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT MAX(value) FROM numbers")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_multiple_aggregations() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE orders (id INT64, amount FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO orders VALUES (1, 100.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (2, 200.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (3, 150.0)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.num_columns(), 5);
}

#[test]
fn test_group_by_single_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE employees (name STRING, department STRING, salary INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO employees VALUES ('Alice', 'Engineering', 80000)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES ('Bob', 'Engineering', 75000)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES ('Charlie', 'Sales', 60000)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES ('David', 'Sales', 65000)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT department, COUNT(*) FROM employees GROUP BY department")
        .unwrap();

    assert_eq!(result.num_rows(), 2, "Should have 2 departments");
}

#[test]
fn test_group_by_with_sum() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sales (product STRING, region STRING, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 'North', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 'South', 150)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Gadget', 'North', 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Gadget', 'South', 175)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT product, SUM(amount) FROM sales GROUP BY product")
        .unwrap();

    assert_eq!(result.num_rows(), 2, "Should have 2 products");
}

#[test]
fn test_group_by_multiple_columns() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sales (product STRING, region STRING, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 'North', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 'South', 150)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Gadget', 'North', 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 'North', 50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) FROM sales GROUP BY product, region")
        .unwrap();

    assert_eq!(
        result.num_rows(),
        3,
        "Should have 3 unique product-region combinations"
    );
}

#[test]
fn test_group_by_with_multiple_aggregations() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE orders (customer STRING, amount FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO orders VALUES ('Alice', 100.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES ('Alice', 200.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES ('Bob', 150.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES ('Bob', 250.0)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT customer, COUNT(*), SUM(amount), AVG(amount) FROM orders GROUP BY customer",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2, "Should have 2 customers");
    assert_eq!(result.num_columns(), 4);
}

#[test]
fn test_having_clause() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE orders (customer STRING, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO orders VALUES ('Alice', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES ('Alice', 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES ('Bob', 50)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES ('Charlie', 300)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES ('Charlie', 250)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT customer, SUM(amount) FROM orders GROUP BY customer HAVING SUM(amount) > 200",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2, "Alice and Charlie have total > 200");
}

#[test]
fn test_having_with_count() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE purchases (customer STRING, item STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO purchases VALUES ('Alice', 'Book')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO purchases VALUES ('Alice', 'Pen')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO purchases VALUES ('Alice', 'Notebook')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO purchases VALUES ('Bob', 'Book')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO purchases VALUES ('Charlie', 'Pen')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT customer, COUNT(*) FROM purchases GROUP BY customer HAVING COUNT(*) > 1",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1, "Only Alice has more than 1 purchase");
}

#[test]
fn test_count_distinct() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE events (user_id INT64, event_type STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (1, 'click')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES (1, 'view')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES (2, 'click')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES (1, 'click')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT user_id) FROM events")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_aggregation_with_where() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE transactions (id INT64, amount INT64, status STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO transactions VALUES (1, 100, 'completed')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO transactions VALUES (2, 200, 'completed')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO transactions VALUES (3, 150, 'pending')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO transactions VALUES (4, 300, 'completed')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*), SUM(amount) FROM transactions WHERE status = 'completed'")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_group_by_with_order_by() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sales (product STRING, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Gadget', 300)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Gizmo', 150)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY total DESC",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_empty_group_by() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*), SUM(value) FROM empty_table")
        .unwrap();

    assert_eq!(
        result.num_rows(),
        1,
        "Aggregations on empty table should return 1 row"
    );
}

#[test]
fn test_aggregation_null_handling() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE nullable_values (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO nullable_values VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable_values VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable_values VALUES (3, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(value), SUM(value) FROM nullable_values")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_string_agg() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE words (id INT64, word STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO words VALUES (1, 'hello')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES (2, 'world')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES (3, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STRING_AGG(word, ', ') FROM words")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_string_agg_with_numbers() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STRING_AGG(value, '-') FROM numbers")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_string_agg_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE nullable_words (id INT64, word STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO nullable_words VALUES (1, 'hello')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable_words VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable_words VALUES (3, 'world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STRING_AGG(word, ', ') FROM nullable_words")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_string_agg_empty_result() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE empty_words (id INT64, word STRING)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STRING_AGG(word, ', ') FROM empty_words")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_string_agg_distinct() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE duplicate_words (id INT64, word STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO duplicate_words VALUES (1, 'hello')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO duplicate_words VALUES (2, 'world')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO duplicate_words VALUES (3, 'hello')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO duplicate_words VALUES (4, 'world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STRING_AGG(DISTINCT word, ', ') FROM duplicate_words")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_agg_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value) FROM numbers")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_agg_strings() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE words (id INT64, word STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO words VALUES (1, 'apple')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES (2, 'banana')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES (3, 'cherry')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(word) FROM words")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_agg_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE nullable_values (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO nullable_values VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable_values VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable_values VALUES (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value) FROM nullable_values")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_agg_empty_result() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value) FROM empty_table")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_agg_distinct() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE duplicate_values (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO duplicate_values VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO duplicate_values VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO duplicate_values VALUES (3, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO duplicate_values VALUES (4, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(DISTINCT value) FROM duplicate_values")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_agg_with_array_length() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO items VALUES (1, 'a')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (2, 'b')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (3, 'c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(name) FROM items")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}
