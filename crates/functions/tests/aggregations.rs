#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

mod common;
use common::create_executor;

#[test]
fn test_count_all() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();

    let result = executor.execute_sql("SELECT COUNT(*) FROM users").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_count_column() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, NULL), (3, 'Charlie')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(name) FROM users")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_sum() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE sales (id INT64, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES (1, 100), (2, 200), (3, 150)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(amount) FROM sales")
        .unwrap();
    assert_table_eq!(result, [[450]]);
}

#[test]
fn test_avg() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE scores (id INT64, score FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO scores VALUES (1, 85.0), (2, 90.0), (3, 95.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(score) FROM scores")
        .unwrap();
    assert_table_eq!(result, [[90.0]]);
}

#[test]
fn test_min() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (42), (17), (99)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT MIN(value) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[17]]);
}

#[test]
fn test_max() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (42), (17), (99)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT MAX(value) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[99]]);
}

#[test]
fn test_multiple_aggregations() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE orders (id INT64, amount FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO orders VALUES (1, 100.0), (2, 200.0), (3, 150.0)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders",
        )
        .unwrap();
    assert_table_eq!(result, [[3, 450.0, 150.0, 100.0, 200.0]]);
}

#[test]
fn test_group_by_single_column() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE employees (name STRING, department STRING, salary INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO employees VALUES ('Alice', 'Engineering', 80000), ('Bob', 'Engineering', 75000), ('Charlie', 'Sales', 60000), ('David', 'Sales', 65000)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT department, COUNT(*) FROM employees GROUP BY department ORDER BY department",
        )
        .unwrap();
    assert_table_eq!(result, [["Engineering", 2], ["Sales", 2]]);
}

#[test]
fn test_group_by_with_sum() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE sales (product STRING, region STRING, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 'North', 100), ('Widget', 'South', 150), ('Gadget', 'North', 200), ('Gadget', 'South', 175)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT product, SUM(amount) FROM sales GROUP BY product ORDER BY product")
        .unwrap();
    assert_table_eq!(result, [["Gadget", 375], ["Widget", 250]]);
}

#[test]
fn test_group_by_multiple_columns() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE sales (product STRING, region STRING, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 'North', 100), ('Widget', 'South', 150), ('Gadget', 'North', 200), ('Widget', 'North', 50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) FROM sales GROUP BY product, region ORDER BY product, region")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "North", 200],
            ["Widget", "North", 150],
            ["Widget", "South", 150]
        ]
    );
}

#[test]
fn test_group_by_with_multiple_aggregations() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE orders (customer STRING, amount FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO orders VALUES ('Alice', 100.0), ('Alice', 200.0), ('Bob', 150.0), ('Bob', 250.0)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT customer, COUNT(*), SUM(amount), AVG(amount) FROM orders GROUP BY customer ORDER BY customer",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["Alice", 2, 300.0, 150.0], ["Bob", 2, 400.0, 200.0]]
    );
}

#[test]
fn test_having_clause() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE orders (customer STRING, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO orders VALUES ('Alice', 100), ('Alice', 200), ('Bob', 50), ('Charlie', 300), ('Charlie', 250)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT customer, SUM(amount) FROM orders GROUP BY customer HAVING SUM(amount) > 200 ORDER BY customer",
        )
        .unwrap();
    assert_table_eq!(result, [["Alice", 300], ["Charlie", 550]]);
}

#[test]
fn test_having_with_count() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE purchases (customer STRING, item STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO purchases VALUES ('Alice', 'Book'), ('Alice', 'Pen'), ('Alice', 'Notebook'), ('Bob', 'Book'), ('Charlie', 'Pen')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT customer, COUNT(*) FROM purchases GROUP BY customer HAVING COUNT(*) > 1 ORDER BY customer",
        )
        .unwrap();
    assert_table_eq!(result, [["Alice", 3]]);
}

#[test]
fn test_count_distinct() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE events (user_id INT64, event_type STRING)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO events VALUES (1, 'click'), (1, 'view'), (2, 'click'), (1, 'click')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT user_id) FROM events")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_aggregation_with_where() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE transactions (id INT64, amount INT64, status STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO transactions VALUES (1, 100, 'completed'), (2, 200, 'completed'), (3, 150, 'pending'), (4, 300, 'completed')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*), SUM(amount) FROM transactions WHERE status = 'completed'")
        .unwrap();
    assert_table_eq!(result, [[3, 600]]);
}

#[test]
fn test_group_by_with_order_by() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE sales (product STRING, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 100), ('Gadget', 300), ('Widget', 200), ('Gizmo', 150)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY total DESC, product",
        )
        .unwrap();
    assert_table_eq!(result, [["Gadget", 300], ["Widget", 300], ["Gizmo", 150]]);
}

#[test]
fn test_empty_group_by() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*), SUM(value) FROM empty_table")
        .unwrap();
    assert_table_eq!(result, [[0, null]]);
}

#[test]
fn test_aggregation_null_handling() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE nullable_values (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO nullable_values VALUES (1, 10), (2, NULL), (3, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(value), SUM(value) FROM nullable_values")
        .unwrap();
    assert_table_eq!(result, [[2, 30]]);
}

#[test]
fn test_string_agg() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE words (id INT64, word STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO words VALUES (1, 'hello'), (2, 'world'), (3, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STRING_AGG(word, ', ' ORDER BY id) FROM words")
        .unwrap();
    assert_table_eq!(result, [["hello, world, test"]]);
}

#[test]
fn test_string_agg_with_cast() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE numbers (id INT64, value STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (1, '10'), (2, '20'), (3, '30')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STRING_AGG(value, '-' ORDER BY id) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [["10-20-30"]]);
}

#[test]
fn test_string_agg_with_nulls() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE nullable_words (id INT64, word STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO nullable_words VALUES (1, 'hello'), (2, NULL), (3, 'world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STRING_AGG(word, ', ' ORDER BY id) FROM nullable_words")
        .unwrap();
    assert_table_eq!(result, [["hello, world"]]);
}

#[test]
fn test_string_agg_empty_result() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE empty_words (id INT64, word STRING)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STRING_AGG(word, ', ') FROM empty_words")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_string_agg_distinct() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE duplicate_words (id INT64, word STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO duplicate_words VALUES (1, 'hello'), (2, 'world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STRING_AGG(word, ', ' ORDER BY word) FROM duplicate_words")
        .unwrap();
    assert_table_eq!(result, [["hello, world"]]);
}

#[test]
fn test_array_agg_basic() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY id) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[[10, 20, 30]]]);
}

#[test]
fn test_array_agg_strings() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE words (id INT64, word STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO words VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(word ORDER BY id) FROM words")
        .unwrap();
    assert_table_eq!(result, [[["apple", "banana", "cherry"]]]);
}

#[test]
fn test_array_agg_with_nulls() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE nullable_values (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO nullable_values VALUES (1, 10), (2, NULL), (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY id) FROM nullable_values")
        .unwrap();
    assert_table_eq!(result, [[[10, null, 30]]]);
}

#[test]
fn test_array_agg_empty_result() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value) FROM empty_table")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_array_agg_distinct() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE duplicate_values (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO duplicate_values VALUES (1, 10), (2, 20), (3, 10), (4, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(DISTINCT value ORDER BY value) FROM duplicate_values")
        .unwrap();
    assert_table_eq!(result, [[[10, 20]]]);
}

#[test]
fn test_array_agg_group_by() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE items (category STRING, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO items VALUES ('fruit', 'apple'), ('fruit', 'banana'), ('vegetable', 'carrot')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, ARRAY_AGG(name ORDER BY name) FROM items GROUP BY category ORDER BY category")
        .unwrap();
    assert_table_eq!(
        result,
        [["fruit", ["apple", "banana"]], ["vegetable", ["carrot"]]]
    );
}
