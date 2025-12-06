#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

mod common;
use common::setup_executor;

#[test]
fn test_in_subquery_basic() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS products")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (product_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE products (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1), (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM orders WHERE product_id IN (SELECT id FROM products)")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_not_in_subquery_basic() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS products")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (product_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE products (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1), (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM orders WHERE product_id NOT IN (SELECT id FROM products)")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(3));
}

#[test]
fn test_not_in_subquery_exclude_sold() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS all_items")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS sold_items")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE all_items (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE sold_items (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO all_items VALUES (1), (2), (3), (4)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sold_items VALUES (1), (3)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM all_items WHERE id NOT IN (SELECT id FROM sold_items) ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(2));
    assert_eq!(col.get(1).unwrap().as_i64(), Some(4));
}

#[test]
fn test_exists_with_true_condition() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS t1").unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS t2").unwrap();
    executor.execute_sql("CREATE TABLE t1 (id INT64)").unwrap();
    executor.execute_sql("CREATE TABLE t2 (id INT64)").unwrap();
    executor.execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    executor.execute_sql("INSERT INTO t2 VALUES (99)").unwrap();

    let result = executor
        .execute_sql("SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE TRUE)")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(1));
}

#[test]
fn test_exists_with_false_condition() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS t1").unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS t2").unwrap();
    executor.execute_sql("CREATE TABLE t1 (id INT64)").unwrap();
    executor.execute_sql("CREATE TABLE t2 (id INT64)").unwrap();
    executor
        .execute_sql("INSERT INTO t1 VALUES (1), (2)")
        .unwrap();
    executor.execute_sql("INSERT INTO t2 VALUES (99)").unwrap();

    let result = executor
        .execute_sql("SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE FALSE)")
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_exists_empty_subquery() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS items").unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS large_set")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE items (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE large_set (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM items WHERE EXISTS (SELECT 1 FROM large_set WHERE val > 0)")
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_not_exists_with_null_condition() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS t1").unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS t2").unwrap();
    executor.execute_sql("CREATE TABLE t1 (id INT64)").unwrap();
    executor.execute_sql("CREATE TABLE t2 (id INT64)").unwrap();
    executor.execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    executor.execute_sql("INSERT INTO t2 VALUES (99)").unwrap();

    let result = executor
        .execute_sql("SELECT id FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE NULL)")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(1));
}

#[test]
fn test_all_with_empty_subquery() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS empty").unwrap();
    executor
        .execute_sql("CREATE TABLE data (val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE empty (val INT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO data VALUES (5)").unwrap();

    let result = executor
        .execute_sql("SELECT val FROM data WHERE val > ALL (SELECT val FROM empty)")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(5));
}

#[test]
fn test_any_with_empty_subquery() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS empty").unwrap();
    executor
        .execute_sql("CREATE TABLE data (val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE empty (val INT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO data VALUES (5)").unwrap();

    let result = executor
        .execute_sql("SELECT val FROM data WHERE val > ANY (SELECT val FROM empty)")
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_all_with_thresholds() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS thresholds")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE thresholds (threshold INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (0), (5), (10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO thresholds VALUES (0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM numbers WHERE value >= ALL (SELECT threshold FROM thresholds) ORDER BY value")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_between_integer() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (5), (10), (15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE value BETWEEN 5 AND 10")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_between_float() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS floats").unwrap();
    executor
        .execute_sql("CREATE TABLE floats (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats VALUES (1.5), (5.5), (10.5), (15.5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM floats WHERE value BETWEEN 5.0 AND 11.0")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_between_string() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('apple'), ('banana'), ('cherry'), ('date')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM words WHERE value BETWEEN 'banana' AND 'cherry'")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_between_date() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS events").unwrap();
    executor
        .execute_sql("CREATE TABLE events (event_date DATE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES (DATE '2024-01-01'), (DATE '2024-01-15'), (DATE '2024-02-01'), (DATE '2024-03-01')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM events WHERE event_date BETWEEN DATE '2024-01-10' AND DATE '2024-02-10'",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_not_between() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (5), (10), (15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE value NOT BETWEEN 5 AND 10")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_between_reversed_bounds() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (5), (10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE value BETWEEN 10 AND 1")
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_between_exact_match() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE value BETWEEN 5 AND 5")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_in_list_integer() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE value IN (2, 4)")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_in_list_string() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('apple'), ('banana'), ('cherry')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM words WHERE value IN ('apple', 'cherry')")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_not_in_list() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE value NOT IN (2, 4)")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_in_list_with_null() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE value IN (1, NULL)")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_like_prefix() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('apple'), ('application'), ('banana')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM words WHERE value LIKE 'app%'")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_like_single_char() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('cat'), ('cot'), ('cut'), ('cart')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM words WHERE value LIKE 'c_t'")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_like_contains() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('pineapple'), ('apple'), ('grapefruit')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM words WHERE value LIKE '%app%'")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_not_like() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('apple'), ('banana'), ('cherry')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM words WHERE value NOT LIKE 'ap%'")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_like_match_all() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('apple'), ('banana')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM words WHERE value LIKE '%'")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_ilike_case_insensitive() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('Apple'), ('APPLE'), ('apple'), ('banana')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM words WHERE value ILIKE 'apple'")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_ilike_prefix_case_insensitive() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('Application'), ('APPLY'), ('apply'), ('banana')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM words WHERE value ILIKE 'app%'")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_not_equal_integer() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE value <> 2")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_not_equal_string() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('apple'), ('banana'), ('cherry')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM words WHERE value <> 'apple'")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_between_and_in() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (5), (7), (10), (13), (15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE value BETWEEN 5 AND 15 AND value IN (7, 10, 13)")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_expression_between() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (5, 3), (10, 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE a + b BETWEEN 8 AND 15")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_expression_in() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (5, 5), (3, 7)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE a + b IN (10, 20)")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_numeric_in_subquery() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS normalized")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE normalized (v NUMERIC)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO normalized SELECT CAST(123.45 AS NUMERIC)")
        .unwrap();

    let result = executor.execute_sql("SELECT v FROM normalized").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_numeric_null() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS nullable")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE nullable (v NUMERIC)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (NULL)")
        .unwrap();

    let result = executor.execute_sql("SELECT v FROM nullable").unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_numeric_case_expression() {
    let mut executor = setup_executor();

    let result = executor
        .execute_sql(
            "SELECT CASE WHEN 1=1 THEN CAST(100.00 AS NUMERIC) ELSE CAST(200.00 AS NUMERIC) END",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_numeric_coalesce() {
    let mut executor = setup_executor();

    let result = executor
        .execute_sql("SELECT COALESCE(NULL, CAST(50.00 AS NUMERIC))")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_in_subquery_with_order_by() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS products")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (product_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE products (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1), (2), (3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT product_id FROM orders WHERE product_id IN (SELECT id FROM products) ORDER BY product_id")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(1));
    assert_eq!(col.get(1).unwrap().as_i64(), Some(2));
    assert_eq!(col.get(2).unwrap().as_i64(), Some(3));
}

#[test]
fn test_not_in_subquery_empty_subquery() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS products")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (product_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE products (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT product_id FROM orders WHERE product_id NOT IN (SELECT id FROM products) ORDER BY product_id")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_all_with_multiple_values() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS thresholds")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE thresholds (threshold INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (5), (10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO thresholds VALUES (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM numbers WHERE value >= ALL (SELECT threshold FROM thresholds) ORDER BY value")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(5));
    assert_eq!(col.get(1).unwrap().as_i64(), Some(10));
}

#[test]
fn test_any_with_multiple_values() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS thresholds")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE thresholds (threshold INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (5), (10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO thresholds VALUES (4), (6)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM numbers WHERE value > ANY (SELECT threshold FROM thresholds) ORDER BY value")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(5));
    assert_eq!(col.get(1).unwrap().as_i64(), Some(10));
}

// ============================================================================
// Tests for uncorrelated subquery caching and comparison operators
// ============================================================================

#[test]
fn test_uncorrelated_scalar_subquery_avg() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS employees")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, salary FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 50000), (2, 'Bob', 60000), (3, 'Carol', 70000)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, (SELECT AVG(salary) FROM employees) as avg_sal FROM employees ORDER BY name")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    let avg_col = result.column(1).unwrap();
    let avg_val = avg_col.get(0).unwrap().as_f64().unwrap();
    assert!((avg_val - 60000.0).abs() < 0.01);
}

#[test]
fn test_uncorrelated_exists_with_less_than() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS products")
        .unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, product_id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'A'), (2, 'B'), (3, 'C')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1), (2, 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM products WHERE EXISTS (SELECT 1 FROM orders WHERE id < 3) ORDER BY name")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_uncorrelated_exists_with_less_than_no_match() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS products")
        .unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'A'), (2, 'B')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (10), (20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM products WHERE EXISTS (SELECT 1 FROM orders WHERE id < 5)")
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_uncorrelated_in_subquery_with_less_than() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS employees")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS departments")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, dept_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Carol', 5)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO departments VALUES (1, 'HR'), (2, 'Eng'), (3, 'Sales')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE id < 3) ORDER BY name")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_str(), Some("Alice"));
    assert_eq!(col.get(1).unwrap().as_str(), Some("Bob"));
}

#[test]
fn test_subquery_with_greater_than() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS thresholds")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE thresholds (t INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (5), (10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO thresholds VALUES (3), (7), (15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM numbers WHERE EXISTS (SELECT 1 FROM thresholds WHERE t > 10) ORDER BY val")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_subquery_with_less_than_or_equal() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS items").unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS limits").unwrap();
    executor
        .execute_sql("CREATE TABLE items (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE limits (max_val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1), (2), (3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO limits VALUES (5), (10)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM items WHERE id IN (SELECT max_val FROM limits WHERE max_val <= 5)",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_subquery_with_greater_than_or_equal() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS scores").unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS passing")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE scores (student STRING, score INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE passing (min_score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES ('Alice', 85), ('Bob', 70), ('Carol', 90)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO passing VALUES (75)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT student FROM scores WHERE EXISTS (SELECT 1 FROM passing WHERE min_score >= 75) ORDER BY student")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_uncorrelated_scalar_subquery_count() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS items").unwrap();
    executor
        .execute_sql("CREATE TABLE items (id INT64, category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, 'A'), (2, 'A'), (3, 'B'), (4, 'C')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, (SELECT COUNT(id) FROM items) as total FROM items ORDER BY category",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);
    let total_col = result.column(1).unwrap();
    assert_eq!(total_col.get(0).unwrap().as_i64(), Some(4));
    assert_eq!(total_col.get(1).unwrap().as_i64(), Some(4));
}

#[test]
fn test_uncorrelated_scalar_subquery_max() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS sales").unwrap();
    executor
        .execute_sql("CREATE TABLE sales (id INT64, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (1, 100), (2, 250), (3, 150)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, (SELECT MAX(amount) FROM sales) as max_sale FROM sales ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    let max_col = result.column(1).unwrap();
    assert_eq!(max_col.get(0).unwrap().as_f64(), Some(250.0));
    assert_eq!(max_col.get(1).unwrap().as_f64(), Some(250.0));
    assert_eq!(max_col.get(2).unwrap().as_f64(), Some(250.0));
}

#[test]
fn test_uncorrelated_scalar_subquery_min() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS prices").unwrap();
    executor
        .execute_sql("CREATE TABLE prices (product STRING, price INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO prices VALUES ('A', 50), ('B', 30), ('C', 80)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT product, (SELECT MIN(price) FROM prices) as min_price FROM prices ORDER BY product")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    let min_col = result.column(1).unwrap();
    assert_eq!(min_col.get(0).unwrap().as_f64(), Some(30.0));
}

#[test]
fn test_uncorrelated_scalar_subquery_sum() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS inventory")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE inventory (item STRING, quantity INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO inventory VALUES ('A', 10), ('B', 20), ('C', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT item, (SELECT SUM(quantity) FROM inventory) as total_qty FROM inventory ORDER BY item")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    let sum_col = result.column(1).unwrap();
    let sum_val = sum_col.get(0).unwrap().as_numeric().unwrap();
    assert_eq!(sum_val.to_string(), "60");
}

#[test]
fn test_not_exists_with_comparison() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS products")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS discounts")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE discounts (product_id INT64, percent INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'A'), (2, 'B')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO discounts VALUES (1, 10), (2, 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM products WHERE NOT EXISTS (SELECT 1 FROM discounts WHERE percent > 50)")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_multiple_comparison_operators_in_subquery() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS ranges").unwrap();
    executor
        .execute_sql("CREATE TABLE data (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE ranges (low INT64, high INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1), (5), (10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ranges VALUES (3, 8)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM data WHERE EXISTS (SELECT 1 FROM ranges WHERE low <= 5 AND high >= 5) ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_correlated_scalar_subquery_avg_by_dept() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS employees")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, dept_id INT64, salary INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 1, 50000), (2, 'Bob', 1, 60000), (3, 'Charlie', 2, 70000), (4, 'Diana', 2, 80000)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT e1.name, e1.dept_id, \
             (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept_id = e1.dept_id) as dept_avg \
             FROM employees e1 ORDER BY e1.name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);
    let name_col = result.column(0).unwrap();
    let dept_avg_col = result.column(2).unwrap();

    // Alice and Bob are in dept 1: avg = (50000 + 60000) / 2 = 55000
    // Charlie and Diana are in dept 2: avg = (70000 + 80000) / 2 = 75000
    assert_eq!(name_col.get(0).unwrap().as_str(), Some("Alice"));
    assert_eq!(dept_avg_col.get(0).unwrap().as_f64(), Some(55000.0));

    assert_eq!(name_col.get(1).unwrap().as_str(), Some("Bob"));
    assert_eq!(dept_avg_col.get(1).unwrap().as_f64(), Some(55000.0));

    assert_eq!(name_col.get(2).unwrap().as_str(), Some("Charlie"));
    assert_eq!(dept_avg_col.get(2).unwrap().as_f64(), Some(75000.0));

    assert_eq!(name_col.get(3).unwrap().as_str(), Some("Diana"));
    assert_eq!(dept_avg_col.get(3).unwrap().as_f64(), Some(75000.0));
}

#[test]
fn test_correlated_scalar_subquery_count_by_category() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS products")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING, category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Apple', 'Fruit'), (2, 'Banana', 'Fruit'), (3, 'Carrot', 'Vegetable'), (4, 'Celery', 'Vegetable'), (5, 'Orange', 'Fruit')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT p1.name, p1.category, \
             (SELECT COUNT(p2.id) FROM products p2 WHERE p2.category = p1.category) as category_count \
             FROM products p1 ORDER BY p1.name"
        )
        .unwrap();

    assert_eq!(result.num_rows(), 5);
    let name_col = result.column(0).unwrap();
    let count_col = result.column(2).unwrap();

    // Apple, Banana, Orange are Fruit (count=3)
    // Carrot, Celery are Vegetable (count=2)
    assert_eq!(name_col.get(0).unwrap().as_str(), Some("Apple"));
    assert_eq!(count_col.get(0).unwrap().as_i64(), Some(3));

    assert_eq!(name_col.get(1).unwrap().as_str(), Some("Banana"));
    assert_eq!(count_col.get(1).unwrap().as_i64(), Some(3));

    assert_eq!(name_col.get(2).unwrap().as_str(), Some("Carrot"));
    assert_eq!(count_col.get(2).unwrap().as_i64(), Some(2));
}

#[test]
fn test_correlated_scalar_subquery_max_by_group() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS scores").unwrap();
    executor
        .execute_sql("CREATE TABLE scores (student STRING, subject STRING, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES ('Alice', 'Math', 90), ('Alice', 'English', 85), ('Bob', 'Math', 95), ('Bob', 'English', 80)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT s1.student, s1.subject, s1.score, \
             (SELECT MAX(s2.score) FROM scores s2 WHERE s2.student = s1.student) as max_score \
             FROM scores s1 ORDER BY s1.student, s1.subject",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);
    let student_col = result.column(0).unwrap();
    let score_col = result.column(2).unwrap();
    let max_col = result.column(3).unwrap();

    // Alice: Math=90, English=85, max=90
    assert_eq!(student_col.get(0).unwrap().as_str(), Some("Alice"));
    assert_eq!(score_col.get(0).unwrap().as_i64(), Some(85));
    assert_eq!(max_col.get(0).unwrap().as_f64(), Some(90.0)); // MAX returns Float64

    // Bob: Math=95, English=80, max=95
    assert_eq!(student_col.get(2).unwrap().as_str(), Some("Bob"));
    assert_eq!(max_col.get(2).unwrap().as_f64(), Some(95.0));
}

#[test]
fn test_correlated_scalar_subquery_sum_by_group() {
    // Get sum per group for each row
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
    executor
        .execute_sql("CREATE TABLE orders (order_id INT64, customer STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 'Alice', 100), (2, 'Alice', 150), (3, 'Bob', 200), (4, 'Bob', 50)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT o1.order_id, o1.customer, o1.amount, \
             (SELECT SUM(o2.amount) FROM orders o2 WHERE o2.customer = o1.customer) as total_spent \
             FROM orders o1 ORDER BY o1.order_id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);
    let customer_col = result.column(1).unwrap();
    let total_col = result.column(3).unwrap();

    // Alice: 100 + 150 = 250
    assert_eq!(customer_col.get(0).unwrap().as_str(), Some("Alice"));
    let alice_total = total_col.get(0).unwrap().as_numeric().unwrap();
    assert_eq!(alice_total.to_string(), "250");

    // Bob: 200 + 50 = 250
    assert_eq!(customer_col.get(2).unwrap().as_str(), Some("Bob"));
    let bob_total = total_col.get(2).unwrap().as_numeric().unwrap();
    assert_eq!(bob_total.to_string(), "250");
}

#[test]
fn test_correlated_scalar_subquery_with_null_groups() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS items").unwrap();
    executor
        .execute_sql("CREATE TABLE items (id INT64, group_id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, 1, 10), (2, 1, 20), (3, NULL, 30), (4, 2, 40)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT i1.id, i1.group_id, \
             (SELECT SUM(i2.value) FROM items i2 WHERE i2.group_id = i1.group_id) as group_sum \
             FROM items i1 ORDER BY i1.id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);
    let id_col = result.column(0).unwrap();
    let group_sum_col = result.column(2).unwrap();

    // group_id=1: sum = 10 + 20 = 30
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(1));
    let sum1 = group_sum_col.get(0).unwrap().as_numeric().unwrap();
    assert_eq!(sum1.to_string(), "30");

    // group_id=NULL: should return NULL (no match)
    assert_eq!(id_col.get(2).unwrap().as_i64(), Some(3));
    assert!(group_sum_col.get(2).unwrap().is_null());

    // group_id=2: sum = 40
    assert_eq!(id_col.get(3).unwrap().as_i64(), Some(4));
    let sum2 = group_sum_col.get(3).unwrap().as_numeric().unwrap();
    assert_eq!(sum2.to_string(), "40");
}

#[test]
fn test_correlated_scalar_subquery_without_alias() {
    let mut executor = setup_executor();

    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
    executor
        .execute_sql("CREATE TABLE orders (order_id INT64, customer STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO orders VALUES (1, 'Alice', 100), (2, 'Alice', 150), (3, 'Bob', 200)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT o1.order_id, \
             (SELECT SUM(o2.amount) FROM orders o2 WHERE o2.customer = o1.customer) \
             FROM orders o1 ORDER BY o1.order_id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    let order_id_col = result.column(0).unwrap();
    let sum_col = result.column(1).unwrap();

    assert_eq!(order_id_col.get(0).unwrap().as_i64(), Some(1));
    let sum1 = sum_col.get(0).unwrap().as_numeric().unwrap();
    assert_eq!(sum1.to_string(), "250");

    assert_eq!(order_id_col.get(1).unwrap().as_i64(), Some(2));
    let sum2 = sum_col.get(1).unwrap().as_numeric().unwrap();
    assert_eq!(sum2.to_string(), "250");

    assert_eq!(order_id_col.get(2).unwrap().as_i64(), Some(3));
    let sum3 = sum_col.get(2).unwrap().as_numeric().unwrap();
    assert_eq!(sum3.to_string(), "200");
}

// ============================================================================
// Phase 3: Correlated EXISTS/IN Subquery Decorrelation Tests
// These tests verify that correlated EXISTS/IN subqueries are correctly
// transformed to Semi-Join/Anti-Join at the optimizer level
// ============================================================================

#[test]
fn test_correlated_exists_employees_with_orders() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS employees")
        .unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, employee_id INT64, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 3, 150)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT e.name FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.employee_id = e.id) \
             ORDER BY e.name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let name_col = result.column(0).unwrap();
    assert_eq!(name_col.get(0).unwrap().as_str(), Some("Alice"));
    assert_eq!(name_col.get(1).unwrap().as_str(), Some("Carol"));
}

#[test]
fn test_correlated_not_exists_employees_without_orders() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS employees")
        .unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, employee_id INT64, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 3, 150)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT e.name FROM employees e \
             WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.employee_id = e.id) \
             ORDER BY e.name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let name_col = result.column(0).unwrap();
    assert_eq!(name_col.get(0).unwrap().as_str(), Some("Bob"));
}

#[test]
fn test_correlated_exists_with_additional_filter() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS customers")
        .unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
    executor
        .execute_sql("CREATE TABLE customers (id INT64, name STRING, tier STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO customers VALUES (1, 'Alice', 'Gold'), (2, 'Bob', 'Silver'), (3, 'Carol', 'Gold')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 500), (2, 2, 100), (3, 3, 200)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT c.name FROM customers c \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 150) \
             ORDER BY c.name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let name_col = result.column(0).unwrap();
    assert_eq!(name_col.get(0).unwrap().as_str(), Some("Alice"));
    assert_eq!(name_col.get(1).unwrap().as_str(), Some("Carol"));
}

#[test]
fn test_correlated_in_subquery_departments() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS employees")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS departments")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, dept_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE departments (id INT64, name STRING, active INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Carol', 3)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO departments VALUES (1, 'HR', 1), (2, 'Engineering', 1), (3, 'Sales', 0)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT e.name FROM employees e \
             WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.active = 1) \
             ORDER BY e.name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let name_col = result.column(0).unwrap();
    assert_eq!(name_col.get(0).unwrap().as_str(), Some("Alice"));
    assert_eq!(name_col.get(1).unwrap().as_str(), Some("Bob"));
}

#[test]
fn test_correlated_not_in_subquery_departments() {
    let mut executor = setup_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS employees")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS departments")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, dept_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE departments (id INT64, name STRING, active INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Carol', 3)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO departments VALUES (1, 'HR', 1), (2, 'Engineering', 1), (3, 'Sales', 0)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT e.name FROM employees e \
             WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.active = 1) \
             ORDER BY e.name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let name_col = result.column(0).unwrap();
    assert_eq!(name_col.get(0).unwrap().as_str(), Some("Carol"));
}
