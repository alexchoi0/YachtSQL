#[macro_use]
mod common;

use common::numeric;
use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_case_simple_when_then() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE orders (id INT64, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO orders VALUES (1, 100), (2, 50), (3, 200)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, CASE WHEN amount > 100 THEN 'high' ELSE 'normal' END AS category FROM orders",
        )
        .unwrap();

    assert_batch_eq!(result, [[1, "normal"], [2, "normal"], [3, "high"],]);
}

#[test]
fn test_case_multiple_when() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO scores VALUES ('Alice', 95), ('Bob', 85), ('Charlie', 70), ('David', 60)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT name, CASE \
                WHEN score >= 90 THEN 'A' \
                WHEN score >= 80 THEN 'B' \
                WHEN score >= 70 THEN 'C' \
                ELSE 'F' \
            END AS grade FROM scores",
        )
        .unwrap();

    assert_batch_eq!(
        result,
        [
            ["Alice", "A"],
            ["Bob", "B"],
            ["Charlie", "C"],
            ["David", "F"],
        ]
    );
}

#[test]
fn test_case_with_operand() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE products (name STRING, category STRING, price INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO products VALUES ('Laptop', 'electronics', 1000), ('Shirt', 'clothing', 50), ('Book', 'media', 20)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT name, CASE category \
                WHEN 'electronics' THEN price * 1.2 \
                WHEN 'clothing' THEN price * 1.1 \
                ELSE price \
            END AS final_price FROM products",
        )
        .unwrap();

    assert_batch_eq!(
        result,
        [
            ["Laptop", numeric(1200)],
            ["Shirt", numeric(55)],
            ["Book", numeric(20)],
        ]
    );
}

#[test]
fn test_case_without_else() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE items (id INT64, status STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO items VALUES (1, 'active'), (2, 'inactive')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, CASE WHEN status = 'active' THEN 'yes' END AS is_active FROM items",
        )
        .unwrap();

    assert_batch_eq!(result, [[1, "yes"], [2, null],]);
}

#[test]
fn test_case_returns_int() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor.execute_sql("CREATE TABLE data (x INT64)").unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (5), (15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CASE WHEN x < 10 THEN 1 ELSE 0 END AS is_small FROM data")
        .unwrap();

    assert_batch_eq!(result, [[1], [0],]);
}

#[test]
fn test_case_with_arithmetic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE sales (product STRING, units INT64, price INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES ('A', 100, 10), ('B', 50, 20)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT product, CASE \
                WHEN units > 75 THEN units * price * 0.9 \
                ELSE units * price \
            END AS revenue FROM sales",
        )
        .unwrap();

    assert_batch_eq!(result, [["A", numeric(900)], ["B", numeric(1000)],]);
}

#[test]
fn test_case_in_where_clause() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE employees (name STRING, salary INT64)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO employees VALUES ('Alice', 50000), ('Bob', 75000), ('Charlie', 100000)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT name FROM employees WHERE CASE WHEN salary > 60000 THEN 1 ELSE 0 END = 1",
        )
        .unwrap();

    assert_batch_eq!(result, [["Bob"], ["Charlie"],]);
}

#[test]
fn test_case_with_string_functions() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE users (name STRING, role STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES ('alice', 'admin'), ('bob', 'user')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT CASE WHEN role = 'admin' THEN UPPER(name) ELSE name END AS display_name FROM users",
        )
        .unwrap();

    assert_batch_eq!(result, [["ALICE"], ["bob"],]);
}

#[test]
fn test_nested_case() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE items (type STRING, priority INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO items VALUES ('urgent', 1), ('normal', 5), ('urgent', 10)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT CASE type \
                WHEN 'urgent' THEN CASE WHEN priority < 5 THEN 'critical' ELSE 'high' END \
                ELSE 'medium' \
            END AS level FROM items",
        )
        .unwrap();

    assert_batch_eq!(result, [["critical"], ["medium"], ["high"],]);
}

#[test]
fn test_case_with_null_handling() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, NULL)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, CASE WHEN value IS NULL THEN 0 ELSE value END AS safe_value FROM data",
        )
        .unwrap();

    assert_batch_eq!(result, [[1, 10], [2, 0],]);
}

#[test]
fn test_case_with_column_comparison() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE inventory (product STRING, stock INT64, threshold INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO inventory VALUES ('A', 100, 50), ('B', 20, 50), ('C', 75, 75)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT product, CASE \
                WHEN stock > threshold THEN 'overstocked' \
                WHEN stock < threshold THEN 'understocked' \
                ELSE 'balanced' \
            END AS status FROM inventory",
        )
        .unwrap();

    assert_batch_eq!(
        result,
        [
            ["A", "overstocked"],
            ["B", "understocked"],
            ["C", "balanced"],
        ]
    );
}

#[test]
fn test_case_boolean_result() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE tests (name STRING, score INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO tests VALUES ('Test1', 80), ('Test2', 55)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT name, CASE WHEN score >= 60 THEN true ELSE false END AS passed FROM tests",
        )
        .unwrap();

    assert_batch_eq!(result, [["Test1", true], ["Test2", false],]);
}

#[test]
fn test_multiple_case_in_select() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE metrics (metric STRING, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO metrics VALUES ('cpu', 85), ('memory', 45)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT metric, \
                CASE WHEN value > 80 THEN 'critical' ELSE 'ok' END AS status, \
                CASE WHEN value > 50 THEN value - 50 ELSE 0 END AS over_threshold \
            FROM metrics",
        )
        .unwrap();

    assert_batch_eq!(result, [["cpu", "critical", 35], ["memory", "ok", 0],]);
}

#[test]
fn test_case_with_complex_conditions() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE orders (id INT64, amount INT64, customer_type STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1000, 'premium'), (2, 500, 'regular'), (3, 2000, 'premium')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, CASE \
                WHEN customer_type = 'premium' AND amount > 1500 THEN amount * 0.8 \
                WHEN customer_type = 'premium' THEN amount * 0.9 \
                WHEN amount > 1000 THEN amount * 0.95 \
                ELSE amount \
            END AS discounted_amount FROM orders",
        )
        .unwrap();

    assert_batch_eq!(
        result,
        [[1, numeric(900)], [2, numeric(500)], [3, numeric(1600)],]
    );
}
