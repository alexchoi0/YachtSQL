#[macro_use]
mod common;

use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_select_arithmetic_add() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE test (a INT64, b INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO test VALUES (10, 20), (5, 15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b AS sum FROM test")
        .unwrap();

    assert_batch_eq!(result, [[30], [20],]);
}

#[test]
fn test_select_arithmetic_multiply() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE prices (item STRING, price INT64, quantity INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO prices VALUES ('apple', 5, 10), ('banana', 3, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT item, price * quantity AS total FROM prices")
        .unwrap();

    assert_batch_eq!(result, [["apple", 50], ["banana", 60],]);
}

#[test]
fn test_select_arithmetic_divide() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (total INT64, count INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (100, 4), (60, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT total / count AS average FROM data")
        .unwrap();

    assert_batch_eq!(result, [[25], [20],]);
}

#[test]
fn test_select_string_upper() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE names (name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO names VALUES ('alice'), ('bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT UPPER(name) AS upper_name FROM names")
        .unwrap();

    assert_batch_eq!(result, [["ALICE"], ["BOB"],]);
}

#[test]
fn test_select_string_lower() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE names (name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO names VALUES ('ALICE'), ('BOB')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT LOWER(name) AS lower_name FROM names")
        .unwrap();

    assert_batch_eq!(result, [["alice"], ["bob"],]);
}

#[test]
fn test_select_string_concat() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE users (first_name STRING, last_name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES ('John', 'Doe'), ('Jane', 'Smith')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users")
        .unwrap();

    assert_batch_eq!(result, [["John Doe"], ["Jane Smith"],]);
}

#[test]
fn test_select_string_concat_operator() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE users (first_name STRING, last_name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES ('John', 'Doe')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT first_name || ' ' || last_name AS full_name FROM users")
        .unwrap();

    assert_batch_eq!(result, [["John Doe"]]);
}

#[test]
fn test_select_string_length() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE words (word STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO words VALUES ('hello'), ('world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT word, LENGTH(word) AS len FROM words")
        .unwrap();

    assert_batch_eq!(result, [["hello", 5], ["world", 5],]);
}

#[test]
fn test_select_string_substring() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE words (word STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO words VALUES ('hello world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUBSTRING(word, 1, 5) AS first_five FROM words")
        .unwrap();

    assert_batch_eq!(result, [["hello"]]);
}

#[test]
fn test_select_mixed_expressions() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE products (name STRING, price INT64, quantity INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO products VALUES ('Widget', 10, 5)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT name, price, quantity, price * quantity AS total, UPPER(name) AS upper_name FROM products",
        )
        .unwrap();

    assert_batch_eq!(result, [["Widget", 10, 5, 50, "WIDGET"]]);
}

#[test]
fn test_select_expression_with_where() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE sales (product STRING, price INT64, quantity INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES ('A', 10, 5), ('B', 20, 3), ('C', 5, 10)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT product, price * quantity AS total FROM sales WHERE price * quantity > 50",
        )
        .unwrap();

    assert_batch_eq!(result, [["B", 60]]);
}

#[test]
fn test_select_complex_arithmetic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE calc (a INT64, b INT64, c INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO calc VALUES (10, 5, 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b * c AS result FROM calc")
        .unwrap();

    assert_batch_eq!(result, [[20]]);
}

#[test]
fn test_select_expression_aliases() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE test (x INT64, y INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO test VALUES (3, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT x + y AS sum, x * y AS product, x - y AS diff FROM test")
        .unwrap();

    assert_eq!(result.schema().fields()[0].name, "sum");
    assert_eq!(result.schema().fields()[1].name, "product");
    assert_eq!(result.schema().fields()[2].name, "diff");
    assert_batch_eq!(result, [[7, 12, -1]]);
}

#[test]
fn test_select_expression_without_alias() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE test (a INT64, b INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO test VALUES (10, 20)")
        .unwrap();

    let result = executor.execute_sql("SELECT a + b FROM test").unwrap();

    assert_batch_eq!(result, [[30]]);
}
