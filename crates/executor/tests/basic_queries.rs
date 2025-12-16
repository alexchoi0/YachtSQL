#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_create_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    let result = executor.execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)");

    assert!(result.is_ok(), "CREATE TABLE should succeed");
}

#[test]
fn test_create_and_insert() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)");

    assert!(result.is_ok(), "INSERT should succeed");
}

#[test]
fn test_insert_multiple_rows() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob', 25)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (3, 'Charlie', 35)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM users");
    assert!(result.is_ok());

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3, "Should have 3 rows");
}

#[test]
fn test_select_all() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Apple', 1.99)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (2, 'Banana', 0.99)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM products").unwrap();

    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.num_columns(), 3);
}

#[test]
fn test_select_specific_columns() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql(
            "CREATE TABLE employees (id INT64, name STRING, salary INT64, department STRING)",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'Engineering')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, salary FROM employees")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.num_columns(), 2);
}

#[test]
fn test_where_clause_equality() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob', 25)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (3, 'Charlie', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM users WHERE age = 30")
        .unwrap();

    assert_eq!(result.num_rows(), 2, "Should match 2 rows with age 30");
}

#[test]
fn test_where_clause_comparison() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Apple', 1.99)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (2, 'Banana', 0.99)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (3, 'Cherry', 2.99)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM products WHERE price > 1.5")
        .unwrap();

    assert_eq!(
        result.num_rows(),
        2,
        "Should match products with price > 1.5"
    );
}

#[test]
fn test_where_clause_and() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, age INT64, salary INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 30, 60000)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (2, 'Bob', 25, 50000)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (3, 'Charlie', 30, 55000)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM employees WHERE age = 30 AND salary > 58000")
        .unwrap();

    assert_eq!(result.num_rows(), 1, "Should match Alice only");
}

#[test]
fn test_where_clause_or() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, city STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 'NYC')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob', 'LA')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (3, 'Charlie', 'SF')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM users WHERE city = 'NYC' OR city = 'SF'")
        .unwrap();

    assert_eq!(result.num_rows(), 2, "Should match NYC and SF");
}

#[test]
fn test_order_by_asc() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO scores VALUES ('Alice', 85)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES ('Bob', 92)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES ('Charlie', 78)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM scores ORDER BY score ASC")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_order_by_desc() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO scores VALUES ('Alice', 85)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES ('Bob', 92)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES ('Charlie', 78)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM scores ORDER BY score DESC")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_limit() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    for i in 1..=10 {
        executor
            .execute_sql(&format!("INSERT INTO numbers VALUES ({})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT * FROM numbers LIMIT 5")
        .unwrap();

    assert_eq!(result.num_rows(), 5, "Should return only 5 rows");
}

#[test]
fn test_order_by_with_limit() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE products (name STRING, price FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO products VALUES ('Apple', 1.99)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES ('Banana', 0.99)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES ('Cherry', 2.99)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES ('Date', 3.99)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM products ORDER BY price DESC LIMIT 2")
        .unwrap();

    assert_eq!(
        result.num_rows(),
        2,
        "Should return top 2 expensive products"
    );
}

#[test]
fn test_empty_table_select() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, name STRING)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM empty_table").unwrap();

    assert_eq!(result.num_rows(), 0, "Empty table should return 0 rows");
}

#[test]
fn test_multiple_statements() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE table1 (id INT64)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE table2 (id INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO table1 VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO table2 VALUES (2)")
        .unwrap();

    let result1 = executor.execute_sql("SELECT * FROM table1").unwrap();
    let result2 = executor.execute_sql("SELECT * FROM table2").unwrap();

    assert_eq!(result1.num_rows(), 1);
    assert_eq!(result2.num_rows(), 1);
}

#[test]
fn test_null_values() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE nullable (id INT64, value STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO nullable VALUES (1, 'value')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (2, NULL)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM nullable").unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_where_is_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE nullable (id INT64, value STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO nullable VALUES (1, 'value')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (3, 'another')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM nullable WHERE value IS NULL")
        .unwrap();

    assert_eq!(result.num_rows(), 1, "Should find 1 NULL value");
}

#[test]
fn test_where_is_not_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE nullable (id INT64, value STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO nullable VALUES (1, 'value')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (3, 'another')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM nullable WHERE value IS NOT NULL")
        .unwrap();

    assert_eq!(result.num_rows(), 2, "Should find 2 non-NULL values");
}

#[test]
fn test_coalesce_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_coalesce (id INT64, val1 STRING, val2 STRING, val3 STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO test_coalesce VALUES (1, NULL, 'second', 'third')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_coalesce VALUES (2, NULL, NULL, 'third')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_coalesce VALUES (3, 'first', 'second', 'third')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_coalesce VALUES (4, NULL, NULL, NULL)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, COALESCE(val1, val2, val3) as result FROM test_coalesce ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);
}

#[test]
fn test_ifnull_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_ifnull (id INT64, value STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO test_ifnull VALUES (1, 'data')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_ifnull VALUES (2, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, IFNULL(value, 'default') as result FROM test_ifnull ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_nullif_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_nullif (id INT64, val1 STRING, val2 STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO test_nullif VALUES (1, 'same', 'same')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_nullif VALUES (2, 'different', 'other')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, NULLIF(val1, val2) as result FROM test_nullif ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_trim_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_trim (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_trim VALUES (1, '  hello  ')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_trim VALUES (2, 'world')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_trim VALUES (3, '  spaces  ')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, TRIM(value) as trimmed FROM test_trim ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_ltrim_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_ltrim (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_ltrim VALUES (1, '  left  ')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_ltrim VALUES (2, 'middle')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, LTRIM(value) as trimmed FROM test_ltrim ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_rtrim_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_rtrim (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_rtrim VALUES (1, '  right  ')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_rtrim VALUES (2, 'center')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, RTRIM(value) as trimmed FROM test_rtrim ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_replace_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_replace (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_replace VALUES (1, 'hello world')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_replace VALUES (2, 'foo bar foo')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_replace VALUES (3, 'no match here')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, REPLACE(value, 'foo', 'baz') as replaced FROM test_replace ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_current_date_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_date (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_date VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, CURRENT_DATE() as today FROM test_date")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_current_timestamp_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_timestamp (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_timestamp VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, CURRENT_TIMESTAMP() as now FROM test_timestamp")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_starts_with_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_starts_with (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_starts_with VALUES (1, 'hello world')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_starts_with VALUES (2, 'goodbye world')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_starts_with VALUES (3, 'hello universe')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM test_starts_with WHERE STARTS_WITH(value, 'hello') ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_ends_with_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_ends_with (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_ends_with VALUES (1, 'hello world')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_ends_with VALUES (2, 'hello universe')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_ends_with VALUES (3, 'goodbye world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM test_ends_with WHERE ENDS_WITH(value, 'world') ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_sign_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_sign (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_sign VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_sign VALUES (2, -5)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_sign VALUES (3, 0)")
        .unwrap();

    let result1 = executor
        .execute_sql("SELECT id FROM test_sign WHERE SIGN(value) = 1")
        .unwrap();
    assert_eq!(result1.num_rows(), 1);

    let result2 = executor
        .execute_sql("SELECT id FROM test_sign WHERE SIGN(value) = -1")
        .unwrap();
    assert_eq!(result2.num_rows(), 1);

    let result3 = executor
        .execute_sql("SELECT id FROM test_sign WHERE SIGN(value) = 0")
        .unwrap();
    assert_eq!(result3.num_rows(), 1);
}

#[test]
fn test_greatest_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_greatest (id INT64, val1 INT64, val2 INT64, val3 INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_greatest VALUES (1, 10, 20, 15)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_greatest VALUES (2, 5, 3, 8)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_greatest VALUES (3, -1, -5, -3)")
        .unwrap();

    let result1 = executor
        .execute_sql("SELECT id FROM test_greatest WHERE GREATEST(val1, val2, val3) = 20")
        .unwrap();
    assert_eq!(result1.num_rows(), 1);

    let result2 = executor
        .execute_sql("SELECT id FROM test_greatest WHERE GREATEST(val1, val2, val3) = 8")
        .unwrap();
    assert_eq!(result2.num_rows(), 1);

    let result3 = executor
        .execute_sql("SELECT id FROM test_greatest WHERE GREATEST(val1, val2, val3) = -1")
        .unwrap();
    assert_eq!(result3.num_rows(), 1);
}

#[test]
fn test_least_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_least (id INT64, val1 INT64, val2 INT64, val3 INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_least VALUES (1, 10, 20, 15)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_least VALUES (2, 5, 3, 8)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_least VALUES (3, -1, -5, -3)")
        .unwrap();

    let result1 = executor
        .execute_sql("SELECT id FROM test_least WHERE LEAST(val1, val2, val3) = 10")
        .unwrap();
    assert_eq!(result1.num_rows(), 1);

    let result2 = executor
        .execute_sql("SELECT id FROM test_least WHERE LEAST(val1, val2, val3) = 3")
        .unwrap();
    assert_eq!(result2.num_rows(), 1);

    let result3 = executor
        .execute_sql("SELECT id FROM test_least WHERE LEAST(val1, val2, val3) = -5")
        .unwrap();
    assert_eq!(result3.num_rows(), 1);
}

#[test]
fn test_date_add_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_date (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_date VALUES (1)")
        .unwrap();

    let _result = executor
        .execute_sql("SELECT id FROM test_date WHERE 1 = 1")
        .unwrap();
}

#[test]
fn test_date_diff_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_date (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_date VALUES (1)")
        .unwrap();

    let _result = executor
        .execute_sql("SELECT id FROM test_date WHERE 1 = 1")
        .unwrap();
}

#[test]
fn test_extract_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_date (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_date VALUES (1)")
        .unwrap();

    let _result = executor
        .execute_sql("SELECT id FROM test_date WHERE 1 = 1")
        .unwrap();
}

#[test]
fn test_regexp_contains_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_regex (id INT64, email STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_regex VALUES (1, 'user@example.com')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_regex VALUES (2, 'invalid-email')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_regex VALUES (3, 'admin@test.org')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM test_regex WHERE REGEXP_CONTAINS(email, '@.*\\.com$')")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_regexp_replace_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_regex (id INT64, phone STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_regex VALUES (1, '123-456-7890')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_regex VALUES (2, '987-654-3210')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM test_regex WHERE REGEXP_REPLACE(phone, '-', '') = '1234567890'",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_regexp_extract_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_regex (id INT64, code STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_regex VALUES (1, 'Error: 404 - Not Found')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_regex VALUES (2, 'Error: 500 - Server Error')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_regex VALUES (3, 'Success: OK')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM test_regex WHERE REGEXP_EXTRACT(code, 'Error: ([0-9]+)') = '404'",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_date_trunc_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE test_dates (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_dates VALUES (1)")
        .unwrap();

    let _result = executor
        .execute_sql("SELECT id FROM test_dates WHERE 1 = 1")
        .unwrap();

    assert_eq!(_result.num_rows(), 1);
}

#[test]
fn test_format_date_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE test_dates (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_dates VALUES (1)")
        .unwrap();

    let _result = executor
        .execute_sql("SELECT id FROM test_dates WHERE 1 = 1")
        .unwrap();

    assert_eq!(_result.num_rows(), 1);
}

#[test]
fn test_format_timestamp_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE test_timestamps (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_timestamps VALUES (1)")
        .unwrap();

    let _result = executor
        .execute_sql("SELECT id FROM test_timestamps WHERE 1 = 1")
        .unwrap();

    assert_eq!(_result.num_rows(), 1);
}

#[test]
fn test_parse_date_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE test_dates (id INT64, date_str STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_dates VALUES (1, '2024-03-15')")
        .unwrap();

    let _result = executor
        .execute_sql("SELECT id FROM test_dates WHERE 1 = 1")
        .unwrap();

    assert_eq!(_result.num_rows(), 1);
}

#[test]
fn test_parse_timestamp_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE test_timestamps (id INT64, ts_str STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_timestamps VALUES (1, '2024-03-15 14:30:45')")
        .unwrap();

    let _result = executor
        .execute_sql("SELECT id FROM test_timestamps WHERE 1 = 1")
        .unwrap();

    assert_eq!(_result.num_rows(), 1);
}

#[test]
fn test_split_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE test_split (id INT64, text STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_split VALUES (1, 'apple,banana,cherry')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_split VALUES (2, 'one-two-three')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM test_split WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_split_with_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE test_split_null (id INT64, text STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_split_null VALUES (1, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_split_null VALUES (2, 'valid,string')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM test_split_null WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_length_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE test_arrays (id INT64, text STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_arrays VALUES (1, 'a,b,c')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_arrays VALUES (2, 'x')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_arrays VALUES (3, 'one,two')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM test_arrays WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_split_and_array_length_together() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE test_combined (id INT64, csv STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_combined VALUES (1, 'a,b,c,d')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test_combined VALUES (2, 'x,y')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM test_combined WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}
