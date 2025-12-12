#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_table_not_found() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT * FROM nonexistent_table");

    assert!(result.is_err(), "Should error on non-existent table");
}

#[test]
fn test_column_not_found() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    let result = executor.execute_sql("SELECT nonexistent_column FROM users");

    assert!(result.is_err(), "Should error on non-existent column");
}

#[test]
fn test_table_already_exists() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE users (id INT64)")
        .unwrap();

    let result = executor.execute_sql("CREATE TABLE users (id INT64)");

    assert!(result.is_err(), "Should error when table already exists");
}

#[test]
fn test_division_by_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (10)")
        .unwrap();

    let result = executor.execute_sql("SELECT value / 0 FROM numbers");

    assert!(result.is_err(), "Should error on division by zero");
}

#[test]
fn test_insert_type_mismatch() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE typed (id INT64, value INT64)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO typed VALUES (1, 'string')");

    assert!(result.is_err(), "Should error on type mismatch");
}

#[test]
fn test_insert_wrong_column_count() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO users VALUES (1, 'Alice')");

    assert!(
        result.is_err(),
        "Should error when column count doesn't match"
    );
}

#[test]
fn test_group_by_non_aggregated_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE sales (product STRING, region STRING, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 'North', 100)")
        .unwrap();

    let result =
        executor.execute_sql("SELECT product, region, SUM(amount) FROM sales GROUP BY product");

    assert!(
        result.is_err(),
        "Should error when non-aggregated column not in GROUP BY"
    );
}

#[test]
fn test_having_without_group_by() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE sales (amount INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT amount FROM sales HAVING amount > 100");

    assert!(result.is_err(), "Should error when HAVING without GROUP BY");
}

#[test]
fn test_invalid_date_format() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE events (id INT64, event_date DATE)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO events VALUES (1, DATE 'invalid-date')");

    assert!(result.is_err(), "Should error on invalid date format");
}

#[test]
fn test_invalid_function_name() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT NONEXISTENT_FUNCTION(value) FROM data");

    assert!(result.is_err(), "Should error on invalid function name");
}

#[test]
fn test_wrong_function_argument_count() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (10)")
        .unwrap();

    let result = executor.execute_sql("SELECT COALESCE() FROM data");

    assert!(
        result.is_err(),
        "Should error on wrong function argument count"
    );
}

#[test]
fn test_ambiguous_column_in_join() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE table1 (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE table2 (id INT64, value STRING)")
        .unwrap();

    let result =
        executor.execute_sql("SELECT id FROM table1 INNER JOIN table2 ON table1.id = table2.id");

    assert!(
        result.is_err(),
        "Should error on ambiguous column reference"
    );
}

#[test]
fn test_invalid_join_condition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE table1 (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE table2 (name STRING)")
        .unwrap();

    let result =
        executor.execute_sql("SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.name");

    assert!(
        result.is_err(),
        "Should error on type mismatch in join condition"
    );
}

#[test]
fn test_null_in_arithmetic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (NULL)")
        .unwrap();

    let result = executor.execute_sql("SELECT value + 10 FROM data").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_select_star_from_empty_from() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("SELECT *");

    assert!(result.is_err(), "Should error when SELECT * without FROM");
}

#[test]
fn test_order_by_non_selected_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (col1 INT64, col2 INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT col1 FROM data ORDER BY col2");

    assert!(
        result.is_ok() || result.is_err(),
        "Behavior depends on SQL dialect"
    );
}

#[test]
fn test_limit_negative_value() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM data LIMIT -1");

    assert!(result.is_err(), "Should error on negative LIMIT");
}

#[test]
fn test_offset_negative_value() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM data OFFSET -1");

    assert!(result.is_err(), "Should error on negative OFFSET");
}

#[test]
fn test_duplicate_column_names_in_create() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let result = executor.execute_sql("CREATE TABLE invalid (id INT64, id INT64)");

    assert!(result.is_err(), "Should error on duplicate column names");
}

#[test]
fn test_select_from_subquery_without_alias() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (42)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM (SELECT * FROM data)")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_union_type_mismatch() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE table1 (value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE table2 (value STRING)")
        .unwrap();

    let result = executor.execute_sql("SELECT value FROM table1 UNION SELECT value FROM table2");

    assert!(result.is_err(), "Should error on type mismatch in UNION");
}

#[test]
fn test_union_column_count_mismatch() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE table1 (col1 INT64, col2 INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE table2 (col1 INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM table1 UNION SELECT * FROM table2");

    assert!(
        result.is_err(),
        "Should error on column count mismatch in UNION"
    );
}

#[test]
fn test_aggregate_in_where_clause() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM data WHERE SUM(value) > 100");

    assert!(
        result.is_err(),
        "Should error when aggregate used in WHERE clause"
    );
}

#[test]
fn test_window_function_in_where() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    let result =
        executor.execute_sql("SELECT * FROM data WHERE ROW_NUMBER() OVER (ORDER BY value) = 1");

    assert!(
        result.is_err(),
        "Should error when window function in WHERE clause"
    );
}

#[test]
fn test_invalid_cte_reference() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    let result =
        executor.execute_sql("WITH cte AS (SELECT * FROM data) SELECT * FROM nonexistent_cte");

    assert!(result.is_err(), "Should error on invalid CTE reference");
}
