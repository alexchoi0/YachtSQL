#[macro_use]
mod common;

use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_count_on_empty_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM empty_table")
        .unwrap();

    assert_batch_eq!(result, [[0]]);
}

#[test]
fn test_sum_on_empty_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(value) FROM empty_table")
        .unwrap();

    assert_batch_eq!(result, [[null]]);
}

#[test]
fn test_avg_on_empty_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE empty_table (id INT64, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(value) FROM empty_table")
        .unwrap();

    assert_batch_eq!(result, [[null]]);
}

#[test]
fn test_empty_result_set() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE empty (id INT64, value STRING)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM empty").unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_empty_result_with_where() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM data WHERE value > 1000")
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_all_rows_match_filter() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM data WHERE value > 0")
        .unwrap();

    assert_batch_eq!(result, [[10], [20], [30]]);
}

#[test]
fn test_single_row_operations() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE single (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO single VALUES (1, 'only')")
        .unwrap();

    let select = executor.execute_sql("SELECT * FROM single").unwrap();
    assert_batch_eq!(select, [[1, "only"]]);

    let count = executor.execute_sql("SELECT COUNT(*) FROM single").unwrap();
    assert_batch_eq!(count, [[1]]);

    let group_by = executor
        .execute_sql("SELECT value, COUNT(*) FROM single GROUP BY value")
        .unwrap();
    assert_batch_eq!(group_by, [["only", 1]]);
}
