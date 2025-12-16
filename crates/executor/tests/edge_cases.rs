#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_empty_string_insert_and_select() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE strings (id INT64, value STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO strings VALUES (1, '')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO strings VALUES (2, 'non-empty')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM strings WHERE value = ''")
        .unwrap();

    assert_eq!(result.num_rows(), 1, "Should find empty string");
}

#[test]
fn test_empty_string_comparison() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE strings (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO strings VALUES ('')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO strings VALUES ('test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM strings WHERE value = ''")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_max_int64_value() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (9223372036854775807)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM numbers").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_min_int64_value() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (-9223372036854775808)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM numbers").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_zero_in_all_contexts() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (0, 0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1, 0)")
        .unwrap();

    let result1 = executor
        .execute_sql("SELECT * FROM numbers WHERE id = 0")
        .unwrap();
    assert_eq!(result1.num_rows(), 1);

    let result2 = executor
        .execute_sql("SELECT * FROM numbers WHERE value = 0")
        .unwrap();
    assert_eq!(result2.num_rows(), 2);

    let result3 = executor
        .execute_sql("SELECT SUM(value) FROM numbers")
        .unwrap();
    assert_eq!(result3.num_rows(), 1);
}

#[test]
fn test_negative_numbers() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (-100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (-1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE value < 0")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_unicode_characters() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE unicode (id INT64, text STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO unicode VALUES (1, 'Hello 世界')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO unicode VALUES (2, 'Привет мир')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO unicode VALUES (3, '😀🎉🚀')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM unicode").unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_special_characters_in_strings() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE special (id INT64, text STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO special VALUES (1, 'Line1\nLine2')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO special VALUES (2, 'Tab\there')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO special VALUES (3, 'Quote''s')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM special").unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_leading_trailing_spaces() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE spaces (id INT64, text STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO spaces VALUES (1, '  leading')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO spaces VALUES (2, 'trailing  ')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO spaces VALUES (3, '  both  ')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM spaces").unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_duplicate_values_in_table() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE duplicates (value INT64)")
        .unwrap();

    for _ in 0..10 {
        executor
            .execute_sql("INSERT INTO duplicates VALUES (42)")
            .unwrap();
    }

    let result = executor.execute_sql("SELECT * FROM duplicates").unwrap();

    assert_eq!(result.num_rows(), 10);

    let distinct = executor
        .execute_sql("SELECT DISTINCT value FROM duplicates")
        .unwrap();

    assert_eq!(distinct.num_rows(), 1);

    let count_distinct = executor
        .execute_sql("SELECT COUNT(DISTINCT value) FROM duplicates")
        .unwrap();

    let count_value = count_distinct
        .column(0)
        .unwrap()
        .get(0)
        .unwrap()
        .as_i64()
        .unwrap();
    assert_eq!(count_value, 1);
}

#[test]
fn test_single_row_operations() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE single (id INT64, value STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO single VALUES (1, 'only')")
        .unwrap();

    let select = executor.execute_sql("SELECT * FROM single").unwrap();
    assert_eq!(select.num_rows(), 1);

    let count = executor.execute_sql("SELECT COUNT(*) FROM single").unwrap();
    assert_eq!(count.num_rows(), 1);

    let group_by = executor
        .execute_sql("SELECT value, COUNT(*) FROM single GROUP BY value")
        .unwrap();
    assert_eq!(group_by.num_rows(), 1);
}

#[test]
fn test_empty_result_set() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE empty (id INT64, value STRING)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM empty").unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_empty_result_with_where() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

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
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

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

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_float_special_values() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE floats (id INT64, value FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO floats VALUES (1, 0.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats VALUES (2, -0.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats VALUES (3, 1.7976931348623157E308)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM floats").unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_very_small_float() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE floats (value FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO floats VALUES (2.2250738585072014E-308)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats VALUES (1e-300)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM floats").unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_nested_subqueries() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (5)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM (SELECT * FROM numbers WHERE value > 3) sub WHERE value < 12")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_deeply_nested_subqueries() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (5)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM (SELECT * FROM (SELECT * FROM numbers WHERE value > 0) s1) s2 WHERE value >= 1")
        .unwrap();

    assert_eq!(result.num_rows(), 3, "Should handle 3-level deep nesting");
}

#[test]
fn test_subquery_empty_result() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM (SELECT * FROM numbers WHERE value > 100) sub")
        .unwrap();

    assert_eq!(result.num_rows(), 0, "Should handle empty subquery result");

    let result2 = executor
        .execute_sql("SELECT * FROM (SELECT * FROM numbers WHERE value > 100) sub WHERE value > 0")
        .unwrap();

    assert_eq!(
        result2.num_rows(),
        0,
        "Should handle empty subquery with additional filter"
    );
}

#[test]
fn test_duplicate_column_values_groupby() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE groups (category STRING, value INT64)")
        .unwrap();

    for i in 0..100 {
        executor
            .execute_sql(&format!("INSERT INTO groups VALUES ('A', {})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT category, COUNT(*) FROM groups GROUP BY category")
        .unwrap();

    assert_eq!(result.num_rows(), 1);

    let distinct_count = executor
        .execute_sql("SELECT COUNT(DISTINCT value) FROM groups")
        .unwrap();

    let count = distinct_count
        .column(0)
        .unwrap()
        .get(0)
        .unwrap()
        .as_i64()
        .unwrap();
    assert_eq!(count, 100);
}

#[test]
fn test_group_by_empty_result() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE empty_groups (category STRING, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, COUNT(*) FROM empty_groups GROUP BY category")
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_group_by_all_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE null_groups (category STRING, value INT64)")
        .unwrap();

    for i in 0..10 {
        executor
            .execute_sql(&format!("INSERT INTO null_groups VALUES (NULL, {})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT category, COUNT(*) FROM null_groups GROUP BY category")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_group_by_with_null_values() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE mixed_groups (category STRING, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO mixed_groups VALUES ('A', 1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed_groups VALUES ('A', 2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed_groups VALUES (NULL, 3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed_groups VALUES (NULL, 4)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed_groups VALUES ('B', 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, COUNT(*) FROM mixed_groups GROUP BY category")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_limit_zero() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor.execute_sql("INSERT INTO data VALUES (1)").unwrap();
    executor.execute_sql("INSERT INTO data VALUES (2)").unwrap();

    let result = executor.execute_sql("SELECT * FROM data LIMIT 0").unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_limit_one() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor.execute_sql("INSERT INTO data VALUES (1)").unwrap();
    executor.execute_sql("INSERT INTO data VALUES (2)").unwrap();
    executor.execute_sql("INSERT INTO data VALUES (3)").unwrap();

    let result = executor.execute_sql("SELECT * FROM data LIMIT 1").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_offset_beyond_result_size() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor.execute_sql("INSERT INTO data VALUES (1)").unwrap();
    executor.execute_sql("INSERT INTO data VALUES (2)").unwrap();

    let result = executor
        .execute_sql("SELECT * FROM data LIMIT 10 OFFSET 100")
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_order_by_with_all_equal_values() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    for _ in 0..10 {
        executor
            .execute_sql("INSERT INTO data VALUES (42)")
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT * FROM data ORDER BY value ASC")
        .unwrap();

    assert_eq!(result.num_rows(), 10);
}

#[test]
fn test_whitespace_in_string_values() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE whitespace (id INT64, text STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO whitespace VALUES (1, '   ')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO whitespace VALUES (2, '\t\t')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO whitespace VALUES (3, '\n\n')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM whitespace").unwrap();

    assert_eq!(result.num_rows(), 3);
}
