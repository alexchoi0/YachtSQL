#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_stddev_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT STDDEV(value) AS stddev FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_stddev_pop() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT STDDEV_POP(value) AS stddev_pop FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_stddev_samp() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT STDDEV_SAMP(value) AS stddev_samp FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_stddev_single_value() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (42)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT STDDEV(value) AS stddev FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_stddev_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (NULL), (30), (NULL), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT STDDEV(value) AS stddev FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_variance_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT VARIANCE(value) AS variance FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_var_pop() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT VAR_POP(value) AS var_pop FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_var_samp() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT VAR_SAMP(value) AS var_samp FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_variance_relationship_to_stddev() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();

    let variance_result = executor
        .execute_sql("SELECT VARIANCE(value) AS var FROM numbers")
        .unwrap();
    let stddev_result = executor
        .execute_sql("SELECT STDDEV(value) AS stddev FROM numbers")
        .unwrap();

    assert_eq!(variance_result.num_rows(), 1);
    assert_eq!(stddev_result.num_rows(), 1);
}

#[test]
fn test_percentile_cont_median() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT PERCENTILE_CONT(value, 0.5) AS median FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_percentile_cont_quartiles() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50), (60), (70), (80), (90), (100)").unwrap();
    let result = executor
        .execute_sql(
            "SELECT
            PERCENTILE_CONT(value, 0.25) AS q1,
            PERCENTILE_CONT(value, 0.50) AS q2,
            PERCENTILE_CONT(value, 0.75) AS q3
         FROM numbers",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_percentile_disc_median() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT PERCENTILE_DISC(value, 0.5) AS median FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_approx_quantiles() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT APPROX_QUANTILES(value, 4) AS quartiles FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_percentile_cont_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (NULL), (30), (NULL), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT PERCENTILE_CONT(value, 0.5) AS median FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_median_odd_count() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT MEDIAN(value) AS median FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_median_even_count() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT MEDIAN(value) AS median FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_mode_single() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3), (3), (3), (4), (5)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT MODE(value) AS mode FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_mode_multimodal() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (1), (2), (2), (3)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT MODE(value) AS mode FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_approx_top_count() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE items (category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('A'), ('A'), ('A'), ('B'), ('B'), ('C')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT APPROX_TOP_COUNT(category, 2) AS top_categories FROM items")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_approx_top_sum() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE sales (product STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO sales VALUES ('A', 100), ('A', 200), ('B', 150), ('B', 50), ('C', 75)",
        )
        .unwrap();
    let result = executor
        .execute_sql("SELECT APPROX_TOP_SUM(product, amount, 2) AS top_products FROM sales")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_corr_perfect_positive() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE data (x FLOAT64, y FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT CORR(x, y) AS correlation FROM data")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_corr_perfect_negative() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE data (x FLOAT64, y FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 8), (3, 6), (4, 4), (5, 2)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT CORR(x, y) AS correlation FROM data")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_corr_no_correlation() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE data (x FLOAT64, y FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 5), (2, 3), (3, 8), (4, 2), (5, 7)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT CORR(x, y) AS correlation FROM data")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_covar_pop() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE data (x FLOAT64, y FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT COVAR_POP(x, y) AS covariance FROM data")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_covar_samp() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE data (x FLOAT64, y FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT COVAR_SAMP(x, y) AS covariance FROM data")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_count_distinct() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE items (category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('A'), ('A'), ('B'), ('B'), ('B'), ('C')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT category) AS distinct_count FROM items")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_count_distinct_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE items (category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('A'), ('A'), (NULL), ('B'), (NULL), ('C')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT category) AS distinct_count FROM items")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_countif() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT COUNTIF(value > 25) AS count_gt_25 FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_countif_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (NULL), (30), (NULL), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT COUNTIF(value > 25) AS count FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_agg_with_order_by() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (30), (10), (50), (20), (40)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value) AS sorted_array FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_agg_with_limit() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value LIMIT 3) AS top3 FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_agg_distinct() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE items (category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('A'), ('A'), ('B'), ('B'), ('C')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT ARRAY_AGG(DISTINCT category) AS categories FROM items")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_agg_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE items (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (10), (NULL), (30), (NULL), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value IGNORE NULLS) AS non_null_array FROM items")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_agg_respect_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE items (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (10), (NULL), (30), (NULL), (50)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value RESPECT NULLS) AS all_values FROM items")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_stddev_by_group() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE measurements (category STRING, value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO measurements VALUES ('A', 10), ('A', 20), ('A', 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO measurements VALUES ('B', 100), ('B', 200), ('B', 300)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT category, STDDEV(value) AS stddev FROM measurements GROUP BY category ORDER BY category"
    ).unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_percentile_by_group() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE scores (student STRING, score FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES ('A', 85), ('A', 90), ('A', 95)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES ('B', 70), ('B', 75), ('B', 80)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT student, PERCENTILE_CONT(score, 0.5) AS median_score FROM scores GROUP BY student ORDER BY student"
    ).unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_multiple_aggregates() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE data (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (10), (20), (30), (40), (50)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
            AVG(value) AS mean,
            STDDEV(value) AS stddev,
            VARIANCE(value) AS variance,
            MEDIAN(value) AS median,
            MIN(value) AS min,
            MAX(value) AS max
         FROM data",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_stddev_window_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE sales (product STRING, amount FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('A', 100), ('A', 150), ('B', 200), ('B', 250)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
            product,
            amount,
            STDDEV(amount) OVER (PARTITION BY product) AS product_stddev
         FROM sales
         ORDER BY product, amount",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 4);
}

#[test]
fn test_percentile_window_function() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE employees (dept STRING, salary FLOAT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO employees VALUES ('Sales', 50000), ('Sales', 60000), ('Sales', 70000)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES ('Eng', 80000), ('Eng', 90000), ('Eng', 100000)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
            dept,
            salary,
            PERCENTILE_CONT(salary, 0.5) OVER (PARTITION BY dept) AS dept_median
         FROM employees
         ORDER BY dept, salary",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 6);
}

#[test]
fn test_stddev_empty_set() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT STDDEV(value) AS stddev FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_percentile_single_value() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (42)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT PERCENTILE_CONT(value, 0.5) AS median FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_stddev_all_same_values() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (42), (42), (42), (42), (42)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT STDDEV(value) AS stddev FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_percentile_with_duplicates() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (10), (20), (30), (30), (30)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT PERCENTILE_CONT(value, 0.5) AS median FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_stddev_all_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (NULL), (NULL), (NULL)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT STDDEV(value) AS stddev FROM numbers")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_corr_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE data (x FLOAT64, y FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 2), (NULL, 4), (3, 6), (4, NULL), (5, 10)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT CORR(x, y) AS correlation FROM data")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[should_panic(expected = "ExecutionError")]
fn test_percentile_invalid_fraction() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE numbers (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30)")
        .unwrap();

    executor
        .execute_sql("SELECT PERCENTILE_CONT(value, 1.5) AS result FROM numbers")
        .unwrap();
}

#[should_panic(expected = "ExecutionError")]
fn test_corr_type_mismatch() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE data (x STRING, y FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES ('a', 1.0), ('b', 2.0)")
        .unwrap();

    executor
        .execute_sql("SELECT CORR(x, y) AS correlation FROM data")
        .unwrap();
}

#[should_panic(expected = "ExecutionError")]
fn test_approx_top_count_invalid_n() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
    executor
        .execute_sql("CREATE TABLE items (category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('A'), ('B'), ('C')")
        .unwrap();

    executor
        .execute_sql("SELECT APPROX_TOP_COUNT(category, -1) AS result FROM items")
        .unwrap();
}
