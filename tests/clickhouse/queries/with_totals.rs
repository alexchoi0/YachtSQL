use crate::common::create_executor;

#[test]
fn test_with_totals_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE totals_basic (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO totals_basic VALUES ('A', 10), ('A', 20), ('B', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) FROM totals_basic GROUP BY category WITH TOTALS")
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[test]
fn test_with_totals_count() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE totals_count (category String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO totals_count VALUES ('A'), ('A'), ('B'), ('C')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, COUNT(*) FROM totals_count GROUP BY category WITH TOTALS")
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[test]
fn test_with_totals_avg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE totals_avg (category String, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO totals_avg VALUES ('A', 80), ('A', 90), ('B', 70)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, AVG(score) FROM totals_avg GROUP BY category WITH TOTALS")
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[test]
fn test_with_totals_multiple_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE totals_multi (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO totals_multi VALUES ('A', 10), ('A', 20), ('B', 30), ('B', 40)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value), AVG(value), COUNT(*) FROM totals_multi GROUP BY category WITH TOTALS")
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[ignore = "Implement me!"]
#[test]
fn test_with_totals_having() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE totals_having (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO totals_having VALUES ('A', 10), ('A', 20), ('B', 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) FROM totals_having GROUP BY category HAVING SUM(value) > 15 WITH TOTALS")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_with_totals_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE totals_order (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO totals_order VALUES ('C', 10), ('A', 30), ('B', 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) as total FROM totals_order GROUP BY category WITH TOTALS ORDER BY total DESC")
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[test]
fn test_with_totals_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE totals_limit (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO totals_limit VALUES ('A', 10), ('B', 20), ('C', 30), ('D', 40)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, SUM(value) FROM totals_limit GROUP BY category WITH TOTALS LIMIT 2",
        )
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[test]
fn test_with_totals_multiple_groups() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE totals_multigrp (region String, category String, value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO totals_multigrp VALUES ('US', 'A', 10), ('US', 'B', 20), ('EU', 'A', 30)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT region, category, SUM(value) FROM totals_multigrp GROUP BY region, category WITH TOTALS")
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Implement me!"]
#[test]
fn test_with_totals_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE totals_sub (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO totals_sub VALUES ('A', 10), ('A', 20), ('B', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM (SELECT category, SUM(value) FROM totals_sub GROUP BY category WITH TOTALS)")
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[test]
fn test_with_totals_min_max() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE totals_minmax (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO totals_minmax VALUES ('A', 10), ('A', 50), ('B', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, MIN(value), MAX(value) FROM totals_minmax GROUP BY category WITH TOTALS")
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[test]
fn test_with_totals_after_having() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE totals_after (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO totals_after VALUES ('A', 10), ('A', 20), ('B', 5), ('C', 100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) AS total FROM totals_after GROUP BY category WITH TOTALS HAVING total > 20 SETTINGS totals_mode = 'after_having_inclusive'")
        .unwrap();
    assert!(result.num_rows() >= 1);
}
