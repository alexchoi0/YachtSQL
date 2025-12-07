#![allow(clippy::approx_constant)]

use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_stddev() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2), (4), (4), (4), (5), (5), (7), (9)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ROUND(STDDEV(val), 6) FROM data")
        .unwrap();
    assert_table_eq!(result, [[2.138090]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_stddev_pop() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ROUND(STDDEV_POP(val), 6) FROM data")
        .unwrap();
    assert_table_eq!(result, [[1.414214]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_stddev_samp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ROUND(STDDEV_SAMP(val), 6) FROM data")
        .unwrap();
    assert_table_eq!(result, [[1.581139]]);
}

#[test]
fn test_variance() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT VARIANCE(val) FROM data")
        .unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[test]
fn test_var_pop() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT VAR_POP(val) FROM data")
        .unwrap();
    assert_table_eq!(result, [[2.0]]);
}

#[test]
fn test_var_samp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT VAR_SAMP(val) FROM data")
        .unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[test]
fn test_corr() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (x INT64, y INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)")
        .unwrap();

    let result = executor.execute_sql("SELECT CORR(x, y) FROM data").unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_covar_pop() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (x INT64, y INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 2), (2, 4), (3, 6)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ROUND(COVAR_POP(x, y), 6) FROM data")
        .unwrap();
    assert_table_eq!(result, [[1.333333]]);
}

#[test]
fn test_covar_samp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (x INT64, y INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 2), (2, 4), (3, 6)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COVAR_SAMP(x, y) FROM data")
        .unwrap();
    assert_table_eq!(result, [[2.0]]);
}

#[test]
fn test_statistical_with_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (region STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('East', 100), ('East', 150), ('East', 200), ('West', 50), ('West', 75), ('West', 100)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, ROUND(STDDEV(amount), 2) AS std FROM sales GROUP BY region ORDER BY region",
        )
        .unwrap();
    assert_table_eq!(result, [["East", 50.0], ["West", 25.0]]);
}
