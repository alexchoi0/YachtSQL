use crate::common::create_executor;

#[ignore = "Requires HAVING alias support"]
#[test]
fn test_with_rollup_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rollup_basic (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rollup_basic VALUES ('A', 10), ('A', 20), ('B', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) FROM rollup_basic GROUP BY ROLLUP(category)")
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Requires HAVING alias support"]
#[test]
fn test_with_rollup_two_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rollup_two (region String, category String, value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO rollup_two VALUES ('US', 'A', 10), ('US', 'B', 20), ('EU', 'A', 30)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, category, SUM(value) FROM rollup_two GROUP BY ROLLUP(region, category)",
        )
        .unwrap();
    assert!(result.num_rows() >= 6);
}

#[ignore = "Requires HAVING alias support"]
#[test]
fn test_with_rollup_three_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE rollup_three (year INT64, region String, category String, value INT64)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO rollup_three VALUES (2023, 'US', 'A', 10), (2023, 'US', 'B', 20), (2024, 'EU', 'A', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT year, region, category, SUM(value) FROM rollup_three GROUP BY ROLLUP(year, region, category)")
        .unwrap();
    assert!(result.num_rows() >= 7);
}

#[ignore = "Requires HAVING alias support"]
#[test]
fn test_with_rollup_count() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rollup_count (category String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rollup_count VALUES ('A'), ('A'), ('B'), ('C')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, COUNT(*) FROM rollup_count GROUP BY ROLLUP(category)")
        .unwrap();
    assert!(result.num_rows() >= 4);
}

#[ignore = "Requires HAVING alias support"]
#[test]
fn test_with_rollup_multiple_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rollup_multi (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rollup_multi VALUES ('A', 10), ('A', 20), ('B', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value), AVG(value), COUNT(*) FROM rollup_multi GROUP BY ROLLUP(category)")
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Requires HAVING alias support"]
#[test]
fn test_with_rollup_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rollup_order (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rollup_order VALUES ('C', 10), ('A', 30), ('B', 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) as total FROM rollup_order GROUP BY ROLLUP(category) ORDER BY total DESC")
        .unwrap();
    assert!(result.num_rows() >= 4);
}

#[ignore = "Requires HAVING alias support"]
#[test]
fn test_with_rollup_having() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rollup_having (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rollup_having VALUES ('A', 10), ('A', 20), ('B', 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) as total FROM rollup_having GROUP BY ROLLUP(category) HAVING total > 15")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[ignore = "Requires HAVING alias support"]
#[test]
fn test_with_rollup_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rollup_where (category String, value INT64, active UInt8)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rollup_where VALUES ('A', 10, 1), ('A', 20, 0), ('B', 30, 1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) FROM rollup_where WHERE active = 1 GROUP BY ROLLUP(category)")
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Requires HAVING alias support"]
#[test]
fn test_grouping_function_rollup() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE grouping_rollup (region String, category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO grouping_rollup VALUES ('US', 'A', 10), ('EU', 'B', 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT region, category, SUM(value), GROUPING(region), GROUPING(category) FROM grouping_rollup GROUP BY ROLLUP(region, category)")
        .unwrap();
    assert!(result.num_rows() >= 5);
}
