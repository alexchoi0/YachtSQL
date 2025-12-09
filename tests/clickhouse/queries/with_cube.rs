use crate::common::create_executor;

#[ignore = "Requires GROUPING support"]
#[test]
fn test_with_cube_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cube_basic (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cube_basic VALUES ('A', 10), ('A', 20), ('B', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) FROM cube_basic GROUP BY CUBE(category)")
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Requires GROUPING support"]
#[test]
fn test_with_cube_two_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cube_two (region String, category String, value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO cube_two VALUES ('US', 'A', 10), ('US', 'B', 20), ('EU', 'A', 30)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, category, SUM(value) FROM cube_two GROUP BY CUBE(region, category)",
        )
        .unwrap();
    assert!(result.num_rows() >= 8);
}

#[ignore = "Requires GROUPING support"]
#[test]
fn test_with_cube_three_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE cube_three (year INT64, region String, category String, value INT64)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO cube_three VALUES (2023, 'US', 'A', 10), (2023, 'EU', 'B', 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT year, region, category, SUM(value) FROM cube_three GROUP BY CUBE(year, region, category)")
        .unwrap();
    assert!(result.num_rows() >= 8);
}

#[ignore = "Requires GROUPING support"]
#[test]
fn test_with_cube_count() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cube_count (region String, category String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cube_count VALUES ('US', 'A'), ('US', 'B'), ('EU', 'A')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, category, COUNT(*) FROM cube_count GROUP BY CUBE(region, category)",
        )
        .unwrap();
    assert!(result.num_rows() >= 8);
}

#[ignore = "Requires GROUPING support"]
#[test]
fn test_with_cube_multiple_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cube_multi (region String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cube_multi VALUES ('US', 10), ('US', 20), ('EU', 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, SUM(value), AVG(value), COUNT(*) FROM cube_multi GROUP BY CUBE(region)",
        )
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Requires GROUPING support"]
#[test]
fn test_with_cube_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cube_order (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cube_order VALUES ('C', 10), ('A', 30), ('B', 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) as total FROM cube_order GROUP BY CUBE(category) ORDER BY total DESC")
        .unwrap();
    assert!(result.num_rows() >= 4);
}

#[ignore = "Requires GROUPING support"]
#[test]
fn test_with_cube_having() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cube_having (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cube_having VALUES ('A', 10), ('A', 20), ('B', 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) as total FROM cube_having GROUP BY CUBE(category) HAVING total > 15")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[ignore = "Requires GROUPING support"]
#[test]
fn test_with_cube_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE cube_where (region String, category String, value INT64, active UInt8)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO cube_where VALUES ('US', 'A', 10, 1), ('US', 'B', 20, 0), ('EU', 'A', 30, 1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT region, category, SUM(value) FROM cube_where WHERE active = 1 GROUP BY CUBE(region, category)")
        .unwrap();
    assert!(result.num_rows() >= 6);
}

#[ignore = "Requires GROUPING support"]
#[test]
fn test_grouping_function_cube() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE grouping_cube (region String, category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO grouping_cube VALUES ('US', 'A', 10), ('EU', 'B', 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT region, category, SUM(value), GROUPING(region), GROUPING(category) FROM grouping_cube GROUP BY CUBE(region, category)")
        .unwrap();
    assert!(result.num_rows() >= 8);
}

#[ignore = "Requires GROUPING support"]
#[test]
fn test_cube_vs_rollup() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cube_rollup (a String, b String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cube_rollup VALUES ('X', 'Y', 100)")
        .unwrap();

    let cube_result = executor
        .execute_sql("SELECT a, b, SUM(value) FROM cube_rollup GROUP BY CUBE(a, b)")
        .unwrap();
    let rollup_result = executor
        .execute_sql("SELECT a, b, SUM(value) FROM cube_rollup GROUP BY ROLLUP(a, b)")
        .unwrap();
    assert!(cube_result.num_rows() >= rollup_result.num_rows());
}

#[ignore = "Requires GROUPING support"]
#[test]
fn test_grouping_sets() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE grouping_sets (region String, category String, value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO grouping_sets VALUES ('US', 'A', 10), ('US', 'B', 20), ('EU', 'A', 30)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT region, category, SUM(value) FROM grouping_sets GROUP BY GROUPING SETS ((region), (category), ())")
        .unwrap();
    assert!(result.num_rows() >= 5);
}
