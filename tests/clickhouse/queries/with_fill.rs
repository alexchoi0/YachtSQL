use crate::common::create_executor;

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fill_test (date Date, value Int64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO fill_test VALUES
            ('2023-01-01', 10),
            ('2023-01-03', 30),
            ('2023-01-05', 50)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT date, value
            FROM fill_test
            ORDER BY date WITH FILL",
        )
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_from_to() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fill_range (n Int64, value Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fill_range VALUES (2, 20), (5, 50), (8, 80)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT n, value
            FROM fill_range
            ORDER BY n WITH FILL FROM 1 TO 10",
        )
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_step() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fill_step (n Int64, value Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fill_step VALUES (0, 0), (10, 100), (20, 200)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT n, value
            FROM fill_step
            ORDER BY n WITH FILL FROM 0 TO 25 STEP 5",
        )
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_date_step() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fill_date_step (date Date, value Int64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO fill_date_step VALUES
            ('2023-01-01', 10),
            ('2023-01-15', 150)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT date, value
            FROM fill_date_step
            ORDER BY date WITH FILL STEP INTERVAL 1 DAY",
        )
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_multiple_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fill_multi (a Int64, b Int64, value Int64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO fill_multi VALUES
            (1, 1, 11), (1, 3, 13), (2, 2, 22)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT a, b, value
            FROM fill_multi
            ORDER BY a WITH FILL FROM 1 TO 3, b WITH FILL FROM 1 TO 3",
        )
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_interpolate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fill_interp (n Int64, value Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fill_interp VALUES (1, 10), (5, 50)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT n, value
            FROM fill_interp
            ORDER BY n WITH FILL FROM 1 TO 5 INTERPOLATE (value AS value)",
        )
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_descending() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fill_desc (n Int64, value Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fill_desc VALUES (10, 100), (5, 50), (1, 10)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT n, value
            FROM fill_desc
            ORDER BY n DESC WITH FILL FROM 10 TO 1 STEP -1",
        )
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_aggregation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fill_agg (category String, day Int64, sales Int64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO fill_agg VALUES
            ('A', 1, 100), ('A', 3, 300),
            ('B', 2, 200), ('B', 4, 400)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, day, sum(sales) AS total
            FROM fill_agg
            GROUP BY category, day
            ORDER BY category, day WITH FILL FROM 1 TO 5",
        )
        .unwrap();
    assert!(result.num_rows() >= 4);
}

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_datetime() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fill_datetime (ts DateTime, value Int64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO fill_datetime VALUES
            ('2023-01-01 10:00:00', 100),
            ('2023-01-01 12:00:00', 120)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT ts, value
            FROM fill_datetime
            ORDER BY ts WITH FILL STEP INTERVAL 1 HOUR",
        )
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_null_handling() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fill_null (n Int64, value Nullable(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fill_null VALUES (1, 10), (3, NULL), (5, 50)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT n, value
            FROM fill_null
            ORDER BY n WITH FILL FROM 1 TO 5",
        )
        .unwrap();
    assert!(result.num_rows() >= 3);
}

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_expression() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fill_expr (n Int64, value Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fill_expr VALUES (2, 20), (6, 60)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT n, value, n * 10 AS computed
            FROM fill_expr
            ORDER BY n WITH FILL FROM 1 TO 7 STEP 1",
        )
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[ignore = "Requires WITH FILL support"]
#[test]
fn test_with_fill_subquery() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT n, value
            FROM (SELECT number AS n, number * 10 AS value FROM numbers(10) WHERE number % 3 = 0)
            ORDER BY n WITH FILL FROM 0 TO 9",
        )
        .unwrap();
    assert!(result.num_rows() >= 4);
}
