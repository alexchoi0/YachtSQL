use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_columns_regex() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE col_regex_test (
                id Int64,
                name String,
                value1 Int64,
                value2 Int64,
                value3 Int64
            )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO col_regex_test VALUES (1, 'test', 10, 20, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COLUMNS('value.*') FROM col_regex_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_columns_except() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE col_except_test (
                id Int64,
                name String,
                secret String,
                value Int64
            )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO col_except_test VALUES (1, 'test', 'hidden', 100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * EXCEPT (secret) FROM col_except_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_columns_replace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE col_replace_test (id Int64, value Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO col_replace_test VALUES (1, 100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * REPLACE (value * 2 AS value) FROM col_replace_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_columns_apply() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE col_apply_test (
                a Int64,
                b Int64,
                c Int64
            )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO col_apply_test VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COLUMNS('.*') APPLY sum FROM col_apply_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_columns_apply_multiple() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE col_multi_apply (str1 String, str2 String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO col_multi_apply VALUES ('hello', 'world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COLUMNS('str.*') APPLY length APPLY toString FROM col_multi_apply")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_negative_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE neg_order_test (a Int64, b String, c Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO neg_order_test VALUES (1, 'x', 1.1), (2, 'y', 2.2), (3, 'z', 3.3)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT a, b, c FROM neg_order_test ORDER BY -1")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_negative_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE neg_group_test (category String, value Int64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO neg_group_test VALUES
            ('A', 10), ('B', 20), ('A', 30), ('B', 40)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, sum(value) FROM neg_group_test GROUP BY -2")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_select_distinct_on() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE distinct_on_test (category String, ts DateTime, value Int64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO distinct_on_test VALUES
            ('A', '2023-01-01 10:00:00', 100),
            ('A', '2023-01-01 11:00:00', 200),
            ('B', '2023-01-01 10:00:00', 300),
            ('B', '2023-01-01 12:00:00', 400)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT DISTINCT ON (category) category, ts, value
            FROM distinct_on_test
            ORDER BY category, ts DESC",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_all_modifier() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE all_mod_test (value Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO all_mod_test VALUES (1), (1), (2), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ALL value FROM all_mod_test")
        .unwrap();
    assert!(result.num_rows() == 5); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_columns_with_aggregates() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE col_agg_test (
                metric1 Float64,
                metric2 Float64,
                metric3 Float64,
                category String
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO col_agg_test VALUES
            (1.0, 2.0, 3.0, 'A'),
            (4.0, 5.0, 6.0, 'B'),
            (7.0, 8.0, 9.0, 'A')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT COLUMNS('metric.*') APPLY avg FROM col_agg_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_columns_in_expression() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE col_expr_test (val1 Int64, val2 Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO col_expr_test VALUES (10, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COLUMNS('val.*') APPLY (x -> x * 2) FROM col_expr_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_select_from_subquery_columns() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT COLUMNS('.*')
            FROM (SELECT 1 AS a, 2 AS b, 3 AS c)",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_columns_except_multiple() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE col_exc_multi (
                id Int64,
                created DateTime,
                updated DateTime,
                name String,
                value Int64
            )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO col_exc_multi VALUES (1, now(), now(), 'test', 100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * EXCEPT (created, updated) FROM col_exc_multi")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_columns_apply_if() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE col_apply_if (a Nullable(Int64), b Nullable(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO col_apply_if VALUES (1, NULL), (NULL, 2), (3, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COLUMNS('.*') APPLY ifNull(0) FROM col_apply_if")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}
