use crate::common::create_executor;

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE paste_left (a Int64, b String)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE paste_right (c Float64, d String)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO paste_left VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO paste_right VALUES (1.1, 'a'), (2.2, 'b'), (3.3, 'c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM paste_left PASTE JOIN paste_right")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_different_row_counts() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE paste_short (id Int64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE paste_long (value Int64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO paste_short VALUES (1), (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO paste_long VALUES (10), (20), (30), (40)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM paste_short PASTE JOIN paste_long")
        .unwrap();
    assert!(result.num_rows() == 4); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_subqueries() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT *
            FROM (SELECT number AS a FROM numbers(3))
            PASTE JOIN (SELECT number * 10 AS b FROM numbers(3))",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_with_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE paste_cols1 (id Int64, name String)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE paste_cols2 (value Float64, category String)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO paste_cols1 VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO paste_cols2 VALUES (100.0, 'A'), (200.0, 'B')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, value, category FROM paste_cols1 PASTE JOIN paste_cols2")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_multiple_tables() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE p1 (a Int64)").unwrap();
    executor.execute_sql("CREATE TABLE p2 (b Int64)").unwrap();
    executor.execute_sql("CREATE TABLE p3 (c Int64)").unwrap();

    executor
        .execute_sql("INSERT INTO p1 VALUES (1), (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO p2 VALUES (10), (20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO p3 VALUES (100), (200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM p1 PASTE JOIN p2 PASTE JOIN p3")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_empty_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE paste_data (id Int64, value Int64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE paste_empty (extra Int64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO paste_data VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM paste_data PASTE JOIN paste_empty")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_with_order() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE paste_ordered1 (a Int64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE paste_ordered2 (b Int64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO paste_ordered1 VALUES (3), (1), (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO paste_ordered2 VALUES (30), (10), (20)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM
            (SELECT a FROM paste_ordered1 ORDER BY a)
            PASTE JOIN
            (SELECT b FROM paste_ordered2 ORDER BY b)",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_with_limit() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT *
            FROM (SELECT number AS a FROM numbers(10) LIMIT 5)
            PASTE JOIN (SELECT number * 2 AS b FROM numbers(10) LIMIT 5)",
        )
        .unwrap();
    assert!(result.num_rows() == 5); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE paste_filter1 (id Int64, val Int64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE paste_filter2 (extra String)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO paste_filter1 VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO paste_filter2 VALUES ('a'), ('b'), ('c')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM paste_filter1 PASTE JOIN paste_filter2
            WHERE val > 15",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_aggregation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE paste_agg1 (category String, value Int64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE paste_agg2 (weight Float64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO paste_agg1 VALUES ('A', 10), ('A', 20), ('B', 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO paste_agg2 VALUES (1.0), (2.0), (1.5)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, sum(value), sum(weight)
            FROM paste_agg1 PASTE JOIN paste_agg2
            GROUP BY category
            ORDER BY category",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_arrays() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE paste_arr1 (id Int64, arr1 Array(Int64))")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE paste_arr2 (arr2 Array(String))")
        .unwrap();

    executor
        .execute_sql("INSERT INTO paste_arr1 VALUES (1, [1, 2]), (2, [3, 4])")
        .unwrap();
    executor
        .execute_sql("INSERT INTO paste_arr2 VALUES (['a', 'b']), (['c', 'd'])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, arr1, arr2 FROM paste_arr1 PASTE JOIN paste_arr2")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_nullables() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE paste_null1 (id Int64, val Nullable(Int64))")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE paste_null2 (extra Nullable(String))")
        .unwrap();

    executor
        .execute_sql("INSERT INTO paste_null1 VALUES (1, 100), (2, NULL), (3, 300)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO paste_null2 VALUES ('x'), (NULL), ('z')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM paste_null1 PASTE JOIN paste_null2")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Requires PASTE JOIN support"]
#[test]
fn test_paste_join_cte() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "WITH
                t1 AS (SELECT number AS a FROM numbers(3)),
                t2 AS (SELECT number * 10 AS b FROM numbers(3))
            SELECT * FROM t1 PASTE JOIN t2",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}
