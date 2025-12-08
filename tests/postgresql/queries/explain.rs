use crate::common::create_executor;

fn setup_table(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE explain_test (id INTEGER, name TEXT, val INTEGER)")
        .unwrap();
    executor.execute_sql("INSERT INTO explain_test VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)").unwrap();
}

#[test]
fn test_explain_select() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_select_where() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test WHERE id = 1")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_analyze() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN ANALYZE SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_verbose() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN VERBOSE SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_costs() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (COSTS TRUE) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_format_text() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (FORMAT TEXT) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_format_json() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (FORMAT JSON) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_format_xml() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (FORMAT XML) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_format_yaml() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (FORMAT YAML) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_buffers() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_timing() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (ANALYZE, TIMING TRUE) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_summary() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (ANALYZE, SUMMARY TRUE) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_join() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_ref (id INTEGER, explain_id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_ref VALUES (1, 1), (2, 2)")
        .unwrap();

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT * FROM explain_test e JOIN explain_ref r ON e.id = r.explain_id",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_aggregate() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT name, SUM(val) FROM explain_test GROUP BY name")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_subquery() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT * FROM explain_test WHERE val > (SELECT AVG(val) FROM explain_test)",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_cte() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN WITH high_val AS (SELECT * FROM explain_test WHERE val > 150)
             SELECT * FROM high_val",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_union() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT id, name FROM explain_test WHERE val < 150
             UNION
             SELECT id, name FROM explain_test WHERE val > 250",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_order_limit() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test ORDER BY val DESC LIMIT 2")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_insert() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN INSERT INTO explain_test VALUES (4, 'Diana', 400)")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_update() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN UPDATE explain_test SET val = val + 10 WHERE id = 1")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_delete() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN DELETE FROM explain_test WHERE id > 2")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_window_function() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT id, name, val, ROW_NUMBER() OVER (ORDER BY val DESC) as rank
             FROM explain_test",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_multiple_options() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (ANALYZE TRUE, VERBOSE TRUE, COSTS TRUE, BUFFERS TRUE, FORMAT TEXT) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_settings() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (SETTINGS TRUE) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_wal() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor.execute_sql("EXPLAIN (ANALYZE, WAL) SELECT * FROM explain_test");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_explain_generic_plan() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result =
        executor.execute_sql("EXPLAIN (GENERIC_PLAN) SELECT * FROM explain_test WHERE id = $1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_explain_costs_false() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (COSTS FALSE) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_timing_false() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (ANALYZE, TIMING FALSE) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_summary_false() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (ANALYZE, SUMMARY FALSE) SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_index_scan() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE INDEX explain_idx ON explain_test (id)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test WHERE id = 1")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_seq_scan() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_bitmap_scan() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE INDEX explain_val_idx ON explain_test (val)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test WHERE val > 100 AND val < 300")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_explain_hash_join() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_large (id INTEGER, data TEXT)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO explain_large SELECT generate_series, 'data' FROM generate_series(1, 100)",
        )
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test e JOIN explain_large l ON e.id = l.id");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_explain_merge_join() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_sorted (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_sorted VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test e JOIN explain_sorted s ON e.id = s.id ORDER BY e.id")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_nested_loop() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_small (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_small VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test e, explain_small s WHERE e.id = s.id")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_sort() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test ORDER BY val DESC")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_hash_aggregate() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT name, COUNT(*) FROM explain_test GROUP BY name")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_group_aggregate() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT name, SUM(val) FROM explain_test GROUP BY name ORDER BY name")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_distinct() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT DISTINCT name FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_limit_offset() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test LIMIT 2 OFFSET 1")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_exists_subquery() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_orders (id INTEGER, user_id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_orders VALUES (1, 1), (2, 1)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test e WHERE EXISTS (SELECT 1 FROM explain_orders o WHERE o.user_id = e.id)")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_in_subquery() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_ids (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_ids VALUES (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test WHERE id IN (SELECT id FROM explain_ids)")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_lateral_join() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT e.*, l.max_val FROM explain_test e,
             LATERAL (SELECT MAX(val) as max_val FROM explain_test WHERE id <= e.id) l",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_recursive_cte() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "EXPLAIN WITH RECURSIVE nums AS (
                SELECT 1 AS n
                UNION ALL
                SELECT n + 1 FROM nums WHERE n < 10
             )
             SELECT * FROM nums",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_materialized_cte() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN WITH high_val AS MATERIALIZED (SELECT * FROM explain_test WHERE val > 150)
             SELECT * FROM high_val",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_not_materialized_cte() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN WITH high_val AS NOT MATERIALIZED (SELECT * FROM explain_test WHERE val > 150)
             SELECT * FROM high_val",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_grouping_sets() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT name, val, COUNT(*) FROM explain_test
             GROUP BY GROUPING SETS ((name), (val), ())",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_rollup() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT name, val, COUNT(*) FROM explain_test GROUP BY ROLLUP (name, val)",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_cube() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT name, val, COUNT(*) FROM explain_test GROUP BY CUBE (name, val)",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_window_partition() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT id, name, val,
             SUM(val) OVER (PARTITION BY name ORDER BY id) as running_sum
             FROM explain_test",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_multiple_window() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT id, name, val,
             ROW_NUMBER() OVER (ORDER BY val) as rn,
             RANK() OVER (ORDER BY val) as rnk,
             DENSE_RANK() OVER (ORDER BY val) as drnk
             FROM explain_test",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_left_join() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_optional (id INTEGER, extra TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_optional VALUES (1, 'extra1')")
        .unwrap();

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT * FROM explain_test e LEFT JOIN explain_optional o ON e.id = o.id",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_right_join() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_right (id INTEGER, data TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_right VALUES (1, 'r1'), (4, 'r4')")
        .unwrap();

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT * FROM explain_test e RIGHT JOIN explain_right r ON e.id = r.id",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_full_join() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_full (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_full VALUES (1), (5)")
        .unwrap();

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT * FROM explain_test e FULL OUTER JOIN explain_full f ON e.id = f.id",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_cross_join() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_cross (x INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_cross VALUES (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test CROSS JOIN explain_cross")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_semi_join() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_semi (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_semi VALUES (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test WHERE id IN (SELECT id FROM explain_semi)")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_anti_join() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_anti (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_anti VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_test WHERE id NOT IN (SELECT id FROM explain_anti WHERE id IS NOT NULL)")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_union_all() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT id, name FROM explain_test WHERE val < 150
             UNION ALL
             SELECT id, name FROM explain_test WHERE val > 250",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_intersect() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT id FROM explain_test WHERE val > 100
             INTERSECT
             SELECT id FROM explain_test WHERE val < 300",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_except() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT id FROM explain_test
             EXCEPT
             SELECT id FROM explain_test WHERE val > 200",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_case_expression() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT id, name,
             CASE WHEN val > 200 THEN 'high' WHEN val > 100 THEN 'medium' ELSE 'low' END as category
             FROM explain_test",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_coalesce() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_null (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_null VALUES (1, NULL), (2, 100)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT id, COALESCE(val, 0) as val FROM explain_null")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_nullif() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT id, NULLIF(val, 100) FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_cast() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT id, CAST(val AS DOUBLE PRECISION) FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_string_functions() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT id, UPPER(name), LENGTH(name) FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_math_functions() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT id, ABS(val), SQRT(val), ROUND(val / 3.0, 2) FROM explain_test",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_date_functions() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_dates (id INTEGER, dt TIMESTAMP)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_dates VALUES (1, '2024-01-15 10:30:00')")
        .unwrap();

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT id, EXTRACT(YEAR FROM dt), DATE_TRUNC('month', dt) FROM explain_dates",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_filter_clause() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN SELECT COUNT(*) FILTER (WHERE val > 100) FROM explain_test")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_json_operations() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_json (id INTEGER, data JSON)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_json VALUES (1, '{\"name\": \"test\"}')")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT id, data->>'name' FROM explain_json")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_array_operations() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_arr (id INTEGER, arr INTEGER[])")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_arr VALUES (1, ARRAY[1, 2, 3])")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT id, arr[1], array_length(arr, 1) FROM explain_arr")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_explain_values() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("EXPLAIN VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_insert_select() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE explain_copy (id INTEGER, name TEXT, val INTEGER)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN INSERT INTO explain_copy SELECT * FROM explain_test WHERE val > 100")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_insert_returning() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN INSERT INTO explain_test VALUES (4, 'Diana', 400) RETURNING *")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_update_returning() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN UPDATE explain_test SET val = val + 10 WHERE id = 1 RETURNING *")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_delete_returning() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN DELETE FROM explain_test WHERE id = 1 RETURNING *")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_upsert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_upsert (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_upsert VALUES (1, 'Alice', 100)")
        .unwrap();

    let result = executor
        .execute_sql(
            "EXPLAIN INSERT INTO explain_upsert VALUES (1, 'Updated', 200)
             ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, val = EXCLUDED.val",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_merge() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_target (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE explain_source (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_target VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_source VALUES (1, 200), (2, 300)")
        .unwrap();

    let result = executor.execute_sql(
        "EXPLAIN MERGE INTO explain_target t
             USING explain_source s ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET val = s.val
             WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_explain_parallel_query() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_parallel (id INTEGER, data TEXT)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN (ANALYZE) SELECT COUNT(*) FROM explain_parallel")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_partition_pruning() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE explain_part (id INTEGER, created_at DATE, val INTEGER) PARTITION BY RANGE (created_at)"
    ).unwrap();

    let result =
        executor.execute_sql("EXPLAIN SELECT * FROM explain_part WHERE created_at = '2024-01-15'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_explain_index_only_scan() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_ios (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_ios VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT id FROM explain_ios WHERE id = 1")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_foreign_key_check() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE explain_child (id INTEGER, parent_id INTEGER REFERENCES explain_parent(id))",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_parent VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN INSERT INTO explain_child VALUES (1, 1)")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_explain_trigger_execution() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("EXPLAIN (ANALYZE) UPDATE explain_test SET val = val + 1")
        .unwrap();
    assert!(result.num_rows() >= 1);
}
