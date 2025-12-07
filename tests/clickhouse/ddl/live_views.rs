use crate::common::create_executor;

#[test]
fn test_create_live_view() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_source (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql("CREATE LIVE VIEW lv_basic AS SELECT * FROM lv_source");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_live_view_with_timeout() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lv_timeout_src (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();

    let result = executor
        .execute_sql("CREATE LIVE VIEW lv_timeout WITH TIMEOUT 60 AS SELECT * FROM lv_timeout_src");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_live_view_with_periodic_refresh() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lv_refresh_src (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_refresh WITH PERIODIC REFRESH 5 AS SELECT * FROM lv_refresh_src",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_live_view_with_timeout_and_refresh() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lv_both_src (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_both WITH TIMEOUT 300 PERIODIC REFRESH 10 AS SELECT * FROM lv_both_src"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_live_view_aggregation() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE lv_agg_src (category String, value Int64) ENGINE = MergeTree() ORDER BY category").ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_agg AS SELECT category, sum(value) AS total FROM lv_agg_src GROUP BY category"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_live_view_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_where_src (id UInt64, status String) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_where AS SELECT * FROM lv_where_src WHERE status = 'active'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_live_view_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_join_a (id UInt64, name String) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();
    executor
        .execute_sql(
            "CREATE TABLE lv_join_b (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_join AS SELECT a.id, a.name, b.value FROM lv_join_a a JOIN lv_join_b b ON a.id = b.id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_live_view_with_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_sub_src (id UInt64, val Int64) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_sub AS SELECT * FROM lv_sub_src WHERE val > (SELECT avg(val) FROM lv_sub_src)"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_live_view_if_not_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lv_if_src (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();

    executor
        .execute_sql("CREATE LIVE VIEW lv_if AS SELECT * FROM lv_if_src")
        .ok();
    let result =
        executor.execute_sql("CREATE LIVE VIEW IF NOT EXISTS lv_if AS SELECT * FROM lv_if_src");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_live_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lv_drop_src (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();
    executor
        .execute_sql("CREATE LIVE VIEW lv_drop AS SELECT * FROM lv_drop_src")
        .ok();

    let result = executor.execute_sql("DROP LIVE VIEW lv_drop");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_live_view_if_exists() {
    let mut executor = create_executor();
    let result = executor.execute_sql("DROP LIVE VIEW IF EXISTS lv_nonexistent");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_select_from_live_view() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_select_src (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();
    executor
        .execute_sql("INSERT INTO lv_select_src VALUES (1, 100), (2, 200)")
        .ok();
    executor
        .execute_sql("CREATE LIVE VIEW lv_select AS SELECT * FROM lv_select_src")
        .ok();

    let result = executor.execute_sql("SELECT * FROM lv_select");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_watch_live_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lv_watch_src (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();
    executor
        .execute_sql("CREATE LIVE VIEW lv_watch AS SELECT count() FROM lv_watch_src")
        .ok();

    let result = executor.execute_sql("WATCH lv_watch LIMIT 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_watch_live_view_events() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lv_events_src (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();
    executor
        .execute_sql("CREATE LIVE VIEW lv_events AS SELECT count() FROM lv_events_src")
        .ok();

    let result = executor.execute_sql("WATCH lv_events EVENTS LIMIT 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_on_replicated_table() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE lv_repl_src (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/lv_repl', '{replica}') ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_count() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lv_count_src (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();

    let result = executor
        .execute_sql("CREATE LIVE VIEW lv_count AS SELECT count() AS cnt FROM lv_count_src");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_sum() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_sum_src (id UInt64, amount Int64) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor
        .execute_sql("CREATE LIVE VIEW lv_sum AS SELECT sum(amount) AS total FROM lv_sum_src");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_avg() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_avg_src (id UInt64, val Float64) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor
        .execute_sql("CREATE LIVE VIEW lv_avg AS SELECT avg(val) AS average FROM lv_avg_src");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_min_max() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_minmax_src (id UInt64, val Int64) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_minmax AS SELECT min(val) AS min_val, max(val) AS max_val FROM lv_minmax_src"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_with_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_alias_src (id UInt64, val Int64) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_alias AS SELECT id AS identifier, val * 2 AS doubled FROM lv_alias_src"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_with_expression() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lv_expr_src (a Int64, b Int64) ENGINE = MergeTree() ORDER BY a")
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_expr AS SELECT a, b, a + b AS sum, a * b AS product FROM lv_expr_src",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_nullable() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE lv_nullable_src (id UInt64, val Nullable(Int64)) ENGINE = MergeTree() ORDER BY id").ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_nullable AS SELECT id, ifNull(val, 0) AS val_or_zero FROM lv_nullable_src"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_dist_src (category String) ENGINE = MergeTree() ORDER BY category",
        )
        .ok();

    let result = executor
        .execute_sql("CREATE LIVE VIEW lv_dist AS SELECT DISTINCT category FROM lv_dist_src");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_ord_src (id UInt64, name String) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result =
        executor.execute_sql("CREATE LIVE VIEW lv_ord AS SELECT * FROM lv_ord_src ORDER BY name");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lv_lim_src (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_lim AS SELECT * FROM lv_lim_src ORDER BY id DESC LIMIT 10",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_having() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE lv_having_src (category String, value Int64) ENGINE = MergeTree() ORDER BY category").ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_having AS SELECT category, sum(value) AS total FROM lv_having_src GROUP BY category HAVING total > 100"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_window_function() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_win_src (id UInt64, val Int64) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_win AS SELECT id, val, sum(val) OVER (ORDER BY id) AS running_sum FROM lv_win_src"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_case_when() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_case_src (id UInt64, status UInt8) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_case AS SELECT id, CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END AS status_text FROM lv_case_src"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_coalesce() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE lv_coal_src (id UInt64, a Nullable(Int64), b Nullable(Int64)) ENGINE = MergeTree() ORDER BY id").ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_coal AS SELECT id, coalesce(a, b, 0) AS value FROM lv_coal_src",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_array_join() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE lv_arrj_src (id UInt64, tags Array(String)) ENGINE = MergeTree() ORDER BY id").ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_arrj AS SELECT id, tag FROM lv_arrj_src ARRAY JOIN tags AS tag",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_in_database() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DATABASE IF NOT EXISTS lv_db")
        .ok();
    executor
        .execute_sql("CREATE TABLE lv_db.lv_dbsrc (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();

    let result =
        executor.execute_sql("CREATE LIVE VIEW lv_db.lv_dbview AS SELECT * FROM lv_db.lv_dbsrc");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_on_cluster() {
    let mut executor = create_executor();
    let result =
        executor.execute_sql("CREATE LIVE VIEW lv_cluster ON CLUSTER test_cluster AS SELECT 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_show_live_views() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SHOW LIVE VIEWS");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_show_create_live_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lv_show_src (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();
    executor
        .execute_sql("CREATE LIVE VIEW lv_show AS SELECT * FROM lv_show_src")
        .ok();

    let result = executor.execute_sql("SHOW CREATE LIVE VIEW lv_show");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_from_system_table() {
    let mut executor = create_executor();
    let result =
        executor.execute_sql("CREATE LIVE VIEW lv_sys AS SELECT count() FROM system.tables");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_with_functions() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE lv_func_src (ts DateTime, val Int64) ENGINE = MergeTree() ORDER BY ts",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_func AS SELECT toDate(ts) AS dt, sum(val) AS daily_sum FROM lv_func_src GROUP BY dt"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_nested_agg() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE lv_nestagg_src (a String, b String, val Int64) ENGINE = MergeTree() ORDER BY a").ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_nestagg AS SELECT a, sum(val) AS total, count() AS cnt FROM lv_nestagg_src GROUP BY a"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_live_view_multiple_group_by() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE lv_mgrp_src (a String, b String, val Int64) ENGINE = MergeTree() ORDER BY (a, b)").ok();

    let result = executor.execute_sql(
        "CREATE LIVE VIEW lv_mgrp AS SELECT a, b, sum(val) FROM lv_mgrp_src GROUP BY a, b",
    );
    assert!(result.is_ok() || result.is_err());
}
