use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_final_replacing_merge_tree() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE final_replacing (
                id INT64,
                version UInt64,
                value String
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_replacing VALUES (1, 1, 'first')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_replacing VALUES (1, 2, 'second')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, version, value FROM final_replacing FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 2, "second"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_final_collapsing_merge_tree() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE final_collapsing (
                id INT64,
                sign Int8,
                value Int64
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_collapsing VALUES (1, 1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_collapsing VALUES (1, -1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_collapsing VALUES (1, 1, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM final_collapsing FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 200]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_final_versioned_collapsing() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE final_versioned (
                id INT64,
                sign Int8,
                version UInt64,
                value String
            ) ENGINE = VersionedCollapsingMergeTree(sign, version)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_versioned VALUES (1, 1, 1, 'v1')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_versioned VALUES (1, -1, 1, 'v1')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_versioned VALUES (1, 1, 2, 'v2')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, version, value FROM final_versioned FINAL")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_final_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE final_where (
                id INT64,
                version UInt64,
                category String
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_where VALUES (1, 1, 'A')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_where VALUES (1, 2, 'B')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_where VALUES (2, 1, 'A')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM final_where FINAL WHERE category = 'A'")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_final_with_order() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE final_order (
                id INT64,
                version UInt64,
                score INT64
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_order VALUES (1, 1, 100), (2, 1, 50), (3, 1, 75)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM final_order FINAL ORDER BY score DESC")
        .unwrap();
    assert_table_eq!(result, [[1], [3], [2]]);
}

#[test]
fn test_final_with_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE final_limit (
                id INT64,
                version UInt64
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY id",
        )
        .unwrap();
    for i in 1..=10 {
        executor
            .execute_sql(&format!("INSERT INTO final_limit VALUES ({}, 1)", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT id FROM final_limit FINAL ORDER BY id LIMIT 3")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
fn test_final_with_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE final_group (
                category String,
                version UInt64,
                value INT64
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (category, value)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_group VALUES ('A', 1, 10), ('A', 2, 20), ('B', 1, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) FROM final_group FINAL GROUP BY category ORDER BY category")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_final_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE final_join_left (
                id INT64,
                version UInt64,
                value INT64
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("CREATE TABLE final_join_right (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_join_left VALUES (1, 1, 100), (1, 2, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_join_right VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT l.id, l.value, r.name FROM final_join_left l FINAL JOIN final_join_right r ON l.id = r.id")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_final_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE final_sub (
                id INT64,
                version UInt64
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_sub VALUES (1, 1), (1, 2), (2, 1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM (SELECT id FROM final_sub FINAL)")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_final_multiple_versions() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE final_multi (
                id INT64,
                version UInt64,
                data String
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY id",
        )
        .unwrap();
    for v in 1..=5 {
        executor
            .execute_sql(&format!(
                "INSERT INTO final_multi VALUES (1, {}, 'v{}')",
                v, v
            ))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT id, version, data FROM final_multi FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 5, "v5"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_final_summing_merge_tree() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE final_summing (
                id INT64,
                value Int64
            ) ENGINE = SummingMergeTree(value)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_summing VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_summing VALUES (1, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_summing VALUES (1, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM final_summing FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 60]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_final_aggregating_merge_tree() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE final_agg_source (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW final_agg_mv
            ENGINE = AggregatingMergeTree()
            ORDER BY id
            AS SELECT id, sumState(value) AS total
            FROM final_agg_source
            GROUP BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO final_agg_source VALUES (1, 10), (1, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, sumMerge(total) FROM final_agg_mv FINAL GROUP BY id")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
