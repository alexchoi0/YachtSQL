use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_optimize_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE optimize_test (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_test VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_test VALUES (2)")
        .unwrap();
    executor
        .execute_sql("OPTIMIZE TABLE optimize_test")
        .unwrap();
}

#[test]
fn test_optimize_final() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE optimize_final (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_final VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_final VALUES (2)")
        .unwrap();
    executor
        .execute_sql("OPTIMIZE TABLE optimize_final FINAL")
        .unwrap();
}

#[test]
fn test_optimize_deduplicate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE optimize_dedup (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_dedup VALUES (1), (1), (2)")
        .unwrap();
    executor
        .execute_sql("OPTIMIZE TABLE optimize_dedup DEDUPLICATE")
        .unwrap();
}

#[test]
fn test_optimize_deduplicate_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE optimize_dedup_by (id INT64, name String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_dedup_by VALUES (1, 'a'), (1, 'b'), (2, 'c')")
        .unwrap();
    executor
        .execute_sql("OPTIMIZE TABLE optimize_dedup_by DEDUPLICATE BY id")
        .unwrap();
}

#[test]
fn test_optimize_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE optimize_part (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_part VALUES (1, '2023-01-15'), (2, '2023-02-15')")
        .unwrap();
    executor
        .execute_sql("OPTIMIZE TABLE optimize_part PARTITION '202301'")
        .unwrap();
}

#[test]
fn test_optimize_replacing_merge_tree() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE optimize_replacing (
                id INT64,
                version UInt64,
                value String
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_replacing VALUES (1, 1, 'first')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_replacing VALUES (1, 2, 'second')")
        .unwrap();
    executor
        .execute_sql("OPTIMIZE TABLE optimize_replacing FINAL")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, version, value FROM optimize_replacing")
        .unwrap();
    assert_table_eq!(result, [[1, 2, "second"]]);
}

#[test]
fn test_optimize_collapsing_merge_tree() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE optimize_collapsing (
                id INT64,
                sign Int8,
                value Int64
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_collapsing VALUES (1, 1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_collapsing VALUES (1, -1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_collapsing VALUES (1, 1, 200)")
        .unwrap();
    executor
        .execute_sql("OPTIMIZE TABLE optimize_collapsing FINAL")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM optimize_collapsing")
        .unwrap();
    assert_table_eq!(result, [[1, 200]]);
}

#[test]
fn test_optimize_aggregating_merge_tree() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE optimize_agg (
                id INT64,
                cnt AggregateFunction(count, UInt64)
            ) ENGINE = AggregatingMergeTree
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("OPTIMIZE TABLE optimize_agg FINAL")
        .unwrap();
}

#[test]
fn test_optimize_summing_merge_tree() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE optimize_summing (
                id INT64,
                value Int64
            ) ENGINE = SummingMergeTree(value)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_summing VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO optimize_summing VALUES (1, 20)")
        .unwrap();
    executor
        .execute_sql("OPTIMIZE TABLE optimize_summing FINAL")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM optimize_summing")
        .unwrap();
    assert_table_eq!(result, [[1, 30]]);
}
