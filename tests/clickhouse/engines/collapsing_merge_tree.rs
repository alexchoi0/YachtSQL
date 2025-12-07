use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_collapsing_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE cmt_basic (
                id INT64,
                sign Int8,
                value INT64
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_basic VALUES (1, 1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_basic VALUES (1, -1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_basic VALUES (1, 1, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM cmt_basic FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 200]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_collapsing_delete() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE cmt_delete (
                id INT64,
                sign Int8,
                name String
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_delete VALUES (1, 1, 'alice')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_delete VALUES (1, -1, 'alice')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM cmt_delete FINAL")
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_collapsing_update() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE cmt_update (
                id INT64,
                sign Int8,
                status String,
                value INT64
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_update VALUES (1, 1, 'pending', 100)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO cmt_update VALUES (1, -1, 'pending', 100), (1, 1, 'complete', 150)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, status, value FROM cmt_update FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, "complete", 150]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_collapsing_multiple_rows() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE cmt_multi (
                id INT64,
                sign Int8,
                value INT64
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_multi VALUES (1, 1, 100), (2, 1, 200), (3, 1, 300)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_multi VALUES (2, -1, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM cmt_multi FINAL ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [3, 300]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_collapsing_with_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE cmt_partition (
                dt Date,
                id INT64,
                sign Int8,
                value INT64
            ) ENGINE = CollapsingMergeTree(sign)
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_partition VALUES ('2023-01-15', 1, 1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_partition VALUES ('2023-01-20', 1, -1, 100), ('2023-01-20', 1, 1, 150)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM cmt_partition FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 150]]);
}

#[test]
fn test_collapsing_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE cmt_agg (
                category String,
                id INT64,
                sign Int8,
                value INT64
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY (category, id)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO cmt_agg VALUES ('A', 1, 1, 100), ('A', 2, 1, 200), ('B', 1, 1, 300)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, SUM(value * sign) FROM cmt_agg GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 300], ["B", 300]]);
}

#[test]
fn test_collapsing_count() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE cmt_count (
                id INT64,
                sign Int8
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_count VALUES (1, 1), (2, 1), (3, 1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_count VALUES (2, -1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(sign) FROM cmt_count")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_versioned_collapsing() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE vcmt_basic (
                id INT64,
                sign Int8,
                version UInt64,
                value INT64
            ) ENGINE = VersionedCollapsingMergeTree(sign, version)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO vcmt_basic VALUES (1, 1, 1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vcmt_basic VALUES (1, -1, 1, 100), (1, 1, 2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, version, value FROM vcmt_basic FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 2, 200]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_versioned_collapsing_out_of_order() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE vcmt_order (
                id INT64,
                sign Int8,
                version UInt64,
                data String
            ) ENGINE = VersionedCollapsingMergeTree(sign, version)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO vcmt_order VALUES (1, 1, 3, 'v3')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vcmt_order VALUES (1, 1, 1, 'v1')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vcmt_order VALUES (1, -1, 1, 'v1')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, version, data FROM vcmt_order FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 3, "v3"]]);
}

#[test]
fn test_collapsing_without_final() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE cmt_no_final (
                id INT64,
                sign Int8,
                value INT64
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_no_final VALUES (1, 1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmt_no_final VALUES (1, -1, 100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, sign, value FROM cmt_no_final ORDER BY sign")
        .unwrap();
    assert_table_eq!(result, [[1, -1, 100], [1, 1, 100]]);
}
