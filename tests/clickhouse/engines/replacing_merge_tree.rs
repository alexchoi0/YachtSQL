use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_replacing_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE rmt_basic (
                id INT64,
                value String
            ) ENGINE = ReplacingMergeTree
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_basic VALUES (1, 'first')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_basic VALUES (1, 'second')")
        .unwrap();

    executor
        .execute_sql("OPTIMIZE TABLE rmt_basic FINAL")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM rmt_basic")
        .unwrap();
    assert_table_eq!(result, [[1, "second"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_replacing_with_version() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE rmt_version (
                id INT64,
                version UInt64,
                value String
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_version VALUES (1, 1, 'v1')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_version VALUES (1, 3, 'v3')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_version VALUES (1, 2, 'v2')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, version, value FROM rmt_version FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 3, "v3"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_replacing_multiple_keys() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE rmt_multi (
                region String,
                id INT64,
                value INT64
            ) ENGINE = ReplacingMergeTree
            ORDER BY (region, id)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_multi VALUES ('US', 1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_multi VALUES ('US', 1, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_multi VALUES ('EU', 1, 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT region, id, value FROM rmt_multi FINAL ORDER BY region")
        .unwrap();
    assert_table_eq!(result, [["EU", 1, 300], ["US", 1, 200]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_replacing_with_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE rmt_partition (
                dt Date,
                id INT64,
                value String
            ) ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_partition VALUES ('2023-01-15', 1, 'jan_v1')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_partition VALUES ('2023-01-20', 1, 'jan_v2')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_partition VALUES ('2023-02-15', 1, 'feb_v1')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM rmt_partition FINAL ORDER BY dt")
        .unwrap();
    assert_table_eq!(result, [[1, "jan_v2"], [1, "feb_v1"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_replacing_final_query() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE rmt_final (
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
                "INSERT INTO rmt_final VALUES (1, {}, 'data{}')",
                v, v
            ))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT id, version, data FROM rmt_final FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 5, "data5"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_replacing_is_deleted() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE rmt_deleted (
                id INT64,
                is_deleted UInt8,
                value String
            ) ENGINE = ReplacingMergeTree(is_deleted)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_deleted VALUES (1, 0, 'active')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_deleted VALUES (1, 1, 'deleted')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM rmt_deleted FINAL WHERE is_deleted = 0")
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_replacing_without_final() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE rmt_no_final (
                id INT64,
                value INT64
            ) ENGINE = ReplacingMergeTree
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_no_final VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_no_final VALUES (1, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM rmt_no_final ORDER BY value")
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_replacing_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE rmt_agg (
                category String,
                id INT64,
                value INT64
            ) ENGINE = ReplacingMergeTree
            ORDER BY (category, id)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_agg VALUES ('A', 1, 10), ('A', 2, 20), ('B', 1, 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, SUM(value) FROM rmt_agg FINAL GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_replacing_with_ttl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE rmt_ttl (
                id INT64,
                created DateTime,
                value String
            ) ENGINE = ReplacingMergeTree
            ORDER BY id
            TTL created + INTERVAL 1 DAY",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO rmt_ttl VALUES (1, now(), 'test')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM rmt_ttl").unwrap();
    assert_table_eq!(result, [[1]]);
}
