use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_versioned_collapsing_merge_tree() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE versioned_collapse (
                user_id UInt64,
                page_views UInt32,
                duration UInt32,
                sign Int8,
                version UInt32
            ) ENGINE = VersionedCollapsingMergeTree(sign, version)
            ORDER BY user_id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_versioned_collapsing_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE vc_insert_test (
                id UInt64,
                value Int64,
                sign Int8,
                version UInt16
            ) ENGINE = VersionedCollapsingMergeTree(sign, version)
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO vc_insert_test VALUES
            (1, 100, 1, 1),
            (1, 100, -1, 1),
            (1, 200, 1, 2)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, sum(value * sign) FROM vc_insert_test GROUP BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 200]]);
}

#[test]
fn test_graphite_merge_tree() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE graphite_test (
                Path String,
                Time DateTime,
                Value Float64,
                Version UInt32
            ) ENGINE = GraphiteMergeTree('graphite_rollup')
            ORDER BY (Path, Time)",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_graphite_merge_tree_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE graphite_data (
                Path String,
                Time DateTime,
                Value Float64,
                Version UInt32
            ) ENGINE = GraphiteMergeTree('graphite_rollup')
            ORDER BY (Path, Time)",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO graphite_data VALUES
            ('test.metric1', '2023-01-01 00:00:00', 100.0, 1),
            ('test.metric1', '2023-01-01 00:01:00', 110.0, 1),
            ('test.metric2', '2023-01-01 00:00:00', 200.0, 1)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT Path, count() FROM graphite_data GROUP BY Path ORDER BY Path")
        .unwrap();
    assert_table_eq!(result, [["test.metric1", 2], ["test.metric2", 1]]);
}

#[test]
fn test_collapsing_merge_tree() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE collapsing_test (
                user_id UInt64,
                page_views UInt32,
                sign Int8
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY user_id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_collapsing_merge_tree_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE collapse_data (
                id UInt64,
                count UInt32,
                sign Int8
            ) ENGINE = CollapsingMergeTree(sign)
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO collapse_data VALUES
            (1, 5, 1),
            (1, 5, -1),
            (1, 10, 1)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, sum(count * sign) FROM collapse_data GROUP BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 10]]);
}

#[test]
fn test_summing_merge_tree() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE summing_test (
                key UInt64,
                value UInt64
            ) ENGINE = SummingMergeTree(value)
            ORDER BY key",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_summing_merge_tree_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE summing_data (
                date Date,
                key String,
                value UInt64
            ) ENGINE = SummingMergeTree(value)
            ORDER BY (date, key)",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO summing_data VALUES
            ('2023-01-01', 'A', 100),
            ('2023-01-01', 'A', 50),
            ('2023-01-01', 'B', 200)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT key, sum(value) FROM summing_data GROUP BY key ORDER BY key")
        .unwrap();
    assert_table_eq!(result, [["A", 150], ["B", 200]]);
}

#[test]
fn test_aggregating_merge_tree() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE agg_mt_test (
                key UInt64,
                value AggregateFunction(sum, UInt64)
            ) ENGINE = AggregatingMergeTree()
            ORDER BY key",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_replacing_merge_tree() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE replacing_test (
                key UInt64,
                value String,
                version UInt32
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY key",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Fix me!"]
#[test]
fn test_replacing_merge_tree_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE replacing_data (
                id UInt64,
                name String,
                ver UInt32
            ) ENGINE = ReplacingMergeTree(ver)
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO replacing_data VALUES
            (1, 'old', 1),
            (1, 'new', 2),
            (2, 'only', 1)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM replacing_data FINAL ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_merge_tree_with_ttl() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ttl_engine_test (
                id UInt64,
                timestamp DateTime,
                data String
            ) ENGINE = MergeTree()
            ORDER BY id
            TTL timestamp + INTERVAL 1 MONTH",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Fix me!"]
#[test]
fn test_merge_tree_with_partition() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE partition_engine (
                id UInt64,
                date Date,
                value Int64
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(date)
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_memory_engine() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE memory_test (
                id UInt64,
                value String
            ) ENGINE = Memory",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO memory_test VALUES (1, 'a'), (2, 'b')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM memory_test ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, "b"]]);
}

#[test]
fn test_log_engine() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE log_test (
                id UInt64,
                message String
            ) ENGINE = Log",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO log_test VALUES (1, 'hello'), (2, 'world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM log_test ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "hello"], [2, "world"]]);
}

#[test]
fn test_tiny_log_engine() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE tinylog_test (
                id UInt64,
                data String
            ) ENGINE = TinyLog",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO tinylog_test VALUES (1, 'test')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM tinylog_test").unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[test]
fn test_stripe_log_engine() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE stripelog_test (
                id UInt64,
                data String
            ) ENGINE = StripeLog",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO stripelog_test VALUES (1, 'stripe1'), (2, 'stripe2')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM stripelog_test ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "stripe1"], [2, "stripe2"]]);
}

#[ignore = "Fix me!"]
#[test]
fn test_merge_engine() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE base1 (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY id")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE base2 (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY id")
        .unwrap();

    executor
        .execute_sql("INSERT INTO base1 VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO base2 VALUES (2, 200)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE merged ENGINE = Merge(currentDatabase(), 'base[0-9]+')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM merged ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[ignore = "Fix me!"]
#[test]
fn test_distributed_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE distributed_test (
                id UInt64,
                value String
            ) ENGINE = Distributed('cluster', 'database', 'table', rand())",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Fix me!"]
#[test]
fn test_buffer_engine() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE buffer_dest (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY id",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "CREATE TABLE buffer_src (id UInt64, value Int64)
            ENGINE = Buffer(currentDatabase(), 'buffer_dest', 16, 10, 100, 10000, 1000000, 10000000, 100000000)"
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Fix me!"]
#[test]
fn test_null_engine() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE null_test (id UInt64, value String) ENGINE = Null")
        .unwrap();

    executor
        .execute_sql("INSERT INTO null_test VALUES (1, 'ignored'), (2, 'also ignored')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT count() FROM null_test")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[ignore = "Fix me!"]
#[test]
fn test_set_engine() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE set_test (id UInt64) ENGINE = Set")
        .unwrap();

    executor
        .execute_sql("INSERT INTO set_test VALUES (1), (2), (3)")
        .unwrap();

    let result = executor.execute_sql("SELECT 2 IN set_test").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_join_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE join_test (id UInt64, name String)
            ENGINE = Join(ANY, LEFT, id)",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_url_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE url_test (
                id UInt64,
                name String
            ) ENGINE = URL('http://example.com/data.csv', CSV)",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_file_engine() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE file_test (
                id UInt64,
                value String
            ) ENGINE = File(CSV)",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Fix me!"]
#[test]
fn test_generate_random_engine() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE random_test (
                id UInt64,
                value Float64
            ) ENGINE = GenerateRandom(1, 5, 3)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM random_test LIMIT 5")
        .unwrap();
    assert!(result.num_rows() == 5); // TODO: use table![[expected_values]]
}
