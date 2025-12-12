use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_merge_tree_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mt_basic (id INT64, name String) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mt_basic VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name FROM mt_basic ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "alice"], [2, "bob"]]);
}

#[test]
fn test_merge_tree_partition_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mt_partition (
                id INT64,
                dt Date,
                value INT64
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO mt_partition VALUES (1, '2023-01-15', 100), (2, '2023-02-20', 200)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM mt_partition ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[test]
fn test_merge_tree_primary_key() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mt_primary (
                id INT64,
                category String,
                value INT64
            ) ENGINE = MergeTree
            PRIMARY KEY (category, id)
            ORDER BY (category, id)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mt_primary VALUES (1, 'A', 100), (2, 'B', 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, category FROM mt_primary ORDER BY category, id")
        .unwrap();
    assert_table_eq!(result, [[1, "A"], [2, "B"]]);
}

#[test]
fn test_merge_tree_sample_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mt_sample (
                id INT64,
                value INT64
            ) ENGINE = MergeTree
            ORDER BY id
            SAMPLE BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mt_sample VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM mt_sample SAMPLE 0.5")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_merge_tree_ttl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mt_ttl (
                id INT64,
                created DateTime,
                data String
            ) ENGINE = MergeTree
            ORDER BY id
            TTL created + INTERVAL 1 MONTH",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mt_ttl VALUES (1, now(), 'test')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM mt_ttl").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_merge_tree_settings() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mt_settings (
                id INT64,
                value INT64
            ) ENGINE = MergeTree
            ORDER BY id
            SETTINGS index_granularity = 8192",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mt_settings VALUES (1, 100)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM mt_settings").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_merge_tree_secondary_index() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mt_index (
                id INT64,
                name String,
                INDEX idx_name name TYPE bloom_filter GRANULARITY 1
            ) ENGINE = MergeTree
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mt_index VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM mt_index WHERE name = 'alice'")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_merge_tree_minmax_index() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mt_minmax (
                id INT64,
                value INT64,
                INDEX idx_value value TYPE minmax GRANULARITY 1
            ) ENGINE = MergeTree
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mt_minmax VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM mt_minmax WHERE value > 15 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_merge_tree_set_index() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mt_set (
                id INT64,
                status String,
                INDEX idx_status status TYPE set(100) GRANULARITY 1
            ) ENGINE = MergeTree
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mt_set VALUES (1, 'active'), (2, 'pending'), (3, 'active')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM mt_set WHERE status = 'active' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_merge_tree_compound_order() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mt_compound (
                region String,
                category String,
                id INT64,
                value INT64
            ) ENGINE = MergeTree
            ORDER BY (region, category, id)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mt_compound VALUES ('US', 'A', 1, 100), ('EU', 'B', 2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM mt_compound ORDER BY region, category, id")
        .unwrap();
    assert_table_eq!(result, [[2], [1]]);
}

#[test]
fn test_merge_tree_projection() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mt_projection (
                id INT64,
                category String,
                value INT64,
                PROJECTION proj_category (
                    SELECT category, SUM(value)
                    GROUP BY category
                )
            ) ENGINE = MergeTree
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mt_projection VALUES (1, 'A', 100), (2, 'A', 200), (3, 'B', 300)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, SUM(value) FROM mt_projection GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 300], ["B", 300]]);
}

#[test]
fn test_merge_tree_column_ttl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mt_col_ttl (
                id INT64,
                created DateTime,
                temporary String TTL created + INTERVAL 1 DAY
            ) ENGINE = MergeTree
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mt_col_ttl VALUES (1, now(), 'temp_data')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM mt_col_ttl").unwrap();
    assert_table_eq!(result, [[1]]);
}
