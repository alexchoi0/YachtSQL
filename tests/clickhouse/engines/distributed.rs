use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_distributed_create() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE local_table (id INT64, name String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE dist_table AS local_table
            ENGINE = Distributed(cluster, default, local_table)",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_distributed_with_sharding_key() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE local_sharded (id INT64, value INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE dist_sharded AS local_sharded
            ENGINE = Distributed(cluster, default, local_sharded, id)",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_distributed_with_rand_sharding() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE local_rand (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE dist_rand AS local_rand
            ENGINE = Distributed(cluster, default, local_rand, rand())",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_distributed_select() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE local_select (id INT64, value INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO local_select VALUES (1, 100), (2, 200)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE dist_select AS local_select
            ENGINE = Distributed(cluster, default, local_select)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM dist_select ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_distributed_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE local_insert (id INT64, data String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE dist_insert AS local_insert
            ENGINE = Distributed(cluster, default, local_insert, id)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO dist_insert VALUES (1, 'test'), (2, 'data')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM local_insert")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_distributed_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE local_agg (category String, value INT64) ENGINE = MergeTree ORDER BY category")
        .unwrap();
    executor
        .execute_sql("INSERT INTO local_agg VALUES ('A', 10), ('A', 20), ('B', 30)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE dist_agg AS local_agg
            ENGINE = Distributed(cluster, default, local_agg)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, SUM(value) FROM dist_agg GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_distributed_global_in() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE local_main (id INT64, value INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("CREATE TABLE local_lookup (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO local_main VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO local_lookup VALUES (1), (3)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE dist_main AS local_main
            ENGINE = Distributed(cluster, default, local_main)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM dist_main WHERE id GLOBAL IN (SELECT id FROM local_lookup) ORDER BY value")
        .unwrap();
    assert_table_eq!(result, [[100], [300]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_distributed_settings() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE local_settings (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO local_settings VALUES (1), (2)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE dist_settings AS local_settings
            ENGINE = Distributed(cluster, default, local_settings)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM dist_settings SETTINGS distributed_product_mode = 'local' ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_distributed_join() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE local_left (id INT64, name String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE local_right (id INT64, value INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO local_left VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO local_right VALUES (1, 100), (2, 200)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE dist_left AS local_left
            ENGINE = Distributed(cluster, default, local_left)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dl.name, r.value FROM dist_left dl JOIN local_right r ON dl.id = r.id ORDER BY r.value")
        .unwrap();
    assert_table_eq!(result, [["alice", 100], ["bob", 200]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_distributed_count() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE local_count (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO local_count VALUES (1), (2), (3), (4), (5)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE dist_count AS local_count
            ENGINE = Distributed(cluster, default, local_count)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM dist_count")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}
