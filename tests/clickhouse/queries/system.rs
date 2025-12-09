use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_system_reload_dictionary() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dict_source (id UInt64, name String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE DICTIONARY reload_dict (
                id UInt64,
                name String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source'))
            LAYOUT(FLAT())
            LIFETIME(300)",
        )
        .unwrap();
    executor
        .execute_sql("SYSTEM RELOAD DICTIONARY reload_dict")
        .unwrap();
}

#[test]
fn test_system_reload_dictionaries() {
    let mut executor = create_executor();
    executor.execute_sql("SYSTEM RELOAD DICTIONARIES").unwrap();
}

#[test]
fn test_system_drop_dns_cache() {
    let mut executor = create_executor();
    executor.execute_sql("SYSTEM DROP DNS CACHE").unwrap();
}

#[test]
fn test_system_drop_mark_cache() {
    let mut executor = create_executor();
    executor.execute_sql("SYSTEM DROP MARK CACHE").unwrap();
}

#[test]
fn test_system_drop_uncompressed_cache() {
    let mut executor = create_executor();
    executor
        .execute_sql("SYSTEM DROP UNCOMPRESSED CACHE")
        .unwrap();
}

#[test]
fn test_system_drop_compiled_expression_cache() {
    let mut executor = create_executor();
    executor
        .execute_sql("SYSTEM DROP COMPILED EXPRESSION CACHE")
        .unwrap();
}

#[test]
fn test_system_flush_logs() {
    let mut executor = create_executor();
    executor.execute_sql("SYSTEM FLUSH LOGS").unwrap();
}

#[test]
fn test_system_stop_merges() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stop_merges (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP MERGES stop_merges")
        .unwrap();
}

#[test]
fn test_system_start_merges() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE start_merges (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP MERGES start_merges")
        .unwrap();
    executor
        .execute_sql("SYSTEM START MERGES start_merges")
        .unwrap();
}

#[test]
fn test_system_stop_ttl_merges() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stop_ttl (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP TTL MERGES stop_ttl")
        .unwrap();
}

#[test]
fn test_system_start_ttl_merges() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE start_ttl (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP TTL MERGES start_ttl")
        .unwrap();
    executor
        .execute_sql("SYSTEM START TTL MERGES start_ttl")
        .unwrap();
}

#[test]
fn test_system_stop_moves() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stop_moves (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP MOVES stop_moves")
        .unwrap();
}

#[test]
fn test_system_start_moves() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE start_moves (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP MOVES start_moves")
        .unwrap();
    executor
        .execute_sql("SYSTEM START MOVES start_moves")
        .unwrap();
}

#[test]
fn test_system_stop_fetches() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stop_fetches (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP FETCHES stop_fetches")
        .unwrap();
}

#[test]
fn test_system_start_fetches() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE start_fetches (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP FETCHES start_fetches")
        .unwrap();
    executor
        .execute_sql("SYSTEM START FETCHES start_fetches")
        .unwrap();
}

#[test]
fn test_system_stop_sends() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stop_sends (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP SENDS stop_sends")
        .unwrap();
}

#[test]
fn test_system_start_sends() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE start_sends (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP SENDS start_sends")
        .unwrap();
    executor
        .execute_sql("SYSTEM START SENDS start_sends")
        .unwrap();
}

#[test]
fn test_system_stop_replication_queues() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stop_repl (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP REPLICATION QUEUES stop_repl")
        .unwrap();
}

#[test]
fn test_system_start_replication_queues() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE start_repl (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM STOP REPLICATION QUEUES start_repl")
        .unwrap();
    executor
        .execute_sql("SYSTEM START REPLICATION QUEUES start_repl")
        .unwrap();
}

#[test]
fn test_system_sync_replica() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sync_repl (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("SYSTEM SYNC REPLICA sync_repl")
        .unwrap();
}

#[test]
fn test_system_drop_replica() {
    let mut executor = create_executor();
    executor
        .execute_sql("SYSTEM DROP REPLICA 'replica_name' FROM TABLE system.some_table")
        .unwrap();
}

#[test]
fn test_system_reload_config() {
    let mut executor = create_executor();
    executor.execute_sql("SYSTEM RELOAD CONFIG").unwrap();
}

#[test]
fn test_system_shutdown() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SYSTEM SHUTDOWN").unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_system_kill() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SYSTEM KILL").unwrap();
    assert_table_eq!(result, []);
}
