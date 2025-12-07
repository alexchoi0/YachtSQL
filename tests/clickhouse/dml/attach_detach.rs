use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_detach_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE detach_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO detach_table VALUES (1)")
        .unwrap();
    executor.execute_sql("DETACH TABLE detach_table").unwrap();
}

#[test]
fn test_attach_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE attach_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO attach_table VALUES (1)")
        .unwrap();
    executor.execute_sql("DETACH TABLE attach_table").unwrap();
    executor.execute_sql("ATTACH TABLE attach_table").unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM attach_table")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_detach_table_permanently() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE detach_perm (id INT64)")
        .unwrap();
    executor
        .execute_sql("DETACH TABLE detach_perm PERMANENTLY")
        .unwrap();
}

#[test]
fn test_detach_table_sync() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE detach_sync (id INT64)")
        .unwrap();
    executor
        .execute_sql("DETACH TABLE detach_sync SYNC")
        .unwrap();
}

#[test]
fn test_detach_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("DETACH TABLE IF EXISTS nonexistent_detach")
        .unwrap();
}

#[test]
fn test_attach_database_qualified() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE attach_db").unwrap();
    executor
        .execute_sql("CREATE TABLE attach_db.qualified (id INT64)")
        .unwrap();
    executor
        .execute_sql("DETACH TABLE attach_db.qualified")
        .unwrap();
    executor
        .execute_sql("ATTACH TABLE attach_db.qualified")
        .unwrap();
}

#[test]
fn test_detach_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE view_base (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW detach_view AS SELECT id FROM view_base")
        .unwrap();
    executor.execute_sql("DETACH VIEW detach_view").unwrap();
}

#[test]
fn test_attach_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE view_base2 (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW attach_view AS SELECT id FROM view_base2")
        .unwrap();
    executor.execute_sql("DETACH VIEW attach_view").unwrap();
    executor.execute_sql("ATTACH VIEW attach_view").unwrap();
}

#[test]
fn test_detach_dictionary() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dict_source (id UInt64, name String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE DICTIONARY detach_dict (
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
        .execute_sql("DETACH DICTIONARY detach_dict")
        .unwrap();
}

#[test]
fn test_attach_dictionary() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dict_source2 (id UInt64, name String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE DICTIONARY attach_dict (
                id UInt64,
                name String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source2'))
            LAYOUT(FLAT())
            LIFETIME(300)",
        )
        .unwrap();
    executor
        .execute_sql("DETACH DICTIONARY attach_dict")
        .unwrap();
    executor
        .execute_sql("ATTACH DICTIONARY attach_dict")
        .unwrap();
}

#[test]
fn test_detach_database() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DATABASE detach_database")
        .unwrap();
    executor
        .execute_sql("DETACH DATABASE detach_database")
        .unwrap();
}

#[test]
fn test_attach_database() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DATABASE attach_database")
        .unwrap();
    executor
        .execute_sql("DETACH DATABASE attach_database")
        .unwrap();
    executor
        .execute_sql("ATTACH DATABASE attach_database")
        .unwrap();
}

#[test]
fn test_detach_preserves_data() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE preserve_data (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO preserve_data VALUES (1, 'test'), (2, 'data')")
        .unwrap();
    executor.execute_sql("DETACH TABLE preserve_data").unwrap();
    executor.execute_sql("ATTACH TABLE preserve_data").unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM preserve_data")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_attach_from_path() {
    let mut executor = create_executor();
    executor
        .execute_sql("ATTACH TABLE path_table FROM '/path/to/data' (id INT64)")
        .unwrap();
}
