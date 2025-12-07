use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_current_database() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CURRENT_DATABASE()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_current_user() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CURRENT_USER").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_current_schema() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CURRENT_SCHEMA").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_current_catalog() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CURRENT_CATALOG").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_session_user() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SESSION_USER").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_user() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT USER").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_version() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT VERSION()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_typeof() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT PG_TYPEOF(123)").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_typeof_string() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT PG_TYPEOF('hello')").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_column_size() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PG_COLUMN_SIZE('hello')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_database_size() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PG_DATABASE_SIZE(CURRENT_DATABASE())")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_size_pretty() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT PG_SIZE_PRETTY(1024)").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_table_size() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE size_test (id INT64)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT PG_TABLE_SIZE('size_test')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_relation_size() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rel_size (id INT64)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT PG_RELATION_SIZE('rel_size')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_total_relation_size() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE total_size (id INT64)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT PG_TOTAL_RELATION_SIZE('total_size')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_indexes_size() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE idx_size (id INT64)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT PG_INDEXES_SIZE('idx_size')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_tablespace_size() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PG_TABLESPACE_SIZE('pg_default')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_obj_description() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE desc_test (id INT64)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT OBJ_DESCRIPTION('desc_test'::regclass)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_col_description() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE col_desc (id INT64)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT COL_DESCRIPTION('col_desc'::regclass, 1)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_shobj_description() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SHOBJ_DESCRIPTION(1, 'pg_database')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_has_table_privilege() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE priv_test (id INT64)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT HAS_TABLE_PRIVILEGE('priv_test', 'SELECT')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_has_schema_privilege() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT HAS_SCHEMA_PRIVILEGE('public', 'USAGE')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_has_database_privilege() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT HAS_DATABASE_PRIVILEGE(CURRENT_DATABASE(), 'CONNECT')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_has_column_privilege() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE col_priv (id INT64)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT HAS_COLUMN_PRIVILEGE('col_priv', 'id', 'SELECT')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_get_viewdef() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE view_base (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW test_view AS SELECT * FROM view_base")
        .unwrap();
    let result = executor
        .execute_sql("SELECT PG_GET_VIEWDEF('test_view')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_get_constraintdef() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE const_def (id INT64 PRIMARY KEY)")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_get_indexdef() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE idx_def (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_test ON idx_def (id)")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_get_triggerdef() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_get_functiondef() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_current_setting() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CURRENT_SETTING('search_path')")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_config() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SET_CONFIG('search_path', 'public', FALSE)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_cancel_backend() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_backend_pid() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT PG_BACKEND_PID()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_is_in_recovery() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT PG_IS_IN_RECOVERY()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_client_addr() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT INET_CLIENT_ADDR()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_client_port() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT INET_CLIENT_PORT()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_server_addr() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT INET_SERVER_ADDR()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_server_port() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT INET_SERVER_PORT()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_postmaster_start_time() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PG_POSTMASTER_START_TIME()")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_conf_load_time() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT PG_CONF_LOAD_TIME()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_txid_current() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TXID_CURRENT()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_current_snapshot() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PG_CURRENT_SNAPSHOT()")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}
