#![allow(unused_variables)]

use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_information_schema_tables() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE info_test_table (id INT64, name String)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_name = 'info_test_table'",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_information_schema_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE info_columns_test (id INT64, name String, value Float64)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns
            WHERE table_name = 'info_columns_test'
            ORDER BY ordinal_position",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_information_schema_schemata() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DATABASE info_schema_db")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = 'info_schema_db'",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_information_schema_views() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE info_view_base (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW info_test_view AS SELECT id FROM info_view_base")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT table_name, view_definition
            FROM information_schema.views
            WHERE table_name = 'info_test_view'",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_information_schema_table_constraints() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE info_constraints (
                id INT64,
                CONSTRAINT pk_info PRIMARY KEY (id)
            ) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'info_constraints'",
        )
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_information_schema_key_column_usage() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE info_keys (
                id INT64,
                ref_id INT64,
                PRIMARY KEY (id)
            ) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT column_name, constraint_name
            FROM information_schema.key_column_usage
            WHERE table_name = 'info_keys'",
        )
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_information_schema_column_privileges() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE info_priv_table (id INT64, data String)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT table_name, column_name, privilege_type
            FROM information_schema.column_privileges
            WHERE table_name = 'info_priv_table'",
        )
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_information_schema_table_privileges() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE info_tbl_priv (id INT64)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT table_name, privilege_type, grantee
            FROM information_schema.table_privileges
            WHERE table_name = 'info_tbl_priv'",
        )
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_information_schema_referential_constraints() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT constraint_name, unique_constraint_name
            FROM information_schema.referential_constraints",
        )
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_information_schema_check_constraints() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT constraint_name, check_clause
            FROM information_schema.check_constraints",
        )
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_tables() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, engine FROM system.tables LIMIT 10")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_system_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sys_col_test (id INT64, name String)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT name, type, position
            FROM system.columns
            WHERE table = 'sys_col_test'
            ORDER BY position",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_system_databases() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE sys_test_db").unwrap();

    let result = executor
        .execute_sql(
            "SELECT name, engine
            FROM system.databases
            WHERE name = 'sys_test_db'",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_system_functions() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, is_aggregate FROM system.functions LIMIT 10")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_system_data_type_families() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name FROM system.data_type_families LIMIT 10")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_system_settings() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, value, description FROM system.settings LIMIT 10")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_system_processes() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT query_id, query, elapsed FROM system.processes")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_query_log() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT query, type, query_duration_ms
            FROM system.query_log
            LIMIT 10",
        )
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_parts() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sys_parts_test (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sys_parts_test VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT table, partition, rows
            FROM system.parts
            WHERE table = 'sys_parts_test'",
        )
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_partitions() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE sys_partitions_test (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO sys_partitions_test VALUES (1, '2023-01-15'), (2, '2023-02-20')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT partition, table
            FROM system.parts
            WHERE table = 'sys_partitions_test'
            GROUP BY partition, table",
        )
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_merges() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT database, table, progress FROM system.merges")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_mutations() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT database, table, mutation_id, command FROM system.mutations")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_replicas() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT database, table, is_leader FROM system.replicas")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_dictionaries() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, type, status FROM system.dictionaries")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_users() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, storage FROM system.users")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_roles() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name FROM system.roles")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_grants() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT user_name, role_name, access_type FROM system.grants")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_quotas() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name FROM system.quotas")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_row_policies() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, short_name, database, table FROM system.row_policies")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_settings_profiles() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, storage FROM system.settings_profiles")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_metrics() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT metric, value, description FROM system.metrics LIMIT 10")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_system_events() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT event, value, description FROM system.events LIMIT 10")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_system_asynchronous_metrics() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT metric, value FROM system.asynchronous_metrics LIMIT 10")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_system_disks() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, path, total_space FROM system.disks")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_storage_policies() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT policy_name, volume_name FROM system.storage_policies")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_clusters() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT cluster, shard_num, replica_num FROM system.clusters")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_table_engines() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, supports_replication FROM system.table_engines")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_system_formats() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, is_input, is_output FROM system.formats")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_system_collations() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, language FROM system.collations")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_system_contributors() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name FROM system.contributors LIMIT 10")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_system_build_options() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT name, value FROM system.build_options LIMIT 10")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_system_time_zones() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT time_zone FROM system.time_zones LIMIT 10")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_describe_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE describe_test (id INT64, name String, value Float64)")
        .unwrap();

    let result = executor
        .execute_sql("DESCRIBE TABLE describe_test")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_show_create_table() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE show_create_test (id INT64, name String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();

    let result = executor
        .execute_sql("SHOW CREATE TABLE show_create_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_show_tables() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE show_tables_test1 (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE show_tables_test2 (id INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SHOW TABLES LIKE 'show_tables_%'")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_show_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE show_columns_test (id INT64, name String, active UInt8)")
        .unwrap();

    let result = executor
        .execute_sql("SHOW COLUMNS FROM show_columns_test")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_exists_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE exists_test (id INT64)")
        .unwrap();

    let result = executor.execute_sql("EXISTS TABLE exists_test").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_exists_database() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE exists_db").unwrap();

    let result = executor.execute_sql("EXISTS DATABASE exists_db").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
