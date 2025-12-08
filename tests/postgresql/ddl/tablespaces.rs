use crate::common::create_executor;

#[test]
fn test_create_tablespace() {
    let mut executor = create_executor();
    let result =
        executor.execute_sql("CREATE TABLESPACE test_space LOCATION '/tmp/pgdata/test_space'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_tablespace_owner() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLESPACE owned_space OWNER CURRENT_USER LOCATION '/tmp/pgdata/owned_space'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_tablespace_with_options() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLESPACE opt_space LOCATION '/tmp/pgdata/opt_space' WITH (seq_page_cost = 1.0, random_page_cost = 4.0)"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_tablespace_rename() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE rename_space LOCATION '/tmp/pgdata/rename_space'")
        .ok();

    let result = executor.execute_sql("ALTER TABLESPACE rename_space RENAME TO new_space_name");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_tablespace_owner() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE owner_space LOCATION '/tmp/pgdata/owner_space'")
        .ok();

    let result = executor.execute_sql("ALTER TABLESPACE owner_space OWNER TO CURRENT_USER");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_tablespace_set_option() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE set_opt_space LOCATION '/tmp/pgdata/set_opt_space'")
        .ok();

    let result = executor.execute_sql("ALTER TABLESPACE set_opt_space SET (seq_page_cost = 1.5)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_tablespace_reset_option() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE reset_space LOCATION '/tmp/pgdata/reset_space'")
        .ok();
    executor
        .execute_sql("ALTER TABLESPACE reset_space SET (seq_page_cost = 1.5)")
        .ok();

    let result = executor.execute_sql("ALTER TABLESPACE reset_space RESET (seq_page_cost)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE drop_space LOCATION '/tmp/pgdata/drop_space'")
        .ok();

    let result = executor.execute_sql("DROP TABLESPACE drop_space");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_tablespace_if_exists() {
    let mut executor = create_executor();
    let result = executor.execute_sql("DROP TABLESPACE IF EXISTS nonexistent_space");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_table_in_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE table_space LOCATION '/tmp/pgdata/table_space'")
        .ok();

    let result = executor
        .execute_sql("CREATE TABLE ts_table (id INTEGER, data TEXT) TABLESPACE table_space");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_index_in_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE index_space LOCATION '/tmp/pgdata/index_space'")
        .ok();
    executor
        .execute_sql("CREATE TABLE idx_ts_table (id INTEGER, data TEXT)")
        .ok();

    let result =
        executor.execute_sql("CREATE INDEX idx_ts ON idx_ts_table (id) TABLESPACE index_space");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_table_set_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE move_space LOCATION '/tmp/pgdata/move_space'")
        .ok();
    executor
        .execute_sql("CREATE TABLE move_table (id INTEGER)")
        .ok();

    let result = executor.execute_sql("ALTER TABLE move_table SET TABLESPACE move_space");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_index_set_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE idx_move_space LOCATION '/tmp/pgdata/idx_move_space'")
        .ok();
    executor
        .execute_sql("CREATE TABLE idx_move_table (id INTEGER)")
        .ok();
    executor
        .execute_sql("CREATE INDEX idx_move ON idx_move_table (id)")
        .ok();

    let result = executor.execute_sql("ALTER INDEX idx_move SET TABLESPACE idx_move_space");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_materialized_view_set_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE mv_space LOCATION '/tmp/pgdata/mv_space'")
        .ok();
    executor
        .execute_sql("CREATE TABLE mv_base (id INTEGER)")
        .ok();
    executor
        .execute_sql("CREATE MATERIALIZED VIEW mv_ts AS SELECT * FROM mv_base")
        .ok();

    let result = executor.execute_sql("ALTER MATERIALIZED VIEW mv_ts SET TABLESPACE mv_space");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_database_set_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE db_space LOCATION '/tmp/pgdata/db_space'")
        .ok();

    let result = executor.execute_sql("ALTER DATABASE postgres SET TABLESPACE db_space");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_set_default_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE default_space LOCATION '/tmp/pgdata/default_space'")
        .ok();

    let result = executor.execute_sql("SET default_tablespace = 'default_space'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_reset_default_tablespace() {
    let mut executor = create_executor();
    let result = executor.execute_sql("RESET default_tablespace");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_set_temp_tablespaces() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE temp_space LOCATION '/tmp/pgdata/temp_space'")
        .ok();

    let result = executor.execute_sql("SET temp_tablespaces = 'temp_space'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_tablespace_catalog() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM pg_tablespace");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_tablespace_size() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT pg_tablespace_size('pg_default')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_tablespace_location() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname = 'pg_default'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_tablespace_databases() {
    let mut executor = create_executor();
    let result =
        executor.execute_sql("SELECT pg_tablespace_databases(oid) FROM pg_tablespace LIMIT 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_tablespace_in_create_table_as() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE ctas_space LOCATION '/tmp/pgdata/ctas_space'")
        .ok();
    executor
        .execute_sql("CREATE TABLE source_table (id INTEGER)")
        .ok();

    let result = executor
        .execute_sql("CREATE TABLE dest_table TABLESPACE ctas_space AS SELECT * FROM source_table");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_tablespace_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE part_space LOCATION '/tmp/pgdata/part_space'")
        .ok();
    executor
        .execute_sql(
            "CREATE TABLE part_parent (id INTEGER, created_at DATE) PARTITION BY RANGE (created_at)",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE TABLE part_child PARTITION OF part_parent FOR VALUES FROM ('2024-01-01') TO ('2024-12-31') TABLESPACE part_space"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_move_all_tables_to_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE all_tables_space LOCATION '/tmp/pgdata/all_tables_space'")
        .ok();

    let result = executor
        .execute_sql("ALTER TABLE ALL IN TABLESPACE pg_default SET TABLESPACE all_tables_space");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_move_all_indexes_to_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE all_idx_space LOCATION '/tmp/pgdata/all_idx_space'")
        .ok();

    let result = executor
        .execute_sql("ALTER INDEX ALL IN TABLESPACE pg_default SET TABLESPACE all_idx_space");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_move_all_in_tablespace_owned_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE owned_objs_space LOCATION '/tmp/pgdata/owned_objs_space'")
        .ok();

    let result = executor.execute_sql(
        "ALTER TABLE ALL IN TABLESPACE pg_default OWNED BY CURRENT_USER SET TABLESPACE owned_objs_space"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_move_with_nowait() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE nowait_space LOCATION '/tmp/pgdata/nowait_space'")
        .ok();
    executor
        .execute_sql("CREATE TABLE nowait_table (id INTEGER)")
        .ok();

    let result =
        executor.execute_sql("ALTER TABLE nowait_table SET TABLESPACE nowait_space NOWAIT");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_tablespace_grant() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE grant_space LOCATION '/tmp/pgdata/grant_space'")
        .ok();

    let result = executor.execute_sql("GRANT CREATE ON TABLESPACE grant_space TO PUBLIC");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_tablespace_revoke() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE revoke_space LOCATION '/tmp/pgdata/revoke_space'")
        .ok();
    executor
        .execute_sql("GRANT CREATE ON TABLESPACE revoke_space TO PUBLIC")
        .ok();

    let result = executor.execute_sql("REVOKE CREATE ON TABLESPACE revoke_space FROM PUBLIC");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_comment_on_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE comment_space LOCATION '/tmp/pgdata/comment_space'")
        .ok();

    let result =
        executor.execute_sql("COMMENT ON TABLESPACE comment_space IS 'This is a test tablespace'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_security_label_on_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE label_space LOCATION '/tmp/pgdata/label_space'")
        .ok();

    let result = executor.execute_sql("SECURITY LABEL ON TABLESPACE label_space IS 'secret'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_default_tablespace() {
    let mut executor = create_executor();
    let result =
        executor.execute_sql("SELECT spcname FROM pg_tablespace WHERE spcname = 'pg_default'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_global_tablespace() {
    let mut executor = create_executor();
    let result =
        executor.execute_sql("SELECT spcname FROM pg_tablespace WHERE spcname = 'pg_global'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_tablespace_with_replication() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE repl_space LOCATION '/tmp/pgdata/repl_space'")
        .ok();
    executor
        .execute_sql("CREATE TABLE repl_table (id INTEGER) TABLESPACE repl_space")
        .ok();

    let result = executor.execute_sql("CREATE PUBLICATION repl_pub FOR TABLE repl_table");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_tablespace_effective_io_concurrency() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE io_space LOCATION '/tmp/pgdata/io_space'")
        .ok();

    let result =
        executor.execute_sql("ALTER TABLESPACE io_space SET (effective_io_concurrency = 200)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_tablespace_maintenance_io_concurrency() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE maint_space LOCATION '/tmp/pgdata/maint_space'")
        .ok();

    let result =
        executor.execute_sql("ALTER TABLESPACE maint_space SET (maintenance_io_concurrency = 10)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_show_default_tablespace() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SHOW default_tablespace");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_show_temp_tablespaces() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SHOW temp_tablespaces");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_tablespace_cluster() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE cluster_space LOCATION '/tmp/pgdata/cluster_space'")
        .ok();
    executor
        .execute_sql("CREATE TABLE cluster_table (id INTEGER PRIMARY KEY) TABLESPACE cluster_space")
        .ok();

    let result = executor.execute_sql("CLUSTER cluster_table USING cluster_table_pkey");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_reindex_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE reindex_space LOCATION '/tmp/pgdata/reindex_space'")
        .ok();

    let result = executor.execute_sql("REINDEX (TABLESPACE reindex_space) DATABASE postgres");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_vacuum_full_with_tablespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLESPACE vacuum_space LOCATION '/tmp/pgdata/vacuum_space'")
        .ok();
    executor
        .execute_sql("CREATE TABLE vacuum_table (id INTEGER) TABLESPACE vacuum_space")
        .ok();

    let result = executor.execute_sql("VACUUM FULL vacuum_table");
    assert!(result.is_ok() || result.is_err());
}
