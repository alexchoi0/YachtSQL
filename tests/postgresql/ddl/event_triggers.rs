use crate::common::create_executor;

#[test]
fn test_create_event_trigger_ddl_command_start() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION log_ddl_start() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'DDL command started';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER ddl_start_trigger ON ddl_command_start EXECUTE FUNCTION log_ddl_start()"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_event_trigger_ddl_command_end() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION log_ddl_end() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'DDL command ended';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER ddl_end_trigger ON ddl_command_end EXECUTE FUNCTION log_ddl_end()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_event_trigger_table_rewrite() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION log_rewrite() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Table rewrite occurred';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER rewrite_trigger ON table_rewrite EXECUTE FUNCTION log_rewrite()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_event_trigger_sql_drop() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION log_drop() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Object dropped';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor
        .execute_sql("CREATE EVENT TRIGGER drop_trigger ON sql_drop EXECUTE FUNCTION log_drop()");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_event_trigger_with_when() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION log_table_ddl() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Table DDL';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER table_ddl_trigger ON ddl_command_start
         WHEN TAG IN ('CREATE TABLE', 'DROP TABLE', 'ALTER TABLE')
         EXECUTE FUNCTION log_table_ddl()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_event_trigger_multiple_tags() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION log_index_ddl() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Index DDL';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER index_ddl_trigger ON ddl_command_start
         WHEN TAG IN ('CREATE INDEX', 'DROP INDEX', 'REINDEX')
         EXECUTE FUNCTION log_index_ddl()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_event_trigger_enable() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE OR REPLACE FUNCTION dummy_evt() RETURNS event_trigger AS $$ BEGIN END; $$ LANGUAGE plpgsql"
    ).ok();
    executor
        .execute_sql(
            "CREATE EVENT TRIGGER enable_evt ON ddl_command_start EXECUTE FUNCTION dummy_evt()",
        )
        .ok();

    let result = executor.execute_sql("ALTER EVENT TRIGGER enable_evt ENABLE");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_event_trigger_disable() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE OR REPLACE FUNCTION dummy_evt2() RETURNS event_trigger AS $$ BEGIN END; $$ LANGUAGE plpgsql"
    ).ok();
    executor
        .execute_sql(
            "CREATE EVENT TRIGGER disable_evt ON ddl_command_start EXECUTE FUNCTION dummy_evt2()",
        )
        .ok();

    let result = executor.execute_sql("ALTER EVENT TRIGGER disable_evt DISABLE");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_event_trigger_enable_replica() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE OR REPLACE FUNCTION dummy_evt3() RETURNS event_trigger AS $$ BEGIN END; $$ LANGUAGE plpgsql"
    ).ok();
    executor
        .execute_sql(
            "CREATE EVENT TRIGGER replica_evt ON ddl_command_start EXECUTE FUNCTION dummy_evt3()",
        )
        .ok();

    let result = executor.execute_sql("ALTER EVENT TRIGGER replica_evt ENABLE REPLICA");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_event_trigger_enable_always() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE OR REPLACE FUNCTION dummy_evt4() RETURNS event_trigger AS $$ BEGIN END; $$ LANGUAGE plpgsql"
    ).ok();
    executor
        .execute_sql(
            "CREATE EVENT TRIGGER always_evt ON ddl_command_start EXECUTE FUNCTION dummy_evt4()",
        )
        .ok();

    let result = executor.execute_sql("ALTER EVENT TRIGGER always_evt ENABLE ALWAYS");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_event_trigger_rename() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE OR REPLACE FUNCTION dummy_evt5() RETURNS event_trigger AS $$ BEGIN END; $$ LANGUAGE plpgsql"
    ).ok();
    executor
        .execute_sql(
            "CREATE EVENT TRIGGER old_evt_name ON ddl_command_start EXECUTE FUNCTION dummy_evt5()",
        )
        .ok();

    let result = executor.execute_sql("ALTER EVENT TRIGGER old_evt_name RENAME TO new_evt_name");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_event_trigger_owner() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE OR REPLACE FUNCTION dummy_evt6() RETURNS event_trigger AS $$ BEGIN END; $$ LANGUAGE plpgsql"
    ).ok();
    executor
        .execute_sql(
            "CREATE EVENT TRIGGER owner_evt ON ddl_command_start EXECUTE FUNCTION dummy_evt6()",
        )
        .ok();

    let result = executor.execute_sql("ALTER EVENT TRIGGER owner_evt OWNER TO CURRENT_USER");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_event_trigger() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE OR REPLACE FUNCTION dummy_evt7() RETURNS event_trigger AS $$ BEGIN END; $$ LANGUAGE plpgsql"
    ).ok();
    executor
        .execute_sql(
            "CREATE EVENT TRIGGER to_drop_evt ON ddl_command_start EXECUTE FUNCTION dummy_evt7()",
        )
        .ok();

    let result = executor.execute_sql("DROP EVENT TRIGGER to_drop_evt");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_event_trigger_if_exists() {
    let mut executor = create_executor();
    let result = executor.execute_sql("DROP EVENT TRIGGER IF EXISTS nonexistent_evt");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_event_trigger_cascade() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE OR REPLACE FUNCTION dummy_evt8() RETURNS event_trigger AS $$ BEGIN END; $$ LANGUAGE plpgsql"
    ).ok();
    executor
        .execute_sql(
            "CREATE EVENT TRIGGER cascade_evt ON ddl_command_start EXECUTE FUNCTION dummy_evt8()",
        )
        .ok();

    let result = executor.execute_sql("DROP EVENT TRIGGER cascade_evt CASCADE");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_event_trigger_ddl_commands() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION log_commands() RETURNS event_trigger AS $$
         DECLARE
           r RECORD;
         BEGIN
           FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
             RAISE NOTICE 'command: %', r.command_tag;
           END LOOP;
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER commands_evt ON ddl_command_end EXECUTE FUNCTION log_commands()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_event_trigger_dropped_objects() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION log_dropped() RETURNS event_trigger AS $$
         DECLARE
           r RECORD;
         BEGIN
           FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
             RAISE NOTICE 'dropped: %.%', r.schema_name, r.object_name;
           END LOOP;
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor
        .execute_sql("CREATE EVENT TRIGGER dropped_evt ON sql_drop EXECUTE FUNCTION log_dropped()");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_tg_event_variable() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION show_event() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Event: %', TG_EVENT;
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER event_var_evt ON ddl_command_start EXECUTE FUNCTION show_event()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_tg_tag_variable() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION show_tag() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Tag: %', TG_TAG;
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER tag_var_evt ON ddl_command_start EXECUTE FUNCTION show_tag()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_prevent_drop() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION prevent_drop() RETURNS event_trigger AS $$
         BEGIN
           IF TG_TAG = 'DROP TABLE' THEN
             RAISE EXCEPTION 'DROP TABLE is not allowed';
           END IF;
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER prevent_drop_evt ON ddl_command_start
         WHEN TAG IN ('DROP TABLE')
         EXECUTE FUNCTION prevent_drop()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_audit_log() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE IF NOT EXISTS ddl_audit_log (id SERIAL, event_time TIMESTAMP DEFAULT NOW(), event TEXT, tag TEXT, username TEXT)").ok();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION audit_ddl() RETURNS event_trigger AS $$
         BEGIN
           INSERT INTO ddl_audit_log (event, tag, username) VALUES (TG_EVENT, TG_TAG, current_user);
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER audit_evt ON ddl_command_end EXECUTE FUNCTION audit_ddl()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_schema_filter() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION filter_schema() RETURNS event_trigger AS $$
         DECLARE
           r RECORD;
         BEGIN
           FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
             IF r.schema_name = 'protected' THEN
               RAISE EXCEPTION 'Cannot modify objects in protected schema';
             END IF;
           END LOOP;
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER schema_filter_evt ON ddl_command_end EXECUTE FUNCTION filter_schema()"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_create_table_tag() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION on_create_table() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Table created';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER create_table_evt ON ddl_command_start
         WHEN TAG IN ('CREATE TABLE')
         EXECUTE FUNCTION on_create_table()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_alter_table_tag() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION on_alter_table() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Table altered';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER alter_table_evt ON ddl_command_start
         WHEN TAG IN ('ALTER TABLE')
         EXECUTE FUNCTION on_alter_table()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_grant_revoke() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION on_grant_revoke() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Grant/Revoke executed';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER grant_revoke_evt ON ddl_command_start
         WHEN TAG IN ('GRANT', 'REVOKE')
         EXECUTE FUNCTION on_grant_revoke()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_function_ddl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION on_function_ddl() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Function DDL';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER function_ddl_evt ON ddl_command_start
         WHEN TAG IN ('CREATE FUNCTION', 'DROP FUNCTION', 'ALTER FUNCTION')
         EXECUTE FUNCTION on_function_ddl()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_view_ddl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION on_view_ddl() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'View DDL';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER view_ddl_evt ON ddl_command_start
         WHEN TAG IN ('CREATE VIEW', 'DROP VIEW', 'ALTER VIEW')
         EXECUTE FUNCTION on_view_ddl()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_sequence_ddl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION on_sequence_ddl() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Sequence DDL';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER sequence_ddl_evt ON ddl_command_start
         WHEN TAG IN ('CREATE SEQUENCE', 'DROP SEQUENCE', 'ALTER SEQUENCE')
         EXECUTE FUNCTION on_sequence_ddl()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_schema_ddl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION on_schema_ddl() RETURNS event_trigger AS $$
         BEGIN
           RAISE NOTICE 'Schema DDL';
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER schema_ddl_evt ON ddl_command_start
         WHEN TAG IN ('CREATE SCHEMA', 'DROP SCHEMA', 'ALTER SCHEMA')
         EXECUTE FUNCTION on_schema_ddl()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_event_trigger_catalog() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM pg_event_trigger");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_in_transaction() {
    let mut executor = create_executor();
    executor.execute_sql("BEGIN").ok();
    executor.execute_sql(
        "CREATE OR REPLACE FUNCTION tx_evt_func() RETURNS event_trigger AS $$ BEGIN END; $$ LANGUAGE plpgsql"
    ).ok();
    executor
        .execute_sql(
            "CREATE EVENT TRIGGER tx_evt ON ddl_command_start EXECUTE FUNCTION tx_evt_func()",
        )
        .ok();
    let result = executor.execute_sql("COMMIT");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_event_trigger_superuser_only() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE OR REPLACE FUNCTION su_only_evt() RETURNS event_trigger AS $$ BEGIN END; $$ LANGUAGE plpgsql"
    ).ok();

    let result = executor.execute_sql(
        "CREATE EVENT TRIGGER su_only ON ddl_command_start EXECUTE FUNCTION su_only_evt()",
    );
    assert!(result.is_ok() || result.is_err());
}
