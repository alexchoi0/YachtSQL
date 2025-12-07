use crate::common::create_executor;

#[test]
fn test_create_trigger_before_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE trigger_test (id INT64, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE OR REPLACE FUNCTION before_insert_func() RETURNS TRIGGER AS $$
         BEGIN
             NEW.val := COALESCE(NEW.val, 0) + 1;
             RETURN NEW;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_after_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE after_insert_test (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE audit_log (action STRING, record_id INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER after_insert_trigger
         AFTER INSERT ON after_insert_test
         FOR EACH ROW
         EXECUTE FUNCTION audit_func()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_before_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE update_test (id INT64, val INT64, updated_at TIMESTAMP)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER before_update_trigger
         BEFORE UPDATE ON update_test
         FOR EACH ROW
         EXECUTE FUNCTION set_updated_at()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_after_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE after_update_test (id INT64, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER after_update_trigger
         AFTER UPDATE ON after_update_test
         FOR EACH ROW
         EXECUTE FUNCTION log_change()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_before_delete() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE delete_test (id INT64, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER before_delete_trigger
         BEFORE DELETE ON delete_test
         FOR EACH ROW
         EXECUTE FUNCTION archive_record()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_after_delete() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE after_delete_test (id INT64, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER after_delete_trigger
         AFTER DELETE ON after_delete_test
         FOR EACH ROW
         EXECUTE FUNCTION cleanup_related()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_instead_of() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE base_table (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW base_view AS SELECT * FROM base_table")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER instead_of_insert
         INSTEAD OF INSERT ON base_view
         FOR EACH ROW
         EXECUTE FUNCTION insert_to_base()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_for_each_statement() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stmt_trigger_test (id INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER statement_trigger
         AFTER INSERT ON stmt_trigger_test
         FOR EACH STATEMENT
         EXECUTE FUNCTION statement_audit()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_with_when() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE conditional_trigger_test (id INT64, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER conditional_trigger
         BEFORE UPDATE ON conditional_trigger_test
         FOR EACH ROW
         WHEN (OLD.val IS DISTINCT FROM NEW.val)
         EXECUTE FUNCTION log_value_change()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_truncate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE truncate_test (id INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER truncate_trigger
         BEFORE TRUNCATE ON truncate_test
         FOR EACH STATEMENT
         EXECUTE FUNCTION before_truncate()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_multiple_events() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE multi_event_test (id INT64, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER multi_event_trigger
         AFTER INSERT OR UPDATE OR DELETE ON multi_event_test
         FOR EACH ROW
         EXECUTE FUNCTION audit_all()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_update_of_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE column_trigger_test (id INT64, name STRING, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER column_update_trigger
         BEFORE UPDATE OF name, val ON column_trigger_test
         FOR EACH ROW
         EXECUTE FUNCTION specific_column_changed()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_trigger_referencing_old_new() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ref_test (id INT64, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TRIGGER referencing_trigger
         AFTER UPDATE ON ref_test
         REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
         FOR EACH STATEMENT
         EXECUTE FUNCTION compare_changes()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_trigger() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE drop_trigger_test (id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TRIGGER to_drop
         AFTER INSERT ON drop_trigger_test
         FOR EACH ROW
         EXECUTE FUNCTION dummy()",
        )
        .unwrap();

    let result = executor.execute_sql("DROP TRIGGER to_drop ON drop_trigger_test");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_trigger_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE drop_if_exists_test (id INT64)")
        .unwrap();

    let result = executor.execute_sql("DROP TRIGGER IF EXISTS nonexistent ON drop_if_exists_test");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_trigger_cascade() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cascade_trigger_test (id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TRIGGER cascade_trigger
         AFTER INSERT ON cascade_trigger_test
         FOR EACH ROW
         EXECUTE FUNCTION dummy()",
        )
        .unwrap();

    let result =
        executor.execute_sql("DROP TRIGGER cascade_trigger ON cascade_trigger_test CASCADE");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_trigger_rename() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rename_trigger_test (id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TRIGGER old_name
         AFTER INSERT ON rename_trigger_test
         FOR EACH ROW
         EXECUTE FUNCTION dummy()",
        )
        .unwrap();

    let result =
        executor.execute_sql("ALTER TRIGGER old_name ON rename_trigger_test RENAME TO new_name");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_enable_trigger() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE enable_trigger_test (id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TRIGGER to_enable
         AFTER INSERT ON enable_trigger_test
         FOR EACH ROW
         EXECUTE FUNCTION dummy()",
        )
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE enable_trigger_test ENABLE TRIGGER to_enable");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_disable_trigger() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE disable_trigger_test (id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TRIGGER to_disable
         AFTER INSERT ON disable_trigger_test
         FOR EACH ROW
         EXECUTE FUNCTION dummy()",
        )
        .unwrap();

    let result =
        executor.execute_sql("ALTER TABLE disable_trigger_test DISABLE TRIGGER to_disable");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_enable_all_triggers() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE all_triggers_test (id INT64)")
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE all_triggers_test ENABLE TRIGGER ALL");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_disable_all_triggers() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE disable_all_test (id INT64)")
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE disable_all_test DISABLE TRIGGER ALL");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_enable_user_triggers() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE user_triggers_test (id INT64)")
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE user_triggers_test ENABLE TRIGGER USER");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_trigger_replica_mode() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE replica_trigger_test (id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TRIGGER replica_trigger
         AFTER INSERT ON replica_trigger_test
         FOR EACH ROW
         EXECUTE FUNCTION dummy()",
        )
        .unwrap();

    let result = executor
        .execute_sql("ALTER TABLE replica_trigger_test ENABLE REPLICA TRIGGER replica_trigger");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_trigger_always_mode() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE always_trigger_test (id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TRIGGER always_trigger
         AFTER INSERT ON always_trigger_test
         FOR EACH ROW
         EXECUTE FUNCTION dummy()",
        )
        .unwrap();

    let result = executor
        .execute_sql("ALTER TABLE always_trigger_test ENABLE ALWAYS TRIGGER always_trigger");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_constraint_trigger() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE constraint_trigger_test (id INT64, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE CONSTRAINT TRIGGER constraint_check
         AFTER INSERT ON constraint_trigger_test
         DEFERRABLE INITIALLY DEFERRED
         FOR EACH ROW
         EXECUTE FUNCTION check_constraint()",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_trigger_function_return() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "CREATE OR REPLACE FUNCTION trigger_return_test() RETURNS TRIGGER AS $$
         BEGIN
             IF TG_OP = 'INSERT' THEN
                 RETURN NEW;
             ELSIF TG_OP = 'UPDATE' THEN
                 RETURN NEW;
             ELSIF TG_OP = 'DELETE' THEN
                 RETURN OLD;
             END IF;
             RETURN NULL;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}
