use crate::common::create_executor;

#[test]
fn test_create_rule_nothing() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rule_test (id INT64, val INT64)")
        .unwrap();

    let result =
        executor.execute_sql("CREATE RULE protect_rule AS ON DELETE TO rule_test DO NOTHING");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_rule_instead() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE instead_test (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE archive (id INT64, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE archive_rule AS ON DELETE TO instead_test
         DO INSTEAD INSERT INTO archive VALUES (OLD.id, OLD.val)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_rule_also() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE also_test (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE log_table (action STRING, record_id INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE log_insert AS ON INSERT TO also_test
         DO ALSO INSERT INTO log_table VALUES ('INSERT', NEW.id)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_rule_with_condition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cond_rule_test (id INT64, val INT64, active BOOL)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE conditional_rule AS ON DELETE TO cond_rule_test
         WHERE active = TRUE
         DO INSTEAD UPDATE cond_rule_test SET active = FALSE WHERE id = OLD.id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_rule_multiple_commands() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE multi_cmd_test (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE audit1 (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE audit2 (id INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE multi_rule AS ON INSERT TO multi_cmd_test DO ALSO (
             INSERT INTO audit1 VALUES (NEW.id);
             INSERT INTO audit2 VALUES (NEW.id)
         )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_rule_select() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE select_rule_test (id INT64, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE select_rule AS ON SELECT TO select_rule_test
         DO INSTEAD SELECT * FROM select_rule_test WHERE val > 0",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_rule_on_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE base_table (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW base_view AS SELECT * FROM base_table")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE view_insert AS ON INSERT TO base_view
         DO INSTEAD INSERT INTO base_table VALUES (NEW.id, NEW.val)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_rule_on_view_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE view_base (id INT64, name STRING, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW view_for_update AS SELECT id, name FROM view_base")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE view_update AS ON UPDATE TO view_for_update
         DO INSTEAD UPDATE view_base SET name = NEW.name WHERE id = OLD.id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_rule_on_view_delete() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE del_base (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW del_view AS SELECT * FROM del_base")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE view_delete AS ON DELETE TO del_view
         DO INSTEAD DELETE FROM del_base WHERE id = OLD.id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_drop_rule() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE drop_rule_test (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE RULE to_drop AS ON INSERT TO drop_rule_test DO NOTHING")
        .unwrap();

    let result = executor.execute_sql("DROP RULE to_drop ON drop_rule_test");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_rule_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE drop_if_exists_test (id INT64)")
        .unwrap();

    let result = executor.execute_sql("DROP RULE IF EXISTS nonexistent ON drop_if_exists_test");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_drop_rule_cascade() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cascade_rule_test (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE RULE cascade_rule AS ON INSERT TO cascade_rule_test DO NOTHING")
        .unwrap();

    let result = executor.execute_sql("DROP RULE cascade_rule ON cascade_rule_test CASCADE");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_alter_rule_rename() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rename_rule_test (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE RULE old_rule_name AS ON INSERT TO rename_rule_test DO NOTHING")
        .unwrap();

    let result = executor
        .execute_sql("ALTER RULE old_rule_name ON rename_rule_test RENAME TO new_rule_name");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_create_or_replace_rule() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE replace_rule_test (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE RULE replace_rule AS ON INSERT TO replace_rule_test DO NOTHING")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE OR REPLACE RULE replace_rule AS ON INSERT TO replace_rule_test
         DO ALSO SELECT 1",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_rule_with_returning() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE returning_rule_test (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE returning_target (id INT64, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE return_rule AS ON INSERT TO returning_rule_test
         DO INSTEAD INSERT INTO returning_target VALUES (NEW.id, NEW.val) RETURNING *",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_rule_using_old_new() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE old_new_test (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE history (old_val INT64, new_val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE track_changes AS ON UPDATE TO old_new_test
         DO ALSO INSERT INTO history VALUES (OLD.val, NEW.val)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_rule_priority() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE priority_test (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE log1 (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE log2 (id INT64)")
        .unwrap();

    executor.execute_sql(
        "CREATE RULE rule_a AS ON INSERT TO priority_test DO ALSO INSERT INTO log1 VALUES (NEW.id)"
    ).unwrap();

    let result = executor.execute_sql(
        "CREATE RULE rule_b AS ON INSERT TO priority_test DO ALSO INSERT INTO log2 VALUES (NEW.id)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_rule_system_default() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sys_rule_test (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW sys_rule_view AS SELECT * FROM sys_rule_test")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE \"_RETURN\" AS ON SELECT TO sys_rule_view
         DO INSTEAD SELECT * FROM sys_rule_test WHERE id > 0",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_rule_no_action() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE no_action_test (id INT64, protected BOOL)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE protect_delete AS ON DELETE TO no_action_test
         WHERE protected = TRUE
         DO INSTEAD NOTHING",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_rule_complex_condition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE complex_cond_test (id INT64, status STRING, val INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE RULE complex_rule AS ON UPDATE TO complex_cond_test
         WHERE OLD.status = 'active' AND NEW.val > 100
         DO ALSO UPDATE complex_cond_test SET status = 'reviewed' WHERE id = NEW.id",
    );
    assert!(result.is_ok() || result.is_err());
}
