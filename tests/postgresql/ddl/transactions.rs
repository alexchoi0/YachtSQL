use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_begin_commit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_test (id INT64, val INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_test VALUES (1, 100)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_test").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_begin_rollback() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_rollback (id INT64, val INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_rollback VALUES (1, 100)")
        .unwrap();
    executor.execute_sql("ROLLBACK").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_rollback").unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_start_transaction() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_start (id INT64)")
        .unwrap();

    executor.execute_sql("START TRANSACTION").unwrap();
    executor
        .execute_sql("INSERT INTO tx_start VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_start").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_transaction_isolation_read_committed() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_iso (id INT64)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_iso VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_iso").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_transaction_isolation_repeatable_read() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_rr (id INT64)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_rr VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_rr").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_transaction_isolation_serializable() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_ser (id INT64)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_ser VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_ser").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_transaction_read_only() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_readonly (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_readonly VALUES (1)")
        .unwrap();

    executor.execute_sql("BEGIN TRANSACTION READ ONLY").unwrap();
    let result = executor.execute_sql("SELECT * FROM tx_readonly").unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_transaction_read_write() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_rw (id INT64)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION READ WRITE")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_rw VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_rw").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_savepoint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_save (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_save VALUES (1)")
        .unwrap();
    executor.execute_sql("SAVEPOINT sp1").unwrap();
    executor
        .execute_sql("INSERT INTO tx_save VALUES (2)")
        .unwrap();
    executor.execute_sql("ROLLBACK TO SAVEPOINT sp1").unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_save").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_release_savepoint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_release (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_release VALUES (1)")
        .unwrap();
    executor.execute_sql("SAVEPOINT sp1").unwrap();
    executor
        .execute_sql("INSERT INTO tx_release VALUES (2)")
        .unwrap();
    executor.execute_sql("RELEASE SAVEPOINT sp1").unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_release").unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
#[ignore = "Implement me!"]
fn test_nested_savepoints() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_nested (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_nested VALUES (1)")
        .unwrap();
    executor.execute_sql("SAVEPOINT sp1").unwrap();
    executor
        .execute_sql("INSERT INTO tx_nested VALUES (2)")
        .unwrap();
    executor.execute_sql("SAVEPOINT sp2").unwrap();
    executor
        .execute_sql("INSERT INTO tx_nested VALUES (3)")
        .unwrap();
    executor.execute_sql("ROLLBACK TO SAVEPOINT sp2").unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_nested").unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_end_transaction() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_end (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_end VALUES (1)")
        .unwrap();
    executor.execute_sql("END").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_end").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_abort_transaction() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_abort (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_abort VALUES (1)")
        .unwrap();
    executor.execute_sql("ABORT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_abort").unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
#[ignore = "Implement me!"]
fn test_transaction_deferrable() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_defer (id INT64)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
        .unwrap();
    let result = executor.execute_sql("SELECT * FROM tx_defer").unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_transaction() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_set (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_set VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_set").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_commit_and_chain() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_chain (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_chain VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT AND CHAIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_chain VALUES (2)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_chain").unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
#[ignore = "Implement me!"]
fn test_rollback_and_chain() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_rb_chain (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_rb_chain VALUES (1)")
        .unwrap();
    executor.execute_sql("ROLLBACK AND CHAIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_rb_chain VALUES (2)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_rb_chain").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_transaction_multiple_statements() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_multi (id INT64, val INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_multi VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_multi VALUES (2, 200)")
        .unwrap();
    executor
        .execute_sql("UPDATE tx_multi SET val = val + 10 WHERE id = 1")
        .unwrap();
    executor
        .execute_sql("DELETE FROM tx_multi WHERE id = 2")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_multi").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_rollback_to_savepoint_multiple() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_sp_multi (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_sp_multi VALUES (1)")
        .unwrap();
    executor.execute_sql("SAVEPOINT sp1").unwrap();
    executor
        .execute_sql("INSERT INTO tx_sp_multi VALUES (2)")
        .unwrap();
    executor.execute_sql("SAVEPOINT sp2").unwrap();
    executor
        .execute_sql("INSERT INTO tx_sp_multi VALUES (3)")
        .unwrap();
    executor.execute_sql("ROLLBACK TO sp1").unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_sp_multi").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_savepoint_same_name_overwrite() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_sp_overwrite (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_sp_overwrite VALUES (1)")
        .unwrap();
    executor.execute_sql("SAVEPOINT sp1").unwrap();
    executor
        .execute_sql("INSERT INTO tx_sp_overwrite VALUES (2)")
        .unwrap();
    executor.execute_sql("SAVEPOINT sp1").unwrap();
    executor
        .execute_sql("INSERT INTO tx_sp_overwrite VALUES (3)")
        .unwrap();
    executor.execute_sql("ROLLBACK TO sp1").unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor
        .execute_sql("SELECT * FROM tx_sp_overwrite")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_begin_work() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_work (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN WORK").unwrap();
    executor
        .execute_sql("INSERT INTO tx_work VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT WORK").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_work").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_rollback_work() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_rb_work (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN WORK").unwrap();
    executor
        .execute_sql("INSERT INTO tx_rb_work VALUES (1)")
        .unwrap();
    executor.execute_sql("ROLLBACK WORK").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_rb_work").unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
#[ignore = "Implement me!"]
fn test_transaction_ddl_create_table() {
    let mut executor = create_executor();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("CREATE TABLE tx_ddl_create (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_ddl_create VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_ddl_create").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_transaction_ddl_rollback() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_ddl_rb_base (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor.execute_sql("DROP TABLE tx_ddl_rb_base").unwrap();
    executor.execute_sql("ROLLBACK").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_ddl_rb_base");
    assert!(result.is_ok());
}

#[test]
fn test_transaction_update_rollback() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_upd_rb (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_upd_rb VALUES (1, 100)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("UPDATE tx_upd_rb SET val = 200 WHERE id = 1")
        .unwrap();
    executor.execute_sql("ROLLBACK").unwrap();

    let result = executor
        .execute_sql("SELECT val FROM tx_upd_rb WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_transaction_delete_rollback() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_del_rb (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_del_rb VALUES (1), (2), (3)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DELETE FROM tx_del_rb WHERE id > 1")
        .unwrap();
    executor.execute_sql("ROLLBACK").unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM tx_del_rb")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_transaction_read_only() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_set_ro (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_set_ro VALUES (1)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor.execute_sql("SET TRANSACTION READ ONLY").unwrap();
    let result = executor.execute_sql("SELECT * FROM tx_set_ro").unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_transaction_read_write() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_set_rw (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor.execute_sql("SET TRANSACTION READ WRITE").unwrap();
    executor
        .execute_sql("INSERT INTO tx_set_rw VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_set_rw").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_prepare_transaction() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_prepare (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_prepare VALUES (1)")
        .unwrap();
    let result = executor.execute_sql("PREPARE TRANSACTION 'tx_prep_1'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_commit_prepared() {
    let mut executor = create_executor();
    let result = executor.execute_sql("COMMIT PREPARED 'tx_prep_1'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_rollback_prepared() {
    let mut executor = create_executor();
    let result = executor.execute_sql("ROLLBACK PREPARED 'tx_prep_1'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_transaction_with_select_for_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_for_upd (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_for_upd VALUES (1, 100)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    let result = executor
        .execute_sql("SELECT * FROM tx_for_upd WHERE id = 1 FOR UPDATE")
        .unwrap();
    executor
        .execute_sql("UPDATE tx_for_upd SET val = 200 WHERE id = 1")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_lock_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_lock (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("LOCK TABLE tx_lock IN ACCESS EXCLUSIVE MODE")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_lock VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_lock").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_lock_table_share_mode() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_lock_share (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_lock_share VALUES (1)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("LOCK TABLE tx_lock_share IN SHARE MODE")
        .unwrap();
    let result = executor.execute_sql("SELECT * FROM tx_lock_share").unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_lock_table_row_exclusive() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_lock_row_ex (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("LOCK TABLE tx_lock_row_ex IN ROW EXCLUSIVE MODE")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_lock_row_ex VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor
        .execute_sql("SELECT * FROM tx_lock_row_ex")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_lock_table_share_row_exclusive() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_lock_sre (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("LOCK TABLE tx_lock_sre IN SHARE ROW EXCLUSIVE MODE")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_lock_sre VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_lock_sre").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_lock_table_exclusive() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_lock_ex (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("LOCK TABLE tx_lock_ex IN EXCLUSIVE MODE")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_lock_ex VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_lock_ex").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_lock_table_nowait() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_lock_nowait (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    let result = executor.execute_sql("LOCK TABLE tx_lock_nowait IN ACCESS EXCLUSIVE MODE NOWAIT");
    executor.execute_sql("COMMIT").unwrap();

    assert!(result.is_ok());
}

#[test]
#[ignore = "Implement me!"]
fn test_transaction_with_cte() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_cte (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_cte VALUES (1, 100), (2, 200)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql(
            "WITH doubled AS (SELECT id, val * 2 AS new_val FROM tx_cte)
         UPDATE tx_cte SET val = doubled.new_val FROM doubled WHERE tx_cte.id = doubled.id",
        )
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT SUM(val) FROM tx_cte").unwrap();
    assert_table_eq!(result, [[600]]);
}

#[test]
fn test_transaction_with_returning() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_ret (id INT64, name STRING)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    let result = executor
        .execute_sql("INSERT INTO tx_ret VALUES (1, 'Alice') RETURNING id, name")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_transaction_implicit_begin() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_implicit (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_implicit VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_implicit").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_constraints_deferred() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_parent (id INT64 PRIMARY KEY)")
        .unwrap();
    executor.execute_sql("CREATE TABLE tx_child (id INT64, parent_id INT64 REFERENCES tx_parent(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_child VALUES (1, 1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_parent VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_child").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_constraints_immediate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_const_imm (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("SET CONSTRAINTS ALL IMMEDIATE")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_const_imm VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_const_imm").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_constraints_deferred_mode() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_const_def (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("SET CONSTRAINTS ALL DEFERRED")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_const_def VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_const_def").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_transaction_isolation_read_uncommitted() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_read_unc (id INT64)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_read_unc VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_read_unc").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_transaction_combined_options() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_combined (id INT64)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE NOT DEFERRABLE")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_combined VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_combined").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_savepoint_release_and_continue() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_sp_cont (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_sp_cont VALUES (1)")
        .unwrap();
    executor.execute_sql("SAVEPOINT sp1").unwrap();
    executor
        .execute_sql("INSERT INTO tx_sp_cont VALUES (2)")
        .unwrap();
    executor.execute_sql("RELEASE sp1").unwrap();
    executor
        .execute_sql("INSERT INTO tx_sp_cont VALUES (3)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM tx_sp_cont")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_rollback_to_savepoint_and_continue() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_rb_cont (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_rb_cont VALUES (1)")
        .unwrap();
    executor.execute_sql("SAVEPOINT sp1").unwrap();
    executor
        .execute_sql("INSERT INTO tx_rb_cont VALUES (2)")
        .unwrap();
    executor.execute_sql("ROLLBACK TO sp1").unwrap();
    executor
        .execute_sql("INSERT INTO tx_rb_cont VALUES (3)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor
        .execute_sql("SELECT * FROM tx_rb_cont ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_transaction_with_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_subq_src (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE tx_subq_dst (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_subq_src VALUES (1, 100), (2, 200)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_subq_dst SELECT * FROM tx_subq_src WHERE val > 100")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor.execute_sql("SELECT * FROM tx_subq_dst").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_two_phase_commit_syntax() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_2pc (id INT64)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO tx_2pc VALUES (1)")
        .unwrap();
    let result = executor.execute_sql("PREPARE TRANSACTION 'two_phase_test'");

    if result.is_ok() {
        executor
            .execute_sql("COMMIT PREPARED 'two_phase_test'")
            .unwrap();
    } else {
        executor.execute_sql("ROLLBACK").unwrap();
    }
}

#[test]
fn test_pg_advisory_lock() {
    let mut executor = create_executor();

    executor.execute_sql("BEGIN").unwrap();
    let result = executor.execute_sql("SELECT pg_advisory_lock(12345)");
    assert!(result.is_ok() || result.is_err());
    executor.execute_sql("COMMIT").unwrap();
}

#[test]
fn test_pg_advisory_unlock() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT pg_advisory_unlock(12345)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_try_advisory_lock() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT pg_try_advisory_lock(12345)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_transaction_snapshot() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_snap (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_snap VALUES (1)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .unwrap();
    let result1 = executor.execute_sql("SELECT * FROM tx_snap").unwrap();
    let result2 = executor.execute_sql("SELECT * FROM tx_snap").unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result1.num_rows(), result2.num_rows());
}

#[test]
#[ignore = "Implement me!"]
fn test_transaction_with_exception_recovery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_except (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tx_except VALUES (1)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor.execute_sql("SAVEPOINT before_error").unwrap();
    let _ = executor.execute_sql("INSERT INTO nonexistent_table VALUES (1)");
    executor.execute_sql("ROLLBACK TO before_error").unwrap();
    executor
        .execute_sql("INSERT INTO tx_except VALUES (2)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM tx_except")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}
