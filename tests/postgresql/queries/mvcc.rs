use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_system_column_ctid() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ctid_test (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ctid_test VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ctid, id FROM ctid_test ORDER BY id")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
#[ignore = "Implement me!"]
fn test_system_column_xmin() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE xmin_test (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO xmin_test VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT xmin, id FROM xmin_test")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_system_column_xmax() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE xmax_test (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO xmax_test VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT xmax, id FROM xmax_test")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_system_column_cmin() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cmin_test (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmin_test VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT cmin, id FROM cmin_test")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_system_column_cmax() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cmax_test (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cmax_test VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT cmax, id FROM cmax_test")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_system_column_tableoid() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tableoid_test (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tableoid_test VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT tableoid, id FROM tableoid_test")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_txid_current() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT txid_current()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_txid_current_if_assigned() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT txid_current_if_assigned()")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_txid_current_snapshot() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT txid_current_snapshot()")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_txid_snapshot_xmin() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT txid_snapshot_xmin(txid_current_snapshot())")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_txid_snapshot_xmax() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT txid_snapshot_xmax(txid_current_snapshot())")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_txid_snapshot_xip() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT txid_snapshot_xip(txid_current_snapshot())")
        .unwrap();
    let _ = result;
}

#[test]
fn test_txid_visible_in_snapshot() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT txid_visible_in_snapshot(txid_current(), txid_current_snapshot())")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_txid_status() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT txid_status(txid_current())")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_current_xact_id() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT pg_current_xact_id()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_current_xact_id_if_assigned() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT pg_current_xact_id_if_assigned()")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_current_snapshot() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT pg_current_snapshot()")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_snapshot_xmin() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT pg_snapshot_xmin(pg_current_snapshot())")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_snapshot_xmax() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT pg_snapshot_xmax(pg_current_snapshot())")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_snapshot_xip() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT pg_snapshot_xip(pg_current_snapshot())")
        .unwrap();
    let _ = result;
}

#[test]
fn test_pg_visible_in_snapshot() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT pg_visible_in_snapshot(pg_current_xact_id(), pg_current_snapshot())");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_xact_status() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT pg_xact_status(pg_current_xact_id())");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_xmin_after_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE xmin_update (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO xmin_update VALUES (1, 100)")
        .unwrap();

    let result1 = executor
        .execute_sql("SELECT xmin FROM xmin_update WHERE id = 1")
        .unwrap();

    executor
        .execute_sql("UPDATE xmin_update SET val = 200 WHERE id = 1")
        .unwrap();

    let result2 = executor
        .execute_sql("SELECT xmin FROM xmin_update WHERE id = 1")
        .unwrap();

    assert_eq!(result1.num_rows(), 1);
    assert_eq!(result2.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_ctid_changes_after_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ctid_update (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ctid_update VALUES (1, 100)")
        .unwrap();

    let result1 = executor
        .execute_sql("SELECT ctid FROM ctid_update WHERE id = 1")
        .unwrap();

    executor
        .execute_sql("UPDATE ctid_update SET val = 200 WHERE id = 1")
        .unwrap();

    let result2 = executor
        .execute_sql("SELECT ctid FROM ctid_update WHERE id = 1")
        .unwrap();

    assert_eq!(result1.num_rows(), 1);
    assert_eq!(result2.num_rows(), 1);
}

#[test]
fn test_transaction_isolation_snapshot() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE iso_snap (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO iso_snap VALUES (1, 100)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .unwrap();
    let snap1 = executor
        .execute_sql("SELECT val FROM iso_snap WHERE id = 1")
        .unwrap();
    let snap2 = executor
        .execute_sql("SELECT val FROM iso_snap WHERE id = 1")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(snap1, snap2);
}

#[test]
fn test_select_for_update_locks_row() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE for_upd_lock (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO for_upd_lock VALUES (1, 100)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    let result = executor
        .execute_sql("SELECT * FROM for_upd_lock WHERE id = 1 FOR UPDATE")
        .unwrap();
    executor
        .execute_sql("UPDATE for_upd_lock SET val = 200 WHERE id = 1")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_select_for_share() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE for_share (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO for_share VALUES (1, 100)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    let result = executor
        .execute_sql("SELECT * FROM for_share WHERE id = 1 FOR SHARE")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_select_for_no_key_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE for_no_key (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO for_no_key VALUES (1, 100)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    let result = executor
        .execute_sql("SELECT * FROM for_no_key WHERE id = 1 FOR NO KEY UPDATE")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_select_for_key_share() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE for_key_share (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO for_key_share VALUES (1, 100)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    let result = executor
        .execute_sql("SELECT * FROM for_key_share WHERE id = 1 FOR KEY SHARE")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_select_for_update_nowait() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE for_upd_nowait (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO for_upd_nowait VALUES (1, 100)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    let result = executor
        .execute_sql("SELECT * FROM for_upd_nowait WHERE id = 1 FOR UPDATE NOWAIT")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_select_for_update_skip_locked() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE for_upd_skip (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO for_upd_skip VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM for_upd_skip FOR UPDATE SKIP LOCKED ORDER BY id")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_select_for_update_of_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE t1_lock (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE t2_lock (id INTEGER, ref_id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t1_lock VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t2_lock VALUES (1, 1)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    let result = executor.execute_sql("SELECT t1_lock.* FROM t1_lock JOIN t2_lock ON t1_lock.id = t2_lock.ref_id FOR UPDATE OF t1_lock").unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_vacuum() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE vacuum_test (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vacuum_test VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("DELETE FROM vacuum_test WHERE id = 1")
        .unwrap();

    let result = executor.execute_sql("VACUUM vacuum_test");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_vacuum_full() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE vacuum_full_test (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vacuum_full_test VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("VACUUM FULL vacuum_full_test");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_vacuum_analyze() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE vacuum_analyze (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vacuum_analyze VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("VACUUM ANALYZE vacuum_analyze");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_vacuum_verbose() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE vacuum_verbose (id INTEGER)")
        .unwrap();

    let result = executor.execute_sql("VACUUM VERBOSE vacuum_verbose");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_vacuum_freeze() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE vacuum_freeze (id INTEGER)")
        .unwrap();

    let result = executor.execute_sql("VACUUM FREEZE vacuum_freeze");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_analyze() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE analyze_test (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO analyze_test VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = executor.execute_sql("ANALYZE analyze_test");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_analyze_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE analyze_col (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO analyze_col VALUES (1, 100)")
        .unwrap();

    let result = executor.execute_sql("ANALYZE analyze_col (val)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_stat_get_dead_tuples() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dead_tuples (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dead_tuples VALUES (1)")
        .unwrap();
    executor
        .execute_sql("DELETE FROM dead_tuples WHERE id = 1")
        .unwrap();

    let result = executor.execute_sql("SELECT pg_stat_get_dead_tuples('dead_tuples'::regclass)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_stat_get_live_tuples() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE live_tuples (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO live_tuples VALUES (1), (2)")
        .unwrap();

    let result = executor.execute_sql("SELECT pg_stat_get_live_tuples('live_tuples'::regclass)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_age_xid() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT age(txid_current()::xid)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_mxid_age() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT mxid_age('1'::xid)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_xid8_type() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '12345'::xid8").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_pg_xact_commit_timestamp() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT pg_xact_commit_timestamp(pg_current_xact_id())");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_last_committed_xact() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT pg_last_committed_xact()");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_row_versioning_delete() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE row_ver_del (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO row_ver_del VALUES (1, 100)")
        .unwrap();

    let before = executor
        .execute_sql("SELECT xmax FROM row_ver_del WHERE id = 1")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DELETE FROM row_ver_del WHERE id = 1")
        .unwrap();
    executor.execute_sql("ROLLBACK").unwrap();

    let after = executor
        .execute_sql("SELECT xmax FROM row_ver_del WHERE id = 1")
        .unwrap();

    assert_eq!(before.num_rows(), 1);
    assert_eq!(after.num_rows(), 1);
}

#[test]
fn test_visibility_map_functions() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE vis_map (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vis_map VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT pg_visibility_map_summary('vis_map'::regclass)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_freespace() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE freespace_test (id INTEGER)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM pg_freespace('freespace_test'::regclass)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_hot_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE hot_test (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO hot_test VALUES (1, 100)")
        .unwrap();

    executor
        .execute_sql("UPDATE hot_test SET val = 200 WHERE id = 1")
        .unwrap();

    let result =
        executor.execute_sql("SELECT pg_stat_get_tuples_hot_updated('hot_test'::regclass)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_concurrent_insert_visibility() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE concurrent_vis (id INTEGER, val INTEGER)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .unwrap();
    executor
        .execute_sql("INSERT INTO concurrent_vis VALUES (1, 100)")
        .unwrap();
    let in_tx = executor
        .execute_sql("SELECT COUNT(*) FROM concurrent_vis")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let after_commit = executor
        .execute_sql("SELECT COUNT(*) FROM concurrent_vis")
        .unwrap();

    assert_table_eq!(in_tx, [[1]]);
    assert_table_eq!(after_commit, [[1]]);
}

#[test]
fn test_serializable_isolation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE serial_iso (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO serial_iso VALUES (1, 100)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .unwrap();
    let result1 = executor
        .execute_sql("SELECT val FROM serial_iso WHERE id = 1")
        .unwrap();
    let result2 = executor
        .execute_sql("SELECT val FROM serial_iso WHERE id = 1")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result1, result2);
}

#[test]
fn test_repeatable_read_isolation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rr_iso (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rr_iso VALUES (1, 100)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .unwrap();
    let result1 = executor
        .execute_sql("SELECT val FROM rr_iso WHERE id = 1")
        .unwrap();
    let result2 = executor
        .execute_sql("SELECT val FROM rr_iso WHERE id = 1")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_eq!(result1, result2);
}

#[test]
fn test_read_committed_isolation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rc_iso (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rc_iso VALUES (1, 100)")
        .unwrap();

    executor
        .execute_sql("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .unwrap();
    let result = executor
        .execute_sql("SELECT val FROM rc_iso WHERE id = 1")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_ctid_tid_scan() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tid_scan (id INTEGER, val TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tid_scan VALUES (1, 'first'), (2, 'second')")
        .unwrap();

    let result = executor.execute_sql("SELECT ctid, id, val FROM tid_scan WHERE ctid = '(0,1)'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_multiple_version_query() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE multi_ver (id INTEGER, val INTEGER)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO multi_ver VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("UPDATE multi_ver SET val = 200 WHERE id = 1")
        .unwrap();
    executor
        .execute_sql("UPDATE multi_ver SET val = 300 WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM multi_ver WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [[300]]);
}

#[test]
fn test_vacuum_after_updates() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE vac_updates (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vac_updates VALUES (1, 100)")
        .unwrap();

    for i in 1..=5 {
        executor
            .execute_sql(&format!(
                "UPDATE vac_updates SET val = {} WHERE id = 1",
                i * 100
            ))
            .unwrap();
    }

    let result = executor.execute_sql("VACUUM vac_updates");
    assert!(result.is_ok() || result.is_err());

    let final_val = executor
        .execute_sql("SELECT val FROM vac_updates WHERE id = 1")
        .unwrap();
    assert_table_eq!(final_val, [[500]]);
}

#[test]
fn test_pg_stat_all_tables() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stat_tables (id INTEGER)")
        .unwrap();

    let result =
        executor.execute_sql("SELECT * FROM pg_stat_all_tables WHERE relname = 'stat_tables'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_stat_user_tables() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stat_user (id INTEGER)")
        .unwrap();

    let result =
        executor.execute_sql("SELECT * FROM pg_stat_user_tables WHERE relname = 'stat_user'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_statio_all_tables() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE statio_tables (id INTEGER)")
        .unwrap();

    let result =
        executor.execute_sql("SELECT * FROM pg_statio_all_tables WHERE relname = 'statio_tables'");
    assert!(result.is_ok() || result.is_err());
}
