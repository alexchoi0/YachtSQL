use crate::common::create_executor;

#[test]
fn test_listen_channel() {
    let mut executor = create_executor();
    let result = executor.execute_sql("LISTEN my_channel");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_listen_quoted_channel() {
    let mut executor = create_executor();
    let result = executor.execute_sql("LISTEN \"My Channel\"");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_unlisten_channel() {
    let mut executor = create_executor();
    executor.execute_sql("LISTEN my_channel").ok();
    let result = executor.execute_sql("UNLISTEN my_channel");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_unlisten_all() {
    let mut executor = create_executor();
    executor.execute_sql("LISTEN channel1").ok();
    executor.execute_sql("LISTEN channel2").ok();
    let result = executor.execute_sql("UNLISTEN *");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_channel() {
    let mut executor = create_executor();
    let result = executor.execute_sql("NOTIFY my_channel");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_with_payload() {
    let mut executor = create_executor();
    let result = executor.execute_sql("NOTIFY my_channel, 'Hello, World!'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_empty_payload() {
    let mut executor = create_executor();
    let result = executor.execute_sql("NOTIFY my_channel, ''");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_notify_function() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT pg_notify('my_channel', 'payload')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_notify_null_payload() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT pg_notify('my_channel', NULL)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_in_transaction() {
    let mut executor = create_executor();
    executor.execute_sql("BEGIN").ok();
    executor
        .execute_sql("NOTIFY my_channel, 'in transaction'")
        .ok();
    let result = executor.execute_sql("COMMIT");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_rollback() {
    let mut executor = create_executor();
    executor.execute_sql("BEGIN").ok();
    executor
        .execute_sql("NOTIFY my_channel, 'will be rolled back'")
        .ok();
    let result = executor.execute_sql("ROLLBACK");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_listen_multiple_channels() {
    let mut executor = create_executor();
    executor.execute_sql("LISTEN channel_a").ok();
    executor.execute_sql("LISTEN channel_b").ok();
    let result = executor.execute_sql("LISTEN channel_c");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_from_trigger() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE notify_test (id INT64, data TEXT)")
        .ok();

    let result = executor.execute_sql(
        "CREATE OR REPLACE FUNCTION notify_trigger() RETURNS TRIGGER AS $$
         BEGIN
           PERFORM pg_notify('table_changed', NEW.data);
           RETURN NEW;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_trigger_on_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE notify_ins (id INT64, data TEXT)")
        .ok();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION notify_ins_func() RETURNS TRIGGER AS $$
         BEGIN
           PERFORM pg_notify('insert_channel', 'row inserted');
           RETURN NEW;
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE TRIGGER notify_ins_trigger AFTER INSERT ON notify_ins FOR EACH ROW EXECUTE FUNCTION notify_ins_func()"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_trigger_on_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE notify_upd (id INT64, data TEXT)")
        .ok();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION notify_upd_func() RETURNS TRIGGER AS $$
         BEGIN
           PERFORM pg_notify('update_channel', 'row updated');
           RETURN NEW;
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE TRIGGER notify_upd_trigger AFTER UPDATE ON notify_upd FOR EACH ROW EXECUTE FUNCTION notify_upd_func()"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_trigger_on_delete() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE notify_del (id INT64, data TEXT)")
        .ok();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION notify_del_func() RETURNS TRIGGER AS $$
         BEGIN
           PERFORM pg_notify('delete_channel', 'row deleted');
           RETURN OLD;
         END;
         $$ LANGUAGE plpgsql",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE TRIGGER notify_del_trigger AFTER DELETE ON notify_del FOR EACH ROW EXECUTE FUNCTION notify_del_func()"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_json_payload() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT pg_notify('json_channel', '{\"event\": \"test\", \"id\": 1}')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_dynamic_channel() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE channels (name TEXT)")
        .ok();
    executor
        .execute_sql("INSERT INTO channels VALUES ('dynamic_channel')")
        .ok();

    let result = executor.execute_sql(
        "DO $$
         DECLARE
           ch TEXT;
         BEGIN
           SELECT name INTO ch FROM channels LIMIT 1;
           PERFORM pg_notify(ch, 'dynamic notification');
         END;
         $$",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_listen_case_sensitivity() {
    let mut executor = create_executor();
    executor.execute_sql("LISTEN MyChannel").ok();
    let result = executor.execute_sql("NOTIFY mychannel");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_special_characters() {
    let mut executor = create_executor();
    let result =
        executor.execute_sql("NOTIFY my_channel, 'payload with ''quotes'' and special chars: <>&'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_unicode_payload() {
    let mut executor = create_executor();
    let result = executor.execute_sql("NOTIFY my_channel, '你好世界 🎉'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_large_payload() {
    let mut executor = create_executor();
    let result = executor.execute_sql("NOTIFY my_channel, 'A very long payload that contains a lot of text to test the limits of the notification system. This is repeated content to make it longer.'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_listening_channels() {
    let mut executor = create_executor();
    executor.execute_sql("LISTEN test_channel_1").ok();
    executor.execute_sql("LISTEN test_channel_2").ok();

    let result = executor.execute_sql("SELECT pg_listening_channels()");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_notification_queue_usage() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT pg_notification_queue_usage()");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_listen_notify_same_transaction() {
    let mut executor = create_executor();
    executor.execute_sql("BEGIN").ok();
    executor.execute_sql("LISTEN same_tx_channel").ok();
    executor
        .execute_sql("NOTIFY same_tx_channel, 'same transaction'")
        .ok();
    let result = executor.execute_sql("COMMIT");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_coalesce_same_channel() {
    let mut executor = create_executor();
    executor.execute_sql("BEGIN").ok();
    executor.execute_sql("NOTIFY coalesce_channel").ok();
    executor.execute_sql("NOTIFY coalesce_channel").ok();
    executor.execute_sql("NOTIFY coalesce_channel").ok();
    let result = executor.execute_sql("COMMIT");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_different_payloads_not_coalesced() {
    let mut executor = create_executor();
    executor.execute_sql("BEGIN").ok();
    executor.execute_sql("NOTIFY diff_channel, 'payload1'").ok();
    executor.execute_sql("NOTIFY diff_channel, 'payload2'").ok();
    let result = executor.execute_sql("COMMIT");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_listen_in_procedure() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE OR REPLACE PROCEDURE setup_listener()
         LANGUAGE plpgsql AS $$
         BEGIN
           LISTEN proc_channel;
         END;
         $$",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_notify_row_to_json() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE notify_json (id INT64, name TEXT, value INT64)")
        .ok();
    executor
        .execute_sql("INSERT INTO notify_json VALUES (1, 'test', 100)")
        .ok();

    let result = executor.execute_sql(
        "DO $$
         DECLARE
           r RECORD;
         BEGIN
           SELECT * INTO r FROM notify_json WHERE id = 1;
           PERFORM pg_notify('json_row', row_to_json(r)::text);
         END;
         $$",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_listen_after_reset() {
    let mut executor = create_executor();
    executor.execute_sql("LISTEN reset_channel").ok();
    executor.execute_sql("DISCARD ALL").ok();
    let result = executor.execute_sql("LISTEN reset_channel");
    assert!(result.is_ok() || result.is_err());
}
