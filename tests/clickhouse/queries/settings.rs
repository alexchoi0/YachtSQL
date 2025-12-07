use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_settings_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_basic (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_basic VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM settings_basic ORDER BY id SETTINGS max_threads = 1")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
fn test_settings_multiple() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_multi (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_multi VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM settings_multi SETTINGS max_threads = 2, max_memory_usage = 1000000000",
        )
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_settings_max_rows_to_read() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_rows (id INT64)")
        .unwrap();
    for i in 1..=100 {
        executor
            .execute_sql(&format!("INSERT INTO settings_rows VALUES ({})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT id FROM settings_rows SETTINGS max_rows_to_read = 50")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[test]
fn test_settings_read_overflow_mode() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_overflow (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_overflow VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM settings_overflow SETTINGS read_overflow_mode = 'break'")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_settings_optimize_read() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_optimize (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_optimize VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM settings_optimize WHERE id = 2 SETTINGS optimize_read_in_order = 1",
        )
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_settings_max_execution_time() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_time (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_time VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM settings_time SETTINGS max_execution_time = 60")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_settings_with_order() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_order (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_order VALUES (3), (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM settings_order ORDER BY id SETTINGS max_threads = 1")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
fn test_settings_with_group() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_group (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_group VALUES ('A', 10), ('A', 20), ('B', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) FROM settings_group GROUP BY category ORDER BY category SETTINGS max_threads = 1")
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}

#[test]
fn test_settings_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_left (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE settings_right (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_left VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_right VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT l.id, r.name FROM settings_left l JOIN settings_right r ON l.id = r.id SETTINGS join_use_nulls = 1")
        .unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[test]
fn test_settings_timeout_overflow() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_timeout (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_timeout VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM settings_timeout SETTINGS timeout_overflow_mode = 'throw'")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_settings_distributed() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_dist (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_dist VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM settings_dist SETTINGS distributed_product_mode = 'local'")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_settings_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_insert (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_insert VALUES (1) SETTINGS insert_deduplicate = 0")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM settings_insert")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_settings_format() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE settings_format (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO settings_format VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM settings_format SETTINGS output_format_pretty_max_rows = 100")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_set_command() {
    let mut executor = create_executor();
    executor.execute_sql("SET max_threads = 4").unwrap();
    executor
        .execute_sql("CREATE TABLE set_cmd (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO set_cmd VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM set_cmd").unwrap();
    assert_table_eq!(result, [[1]]);
}
