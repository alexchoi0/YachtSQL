use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_log_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE log_basic (id INT64, name String) ENGINE = Log")
        .unwrap();
    executor
        .execute_sql("INSERT INTO log_basic VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name FROM log_basic ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "alice"], [2, "bob"]]);
}

#[test]
fn test_tinylog_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tinylog_basic (id INT64, value INT64) ENGINE = TinyLog")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tinylog_basic VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM tinylog_basic ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[test]
fn test_stripelog_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stripelog_basic (id INT64, data String) ENGINE = StripeLog")
        .unwrap();
    executor
        .execute_sql("INSERT INTO stripelog_basic VALUES (1, 'test'), (2, 'data')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, data FROM stripelog_basic ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "test"], [2, "data"]]);
}

#[test]
fn test_log_multiple_inserts() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE log_multi (id INT64) ENGINE = Log")
        .unwrap();
    executor
        .execute_sql("INSERT INTO log_multi VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO log_multi VALUES (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO log_multi VALUES (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM log_multi")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_log_select_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE log_where (id INT64, value INT64) ENGINE = Log")
        .unwrap();
    executor
        .execute_sql("INSERT INTO log_where VALUES (1, 100), (2, 200), (3, 150)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM log_where WHERE value > 120 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_log_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE log_agg (category String, value INT64) ENGINE = Log")
        .unwrap();
    executor
        .execute_sql("INSERT INTO log_agg VALUES ('A', 10), ('A', 20), ('B', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) FROM log_agg GROUP BY category ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}

#[test]
fn test_tinylog_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tinylog_agg (category String, value INT64) ENGINE = TinyLog")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tinylog_agg VALUES ('X', 100), ('Y', 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(value) FROM tinylog_agg")
        .unwrap();
    assert_table_eq!(result, [[300]]);
}

#[test]
fn test_stripelog_large_batch() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stripelog_batch (id INT64) ENGINE = StripeLog")
        .unwrap();

    let values: Vec<String> = (1..=100).map(|i| format!("({})", i)).collect();
    executor
        .execute_sql(&format!(
            "INSERT INTO stripelog_batch VALUES {}",
            values.join(", ")
        ))
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM stripelog_batch")
        .unwrap();
    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_log_nullable() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE log_null (id INT64, value Nullable(Int64)) ENGINE = Log")
        .unwrap();
    executor
        .execute_sql("INSERT INTO log_null VALUES (1, 100), (2, NULL), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM log_null WHERE value IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_log_array_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE log_array (id INT64, arr Array(Int64)) ENGINE = Log")
        .unwrap();
    executor
        .execute_sql("INSERT INTO log_array VALUES (1, [1, 2, 3]), (2, [4, 5])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, length(arr) FROM log_array ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 3], [2, 2]]);
}

#[test]
fn test_log_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE log_left (id INT64, name String) ENGINE = Log")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE log_right (id INT64, value INT64) ENGINE = Log")
        .unwrap();
    executor
        .execute_sql("INSERT INTO log_left VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO log_right VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT l.name, r.value FROM log_left l JOIN log_right r ON l.id = r.id ORDER BY r.value")
        .unwrap();
    assert_table_eq!(result, [["alice", 100], ["bob", 200]]);
}
