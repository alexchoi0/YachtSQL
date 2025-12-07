use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_global_in_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE global_in_test (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO global_in_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM global_in_test WHERE id GLOBAL IN (1, 3, 5) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3], [5]]);
}

#[test]
fn test_global_not_in() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE global_not_in (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO global_not_in VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM global_not_in WHERE id GLOBAL NOT IN (1, 3, 5) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);
}

#[test]
fn test_global_in_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE global_main (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE global_lookup (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO global_main VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO global_lookup VALUES (1), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM global_main WHERE id GLOBAL IN (SELECT id FROM global_lookup) ORDER BY value")
        .unwrap();
    assert_table_eq!(result, [[100], [300]]);
}

#[test]
fn test_global_in_distributed_context() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dist_left (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE dist_right (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dist_left VALUES (1), (2), (3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dist_right VALUES (2), (3), (4)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM dist_left WHERE id GLOBAL IN (SELECT id FROM dist_right) ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_global_in_with_tuple() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE global_tuple (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO global_tuple VALUES (1, 2), (3, 4), (5, 6)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT a FROM global_tuple WHERE (a, b) GLOBAL IN ((1, 2), (5, 6)) ORDER BY a",
        )
        .unwrap();
    assert_table_eq!(result, [[1], [5]]);
}

#[test]
fn test_global_in_string() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE global_str (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO global_str VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM global_str WHERE name GLOBAL IN ('alice', 'charlie') ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_global_in_null_handling() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE global_null (id INT64, val Nullable(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO global_null VALUES (1, 10), (2, NULL), (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM global_null WHERE val GLOBAL IN (10, 30) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_global_in_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE global_join_a (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE global_join_b (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE global_join_lookup (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO global_join_a VALUES (1), (2), (3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO global_join_b VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO global_join_lookup VALUES (1), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a.id, b.value FROM global_join_a a JOIN global_join_b b ON a.id = b.id WHERE a.id GLOBAL IN (SELECT id FROM global_join_lookup) ORDER BY a.id")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_global_in_empty() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE global_empty (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO global_empty VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM global_empty WHERE id GLOBAL IN (SELECT id FROM global_empty WHERE id > 100)")
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_global_in_large_set() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE global_large (id INT64)")
        .unwrap();
    for i in 1..=100 {
        executor
            .execute_sql(&format!("INSERT INTO global_large VALUES ({})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM global_large WHERE id GLOBAL IN (SELECT id FROM global_large WHERE id % 2 = 0)")
        .unwrap();
    assert_table_eq!(result, [[50]]);
}

#[test]
fn test_global_in_array() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE global_arr (id INT64, tags Array(String))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO global_arr VALUES (1, ['a', 'b']), (2, ['c', 'd']), (3, ['a', 'c'])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM global_arr WHERE hasAny(tags, ['a']) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}
