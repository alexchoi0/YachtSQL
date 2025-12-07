use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_buffer_create() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE dest_table (id INT64, value INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE buffer_table AS dest_table
            ENGINE = Buffer(default, dest_table, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_buffer_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE dest_insert (id INT64, name String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE buffer_insert AS dest_insert
            ENGINE = Buffer(default, dest_insert, 16, 10, 100, 10000, 1000000, 10000000, 100000000)"
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO buffer_insert VALUES (1, 'test'), (2, 'data')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name FROM buffer_insert ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "test"], [2, "data"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_buffer_select() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE dest_select (id INT64, value INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE buffer_select AS dest_select
            ENGINE = Buffer(default, dest_select, 16, 10, 100, 10000, 1000000, 10000000, 100000000)"
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO buffer_select VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM buffer_select WHERE value > 150 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_buffer_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dest_agg (category String, value INT64) ENGINE = MergeTree ORDER BY category")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE buffer_agg AS dest_agg
            ENGINE = Buffer(default, dest_agg, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO buffer_agg VALUES ('A', 10), ('A', 20), ('B', 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, SUM(value) FROM buffer_agg GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_buffer_flush() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dest_flush (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE buffer_flush AS dest_flush
            ENGINE = Buffer(default, dest_flush, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO buffer_flush VALUES (1), (2), (3)")
        .unwrap();
    executor.execute_sql("OPTIMIZE TABLE buffer_flush").unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM dest_flush")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_buffer_multiple_inserts() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dest_multi (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE buffer_multi AS dest_multi
            ENGINE = Buffer(default, dest_multi, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO buffer_multi VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO buffer_multi VALUES (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO buffer_multi VALUES (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM buffer_multi")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_buffer_join() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE dest_left (id INT64, name String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE lookup_table (id INT64, value INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE buffer_left AS dest_left
            ENGINE = Buffer(default, dest_left, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO buffer_left VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lookup_table VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT b.name, l.value FROM buffer_left b JOIN lookup_table l ON b.id = l.id ORDER BY l.value")
        .unwrap();
    assert_table_eq!(result, [["alice", 100], ["bob", 200]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_buffer_with_nullable() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dest_null (id INT64, value Nullable(Int64)) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE buffer_null AS dest_null
            ENGINE = Buffer(default, dest_null, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO buffer_null VALUES (1, 100), (2, NULL), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM buffer_null WHERE value IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_buffer_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE dest_order (id INT64, score INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE buffer_order AS dest_order
            ENGINE = Buffer(default, dest_order, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO buffer_order VALUES (1, 50), (2, 100), (3, 75)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM buffer_order ORDER BY score DESC")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}
