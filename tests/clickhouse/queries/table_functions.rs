#![allow(unused_variables)]

use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_numbers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM numbers(5)").unwrap();
    assert!(result.num_rows() == 5);
}

#[test]
fn test_numbers_with_offset() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM numbers(10, 5)")
        .unwrap();
    assert!(result.num_rows() == 5);
}

#[test]
fn test_numbers_mt() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM numbers_mt(10)")
        .unwrap();
    assert!(result.num_rows() == 10);
}

#[test]
fn test_zeros() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM zeros(5)").unwrap();
    assert!(result.num_rows() == 5);
}

#[test]
fn test_zeros_mt() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM zeros_mt(10)").unwrap();
    assert!(result.num_rows() == 10);
}

#[ignore = "Requires implicit system table resolution (SELECT * FROM one -> system.one)"]
#[test]
fn test_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM one").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_system_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM system.one").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_generate_series() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM generateSeries(1, 10)")
        .unwrap();
    assert!(result.num_rows() == 10);
}

#[test]
fn test_generate_series_with_step() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM generateSeries(0, 10, 2)")
        .unwrap();
    assert!(result.num_rows() == 6);
}

#[ignore = "Requires streaming table function support"]
#[test]
fn test_generate_random() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM generateRandom('a UInt64, b String, c Float64') LIMIT 5")
        .unwrap();
    assert!(result.num_rows() == 5);
}

#[ignore = "Requires streaming table function support"]
#[test]
fn test_generate_random_with_seed() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM generateRandom('id UInt32, name String', 12345) LIMIT 3")
        .unwrap();
    assert!(result.num_rows() == 3);
}

#[ignore = "Requires VALUES table function parsing"]
#[test]
fn test_values() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT * FROM VALUES('a UInt32, b String', (1, 'one'), (2, 'two'), (3, 'three'))",
        )
        .unwrap();
    assert!(result.num_rows() == 3);
}

#[test]
fn test_null_table_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM null('a Int64, b String')")
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Requires MERGE table function"]
#[test]
fn test_merge() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE merge_t1 (id Int64, val String)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE merge_t2 (id Int64, val String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO merge_t1 VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO merge_t2 VALUES (3, 'c'), (4, 'd')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM merge(currentDatabase(), 'merge_t.*') ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 4);
}

#[ignore = "Requires cluster function support"]
#[test]
fn test_cluster() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM cluster('test_cluster', system.one)")
        .unwrap();
}

#[ignore = "Requires cluster function support"]
#[test]
fn test_cluster_all_replicas() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM clusterAllReplicas('test_cluster', system.one)")
        .unwrap();
}

#[ignore = "Requires VIEW table function parsing"]
#[test]
fn test_view_table_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM view(SELECT 1 AS n, 'hello' AS s)")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[ignore = "Requires INPUT table function with FORMAT"]
#[test]
fn test_input_table_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT * FROM input('a UInt32, b String') FORMAT Values (1, 'test'), (2, 'data')",
        )
        .unwrap();
}

#[test]
fn test_numbers_in_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id UInt64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT n.number, i.name
            FROM numbers(5) AS n
            LEFT JOIN items AS i ON n.number = i.id
            ORDER BY n.number",
        )
        .unwrap();
    assert!(result.num_rows() == 5);
}

#[test]
fn test_numbers_aggregate() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT sum(number), avg(number), max(number) FROM numbers(100)")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_numbers_with_calculation() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT number, number * 2 AS doubled, number * number AS squared FROM numbers(5)",
        )
        .unwrap();
    assert!(result.num_rows() == 5);
}

#[test]
fn test_numbers_filtered() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT * FROM numbers(100) WHERE number % 10 = 0")
        .unwrap();
    assert!(result.num_rows() == 10);
}
