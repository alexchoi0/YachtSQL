use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_summing_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE smt_basic (
                id INT64,
                value Int64
            ) ENGINE = SummingMergeTree(value)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_basic VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_basic VALUES (1, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_basic VALUES (1, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM smt_basic FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 60]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_summing_multiple_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE smt_multi (
                id INT64,
                count Int64,
                amount Int64
            ) ENGINE = SummingMergeTree((count, amount))
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_multi VALUES (1, 1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_multi VALUES (1, 2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, count, amount FROM smt_multi FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 3, 300]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_summing_with_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE smt_partition (
                dt Date,
                id INT64,
                value Int64
            ) ENGINE = SummingMergeTree(value)
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_partition VALUES ('2023-01-15', 1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_partition VALUES ('2023-01-20', 1, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_partition VALUES ('2023-02-15', 1, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM smt_partition FINAL ORDER BY dt")
        .unwrap();
    assert_table_eq!(result, [[1, 30], [1, 30]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_summing_compound_key() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE smt_compound (
                region String,
                category String,
                value Int64
            ) ENGINE = SummingMergeTree(value)
            ORDER BY (region, category)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_compound VALUES ('US', 'A', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_compound VALUES ('US', 'A', 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_compound VALUES ('US', 'B', 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT region, category, value FROM smt_compound FINAL ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["US", "A", 300], ["US", "B", 300]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_summing_no_columns_specified() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE smt_auto (
                id INT64,
                count Int64,
                amount Int64
            ) ENGINE = SummingMergeTree
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_auto VALUES (1, 1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_auto VALUES (1, 2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, count, amount FROM smt_auto FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 3, 300]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_summing_with_map() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE smt_map (
                id INT64,
                metrics SimpleAggregateFunction(sumMap, Tuple(Array(String), Array(Int64)))
            ) ENGINE = SummingMergeTree
            ORDER BY id",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_summing_negative_values() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE smt_negative (
                id INT64,
                value Int64
            ) ENGINE = SummingMergeTree(value)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_negative VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_negative VALUES (1, -50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM smt_negative FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 50]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_summing_zero_result() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE smt_zero (
                id INT64,
                value Int64
            ) ENGINE = SummingMergeTree(value)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_zero VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_zero VALUES (1, -100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM smt_zero FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, 0]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_summing_with_non_summed_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE smt_mixed (
                id INT64,
                name String,
                value Int64
            ) ENGINE = SummingMergeTree(value)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_mixed VALUES (1, 'first', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_mixed VALUES (1, 'second', 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, value FROM smt_mixed FINAL")
        .unwrap();
    assert_table_eq!(result, [[1, "first", 300]]);
}

#[test]
fn test_summing_aggregate_query() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE smt_agg_query (
                category String,
                id INT64,
                value Int64
            ) ENGINE = SummingMergeTree(value)
            ORDER BY (category, id)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO smt_agg_query VALUES ('A', 1, 10), ('A', 2, 20), ('B', 1, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) FROM smt_agg_query FINAL GROUP BY category ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}
