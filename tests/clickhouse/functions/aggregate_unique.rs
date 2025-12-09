use crate::assert_table_eq;
use crate::common::{create_executor, d};

#[test]
fn test_uniq() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uniq_test VALUES ('a'), ('b'), ('a'), ('c'), ('b'), ('a')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniq(value) FROM uniq_test")
        .unwrap();
    assert_table_eq!(result, [[3]]); // 'a', 'b', 'c' are unique
}

#[test]
fn test_uniq_exact() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_exact_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uniq_exact_test VALUES ('a'), ('b'), ('a'), ('c'), ('b')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniqExact(value) FROM uniq_exact_test")
        .unwrap();
    assert_table_eq!(result, [[3]]); // 'a', 'b', 'c' are unique
}

#[test]
fn test_uniq_combined() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_comb_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uniq_comb_test VALUES ('a'), ('b'), ('c'), ('d'), ('e')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniqCombined(value) FROM uniq_comb_test")
        .unwrap();
    assert_table_eq!(result, [[5]]); // 'a', 'b', 'c', 'd', 'e' are unique
}

#[test]
fn test_uniq_combined64() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_comb64_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uniq_comb64_test VALUES ('a'), ('b'), ('c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniqCombined64(value) FROM uniq_comb64_test")
        .unwrap();
    assert_table_eq!(result, [[3]]); // 'a', 'b', 'c' are unique
}

#[test]
fn test_uniq_hll12() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_hll_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uniq_hll_test VALUES ('a'), ('b'), ('c'), ('d'), ('e')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniqHLL12(value) FROM uniq_hll_test")
        .unwrap();
    assert_table_eq!(result, [[5]]); // 'a', 'b', 'c', 'd', 'e' are unique
}

#[ignore = "Implement me!"]
#[test]
fn test_uniq_theta() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_theta_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uniq_theta_test VALUES ('a'), ('b'), ('c'), ('d')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniqTheta(value) FROM uniq_theta_test")
        .unwrap();
    assert_table_eq!(result, [[4]]); // 'a', 'b', 'c', 'd' are unique
}

#[ignore = "Implement me!"]
#[test]
fn test_uniq_up_to() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_upto_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uniq_upto_test VALUES ('a'), ('b'), ('c'), ('d'), ('e')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniqUpTo(3)(value) FROM uniq_upto_test")
        .unwrap();
    assert_table_eq!(result, [[4]]); // Returns 4 when >3 unique values (capped at N+1)
}

#[ignore = "Implement me!"]
#[test]
fn test_uniq_multiple_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_multi_test (col1 String, col2 String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO uniq_multi_test VALUES ('a', '1'), ('a', '2'), ('b', '1'), ('b', '1')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniq(col1, col2) FROM uniq_multi_test")
        .unwrap();
    assert_table_eq!(result, [[3]]); // ('a','1'), ('a','2'), ('b','1') are unique pairs
}

#[test]
fn test_uniq_grouped() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_grouped (category String, value String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO uniq_grouped VALUES
            ('A', 'x'), ('A', 'y'), ('A', 'x'),
            ('B', 'x'), ('B', 'y'), ('B', 'z')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, uniq(value)
            FROM uniq_grouped
            GROUP BY category
            ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["A", 2], // 'x', 'y' are unique in category A
            ["B", 3]  // 'x', 'y', 'z' are unique in category B
        ]
    );
}

#[ignore = "Implement me!"]
#[test]
fn test_uniq_with_if() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_if_test (active UInt8, value String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO uniq_if_test VALUES
            (1, 'a'), (1, 'b'), (1, 'a'),
            (0, 'c'), (0, 'd')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniqIf(value, active = 1) FROM uniq_if_test")
        .unwrap();
    assert_table_eq!(result, [[2]]); // 'a', 'b' are unique where active = 1
}

#[test]
fn test_uniq_numeric() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_num_test (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uniq_num_test VALUES (1), (2), (1), (3), (2), (4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniqExact(value) FROM uniq_num_test")
        .unwrap();
    assert_table_eq!(result, [[4]]); // 1, 2, 3, 4 are unique
}

#[test]
fn test_uniq_null_handling() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_null_test (value Nullable(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uniq_null_test VALUES ('a'), (NULL), ('b'), (NULL), ('a')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniq(value), uniqExact(value) FROM uniq_null_test")
        .unwrap();
    assert_table_eq!(result, [[2, 2]]); // 'a', 'b' are unique (NULL not counted)
}

#[test]
fn test_count_distinct_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE count_distinct_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO count_distinct_test VALUES ('a'), ('b'), ('a'), ('c'), ('b')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT COUNT(DISTINCT value), uniq(value), uniqExact(value) FROM count_distinct_test",
        )
        .unwrap();
    assert_table_eq!(result, [[3, 3, 3]]); // 'a', 'b', 'c' are unique
}

#[ignore = "Implement me!"]
#[test]
fn test_uniq_array_values() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_array_test (arr Array(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uniq_array_test VALUES (['a', 'b']), (['b', 'c']), (['a', 'd'])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniqExact(arrayJoin(arr)) FROM uniq_array_test")
        .unwrap();
    assert_table_eq!(result, [[4]]); // 'a', 'b', 'c', 'd' are unique across all arrays
}

#[ignore = "Implement me!"]
#[test]
fn test_sum_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sum_distinct_test (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sum_distinct_test VALUES (1), (2), (1), (3), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumDistinct(value) FROM sum_distinct_test")
        .unwrap();
    assert_table_eq!(result, [[6]]); // 1 + 2 + 3 = 6
}

#[ignore = "Implement me!"]
#[test]
fn test_avg_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE avg_distinct_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO avg_distinct_test VALUES (1), (2), (1), (3), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT avgDistinct(value) FROM avg_distinct_test")
        .unwrap();
    assert_table_eq!(result, [[2.0]]); // (1 + 2 + 3) / 3 = 2.0
}

#[test]
fn test_uniq_combined_with_precision() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_prec_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uniq_prec_test VALUES ('a'), ('b'), ('c'), ('d'), ('e')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniqCombined(12)(value) FROM uniq_prec_test")
        .unwrap();
    assert_table_eq!(result, [[5]]); // 'a', 'b', 'c', 'd', 'e' are unique
}

#[ignore = "Implement me!"]
#[test]
fn test_distinct_nested_array() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE distinct_nested (id INT64, tags Array(String))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO distinct_nested VALUES
            (1, ['a', 'b']),
            (2, ['b', 'c']),
            (3, ['a', 'c'])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT uniqExact(tag) FROM distinct_nested ARRAY JOIN tags AS tag")
        .unwrap();
    assert_table_eq!(result, [[3]]); // 'a', 'b', 'c' are unique tags
}

#[test]
fn test_uniq_state_merge() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_state_test (id INT64, value String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO uniq_state_test VALUES
            (1, 'a'), (1, 'b'), (1, 'a'),
            (2, 'b'), (2, 'c'), (2, 'd')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, uniq(value)
            FROM uniq_state_test
            GROUP BY id
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, 2], // 'a', 'b' unique for id 1
            [2, 3]  // 'b', 'c', 'd' unique for id 2
        ]
    );
}

#[test]
fn test_uniq_large_cardinality() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_large_test (value String)")
        .unwrap();

    let values: Vec<String> = (0..100).map(|i| format!("('value_{}')", i)).collect();
    executor
        .execute_sql(&format!(
            "INSERT INTO uniq_large_test VALUES {}",
            values.join(", ")
        ))
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT uniq(value), uniqExact(value), uniqCombined(value) FROM uniq_large_test",
        )
        .unwrap();
    assert_table_eq!(result, [[100, 100, 100]]); // 100 unique values
}

#[ignore = "Implement me!"]
#[test]
fn test_uniq_by_timestamp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_ts_test (ts DateTime, user_id String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO uniq_ts_test VALUES
            ('2023-01-01 10:00:00', 'user1'),
            ('2023-01-01 11:00:00', 'user2'),
            ('2023-01-01 12:00:00', 'user1'),
            ('2023-01-02 10:00:00', 'user3')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT toDate(ts) AS date, uniq(user_id)
            FROM uniq_ts_test
            GROUP BY date
            ORDER BY date",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(2023, 1, 1), 2], // user1, user2 on 2023-01-01
            [d(2023, 1, 2), 1]  // user3 on 2023-01-02
        ]
    );
}
