use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_memory_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mem_basic (id INT64, name String) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_basic VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name FROM mem_basic ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "alice"], [2, "bob"]]);
}

#[test]
fn test_memory_multiple_inserts() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mem_multi (id INT64) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_multi VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_multi VALUES (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_multi VALUES (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM mem_multi")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_memory_select_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mem_where (id INT64, value INT64) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_where VALUES (1, 100), (2, 200), (3, 150)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM mem_where WHERE value > 120 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_memory_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mem_agg (category String, value INT64) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_agg VALUES ('A', 10), ('A', 20), ('B', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) FROM mem_agg GROUP BY category ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 30]]);
}

#[test]
fn test_memory_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mem_left (id INT64, name String) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE mem_right (id INT64, value INT64) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_left VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_right VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT l.name, r.value FROM mem_left l JOIN mem_right r ON l.id = r.id ORDER BY r.value")
        .unwrap();
    assert_table_eq!(result, [["alice", 100], ["bob", 200]]);
}

#[test]
fn test_memory_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mem_order (id INT64, score INT64) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_order VALUES (1, 50), (2, 100), (3, 75)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM mem_order ORDER BY score DESC")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[test]
fn test_memory_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mem_limit (id INT64) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_limit VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM mem_limit ORDER BY id LIMIT 3")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
fn test_memory_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mem_distinct (value INT64) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_distinct VALUES (1), (2), (1), (3), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT value FROM mem_distinct ORDER BY value")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
fn test_memory_null_handling() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mem_null (id INT64, value Nullable(Int64)) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_null VALUES (1, 100), (2, NULL), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM mem_null WHERE value IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_memory_all_types() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mem_types (
                int_col INT64,
                str_col String,
                float_col Float64,
                date_col Date,
                arr_col Array(Int64)
            ) ENGINE = Memory",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_types VALUES (1, 'test', 3.14, '2023-01-15', [1, 2, 3])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT int_col, str_col FROM mem_types")
        .unwrap();
    assert_table_eq!(result, [[1, "test"]]);
}

#[test]
fn test_memory_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mem_sub (id INT64, value INT64) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mem_sub VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM mem_sub WHERE value > (SELECT AVG(value) FROM mem_sub)")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}
