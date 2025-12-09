use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE prewhere_basic (id INT64, name String, value INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO prewhere_basic VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name FROM prewhere_basic PREWHERE id > 1 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2, "b"], [3, "c"]]);
}

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE prewhere_where (id INT64, status String, value INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO prewhere_where VALUES (1, 'active', 100), (2, 'inactive', 200), (3, 'active', 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM prewhere_where PREWHERE status = 'active' WHERE value > 150")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_and_condition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE prewhere_and (id INT64, a INT64, b INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO prewhere_and VALUES (1, 10, 20), (2, 30, 40), (3, 50, 60)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM prewhere_and PREWHERE a > 15 AND b < 55 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_or_condition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE prewhere_or (id INT64, type String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO prewhere_or VALUES (1, 'A'), (2, 'B'), (3, 'C')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM prewhere_or PREWHERE type = 'A' OR type = 'C' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_in() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE prewhere_in (id INT64, category INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO prewhere_in VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM prewhere_in PREWHERE category IN (10, 30) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_between() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE prewhere_between (id INT64, value INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO prewhere_between VALUES (1, 10), (2, 25), (3, 50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM prewhere_between PREWHERE value BETWEEN 20 AND 40 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_like() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE prewhere_like (id INT64, name String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO prewhere_like VALUES (1, 'alice'), (2, 'bob'), (3, 'alicia')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM prewhere_like PREWHERE name LIKE 'ali%' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_is_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE prewhere_null (id INT64, value Nullable(Int64)) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO prewhere_null VALUES (1, 100), (2, NULL), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM prewhere_null PREWHERE value IS NOT NULL ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_function() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE prewhere_func (id INT64, name String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO prewhere_func VALUES (1, 'SHORT'), (2, 'VERYLONGNAME'), (3, 'MED')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM prewhere_func PREWHERE length(name) > 5 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE prewhere_agg (category String, value INT64) ENGINE = MergeTree ORDER BY category")
        .unwrap();
    executor
        .execute_sql("INSERT INTO prewhere_agg VALUES ('A', 10), ('A', 20), ('B', 30), ('B', 40)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) FROM prewhere_agg PREWHERE value > 15 GROUP BY category ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", 20], ["B", 70]]);
}

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE prewhere_order (id INT64, score INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO prewhere_order VALUES (1, 100), (2, 50), (3, 75)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM prewhere_order PREWHERE score > 60 ORDER BY score DESC")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[ignore = "Requires PREWHERE support"]
#[test]
fn test_prewhere_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE prewhere_limit (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO prewhere_limit VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM prewhere_limit PREWHERE id > 1 ORDER BY id LIMIT 2")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}
