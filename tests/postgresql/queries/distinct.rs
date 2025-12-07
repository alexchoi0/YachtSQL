use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_distinct_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (name STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO items VALUES ('apple'), ('banana'), ('apple'), ('cherry'), ('banana')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT name FROM items ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["apple"], ["banana"], ["cherry"]]);
}

#[test]
fn test_distinct_integers() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (1), (2), (1), (3), (2), (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT val FROM nums ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
fn test_distinct_multiple_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1, 1), (1, 2), (1, 1), (2, 1), (1, 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT a, b FROM pairs ORDER BY a, b")
        .unwrap();
    assert_table_eq!(result, [[1, 1], [1, 2], [2, 1],]);
}

#[test]
fn test_distinct_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (category STRING, name STRING)")
        .unwrap();
    executor.execute_sql("INSERT INTO items VALUES ('fruit', 'apple'), ('fruit', 'apple'), ('veg', 'carrot'), ('fruit', 'banana')").unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT name FROM items WHERE category = 'fruit' ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["apple"], ["banana"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_distinct_all() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (1), (2), (1), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT ALL val FROM nums ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
fn test_distinct_on_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (product STRING, region STRING, amount INT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO sales VALUES ('A', 'East', 100), ('A', 'West', 150), ('B', 'East', 200), ('B', 'West', 50)").unwrap();

    let result = executor.execute_sql("SELECT DISTINCT ON (product) product, region, amount FROM sales ORDER BY product, amount DESC").unwrap();
    assert_table_eq!(result, [["A", "West", 150], ["B", "East", 200]]);
}

#[test]
fn test_distinct_with_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (name STRING, price INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO items VALUES ('apple', 5), ('banana', 3), ('apple', 7), ('cherry', 4)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT name FROM items ORDER BY name DESC")
        .unwrap();
    assert_table_eq!(result, [["cherry"], ["banana"], ["apple"]]);
}

#[test]
fn test_distinct_with_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (3), (1), (2), (1), (3), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT val FROM nums ORDER BY val LIMIT 2")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_count_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('a'), ('b'), ('a'), ('c'), ('b')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT name) FROM items")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_distinct_null_handling() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1), (NULL), (2), (NULL), (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT val FROM items ORDER BY val NULLS FIRST")
        .unwrap();
    assert_table_eq!(result, [[null], [1], [2]]);
}
