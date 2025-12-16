use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_distinct_single_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE colors (id INT64, color STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO colors VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green'), (5, 'blue')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT color FROM colors ORDER BY color")
        .unwrap();
    assert_table_eq!(result, [["blue"], ["green"], ["red"]]);
}

#[test]
fn test_distinct_multiple_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE orders (customer STRING, product STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES ('alice', 'apple'), ('bob', 'banana'), ('alice', 'apple'), ('alice', 'banana')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT customer, product FROM orders ORDER BY customer, product")
        .unwrap();
    assert_table_eq!(
        result,
        [["alice", "apple"], ["alice", "banana"], ["bob", "banana"]]
    );
}

#[test]
fn test_distinct_with_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nullable (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (2), (NULL), (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT value FROM nullable ORDER BY value NULLS FIRST")
        .unwrap();
    assert_table_eq!(result, [[null], [1], [2]]);
}

#[test]
fn test_count_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('a'), ('b'), ('a'), ('c'), ('b'), ('a')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT category) FROM items")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_distinct_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE products (name STRING, price INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO products VALUES ('a', 10), ('b', 20), ('a', 15), ('c', 10), ('b', 25)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT name FROM products WHERE price > 10 ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["a"], ["b"]]);
}

#[test]
fn test_distinct_with_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE scores (score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES (100), (50), (100), (75), (50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT score FROM scores ORDER BY score DESC")
        .unwrap();
    assert_table_eq!(result, [[100], [75], [50]]);
}

#[test]
fn test_distinct_with_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (1), (3), (2), (4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT val FROM data ORDER BY val LIMIT 2")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_distinct_all_same() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE same (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO same VALUES (5), (5), (5), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT val FROM same")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_distinct_all_different() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE diff (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO diff VALUES (1), (2), (3), (4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT val FROM diff ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4]]);
}
