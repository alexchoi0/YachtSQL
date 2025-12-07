use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_distinct_single_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id INT64, category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT category FROM items ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A"], ["B"], ["C"]]);
}

#[test]
fn test_distinct_multiple_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (product STRING, region STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 'North', 100), ('Widget', 'South', 200), ('Widget', 'North', 150), ('Gadget', 'North', 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT product, region FROM sales ORDER BY product, region")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "North"],
            ["Widget", "North"],
            ["Widget", "South"],
        ]
    );
}

#[test]
fn test_distinct_with_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1), (NULL), (2), (NULL), (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT value FROM data WHERE value IS NOT NULL ORDER BY value")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_distinct_count() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id INT64, category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT category) FROM items")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_distinct_on_integers() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nums (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (1), (2), (2), (3), (3), (3), (4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT val FROM nums ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4]]);
}

#[test]
fn test_distinct_empty_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE empty (val INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT val FROM empty")
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_distinct_all_same() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE same (val STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO same VALUES ('same'), ('same'), ('same')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT val FROM same")
        .unwrap();
    assert_table_eq!(result, [["same"]]);
}

#[test]
fn test_distinct_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE products (name STRING, price INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO products VALUES ('A', 10), ('B', 20), ('A', 30), ('C', 20), ('B', 40)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT name FROM products WHERE price > 15 ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["A"], ["B"], ["C"]]);
}

#[test]
fn test_distinct_with_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (category STRING, priority INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('A', 3), ('B', 1), ('A', 2), ('C', 1), ('B', 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT category FROM items ORDER BY category DESC")
        .unwrap();
    assert_table_eq!(result, [["C"], ["B"], ["A"]]);
}

#[test]
fn test_distinct_with_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (5), (3), (5), (1), (3), (2), (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT val FROM data ORDER BY val LIMIT 3")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}
