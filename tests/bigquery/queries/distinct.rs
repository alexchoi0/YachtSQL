use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_single_column() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE colors (id INT64, color STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO colors VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green'), (5, 'blue')").await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT color FROM colors ORDER BY color")
        .await
        .unwrap();
    assert_table_eq!(result, [["blue"], ["green"], ["red"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_multiple_columns() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (customer STRING, product STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES ('alice', 'apple'), ('bob', 'banana'), ('alice', 'apple'), ('alice', 'banana')").await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT customer, product FROM orders ORDER BY customer, product")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [["alice", "apple"], ["alice", "banana"], ["bob", "banana"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE nullable (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (2), (NULL), (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT value FROM nullable ORDER BY value NULLS FIRST")
        .await
        .unwrap();
    assert_table_eq!(result, [[null], [1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE items (category STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES ('a'), ('b'), ('a'), ('c'), ('b'), ('a')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT category) FROM items")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_where() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE products (name STRING, price INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO products VALUES ('a', 10), ('b', 20), ('a', 15), ('c', 10), ('b', 25)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT name FROM products WHERE price > 10 ORDER BY name")
        .await
        .unwrap();
    assert_table_eq!(result, [["a"], ["b"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_order_by() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE scores (score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES (100), (50), (100), (75), (50)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT score FROM scores ORDER BY score DESC")
        .await
        .unwrap();
    assert_table_eq!(result, [[100], [75], [50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_with_limit() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1), (2), (1), (3), (2), (4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT val FROM data ORDER BY val LIMIT 2")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_all_same() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE same (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO same VALUES (5), (5), (5), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT val FROM same")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_distinct_all_different() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE diff (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO diff VALUES (1), (2), (3), (4)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT DISTINCT val FROM diff ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4]]);
}
