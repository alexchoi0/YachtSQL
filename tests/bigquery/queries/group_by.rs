use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::{create_session, null};

async fn setup_sales_table(session: &YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE sales (id INT64, product STRING, category STRING, amount INT64, quantity INT64)").await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 'Widget', 'Electronics', 100, 2), (2, 'Gadget', 'Electronics', 200, 1), (3, 'Chair', 'Furniture', 150, 3), (4, 'Table', 'Furniture', 300, 1), (5, 'Widget', 'Electronics', 100, 5)").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_single_column() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 400], ["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_count() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, COUNT(*) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 3], ["Furniture", 2],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_avg() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, AVG(amount) FROM sales GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 133.33333333333334], ["Furniture", 225.0]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_min_max() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, MIN(amount), MAX(amount) FROM sales GROUP BY category ORDER BY category").await
        .unwrap();

    assert_table_eq!(
        result,
        [["Electronics", 100, 200], ["Furniture", 150, 300],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_multiple_columns() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, product, SUM(quantity) FROM sales GROUP BY category, product ORDER BY category, product").await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Electronics", "Gadget", 1],
            ["Electronics", "Widget", 7],
            ["Furniture", "Chair", 3],
            ["Furniture", "Table", 1],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_having() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) FROM sales GROUP BY category HAVING SUM(amount) > 400 ORDER BY category").await
        .unwrap();

    assert_table_eq!(result, [["Furniture", 450],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_having_count() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, COUNT(*) FROM sales GROUP BY category HAVING COUNT(*) > 2 ORDER BY category").await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 3],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_where_and_having() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, SUM(amount) FROM sales WHERE quantity > 1 GROUP BY category HAVING SUM(amount) > 100 ORDER BY category").await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 200], ["Furniture", 150],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_all_rows_same_group() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE items (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (10), (20), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT SUM(value), COUNT(*), AVG(value) FROM items")
        .await
        .unwrap();

    assert_table_eq!(result, [[60, 3, 20.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_group_by_with_null_values() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data (category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES ('A', 10), ('A', 20), (NULL, 30), (NULL, 40)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT category, SUM(value) FROM data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    assert_table_eq!(result, [["A", 30], [null(), 70]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_count_distinct() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT category, COUNT(DISTINCT product) FROM sales GROUP BY category ORDER BY category").await
        .unwrap();

    assert_table_eq!(result, [["Electronics", 2], ["Furniture", 2],]);
}
