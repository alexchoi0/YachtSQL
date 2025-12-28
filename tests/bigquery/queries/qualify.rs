use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_sales_table(session: &yachtsql::YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE sales (id INT64, product STRING, region STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 'Widget', 'East', 100), (2, 'Widget', 'East', 150), (3, 'Widget', 'West', 200), (4, 'Gadget', 'East', 80), (5, 'Gadget', 'West', 120), (6, 'Gadget', 'West', 90)").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_row_number() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, region, amount FROM sales QUALIFY ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) = 1 ORDER BY product").await
        .unwrap();
    assert_table_eq!(
        result,
        [[5, "Gadget", "West", 120], [3, "Widget", "West", 200],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_rank() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY RANK() OVER (ORDER BY amount DESC) <= 3 ORDER BY amount DESC").await
        .unwrap();
    assert_table_eq!(
        result,
        [[3, "Widget", 200], [2, "Widget", 150], [5, "Gadget", 120],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_dense_rank() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE scores (id INT64, name STRING, score INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES (1, 'Alice', 100), (2, 'Bob', 100), (3, 'Charlie', 90), (4, 'Diana', 90), (5, 'Eve', 80)").await
        .unwrap();

    let result = session
        .execute_sql("SELECT name, score FROM scores QUALIFY DENSE_RANK() OVER (ORDER BY score DESC) <= 2 ORDER BY name").await
        .unwrap();
    assert_table_eq!(
        result,
        [["Alice", 100], ["Bob", 100], ["Charlie", 90], ["Diana", 90],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_where() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales WHERE region = 'East' QUALIFY ROW_NUMBER() OVER (ORDER BY amount DESC) = 1").await
        .unwrap();
    assert_table_eq!(result, [[2, "Widget", 150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_group_by() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT region, SUM(amount) AS total FROM sales GROUP BY region QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) = 1").await
        .unwrap();
    assert_table_eq!(result, [["West", 410]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_ntile() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql(
            "SELECT id, amount FROM sales QUALIFY NTILE(3) OVER (ORDER BY amount) = 1 ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[4, 80], [6, 90]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_percent_rank() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, amount FROM sales QUALIFY PERCENT_RANK() OVER (ORDER BY amount) >= 0.5 ORDER BY id").await
        .unwrap();
    assert_table_eq!(result, [[2, 150], [3, 200], [5, 120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_cume_dist() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, amount FROM sales QUALIFY CUME_DIST() OVER (ORDER BY amount) <= 0.5 ORDER BY id").await
        .unwrap();
    assert_table_eq!(result, [[1, 100], [4, 80], [6, 90]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_lag() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 15), (4, 25)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value FROM data QUALIFY value > LAG(value) OVER (ORDER BY id) ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[2, 20], [4, 25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_lead() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 15), (4, 25)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT id, value FROM data QUALIFY value < LEAD(value) OVER (ORDER BY id) ORDER BY id",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [3, 15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_first_value() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount = FIRST_VALUE(amount) OVER (PARTITION BY product ORDER BY amount DESC) ORDER BY product").await
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_last_value() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount = LAST_VALUE(amount) OVER (PARTITION BY product ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY product").await
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_multiple_conditions() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT id, product, region, amount FROM sales QUALIFY ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) <= 2 AND ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) = 1 ORDER BY id").await
        .unwrap();
    assert_table_eq!(
        result,
        [[2, "Widget", "East", 150], [3, "Widget", "West", 200],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_cte() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) AS rn FROM sales) SELECT id, product, amount FROM ranked QUALIFY rn = 1 ORDER BY product").await
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_qualify_with_aggregation() {
    let session = create_session();
    setup_sales_table(&session).await;

    let result = session
        .execute_sql("SELECT product, SUM(amount) AS total, AVG(amount) AS avg_amount FROM sales GROUP BY product QUALIFY SUM(amount) = MAX(SUM(amount)) OVER ()").await
        .unwrap();
    assert_table_eq!(result, [["Widget", 450, 150.0]]);
}
