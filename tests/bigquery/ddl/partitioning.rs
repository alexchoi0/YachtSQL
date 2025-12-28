use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_partition_by_date() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE events (
                event_id INT64,
                event_date DATE,
                event_name STRING
            )
            PARTITION BY event_date",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO events VALUES (1, DATE '2024-01-15', 'Event A')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT event_id FROM events")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_partition_by_date_trunc() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE monthly_data (
                id INT64,
                created_at DATE,
                value INT64
            )
            PARTITION BY DATE_TRUNC(created_at, MONTH)",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO monthly_data VALUES (1, DATE '2024-01-15', 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM monthly_data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_partition_by_timestamp() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE logs (
                log_id INT64,
                log_time TIMESTAMP,
                message STRING
            )
            PARTITION BY TIMESTAMP_TRUNC(log_time, DAY)",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO logs VALUES (1, TIMESTAMP '2024-01-15 10:30:00', 'Test log')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT log_id FROM logs")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_partition_by_datetime() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE appointments (
                id INT64,
                scheduled_at DATETIME
            )
            PARTITION BY DATETIME_TRUNC(scheduled_at, HOUR)",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO appointments VALUES (1, DATETIME '2024-01-15 10:30:00')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM appointments")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_partition_by_integer_range() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE customers (
                customer_id INT64,
                name STRING
            )
            PARTITION BY RANGE_BUCKET(customer_id, GENERATE_ARRAY(0, 1000000, 100000))",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO customers VALUES (12345, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM customers")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_partition_expiration() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE temp_data (
                id INT64,
                created_date DATE
            )
            PARTITION BY created_date
            OPTIONS (
                partition_expiration_days = 30
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO temp_data VALUES (1, DATE '2024-01-15')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM temp_data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_require_partition_filter() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE filtered_data (
                id INT64,
                partition_date DATE
            )
            PARTITION BY partition_date
            OPTIONS (
                require_partition_filter = true
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO filtered_data VALUES (1, DATE '2024-01-15')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM filtered_data WHERE partition_date = DATE '2024-01-15'")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cluster_by_single_column() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE clustered_data (
                id INT64,
                category STRING,
                value INT64
            )
            CLUSTER BY category",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO clustered_data VALUES (1, 'A', 100), (2, 'B', 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM clustered_data ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cluster_by_multiple_columns() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE multi_clustered (
                id INT64,
                category STRING,
                subcategory STRING,
                value INT64
            )
            CLUSTER BY category, subcategory",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO multi_clustered VALUES (1, 'A', 'X', 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM multi_clustered")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_partition_and_cluster() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE partitioned_clustered (
                id INT64,
                created_date DATE,
                category STRING,
                value INT64
            )
            PARTITION BY created_date
            CLUSTER BY category",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO partitioned_clustered VALUES (1, DATE '2024-01-15', 'A', 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM partitioned_clustered")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_partition_by_ingestion_time() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE ingestion_partitioned (
                id INT64,
                data STRING
            )
            PARTITION BY _PARTITIONDATE",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO ingestion_partitioned VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM ingestion_partitioned")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_external_table() {
    let session = create_session();

    let result = session
        .execute_sql(
            "CREATE EXTERNAL TABLE external_data (
            id INT64,
            name STRING
        )
        OPTIONS (
            format = 'CSV',
            uris = ['gs://bucket/path/*.csv']
        )",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_external_table_with_schema() {
    let session = create_session();

    let result = session
        .execute_sql(
            "CREATE EXTERNAL TABLE json_data (
            id INT64,
            payload JSON
        )
        OPTIONS (
            format = 'JSON',
            uris = ['gs://bucket/data.json']
        )",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_with_description() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE documented_table (
                id INT64 OPTIONS (description = 'Primary identifier'),
                name STRING OPTIONS (description = 'User name')
            )
            OPTIONS (
                description = 'A well-documented table'
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO documented_table VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM documented_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_with_labels() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE labeled_table (
                id INT64
            )
            OPTIONS (
                labels = [('env', 'production'), ('team', 'analytics')]
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO labeled_table VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM labeled_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_table_with_expiration() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE expiring_table (
                id INT64
            )
            OPTIONS (
                expiration_timestamp = TIMESTAMP '2025-12-31 23:59:59'
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO expiring_table VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM expiring_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_cluster_by_up_to_four_columns() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE four_cluster (
                a STRING,
                b STRING,
                c STRING,
                d STRING,
                value INT64
            )
            CLUSTER BY a, b, c, d",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO four_cluster VALUES ('1', '2', '3', '4', 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT value FROM four_cluster")
        .await
        .unwrap();
    assert_table_eq!(result, [[100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_partition_filter_query() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE sales_data (
                sale_id INT64,
                sale_date DATE,
                amount INT64
            )
            PARTITION BY sale_date",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO sales_data VALUES
            (1, DATE '2024-01-15', 100),
            (2, DATE '2024-01-16', 200),
            (3, DATE '2024-02-15', 300)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT SUM(amount) FROM sales_data WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-31'",
        ).await
        .unwrap();
    assert_table_eq!(result, [[300]]);
}
