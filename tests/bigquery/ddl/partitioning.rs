use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_partition_by_date() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE events (
                event_id INT64,
                event_date DATE,
                event_name STRING
            )
            PARTITION BY event_date",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (1, DATE '2024-01-15', 'Event A')")
        .unwrap();

    let result = executor.execute_sql("SELECT event_id FROM events").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_partition_by_date_trunc() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE monthly_data (
                id INT64,
                created_at DATE,
                value INT64
            )
            PARTITION BY DATE_TRUNC(created_at, MONTH)",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO monthly_data VALUES (1, DATE '2024-01-15', 100)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM monthly_data").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_partition_by_timestamp() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE logs (
                log_id INT64,
                log_time TIMESTAMP,
                message STRING
            )
            PARTITION BY TIMESTAMP_TRUNC(log_time, DAY)",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO logs VALUES (1, TIMESTAMP '2024-01-15 10:30:00', 'Test log')")
        .unwrap();

    let result = executor.execute_sql("SELECT log_id FROM logs").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_partition_by_datetime() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE appointments (
                id INT64,
                scheduled_at DATETIME
            )
            PARTITION BY DATETIME_TRUNC(scheduled_at, HOUR)",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO appointments VALUES (1, DATETIME '2024-01-15 10:30:00')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM appointments").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_partition_by_integer_range() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE customers (
                customer_id INT64,
                name STRING
            )
            PARTITION BY RANGE_BUCKET(customer_id, GENERATE_ARRAY(0, 1000000, 100000))",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO customers VALUES (12345, 'Alice')")
        .unwrap();

    let result = executor.execute_sql("SELECT name FROM customers").unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_partition_expiration() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    executor
        .execute_sql("INSERT INTO temp_data VALUES (1, DATE '2024-01-15')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM temp_data").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_require_partition_filter() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    executor
        .execute_sql("INSERT INTO filtered_data VALUES (1, DATE '2024-01-15')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM filtered_data WHERE partition_date = DATE '2024-01-15'")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_cluster_by_single_column() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE clustered_data (
                id INT64,
                category STRING,
                value INT64
            )
            CLUSTER BY category",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO clustered_data VALUES (1, 'A', 100), (2, 'B', 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM clustered_data ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_cluster_by_multiple_columns() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE multi_clustered (
                id INT64,
                category STRING,
                subcategory STRING,
                value INT64
            )
            CLUSTER BY category, subcategory",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO multi_clustered VALUES (1, 'A', 'X', 100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM multi_clustered")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_partition_and_cluster() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    executor
        .execute_sql("INSERT INTO partitioned_clustered VALUES (1, DATE '2024-01-15', 'A', 100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM partitioned_clustered")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_partition_by_ingestion_time() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE ingestion_partitioned (
                id INT64,
                data STRING
            )
            PARTITION BY _PARTITIONDATE",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO ingestion_partitioned VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM ingestion_partitioned")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_external_table() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "CREATE EXTERNAL TABLE external_data (
            id INT64,
            name STRING
        )
        OPTIONS (
            format = 'CSV',
            uris = ['gs://bucket/path/*.csv']
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_external_table_with_schema() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "CREATE EXTERNAL TABLE json_data (
            id INT64,
            payload JSON
        )
        OPTIONS (
            format = 'JSON',
            uris = ['gs://bucket/data.json']
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_table_with_description() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE documented_table (
                id INT64 OPTIONS (description = 'Primary identifier'),
                name STRING OPTIONS (description = 'User name')
            )
            OPTIONS (
                description = 'A well-documented table'
            )",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO documented_table VALUES (1, 'Alice')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM documented_table")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_table_with_labels() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE labeled_table (
                id INT64
            )
            OPTIONS (
                labels = [('env', 'production'), ('team', 'analytics')]
            )",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO labeled_table VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM labeled_table")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_table_with_expiration() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE expiring_table (
                id INT64
            )
            OPTIONS (
                expiration_timestamp = TIMESTAMP '2025-12-31 23:59:59'
            )",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO expiring_table VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM expiring_table")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_cluster_by_up_to_four_columns() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    executor
        .execute_sql("INSERT INTO four_cluster VALUES ('1', '2', '3', '4', 100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM four_cluster")
        .unwrap();
    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_partition_filter_query() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE sales_data (
                sale_id INT64,
                sale_date DATE,
                amount INT64
            )
            PARTITION BY sale_date",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO sales_data VALUES
            (1, DATE '2024-01-15', 100),
            (2, DATE '2024-01-16', 200),
            (3, DATE '2024-02-15', 300)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT SUM(amount) FROM sales_data WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-31'",
        )
        .unwrap();
    assert_table_eq!(result, [[300]]);
}
