use crate::common::create_executor;

#[test]
fn test_export_data_csv() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_test (id INT64, name STRING, value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_test VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.75)")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.csv',
            format='CSV',
            overwrite=true,
            header=true
        ) AS SELECT * FROM export_test",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_json() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_json (id INT64, data STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_json VALUES (1, 'test')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.json',
            format='JSON'
        ) AS SELECT * FROM export_json",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_parquet() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_parquet (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_parquet VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.parquet',
            format='PARQUET',
            compression='SNAPPY'
        ) AS SELECT * FROM export_parquet",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_avro() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_avro (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_avro VALUES (1, 'test')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.avro',
            format='AVRO'
        ) AS SELECT * FROM export_avro",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_with_field_delimiter() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_delim (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_delim VALUES (1, 'test')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.csv',
            format='CSV',
            field_delimiter='|'
        ) AS SELECT * FROM export_delim",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_with_query() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (id INT64, product STRING, amount FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.csv',
            format='CSV'
        ) AS SELECT product, SUM(amount) AS total FROM sales GROUP BY product",
    );
    assert!(result.is_ok());
}

#[test]
fn test_load_data_csv() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE load_test (id INT64, name STRING, value FLOAT64)")
        .unwrap();

    let result = executor.execute_sql(
        "LOAD DATA INTO load_test
        FROM FILES (
            format='CSV',
            uris=['gs://bucket/data/*.csv']
        )",
    );
    assert!(result.is_ok());
}

#[test]
fn test_load_data_overwrite() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE load_overwrite (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO load_overwrite VALUES (1, 100)")
        .unwrap();

    let result = executor.execute_sql(
        "LOAD DATA OVERWRITE load_overwrite
        FROM FILES (
            format='CSV',
            uris=['gs://bucket/data/*.csv']
        )",
    );
    assert!(result.is_ok());
}

#[test]
fn test_load_data_json() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE load_json (id INT64, data STRING)")
        .unwrap();

    let result = executor.execute_sql(
        "LOAD DATA INTO load_json
        FROM FILES (
            format='JSON',
            uris=['gs://bucket/data/*.json']
        )",
    );
    assert!(result.is_ok());
}

#[test]
fn test_load_data_parquet() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE load_parquet (id INT64, value FLOAT64)")
        .unwrap();

    let result = executor.execute_sql(
        "LOAD DATA INTO load_parquet
        FROM FILES (
            format='PARQUET',
            uris=['gs://bucket/data/*.parquet']
        )",
    );
    assert!(result.is_ok());
}

#[test]
fn test_load_data_with_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE load_partitioned (id INT64, dt DATE, value INT64) PARTITION BY dt",
        )
        .unwrap();

    let result = executor.execute_sql(
        "LOAD DATA INTO load_partitioned
        FROM FILES (
            format='CSV',
            uris=['gs://bucket/data/*.csv']
        )",
    );
    assert!(result.is_ok());
}

#[test]
fn test_load_data_with_schema_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE load_schema (id INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "LOAD DATA INTO load_schema
        FROM FILES (
            format='CSV',
            uris=['gs://bucket/data/*.csv'],
            allow_schema_update=true
        )",
    );
    assert!(result.is_ok());
}

#[test]
fn test_load_data_temp_table() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "LOAD DATA INTO TEMP TABLE temp_load (id INT64, value STRING)
        FROM FILES (
            format='CSV',
            uris=['gs://bucket/data/*.csv']
        )",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_partitioned() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_part (dt DATE, category STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_part VALUES (DATE '2024-01-01', 'A', 100), (DATE '2024-01-02', 'B', 200)")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*',
            format='PARQUET'
        ) AS SELECT * FROM export_part",
    );
    assert!(result.is_ok());
}
