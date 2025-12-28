use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_csv() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_test (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_test VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.75)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.csv',
            format='CSV',
            overwrite=true,
            header=true
        ) AS SELECT * FROM export_test",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_json() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_json (id INT64, data STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_json VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.json',
            format='JSON'
        ) AS SELECT * FROM export_json",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_parquet() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_parquet (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_parquet VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.parquet',
            format='PARQUET',
            compression='SNAPPY'
        ) AS SELECT * FROM export_parquet",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_avro() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_avro (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_avro VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.avro',
            format='AVRO'
        ) AS SELECT * FROM export_avro",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_field_delimiter() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_delim (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_delim VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.csv',
            format='CSV',
            field_delimiter='|'
        ) AS SELECT * FROM export_delim",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_query() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (id INT64, product STRING, amount FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.csv',
            format='CSV'
        ) AS SELECT product, SUM(amount) AS total FROM sales GROUP BY product",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_csv() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE load_test (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "LOAD DATA INTO load_test
        FROM FILES (
            format='CSV',
            uris=['gs://bucket/data/*.csv']
        )",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_overwrite() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE load_overwrite (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO load_overwrite VALUES (1, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "LOAD DATA OVERWRITE load_overwrite
        FROM FILES (
            format='CSV',
            uris=['gs://bucket/data/*.csv']
        )",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_json() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE load_json (id INT64, data STRING)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "LOAD DATA INTO load_json
        FROM FILES (
            format='JSON',
            uris=['gs://bucket/data/*.json']
        )",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_parquet() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE load_parquet (id INT64, value FLOAT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "LOAD DATA INTO load_parquet
        FROM FILES (
            format='PARQUET',
            uris=['gs://bucket/data/*.parquet']
        )",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_partition() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE load_partitioned (id INT64, dt DATE, value INT64) PARTITION BY dt",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "LOAD DATA INTO load_partitioned
        FROM FILES (
            format='CSV',
            uris=['gs://bucket/data/*.csv']
        )",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_with_schema_update() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE load_schema (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "LOAD DATA INTO load_schema
        FROM FILES (
            format='CSV',
            uris=['gs://bucket/data/*.csv'],
            allow_schema_update=true
        )",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_load_data_temp_table() {
    let session = create_session();

    let result = session
        .execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_load (id INT64, value STRING)
        FROM FILES (
            format='CSV',
            uris=['gs://bucket/data/*.csv']
        )",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_partitioned() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_part (dt DATE, category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_part VALUES (DATE '2024-01-01', 'A', 100), (DATE '2024-01-02', 'B', 200)").await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*',
            format='PARQUET'
        ) AS SELECT * FROM export_part",
        )
        .await;
    assert!(result.is_ok());
}

mod local_parquet {
    use std::sync::Arc;

    use arrow::array::{
        BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMicrosecondBuilder,
    };
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use tempfile::NamedTempFile;
    use yachtsql_test_utils::{get_f64, get_i64, get_string, is_null};

    use super::*;

    fn create_simple_parquet() -> NamedTempFile {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new("score", ArrowDataType::Float64, true),
        ]));

        let mut id_builder = Int64Builder::new();
        let mut name_builder = StringBuilder::new();
        let mut score_builder = Float64Builder::new();

        id_builder.append_value(1);
        name_builder.append_value("Alice");
        score_builder.append_value(95.5);

        id_builder.append_value(2);
        name_builder.append_value("Bob");
        score_builder.append_null();

        id_builder.append_value(3);
        name_builder.append_null();
        score_builder.append_value(88.0);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(name_builder.finish()),
                Arc::new(score_builder.finish()),
            ],
        )
        .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        {
            let file = temp_file.reopen().unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        temp_file
    }

    fn create_typed_parquet() -> NamedTempFile {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("active", ArrowDataType::Boolean, true),
            ArrowField::new("created_date", ArrowDataType::Date32, true),
            ArrowField::new(
                "updated_at",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        let mut id_builder = Int64Builder::new();
        let mut active_builder = BooleanBuilder::new();
        let mut date_builder = Date32Builder::new();
        let mut ts_builder = TimestampMicrosecondBuilder::new();

        id_builder.append_value(1);
        active_builder.append_value(true);
        date_builder.append_value(19724);
        ts_builder.append_value(1704067200000000);

        id_builder.append_value(2);
        active_builder.append_value(false);
        date_builder.append_null();
        ts_builder.append_value(1704153600000000);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(active_builder.finish()),
                Arc::new(date_builder.finish()),
                Arc::new(ts_builder.finish()),
            ],
        )
        .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        {
            let file = temp_file.reopen().unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        temp_file
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_parquet_basic() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .await
            .unwrap();

        let temp_file = create_simple_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM users ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
        assert_eq!(get_f64(&result, 2, 0), 95.5);

        assert_eq!(get_i64(&result, 0, 1), 2);
        assert_eq!(get_string(&result, 1, 1), "Bob");
        assert!(is_null(&result, 2, 1));

        assert_eq!(get_i64(&result, 0, 2), 3);
        assert!(is_null(&result, 1, 2));
        assert_eq!(get_f64(&result, 2, 2), 88.0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_parquet_with_file_uri() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .await
            .unwrap();

        let temp_file = create_simple_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='PARQUET', URIS=['file://{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT COUNT(*) as cnt FROM users")
            .await
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_parquet_overwrite() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .await
            .unwrap();
        session
            .execute_sql("INSERT INTO users VALUES (100, 'Existing', 0.0)")
            .await
            .unwrap();

        let temp_file = create_simple_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA OVERWRITE users FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM users ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_i64(&result, 0, 1), 2);
        assert_eq!(get_i64(&result, 0, 2), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_parquet_append() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .await
            .unwrap();
        session
            .execute_sql("INSERT INTO users VALUES (0, 'Existing', 50.0)")
            .await
            .unwrap();

        let temp_file = create_simple_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT COUNT(*) as cnt FROM users")
            .await
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_parquet_typed_columns() {
        let session = create_session();
        session
            .execute_sql(
                "CREATE TABLE events (id INT64, active BOOL, created_date DATE, updated_at DATETIME)",
            ).await
            .unwrap();

        let temp_file = create_typed_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO events FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT id, active FROM events ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_i64(&result, 0, 1), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_parquet_basic() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64)")
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO products VALUES (1, 'Apple', 1.99), (2, 'Banana', 0.99), (3, 'Cherry', 2.50)",
            ).await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT * FROM products ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        let session2 = create_session();
        session2
            .execute_sql("CREATE TABLE imported (id INT64, name STRING, price FLOAT64)")
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        session2.execute_sql(&load_sql).await.unwrap();

        let result = session2
            .execute_sql("SELECT * FROM imported ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Apple");
        assert_eq!(get_f64(&result, 2, 0), 1.99);

        assert_eq!(get_i64(&result, 0, 1), 2);
        assert_eq!(get_string(&result, 1, 1), "Banana");
        assert_eq!(get_f64(&result, 2, 1), 0.99);

        assert_eq!(get_i64(&result, 0, 2), 3);
        assert_eq!(get_string(&result, 1, 2), "Cherry");
        assert_eq!(get_f64(&result, 2, 2), 2.5);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_parquet_with_aggregation() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE sales (region STRING, amount INT64)")
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO sales VALUES ('North', 100), ('South', 200), ('North', 150), ('South', 50)",
            ).await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT region, SUM(amount) as total FROM sales GROUP BY region ORDER BY region",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        let session2 = create_session();
        session2
            .execute_sql("CREATE TABLE region_totals (region STRING, total INT64)")
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO region_totals FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        session2.execute_sql(&load_sql).await.unwrap();

        let result = session2
            .execute_sql("SELECT * FROM region_totals ORDER BY region")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_string(&result, 0, 0), "North");
        assert_eq!(get_i64(&result, 1, 0), 250);
        assert_eq!(get_string(&result, 0, 1), "South");
        assert_eq!(get_i64(&result, 1, 1), 250);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_parquet_with_nulls() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE nullable_data (id INT64, value STRING)")
            .await
            .unwrap();
        session
            .execute_sql("INSERT INTO nullable_data VALUES (1, 'A'), (2, NULL), (3, 'C')")
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT * FROM nullable_data ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        let session2 = create_session();
        session2
            .execute_sql("CREATE TABLE imported_nullable (id INT64, value STRING)")
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_nullable FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        session2.execute_sql(&load_sql).await.unwrap();

        let result = session2
            .execute_sql("SELECT * FROM imported_nullable ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "A");
        assert_eq!(get_i64(&result, 0, 1), 2);
        assert!(is_null(&result, 1, 1));
        assert_eq!(get_i64(&result, 0, 2), 3);
        assert_eq!(get_string(&result, 1, 2), "C");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_roundtrip_all_basic_types() {
        let session = create_session();
        session
            .execute_sql(
                "CREATE TABLE all_types (
                    id INT64,
                    flag BOOL,
                    amount FLOAT64,
                    name STRING
                )",
            )
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO all_types VALUES
                    (1, TRUE, 100.5, 'test'),
                    (2, FALSE, NULL, NULL)",
            )
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT * FROM all_types ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        session
            .execute_sql(
                "CREATE TABLE imported_types (
                    id INT64,
                    flag BOOL,
                    amount FLOAT64,
                    name STRING
                )",
            )
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_types FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let original = session
            .execute_sql("SELECT * FROM all_types ORDER BY id")
            .await
            .unwrap();
        let imported = session
            .execute_sql("SELECT * FROM imported_types ORDER BY id")
            .await
            .unwrap();

        assert_eq!(original.num_rows(), imported.num_rows());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_roundtrip_date_datetime() {
        let session = create_session();
        session
            .execute_sql(
                "CREATE TABLE time_data (
                    id INT64,
                    event_date DATE,
                    event_time DATETIME
                )",
            )
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO time_data VALUES
                    (1, DATE '2024-01-15', DATETIME '2024-01-15 10:30:00'),
                    (2, DATE '2024-06-20', DATETIME '2024-06-20 14:45:30')",
            )
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT * FROM time_data ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        session
            .execute_sql(
                "CREATE TABLE imported_time (
                    id INT64,
                    event_date DATE,
                    event_time DATETIME
                )",
            )
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_time FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT id, event_date FROM imported_time ORDER BY id")
            .await
            .unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_filtered_query() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE orders (id INT64, status STRING, amount FLOAT64)")
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO orders VALUES
                    (1, 'completed', 100.0),
                    (2, 'pending', 50.0),
                    (3, 'completed', 200.0),
                    (4, 'cancelled', 75.0)",
            )
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT id, amount FROM orders WHERE status = 'completed' ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        let session2 = create_session();
        session2
            .execute_sql("CREATE TABLE completed_orders (id INT64, amount FLOAT64)")
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO completed_orders FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        session2.execute_sql(&load_sql).await.unwrap();

        let result = session2
            .execute_sql("SELECT * FROM completed_orders ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_f64(&result, 1, 0), 100.0);
        assert_eq!(get_i64(&result, 0, 1), 3);
        assert_eq!(get_f64(&result, 1, 1), 200.0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_parquet_column_subset() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE partial (id INT64, name STRING)")
            .await
            .unwrap();

        let temp_file = create_simple_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO partial FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM partial ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
        assert_eq!(get_i64(&result, 0, 1), 2);
        assert_eq!(get_string(&result, 1, 1), "Bob");
        assert_eq!(get_i64(&result, 0, 2), 3);
        assert!(is_null(&result, 1, 2));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_empty_result() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE empty_source (id INT64, value STRING)")
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT * FROM empty_source",
            path
        );
        let result = session.execute_sql(&export_sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_parquet_file_not_found() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE test_table (id INT64)")
            .await
            .unwrap();

        let result = session.execute_sql(
        "LOAD DATA INTO test_table FROM FILES (FORMAT='PARQUET', URIS=['/nonexistent/path/file.parquet'])",
    ).await;
        assert!(result.is_err());
    }
}

mod load_data_parsing {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_into_basic() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE target (id INT64, name STRING)")
            .await
            .unwrap();

        let result = session.execute_sql(
        "LOAD DATA INTO target FROM FILES (format='PARQUET', uris=['gs://bucket/file.parquet'])",
    ).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_overwrite_keyword() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE target (id INT64)")
            .await
            .unwrap();
        session
            .execute_sql("INSERT INTO target VALUES (1)")
            .await
            .unwrap();

        let result = session.execute_sql(
        "LOAD DATA OVERWRITE target FROM FILES (format='CSV', uris=['gs://bucket/data.csv'])",
    ).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_with_column_list() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE target (id INT64, name STRING, value FLOAT64)")
            .await
            .unwrap();

        let result = session.execute_sql(
        "LOAD DATA INTO target (id, name, value) FROM FILES (format='CSV', uris=['gs://bucket/data.csv'])",
    ).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_temp_table_with_schema() {
        let session = create_session();

        let result = session
            .execute_sql(
                "LOAD DATA INTO TEMP TABLE temp_data (id INT64, name STRING)
             FROM FILES (format='CSV', uris=['gs://bucket/data.csv'])",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_format_csv() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE csv_table (x INT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO csv_table FROM FILES (format='CSV', uris=['gs://b/f.csv'])",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_format_json() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE json_table (x INT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO json_table FROM FILES (format='JSON', uris=['gs://b/f.json'])",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_format_parquet() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE parquet_table (x INT64)")
            .await
            .unwrap();

        let result = session.execute_sql(
        "LOAD DATA INTO parquet_table FROM FILES (format='PARQUET', uris=['gs://b/f.parquet'])",
    ).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_format_newline_delimited_json() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE ndjson_table (x INT64)")
            .await
            .unwrap();

        let result = session.execute_sql(
        "LOAD DATA INTO ndjson_table FROM FILES (format='NEWLINE_DELIMITED_JSON', uris=['gs://b/f.json'])",
    ).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_multiple_uris() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE multi_file (id INT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO multi_file FROM FILES (
                format='CSV',
                uris=['gs://bucket/file1.csv', 'gs://bucket/file2.csv', 'gs://bucket/file3.csv']
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_uri_with_wildcard() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE wildcard_load (id INT64)")
            .await
            .unwrap();

        let result = session.execute_sql(
        "LOAD DATA INTO wildcard_load FROM FILES (format='CSV', uris=['gs://bucket/path/*.csv'])",
    ).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_csv_skip_leading_rows() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE skip_rows (id INT64, name STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO skip_rows FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                skip_leading_rows=1
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_csv_field_delimiter() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE delim_table (a INT64, b STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO delim_table FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.tsv'],
                field_delimiter='\\t'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_csv_allow_jagged_rows() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE jagged (a INT64, b STRING, c STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO jagged FROM FILES (
                format='CSV',
                uris=['gs://bucket/jagged.csv'],
                allow_jagged_rows=true
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_csv_allow_quoted_newlines() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE quoted_nl (id INT64, text STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO quoted_nl FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                allow_quoted_newlines=true
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_csv_null_marker() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE null_marker_table (id INT64, val STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO null_marker_table FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                null_marker='NA'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_csv_encoding() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE encoded_table (id INT64, text STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO encoded_table FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                encoding='UTF-8'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_csv_quote_character() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE quoted_table (id INT64, name STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO quoted_table FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                quote='\"'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_ignore_unknown_values() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE ignore_unknown (id INT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO ignore_unknown FROM FILES (
                format='JSON',
                uris=['gs://bucket/data.json'],
                ignore_unknown_values=true
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_max_bad_records() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE bad_records (id INT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO bad_records FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                max_bad_records=10
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_compression_gzip() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE compressed (id INT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO compressed FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv.gz'],
                compression='GZIP'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_case_insensitive_options() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE case_test (id INT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO case_test FROM FILES (
                FORMAT='csv',
                URIS=['gs://bucket/data.csv'],
                SKIP_LEADING_ROWS=1
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_qualified_table_name() {
        let session = create_session();
        session
            .execute_sql("CREATE SCHEMA IF NOT EXISTS test_dataset")
            .await
            .unwrap();
        session
            .execute_sql("CREATE TABLE test_dataset.target (id INT64)")
            .await
            .unwrap();

        let result = session.execute_sql(
        "LOAD DATA INTO test_dataset.target FROM FILES (format='CSV', uris=['gs://b/f.csv'])",
    ).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_combined_csv_options() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE combined_opts (id INT64, name STRING, value FLOAT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO combined_opts FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                skip_leading_rows=1,
                field_delimiter=',',
                allow_quoted_newlines=true,
                null_marker='NULL',
                encoding='UTF-8',
                max_bad_records=100
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_parquet_enable_list_inference() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE list_infer (id INT64, items ARRAY<STRING>)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO list_infer FROM FILES (
                format='PARQUET',
                uris=['gs://bucket/data.parquet'],
                enable_list_inference=true
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_decimal_target_types() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE decimal_types (id INT64, amount NUMERIC)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO decimal_types FROM FILES (
                format='PARQUET',
                uris=['gs://bucket/data.parquet'],
                decimal_target_types=['NUMERIC', 'BIGNUMERIC']
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_missing_format_defaults_to_parquet() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE no_format (id INT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql("LOAD DATA INTO no_format FROM FILES (uris=['gs://bucket/data.parquet'])")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_missing_uris_error() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE no_uris (id INT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql("LOAD DATA INTO no_uris FROM FILES (format='CSV')")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_missing_from_files_error() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE no_from (id INT64)")
            .await
            .unwrap();

        let result = session.execute_sql("LOAD DATA INTO no_from").await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_hive_partition_uri_prefix() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE hive_partitioned (id INT64, dt DATE, region STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO hive_partitioned FROM FILES (
                format='PARQUET',
                uris=['gs://bucket/data/*'],
                hive_partition_uri_prefix='gs://bucket/data'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_preserve_ascii_control_characters() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE ascii_control (id INT64, data STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO ascii_control FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                preserve_ascii_control_characters=true
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_source_column_match_position() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE match_pos (id INT64, name STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO match_pos FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                source_column_match='POSITION'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_source_column_match_name() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE match_name (id INT64, name STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO match_name FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                source_column_match='NAME',
                skip_leading_rows=1
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_file_set_spec_type_file_system_match() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE file_match (id INT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO file_match FROM FILES (
                format='CSV',
                uris=['gs://bucket/data/*.csv'],
                file_set_spec_type='FILE_SYSTEM_MATCH'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_json_extension_geojson() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE geojson_data (id INT64, geometry STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO geojson_data FROM FILES (
                format='JSON',
                uris=['gs://bucket/data.geojson'],
                json_extension='GEOJSON'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_max_staleness() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE staleness_test (id INT64)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO staleness_test FROM FILES (
                format='PARQUET',
                uris=['gs://bucket/data.parquet'],
                max_staleness=INTERVAL '4:0:0' HOUR TO SECOND
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_timestamp_format() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE ts_format (id INT64, created_at TIMESTAMP)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO ts_format FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                timestamp_format='YYYY-MM-DD HH24:MI:SS'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_date_format() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE date_format (id INT64, event_date DATE)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO date_format FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                date_format='MM/DD/YYYY'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_time_zone() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE tz_data (id INT64, event_ts TIMESTAMP)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO tz_data FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                time_zone='America/New_York'
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_enum_as_string() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE enum_data (id INT64, status STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO enum_data FROM FILES (
                format='PARQUET',
                uris=['gs://bucket/data.parquet'],
                enum_as_string=true
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_enable_logical_types() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE avro_logical (id INT64, created DATE)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO avro_logical FROM FILES (
                format='AVRO',
                uris=['gs://bucket/data.avro'],
                enable_logical_types=true
            )",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_data_column_name_character_map() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE char_map (id INT64, name STRING)")
            .await
            .unwrap();

        let result = session
            .execute_sql(
                "LOAD DATA INTO char_map FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                column_name_character_map='V2'
            )",
            )
            .await;
        assert!(result.is_ok());
    }
}

mod local_json {
    use std::io::Write;

    use tempfile::NamedTempFile;
    use yachtsql_test_utils::{get_f64, get_i64, get_string, is_null};

    use super::*;

    fn create_simple_json_file() -> NamedTempFile {
        let content = r#"{"id": 1, "name": "Alice", "score": 95.5}
{"id": 2, "name": "Bob", "score": null}
{"id": 3, "name": null, "score": 88.0}"#;
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    fn create_typed_json_file() -> NamedTempFile {
        let content = r#"{"id": 1, "active": true, "created_date": "2024-01-15"}
{"id": 2, "active": false, "created_date": "2024-06-20"}"#;
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_json_basic() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .await
            .unwrap();

        let temp_file = create_simple_json_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM users ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
        assert_eq!(get_f64(&result, 2, 0), 95.5);

        assert_eq!(get_i64(&result, 0, 1), 2);
        assert_eq!(get_string(&result, 1, 1), "Bob");
        assert!(is_null(&result, 2, 1));

        assert_eq!(get_i64(&result, 0, 2), 3);
        assert!(is_null(&result, 1, 2));
        assert_eq!(get_f64(&result, 2, 2), 88.0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_json_with_file_uri() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .await
            .unwrap();

        let temp_file = create_simple_json_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='JSON', URIS=['file://{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT COUNT(*) as cnt FROM users")
            .await
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_json_overwrite() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .await
            .unwrap();
        session
            .execute_sql("INSERT INTO users VALUES (100, 'Existing', 0.0)")
            .await
            .unwrap();

        let temp_file = create_simple_json_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA OVERWRITE users FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM users ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_i64(&result, 0, 1), 2);
        assert_eq!(get_i64(&result, 0, 2), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_json_append() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .await
            .unwrap();
        session
            .execute_sql("INSERT INTO users VALUES (0, 'Existing', 50.0)")
            .await
            .unwrap();

        let temp_file = create_simple_json_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT COUNT(*) as cnt FROM users")
            .await
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_json_typed_columns() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE events (id INT64, active BOOL, created_date DATE)")
            .await
            .unwrap();

        let temp_file = create_typed_json_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO events FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT id, active FROM events ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_i64(&result, 0, 1), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_json_basic() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64)")
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO products VALUES (1, 'Apple', 1.99), (2, 'Banana', 0.99), (3, 'Cherry', 2.50)",
            ).await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT * FROM products ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        let session2 = create_session();
        session2
            .execute_sql("CREATE TABLE imported (id INT64, name STRING, price FLOAT64)")
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        session2.execute_sql(&load_sql).await.unwrap();

        let result = session2
            .execute_sql("SELECT * FROM imported ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Apple");
        assert_eq!(get_f64(&result, 2, 0), 1.99);

        assert_eq!(get_i64(&result, 0, 1), 2);
        assert_eq!(get_string(&result, 1, 1), "Banana");
        assert_eq!(get_f64(&result, 2, 1), 0.99);

        assert_eq!(get_i64(&result, 0, 2), 3);
        assert_eq!(get_string(&result, 1, 2), "Cherry");
        assert_eq!(get_f64(&result, 2, 2), 2.5);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_json_with_aggregation() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE sales (region STRING, amount INT64)")
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO sales VALUES ('North', 100), ('South', 200), ('North', 150), ('South', 50)",
            ).await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT region, SUM(amount) as total FROM sales GROUP BY region ORDER BY region",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        let session2 = create_session();
        session2
            .execute_sql("CREATE TABLE region_totals (region STRING, total INT64)")
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO region_totals FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        session2.execute_sql(&load_sql).await.unwrap();

        let result = session2
            .execute_sql("SELECT * FROM region_totals ORDER BY region")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_string(&result, 0, 0), "North");
        assert_eq!(get_i64(&result, 1, 0), 250);
        assert_eq!(get_string(&result, 0, 1), "South");
        assert_eq!(get_i64(&result, 1, 1), 250);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_json_with_nulls() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE nullable_data (id INT64, value STRING)")
            .await
            .unwrap();
        session
            .execute_sql("INSERT INTO nullable_data VALUES (1, 'A'), (2, NULL), (3, 'C')")
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT * FROM nullable_data ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        let session2 = create_session();
        session2
            .execute_sql("CREATE TABLE imported_nullable (id INT64, value STRING)")
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_nullable FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        session2.execute_sql(&load_sql).await.unwrap();

        let result = session2
            .execute_sql("SELECT * FROM imported_nullable ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "A");
        assert_eq!(get_i64(&result, 0, 1), 2);
        assert!(is_null(&result, 1, 1));
        assert_eq!(get_i64(&result, 0, 2), 3);
        assert_eq!(get_string(&result, 1, 2), "C");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_roundtrip_json_all_basic_types() {
        let session = create_session();
        session
            .execute_sql(
                "CREATE TABLE all_types (
                    id INT64,
                    flag BOOL,
                    amount FLOAT64,
                    name STRING
                )",
            )
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO all_types VALUES
                    (1, TRUE, 100.5, 'test'),
                    (2, FALSE, NULL, NULL)",
            )
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT * FROM all_types ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        session
            .execute_sql(
                "CREATE TABLE imported_types (
                    id INT64,
                    flag BOOL,
                    amount FLOAT64,
                    name STRING
                )",
            )
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_types FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let original = session
            .execute_sql("SELECT * FROM all_types ORDER BY id")
            .await
            .unwrap();
        let imported = session
            .execute_sql("SELECT * FROM imported_types ORDER BY id")
            .await
            .unwrap();

        assert_eq!(original.num_rows(), imported.num_rows());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_roundtrip_json_date() {
        let session = create_session();
        session
            .execute_sql(
                "CREATE TABLE time_data (
                    id INT64,
                    event_date DATE
                )",
            )
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO time_data VALUES
                    (1, DATE '2024-01-15'),
                    (2, DATE '2024-06-20')",
            )
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT * FROM time_data ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        session
            .execute_sql(
                "CREATE TABLE imported_time (
                    id INT64,
                    event_date DATE
                )",
            )
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_time FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT id, event_date FROM imported_time ORDER BY id")
            .await
            .unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_json_filtered_query() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE orders (id INT64, status STRING, amount FLOAT64)")
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO orders VALUES
                    (1, 'completed', 100.0),
                    (2, 'pending', 50.0),
                    (3, 'completed', 200.0),
                    (4, 'cancelled', 75.0)",
            )
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT id, amount FROM orders WHERE status = 'completed' ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        let session2 = create_session();
        session2
            .execute_sql("CREATE TABLE completed_orders (id INT64, amount FLOAT64)")
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO completed_orders FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        session2.execute_sql(&load_sql).await.unwrap();

        let result = session2
            .execute_sql("SELECT * FROM completed_orders ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_f64(&result, 1, 0), 100.0);
        assert_eq!(get_i64(&result, 0, 1), 3);
        assert_eq!(get_f64(&result, 1, 1), 200.0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_json_empty_result() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE empty_source (id INT64, value STRING)")
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT * FROM empty_source",
            path
        );
        let result = session.execute_sql(&export_sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_json_file_not_found() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE test_table (id INT64)")
            .await
            .unwrap();

        let result = session.execute_sql(
        "LOAD DATA INTO test_table FROM FILES (FORMAT='JSON', URIS=['/nonexistent/path/file.json'])",
    ).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_json_case_insensitive_columns() {
        let content = r#"{"ID": 1, "NAME": "Alice"}
{"ID": 2, "NAME": "Bob"}"#;
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING)")
            .await
            .unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM users ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_gzip_compression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_gzip (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_gzip VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.csv',
            format='CSV',
            compression='GZIP'
        ) AS SELECT * FROM export_gzip",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_deflate_compression() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_deflate (id INT64, value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_deflate VALUES (1, 100.5)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.json',
            format='JSON',
            compression='DEFLATE'
        ) AS SELECT * FROM export_deflate",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_snappy_compression_avro() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_snappy (id INT64, data STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_snappy VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*',
            format='AVRO',
            compression='SNAPPY'
        ) AS SELECT * FROM export_snappy",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_to_s3() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_s3 (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_s3 VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='s3://bucket/folder/*',
            format='JSON',
            overwrite=true
        ) AS SELECT * FROM export_s3",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_csv_all_options() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_full (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_full VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.75)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/folder/*.csv',
            format='CSV',
            overwrite=true,
            header=true,
            field_delimiter=';'
        ) AS SELECT * FROM export_full ORDER BY id LIMIT 10",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_order_and_limit() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_ordered (field1 INT64, field2 STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_ordered VALUES (3, 'c'), (1, 'a'), (2, 'b')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/folder/*.csv',
            format='CSV',
            overwrite=true,
            header=true,
            field_delimiter=';'
        ) AS SELECT field1, field2 FROM export_ordered ORDER BY field1 LIMIT 10",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_missing_uri_error() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_no_uri (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            format='CSV'
        ) AS SELECT * FROM export_no_uri",
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_connection() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_conn (field1 INT64, field2 STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_conn VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA
            WITH CONNECTION myproject.us.myconnection
            OPTIONS(
                uri='s3://bucket/folder/*',
                format='JSON',
                overwrite=true
            ) AS SELECT field1, field2 FROM export_conn ORDER BY field1 LIMIT 10",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_backtick_connection() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_conn2 (id INT64, data STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_conn2 VALUES (1, 'value')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA
            WITH CONNECTION `my-project.us-east1.my-connection`
            OPTIONS(
                uri='s3://bucket/path/*',
                format='PARQUET'
            ) AS SELECT * FROM export_conn2",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_to_bigtable() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE export_bt (field1 STRING, field2 INT64, field3 STRING, field4 FLOAT64)",
        )
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_bt VALUES ('row1', 100, 'data1', 1.5)")
        .await
        .unwrap();

    let result = session.execute_sql(
        r#"EXPORT DATA OPTIONS (
            uri="https://bigtable.googleapis.com/projects/my-project/instances/my-instance/tables/my-table",
            format="CLOUD_BIGTABLE",
            bigtable_options="""{
                "columnFamilies": [
                    {
                        "familyId": "column_family",
                        "columns": [
                            {"qualifierString": "cbtField2", "fieldName": "field2"},
                            {"qualifierString": "cbtField3", "fieldName": "field3"},
                            {"qualifierString": "cbtField4", "fieldName": "field4"}
                        ]
                    }
                ]
            }"""
        ) AS
        SELECT
            CAST(field1 AS STRING) AS rowkey,
            STRUCT(field2, field3, field4) AS column_family
        FROM export_bt"#,
    ).await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_to_bigtable_with_overwrite() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_bt_ow (id STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_bt_ow VALUES ('key1', 100)")
        .await
        .unwrap();

    let result = session.execute_sql(
        r#"EXPORT DATA OPTIONS (
            uri="https://bigtable.googleapis.com/projects/my-project/instances/my-instance/tables/my-table",
            format="CLOUD_BIGTABLE",
            overwrite=true
        ) AS SELECT id AS rowkey, value FROM export_bt_ow"#,
    ).await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_to_bigtable_with_truncate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_bt_tr (id STRING, data STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_bt_tr VALUES ('key1', 'value1')")
        .await
        .unwrap();

    let result = session.execute_sql(
        r#"EXPORT DATA OPTIONS (
            uri="https://bigtable.googleapis.com/projects/my-project/instances/my-instance/tables/target-table",
            format="CLOUD_BIGTABLE",
            truncate=true
        ) AS SELECT id AS rowkey, data FROM export_bt_tr"#,
    ).await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_to_bigtable_auto_create_column_families() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_bt_auto (rowkey STRING, col1 INT64, col2 STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_bt_auto VALUES ('row1', 1, 'data')")
        .await
        .unwrap();

    let result = session.execute_sql(
        r#"EXPORT DATA OPTIONS (
            uri="https://bigtable.googleapis.com/projects/my-project/instances/my-instance/appProfiles/my-profile/tables/my-table",
            format="CLOUD_BIGTABLE",
            auto_create_column_families=true
        ) AS SELECT * FROM export_bt_auto"#,
    ).await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_to_pubsub() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE taxi_rides (ride_id STRING, ts DATETIME, latitude FLOAT64, longitude FLOAT64, ride_status STRING)").await
        .unwrap();
    session
        .execute_sql("INSERT INTO taxi_rides VALUES ('ride1', DATETIME '2024-01-15 10:30:00', 40.7128, -74.0060, 'enroute')").await
        .unwrap();

    let result = session
        .execute_sql(
            r#"EXPORT DATA
            OPTIONS (
                format = 'CLOUD_PUBSUB',
                uri = 'https://pubsub.googleapis.com/projects/myproject/topics/taxi-real-time-rides'
            )
        AS (
            SELECT
                TO_JSON_STRING(
                    STRUCT(
                        ride_id,
                        ts,
                        latitude,
                        longitude
                    )
                ) AS message
            FROM taxi_rides
            WHERE ride_status = 'enroute'
        )"#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_to_spanner() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE bigquery_table (id INT64, name STRING, value FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO bigquery_table VALUES (1, 'Alice', 100.5)")
        .await
        .unwrap();

    let result = session.execute_sql(
        r#"EXPORT DATA OPTIONS (
            uri="https://spanner.googleapis.com/projects/my-project/instances/my-instance/databases/my-database",
            format="CLOUD_SPANNER",
            spanner_options="""{ "table": "my_table" }"""
        ) AS SELECT * FROM bigquery_table"#,
    ).await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_avro_with_logical_types() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_avro_types (id INT64, ts TIMESTAMP, dt DATE, tm TIME)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_avro_types VALUES (1, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15', TIME '10:30:00')").await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*',
            format='AVRO',
            use_avro_logical_types=true
        ) AS SELECT * FROM export_avro_types",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_case_insensitive_format() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_case (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_case VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.csv',
            format='csv'
        ) AS SELECT * FROM export_case",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_parquet_with_overwrite() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE export_par_ow (field1 INT64, field2 STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO export_par_ow VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/folder/*',
            format='PARQUET',
            overwrite=true
        ) AS SELECT field1, field2 FROM export_par_ow ORDER BY field1 LIMIT 10",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_subquery() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE orders (order_id INT64, customer_id INT64, amount FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (1, 100, 50.0), (2, 100, 75.0), (3, 200, 100.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.parquet',
            format='PARQUET'
        ) AS
        SELECT customer_id, SUM(amount) AS total_amount
        FROM orders
        GROUP BY customer_id
        HAVING SUM(amount) > 100
        ORDER BY customer_id",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_join() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE customers (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE purchases (customer_id INT64, product STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO purchases VALUES (1, 'Laptop'), (1, 'Phone'), (2, 'Tablet')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.json',
            format='JSON'
        ) AS
        SELECT c.name, p.product
        FROM customers c
        JOIN purchases p ON c.id = p.customer_id
        ORDER BY c.name, p.product",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_cte() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales_data (region STRING, quarter INT64, revenue FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sales_data VALUES ('North', 1, 1000), ('North', 2, 1500), ('South', 1, 800)").await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.csv',
            format='CSV'
        ) AS
        WITH quarterly_totals AS (
            SELECT region, SUM(revenue) AS total_revenue
            FROM sales_data
            GROUP BY region
        )
        SELECT * FROM quarterly_totals ORDER BY region",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_data_with_window_function() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE employee_sales (employee STRING, month INT64, sales FLOAT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employee_sales VALUES ('Alice', 1, 100), ('Alice', 2, 150), ('Bob', 1, 200)").await
        .unwrap();

    let result = session
        .execute_sql(
            "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.parquet',
            format='PARQUET'
        ) AS
        SELECT
            employee,
            month,
            sales,
            SUM(sales) OVER (PARTITION BY employee ORDER BY month) AS running_total
        FROM employee_sales
        ORDER BY employee, month",
        )
        .await;
    assert!(result.is_ok());
}

mod local_csv {
    use std::io::Write;

    use tempfile::NamedTempFile;
    use yachtsql_test_utils::{get_f64, get_i64, get_string, is_null};

    use super::*;

    fn create_simple_csv() -> NamedTempFile {
        let content = "id,name,score\n1,Alice,95.5\n2,Bob,\n3,,88.0";
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    fn create_csv_with_header() -> NamedTempFile {
        let content = "id,name,value\n1,Product A,100.50\n2,Product B,200.75\n3,Product C,50.25";
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    fn create_csv_no_header() -> NamedTempFile {
        let content = "1,Alice,100\n2,Bob,200\n3,Charlie,300";
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    fn create_tsv_file() -> NamedTempFile {
        let content = "id\tname\tvalue\n1\tAlpha\t10.5\n2\tBeta\t20.5";
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    fn create_csv_with_quotes() -> NamedTempFile {
        let content = r#"id,name,description
1,"Alice","A simple description"
2,"Bob","A description with ""quotes"""
3,"Charlie","Description with, comma"
"#;
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    fn create_csv_with_nulls() -> NamedTempFile {
        let content = "id,value,text\n1,100,hello\n2,NA,world\n3,200,NA";
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    fn create_csv_with_different_types() -> NamedTempFile {
        let content =
            "id,active,created_date,amount\n1,true,2024-01-15,100.50\n2,false,2024-06-20,200.75";
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_basic() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .await
            .unwrap();

        let temp_file = create_simple_csv();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM users ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
        assert_eq!(get_f64(&result, 2, 0), 95.5);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_with_file_uri() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE products (id INT64, name STRING, value FLOAT64)")
            .await
            .unwrap();

        let temp_file = create_csv_with_header();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO products FROM FILES (FORMAT='CSV', URIS=['file://{}'], skip_leading_rows=1)",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT COUNT(*) as cnt FROM products")
            .await
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_no_header() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE data (id INT64, name STRING, value INT64)")
            .await
            .unwrap();

        let temp_file = create_csv_no_header();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO data FROM FILES (FORMAT='CSV', URIS=['{}'])",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM data ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
        assert_eq!(get_i64(&result, 2, 0), 100);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_overwrite() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .await
            .unwrap();
        session
            .execute_sql("INSERT INTO users VALUES (100, 'Existing', 0.0)")
            .await
            .unwrap();

        let temp_file = create_simple_csv();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA OVERWRITE users FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM users ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_append() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .await
            .unwrap();
        session
            .execute_sql("INSERT INTO users VALUES (0, 'Existing', 50.0)")
            .await
            .unwrap();

        let temp_file = create_simple_csv();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT COUNT(*) as cnt FROM users")
            .await
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_with_tab_delimiter() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE tsv_data (id INT64, name STRING, value FLOAT64)")
            .await
            .unwrap();

        let temp_file = create_tsv_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO tsv_data FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1, field_delimiter='\\t')",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM tsv_data ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alpha");
        assert_eq!(get_f64(&result, 2, 0), 10.5);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_with_quoted_fields() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE quoted_csv (id INT64, name STRING, description STRING)")
            .await
            .unwrap();

        let temp_file = create_csv_with_quotes();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO quoted_csv FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM quoted_csv ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_string(&result, 1, 0), "Alice");
        assert_eq!(get_string(&result, 2, 2), "Description with, comma");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_with_null_marker() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE null_test (id INT64, value INT64, text STRING)")
            .await
            .unwrap();

        let temp_file = create_csv_with_nulls();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO null_test FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1, null_marker='NA')",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM null_test ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert!(is_null(&result, 1, 1));
        assert!(is_null(&result, 2, 2));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_with_typed_columns() {
        let session = create_session();
        session
            .execute_sql(
                "CREATE TABLE typed_csv (id INT64, active BOOL, created_date DATE, amount FLOAT64)",
            )
            .await
            .unwrap();

        let temp_file = create_csv_with_different_types();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO typed_csv FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT id, amount FROM typed_csv ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_f64(&result, 1, 0), 100.50);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_csv_basic() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64)")
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO products VALUES (1, 'Apple', 1.99), (2, 'Banana', 0.99), (3, 'Cherry', 2.50)",
            ).await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='CSV', header=true) AS SELECT * FROM products ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        let session2 = create_session();
        session2
            .execute_sql("CREATE TABLE imported (id INT64, name STRING, price FLOAT64)")
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        session2.execute_sql(&load_sql).await.unwrap();

        let result = session2
            .execute_sql("SELECT * FROM imported ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Apple");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_csv_with_field_delimiter() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE data (id INT64, value STRING)")
            .await
            .unwrap();
        session
            .execute_sql("INSERT INTO data VALUES (1, 'A'), (2, 'B')")
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='CSV', field_delimiter='|') AS SELECT * FROM data ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        let session2 = create_session();
        session2
            .execute_sql("CREATE TABLE imported (id INT64, value STRING)")
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported FROM FILES (FORMAT='CSV', URIS=['{}'], field_delimiter='|')",
            path
        );
        session2.execute_sql(&load_sql).await.unwrap();

        let result = session2
            .execute_sql("SELECT * FROM imported ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "A");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_roundtrip_csv_all_basic_types() {
        let session = create_session();
        session
            .execute_sql(
                "CREATE TABLE all_types (
                    id INT64,
                    flag BOOL,
                    amount FLOAT64,
                    name STRING
                )",
            )
            .await
            .unwrap();
        session
            .execute_sql(
                "INSERT INTO all_types VALUES
                    (1, TRUE, 100.5, 'test'),
                    (2, FALSE, 200.25, 'sample')",
            )
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='CSV', header=true) AS SELECT * FROM all_types ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        session
            .execute_sql(
                "CREATE TABLE imported_types (
                    id INT64,
                    flag BOOL,
                    amount FLOAT64,
                    name STRING
                )",
            )
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_types FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let original = session
            .execute_sql("SELECT * FROM all_types ORDER BY id")
            .await
            .unwrap();
        let imported = session
            .execute_sql("SELECT * FROM imported_types ORDER BY id")
            .await
            .unwrap();

        assert_eq!(original.num_rows(), imported.num_rows());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_file_not_found() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE test_table (id INT64)")
            .await
            .unwrap();

        let result = session.execute_sql(
        "LOAD DATA INTO test_table FROM FILES (FORMAT='CSV', URIS=['/nonexistent/path/file.csv'])",
    ).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_column_subset() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE partial (id INT64, name STRING)")
            .await
            .unwrap();

        let temp_file = create_simple_csv();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO partial FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT * FROM partial ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_csv_with_nulls() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE nullable_data (id INT64, value STRING)")
            .await
            .unwrap();
        session
            .execute_sql("INSERT INTO nullable_data VALUES (1, 'A'), (2, NULL), (3, 'C')")
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='CSV', header=true) AS SELECT * FROM nullable_data ORDER BY id",
            path
        );
        session.execute_sql(&export_sql).await.unwrap();

        let session2 = create_session();
        session2
            .execute_sql("CREATE TABLE imported_nullable (id INT64, value STRING)")
            .await
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_nullable FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        session2.execute_sql(&load_sql).await.unwrap();

        let result = session2
            .execute_sql("SELECT * FROM imported_nullable ORDER BY id")
            .await
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert!(is_null(&result, 1, 1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_export_csv_empty_result() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE empty_source (id INT64, value STRING)")
            .await
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='CSV') AS SELECT * FROM empty_source",
            path
        );
        let result = session.execute_sql(&export_sql).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_load_csv_multiple_files() {
        let session = create_session();
        session
            .execute_sql("CREATE TABLE multi_source (id INT64, name STRING, value INT64)")
            .await
            .unwrap();

        let content1 = "1,Alice,100\n2,Bob,200";
        let content2 = "3,Charlie,300\n4,Diana,400";

        let mut temp_file1 = NamedTempFile::new().unwrap();
        temp_file1.write_all(content1.as_bytes()).unwrap();
        temp_file1.flush().unwrap();

        let mut temp_file2 = NamedTempFile::new().unwrap();
        temp_file2.write_all(content2.as_bytes()).unwrap();
        temp_file2.flush().unwrap();

        let path1 = temp_file1.path().to_str().unwrap();
        let path2 = temp_file2.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO multi_source FROM FILES (FORMAT='CSV', URIS=['{}', '{}'])",
            path1, path2
        );
        session.execute_sql(&load_sql).await.unwrap();

        let result = session
            .execute_sql("SELECT COUNT(*) as cnt FROM multi_source")
            .await
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 4);
    }
}
