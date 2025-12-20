use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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
#[ignore = "Implement me!"]
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

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_parquet_basic() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .unwrap();

        let temp_file = create_simple_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM users ORDER BY id")
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

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_parquet_with_file_uri() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .unwrap();

        let temp_file = create_simple_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='PARQUET', URIS=['file://{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM users")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 3);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_parquet_overwrite() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO users VALUES (100, 'Existing', 0.0)")
            .unwrap();

        let temp_file = create_simple_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA OVERWRITE users FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM users ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_i64(&result, 0, 1), 2);
        assert_eq!(get_i64(&result, 0, 2), 3);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_parquet_append() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO users VALUES (0, 'Existing', 50.0)")
            .unwrap();

        let temp_file = create_simple_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM users")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 4);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_parquet_typed_columns() {
        let mut executor = create_executor();
        executor
            .execute_sql(
                "CREATE TABLE events (id INT64, active BOOL, created_date DATE, updated_at DATETIME)",
            )
            .unwrap();

        let temp_file = create_typed_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO events FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT id, active FROM events ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_i64(&result, 0, 1), 2);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_export_parquet_basic() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64)")
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO products VALUES (1, 'Apple', 1.99), (2, 'Banana', 0.99), (3, 'Cherry', 2.50)",
            )
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT * FROM products ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        let mut executor2 = create_executor();
        executor2
            .execute_sql("CREATE TABLE imported (id INT64, name STRING, price FLOAT64)")
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        executor2.execute_sql(&load_sql).unwrap();

        let result = executor2
            .execute_sql("SELECT * FROM imported ORDER BY id")
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

    #[test]
    #[ignore = "Implement me!"]
    fn test_export_parquet_with_aggregation() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE sales (region STRING, amount INT64)")
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO sales VALUES ('North', 100), ('South', 200), ('North', 150), ('South', 50)",
            )
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT region, SUM(amount) as total FROM sales GROUP BY region ORDER BY region",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        let mut executor2 = create_executor();
        executor2
            .execute_sql("CREATE TABLE region_totals (region STRING, total INT64)")
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO region_totals FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        executor2.execute_sql(&load_sql).unwrap();

        let result = executor2
            .execute_sql("SELECT * FROM region_totals ORDER BY region")
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_string(&result, 0, 0), "North");
        assert_eq!(get_i64(&result, 1, 0), 250);
        assert_eq!(get_string(&result, 0, 1), "South");
        assert_eq!(get_i64(&result, 1, 1), 250);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_export_parquet_with_nulls() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE nullable_data (id INT64, value STRING)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO nullable_data VALUES (1, 'A'), (2, NULL), (3, 'C')")
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT * FROM nullable_data ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        let mut executor2 = create_executor();
        executor2
            .execute_sql("CREATE TABLE imported_nullable (id INT64, value STRING)")
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_nullable FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        executor2.execute_sql(&load_sql).unwrap();

        let result = executor2
            .execute_sql("SELECT * FROM imported_nullable ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "A");
        assert_eq!(get_i64(&result, 0, 1), 2);
        assert!(is_null(&result, 1, 1));
        assert_eq!(get_i64(&result, 0, 2), 3);
        assert_eq!(get_string(&result, 1, 2), "C");
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_roundtrip_all_basic_types() {
        let mut executor = create_executor();
        executor
            .execute_sql(
                "CREATE TABLE all_types (
                    id INT64,
                    flag BOOL,
                    amount FLOAT64,
                    name STRING
                )",
            )
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO all_types VALUES
                    (1, TRUE, 100.5, 'test'),
                    (2, FALSE, NULL, NULL)",
            )
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT * FROM all_types ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        executor
            .execute_sql(
                "CREATE TABLE imported_types (
                    id INT64,
                    flag BOOL,
                    amount FLOAT64,
                    name STRING
                )",
            )
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_types FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let original = executor
            .execute_sql("SELECT * FROM all_types ORDER BY id")
            .unwrap();
        let imported = executor
            .execute_sql("SELECT * FROM imported_types ORDER BY id")
            .unwrap();

        assert_eq!(original.num_rows(), imported.num_rows());
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_roundtrip_date_datetime() {
        let mut executor = create_executor();
        executor
            .execute_sql(
                "CREATE TABLE time_data (
                    id INT64,
                    event_date DATE,
                    event_time DATETIME
                )",
            )
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO time_data VALUES
                    (1, DATE '2024-01-15', DATETIME '2024-01-15 10:30:00'),
                    (2, DATE '2024-06-20', DATETIME '2024-06-20 14:45:30')",
            )
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT * FROM time_data ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        executor
            .execute_sql(
                "CREATE TABLE imported_time (
                    id INT64,
                    event_date DATE,
                    event_time DATETIME
                )",
            )
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_time FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT id, event_date FROM imported_time ORDER BY id")
            .unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_export_filtered_query() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE orders (id INT64, status STRING, amount FLOAT64)")
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO orders VALUES
                    (1, 'completed', 100.0),
                    (2, 'pending', 50.0),
                    (3, 'completed', 200.0),
                    (4, 'cancelled', 75.0)",
            )
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT id, amount FROM orders WHERE status = 'completed' ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        let mut executor2 = create_executor();
        executor2
            .execute_sql("CREATE TABLE completed_orders (id INT64, amount FLOAT64)")
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO completed_orders FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        executor2.execute_sql(&load_sql).unwrap();

        let result = executor2
            .execute_sql("SELECT * FROM completed_orders ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_f64(&result, 1, 0), 100.0);
        assert_eq!(get_i64(&result, 0, 1), 3);
        assert_eq!(get_f64(&result, 1, 1), 200.0);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_parquet_column_subset() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE partial (id INT64, name STRING)")
            .unwrap();

        let temp_file = create_simple_parquet();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO partial FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM partial ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
        assert_eq!(get_i64(&result, 0, 1), 2);
        assert_eq!(get_string(&result, 1, 1), "Bob");
        assert_eq!(get_i64(&result, 0, 2), 3);
        assert!(is_null(&result, 1, 2));
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_export_empty_result() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE empty_source (id INT64, value STRING)")
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT * FROM empty_source",
            path
        );
        let result = executor.execute_sql(&export_sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_parquet_file_not_found() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE test_table (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO test_table FROM FILES (FORMAT='PARQUET', URIS=['/nonexistent/path/file.parquet'])",
        );
        assert!(result.is_err());
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

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_json_basic() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .unwrap();

        let temp_file = create_simple_json_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM users ORDER BY id")
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

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_json_with_file_uri() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .unwrap();

        let temp_file = create_simple_json_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='JSON', URIS=['file://{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM users")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 3);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_json_overwrite() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO users VALUES (100, 'Existing', 0.0)")
            .unwrap();

        let temp_file = create_simple_json_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA OVERWRITE users FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM users ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_i64(&result, 0, 1), 2);
        assert_eq!(get_i64(&result, 0, 2), 3);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_json_append() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO users VALUES (0, 'Existing', 50.0)")
            .unwrap();

        let temp_file = create_simple_json_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM users")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 4);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_json_typed_columns() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE events (id INT64, active BOOL, created_date DATE)")
            .unwrap();

        let temp_file = create_typed_json_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO events FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT id, active FROM events ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_i64(&result, 0, 1), 2);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_export_json_basic() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64)")
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO products VALUES (1, 'Apple', 1.99), (2, 'Banana', 0.99), (3, 'Cherry', 2.50)",
            )
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT * FROM products ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        let mut executor2 = create_executor();
        executor2
            .execute_sql("CREATE TABLE imported (id INT64, name STRING, price FLOAT64)")
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        executor2.execute_sql(&load_sql).unwrap();

        let result = executor2
            .execute_sql("SELECT * FROM imported ORDER BY id")
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

    #[test]
    #[ignore = "Implement me!"]
    fn test_export_json_with_aggregation() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE sales (region STRING, amount INT64)")
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO sales VALUES ('North', 100), ('South', 200), ('North', 150), ('South', 50)",
            )
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT region, SUM(amount) as total FROM sales GROUP BY region ORDER BY region",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        let mut executor2 = create_executor();
        executor2
            .execute_sql("CREATE TABLE region_totals (region STRING, total INT64)")
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO region_totals FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        executor2.execute_sql(&load_sql).unwrap();

        let result = executor2
            .execute_sql("SELECT * FROM region_totals ORDER BY region")
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_string(&result, 0, 0), "North");
        assert_eq!(get_i64(&result, 1, 0), 250);
        assert_eq!(get_string(&result, 0, 1), "South");
        assert_eq!(get_i64(&result, 1, 1), 250);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_export_json_with_nulls() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE nullable_data (id INT64, value STRING)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO nullable_data VALUES (1, 'A'), (2, NULL), (3, 'C')")
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT * FROM nullable_data ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        let mut executor2 = create_executor();
        executor2
            .execute_sql("CREATE TABLE imported_nullable (id INT64, value STRING)")
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_nullable FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        executor2.execute_sql(&load_sql).unwrap();

        let result = executor2
            .execute_sql("SELECT * FROM imported_nullable ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "A");
        assert_eq!(get_i64(&result, 0, 1), 2);
        assert!(is_null(&result, 1, 1));
        assert_eq!(get_i64(&result, 0, 2), 3);
        assert_eq!(get_string(&result, 1, 2), "C");
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_roundtrip_json_all_basic_types() {
        let mut executor = create_executor();
        executor
            .execute_sql(
                "CREATE TABLE all_types (
                    id INT64,
                    flag BOOL,
                    amount FLOAT64,
                    name STRING
                )",
            )
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO all_types VALUES
                    (1, TRUE, 100.5, 'test'),
                    (2, FALSE, NULL, NULL)",
            )
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT * FROM all_types ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        executor
            .execute_sql(
                "CREATE TABLE imported_types (
                    id INT64,
                    flag BOOL,
                    amount FLOAT64,
                    name STRING
                )",
            )
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_types FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let original = executor
            .execute_sql("SELECT * FROM all_types ORDER BY id")
            .unwrap();
        let imported = executor
            .execute_sql("SELECT * FROM imported_types ORDER BY id")
            .unwrap();

        assert_eq!(original.num_rows(), imported.num_rows());
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_roundtrip_json_date() {
        let mut executor = create_executor();
        executor
            .execute_sql(
                "CREATE TABLE time_data (
                    id INT64,
                    event_date DATE
                )",
            )
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO time_data VALUES
                    (1, DATE '2024-01-15'),
                    (2, DATE '2024-06-20')",
            )
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT * FROM time_data ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        executor
            .execute_sql(
                "CREATE TABLE imported_time (
                    id INT64,
                    event_date DATE
                )",
            )
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_time FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT id, event_date FROM imported_time ORDER BY id")
            .unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_export_json_filtered_query() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE orders (id INT64, status STRING, amount FLOAT64)")
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO orders VALUES
                    (1, 'completed', 100.0),
                    (2, 'pending', 50.0),
                    (3, 'completed', 200.0),
                    (4, 'cancelled', 75.0)",
            )
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT id, amount FROM orders WHERE status = 'completed' ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        let mut executor2 = create_executor();
        executor2
            .execute_sql("CREATE TABLE completed_orders (id INT64, amount FLOAT64)")
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO completed_orders FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        executor2.execute_sql(&load_sql).unwrap();

        let result = executor2
            .execute_sql("SELECT * FROM completed_orders ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_f64(&result, 1, 0), 100.0);
        assert_eq!(get_i64(&result, 0, 1), 3);
        assert_eq!(get_f64(&result, 1, 1), 200.0);
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_export_json_empty_result() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE empty_source (id INT64, value STRING)")
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='JSON') AS SELECT * FROM empty_source",
            path
        );
        let result = executor.execute_sql(&export_sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_json_file_not_found() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE test_table (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO test_table FROM FILES (FORMAT='JSON', URIS=['/nonexistent/path/file.json'])",
        );
        assert!(result.is_err());
    }

    #[test]
    #[ignore = "Implement me!"]
    fn test_load_json_case_insensitive_columns() {
        let content = r#"{"ID": 1, "NAME": "Alice"}
{"ID": 2, "NAME": "Bob"}"#;
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING)")
            .unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='JSON', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM users ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
    }
}
