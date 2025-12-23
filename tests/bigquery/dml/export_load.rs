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
#[ignore = "AVRO export not yet implemented"]
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

mod load_data_parsing {
    use super::*;

    #[test]
    fn test_load_data_into_basic() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE target (id INT64, name STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO target FROM FILES (format='PARQUET', uris=['gs://bucket/file.parquet'])",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_overwrite_keyword() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE target (id INT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO target VALUES (1)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA OVERWRITE target FROM FILES (format='CSV', uris=['gs://bucket/data.csv'])",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_with_column_list() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE target (id INT64, name STRING, value FLOAT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO target (id, name, value) FROM FILES (format='CSV', uris=['gs://bucket/data.csv'])",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_temp_table_with_schema() {
        let mut executor = create_executor();

        let result = executor.execute_sql(
            "LOAD DATA INTO TEMP TABLE temp_data (id INT64, name STRING)
             FROM FILES (format='CSV', uris=['gs://bucket/data.csv'])",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_format_csv() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE csv_table (x INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO csv_table FROM FILES (format='CSV', uris=['gs://b/f.csv'])",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_format_json() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE json_table (x INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO json_table FROM FILES (format='JSON', uris=['gs://b/f.json'])",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_format_parquet() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE parquet_table (x INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO parquet_table FROM FILES (format='PARQUET', uris=['gs://b/f.parquet'])",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_format_newline_delimited_json() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE ndjson_table (x INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO ndjson_table FROM FILES (format='NEWLINE_DELIMITED_JSON', uris=['gs://b/f.json'])",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_multiple_uris() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE multi_file (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO multi_file FROM FILES (
                format='CSV',
                uris=['gs://bucket/file1.csv', 'gs://bucket/file2.csv', 'gs://bucket/file3.csv']
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_uri_with_wildcard() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE wildcard_load (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO wildcard_load FROM FILES (format='CSV', uris=['gs://bucket/path/*.csv'])",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_csv_skip_leading_rows() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE skip_rows (id INT64, name STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO skip_rows FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                skip_leading_rows=1
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_csv_field_delimiter() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE delim_table (a INT64, b STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO delim_table FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.tsv'],
                field_delimiter='\\t'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_csv_allow_jagged_rows() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE jagged (a INT64, b STRING, c STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO jagged FROM FILES (
                format='CSV',
                uris=['gs://bucket/jagged.csv'],
                allow_jagged_rows=true
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_csv_allow_quoted_newlines() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE quoted_nl (id INT64, text STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO quoted_nl FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                allow_quoted_newlines=true
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_csv_null_marker() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE null_marker_table (id INT64, val STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO null_marker_table FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                null_marker='NA'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_csv_encoding() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE encoded_table (id INT64, text STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO encoded_table FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                encoding='UTF-8'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_csv_quote_character() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE quoted_table (id INT64, name STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO quoted_table FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                quote='\"'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_ignore_unknown_values() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE ignore_unknown (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO ignore_unknown FROM FILES (
                format='JSON',
                uris=['gs://bucket/data.json'],
                ignore_unknown_values=true
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_max_bad_records() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE bad_records (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO bad_records FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                max_bad_records=10
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_compression_gzip() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE compressed (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO compressed FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv.gz'],
                compression='GZIP'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_case_insensitive_options() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE case_test (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO case_test FROM FILES (
                FORMAT='csv',
                URIS=['gs://bucket/data.csv'],
                SKIP_LEADING_ROWS=1
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_qualified_table_name() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE SCHEMA IF NOT EXISTS test_dataset")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_dataset.target (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO test_dataset.target FROM FILES (format='CSV', uris=['gs://b/f.csv'])",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_combined_csv_options() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE combined_opts (id INT64, name STRING, value FLOAT64)")
            .unwrap();

        let result = executor.execute_sql(
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
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_parquet_enable_list_inference() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE list_infer (id INT64, items ARRAY<STRING>)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO list_infer FROM FILES (
                format='PARQUET',
                uris=['gs://bucket/data.parquet'],
                enable_list_inference=true
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_decimal_target_types() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE decimal_types (id INT64, amount NUMERIC)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO decimal_types FROM FILES (
                format='PARQUET',
                uris=['gs://bucket/data.parquet'],
                decimal_target_types=['NUMERIC', 'BIGNUMERIC']
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_missing_format_defaults_to_parquet() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE no_format (id INT64)")
            .unwrap();

        let result = executor
            .execute_sql("LOAD DATA INTO no_format FROM FILES (uris=['gs://bucket/data.parquet'])");
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_missing_uris_error() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE no_uris (id INT64)")
            .unwrap();

        let result = executor.execute_sql("LOAD DATA INTO no_uris FROM FILES (format='CSV')");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_data_missing_from_files_error() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE no_from (id INT64)")
            .unwrap();

        let result = executor.execute_sql("LOAD DATA INTO no_from");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_data_hive_partition_uri_prefix() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE hive_partitioned (id INT64, dt DATE, region STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO hive_partitioned FROM FILES (
                format='PARQUET',
                uris=['gs://bucket/data/*'],
                hive_partition_uri_prefix='gs://bucket/data'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_preserve_ascii_control_characters() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE ascii_control (id INT64, data STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO ascii_control FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                preserve_ascii_control_characters=true
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_source_column_match_position() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE match_pos (id INT64, name STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO match_pos FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                source_column_match='POSITION'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_source_column_match_name() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE match_name (id INT64, name STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO match_name FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                source_column_match='NAME',
                skip_leading_rows=1
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_file_set_spec_type_file_system_match() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE file_match (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO file_match FROM FILES (
                format='CSV',
                uris=['gs://bucket/data/*.csv'],
                file_set_spec_type='FILE_SYSTEM_MATCH'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_json_extension_geojson() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE geojson_data (id INT64, geometry STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO geojson_data FROM FILES (
                format='JSON',
                uris=['gs://bucket/data.geojson'],
                json_extension='GEOJSON'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_max_staleness() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE staleness_test (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO staleness_test FROM FILES (
                format='PARQUET',
                uris=['gs://bucket/data.parquet'],
                max_staleness=INTERVAL '4:0:0' HOUR TO SECOND
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_timestamp_format() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE ts_format (id INT64, created_at TIMESTAMP)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO ts_format FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                timestamp_format='YYYY-MM-DD HH24:MI:SS'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_date_format() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE date_format (id INT64, event_date DATE)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO date_format FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                date_format='MM/DD/YYYY'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_time_zone() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE tz_data (id INT64, event_ts TIMESTAMP)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO tz_data FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                time_zone='America/New_York'
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_enum_as_string() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE enum_data (id INT64, status STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO enum_data FROM FILES (
                format='PARQUET',
                uris=['gs://bucket/data.parquet'],
                enum_as_string=true
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_enable_logical_types() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE avro_logical (id INT64, created DATE)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO avro_logical FROM FILES (
                format='AVRO',
                uris=['gs://bucket/data.avro'],
                enable_logical_types=true
            )",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_data_column_name_character_map() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE char_map (id INT64, name STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO char_map FROM FILES (
                format='CSV',
                uris=['gs://bucket/data.csv'],
                column_name_character_map='V2'
            )",
        );
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

    #[test]
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

#[test]
fn test_export_data_gzip_compression() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_gzip (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_gzip VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.csv',
            format='CSV',
            compression='GZIP'
        ) AS SELECT * FROM export_gzip",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_deflate_compression() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_deflate (id INT64, value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_deflate VALUES (1, 100.5)")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.json',
            format='JSON',
            compression='DEFLATE'
        ) AS SELECT * FROM export_deflate",
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_snappy_compression_avro() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_snappy (id INT64, data STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_snappy VALUES (1, 'test')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*',
            format='AVRO',
            compression='SNAPPY'
        ) AS SELECT * FROM export_snappy",
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_to_s3() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_s3 (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_s3 VALUES (1, 'test')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='s3://bucket/folder/*',
            format='JSON',
            overwrite=true
        ) AS SELECT * FROM export_s3",
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_csv_all_options() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_full (id INT64, name STRING, value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_full VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.75)")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/folder/*.csv',
            format='CSV',
            overwrite=true,
            header=true,
            field_delimiter=';'
        ) AS SELECT * FROM export_full ORDER BY id LIMIT 10",
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_with_order_and_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_ordered (field1 INT64, field2 STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_ordered VALUES (3, 'c'), (1, 'a'), (2, 'b')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/folder/*.csv',
            format='CSV',
            overwrite=true,
            header=true,
            field_delimiter=';'
        ) AS SELECT field1, field2 FROM export_ordered ORDER BY field1 LIMIT 10",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_missing_uri_error() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_no_uri (id INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            format='CSV'
        ) AS SELECT * FROM export_no_uri",
    );
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_export_data_with_connection() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_conn (field1 INT64, field2 STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_conn VALUES (1, 'test')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA
            WITH CONNECTION myproject.us.myconnection
            OPTIONS(
                uri='s3://bucket/folder/*',
                format='JSON',
                overwrite=true
            ) AS SELECT field1, field2 FROM export_conn ORDER BY field1 LIMIT 10",
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_with_backtick_connection() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_conn2 (id INT64, data STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_conn2 VALUES (1, 'value')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA
            WITH CONNECTION `my-project.us-east1.my-connection`
            OPTIONS(
                uri='s3://bucket/path/*',
                format='PARQUET'
            ) AS SELECT * FROM export_conn2",
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_to_bigtable() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE export_bt (field1 STRING, field2 INT64, field3 STRING, field4 FLOAT64)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_bt VALUES ('row1', 100, 'data1', 1.5)")
        .unwrap();

    let result = executor.execute_sql(
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
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_to_bigtable_with_overwrite() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_bt_ow (id STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_bt_ow VALUES ('key1', 100)")
        .unwrap();

    let result = executor.execute_sql(
        r#"EXPORT DATA OPTIONS (
            uri="https://bigtable.googleapis.com/projects/my-project/instances/my-instance/tables/my-table",
            format="CLOUD_BIGTABLE",
            overwrite=true
        ) AS SELECT id AS rowkey, value FROM export_bt_ow"#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_to_bigtable_with_truncate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_bt_tr (id STRING, data STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_bt_tr VALUES ('key1', 'value1')")
        .unwrap();

    let result = executor.execute_sql(
        r#"EXPORT DATA OPTIONS (
            uri="https://bigtable.googleapis.com/projects/my-project/instances/my-instance/tables/target-table",
            format="CLOUD_BIGTABLE",
            truncate=true
        ) AS SELECT id AS rowkey, data FROM export_bt_tr"#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_to_bigtable_auto_create_column_families() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_bt_auto (rowkey STRING, col1 INT64, col2 STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_bt_auto VALUES ('row1', 1, 'data')")
        .unwrap();

    let result = executor.execute_sql(
        r#"EXPORT DATA OPTIONS (
            uri="https://bigtable.googleapis.com/projects/my-project/instances/my-instance/appProfiles/my-profile/tables/my-table",
            format="CLOUD_BIGTABLE",
            auto_create_column_families=true
        ) AS SELECT * FROM export_bt_auto"#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_to_pubsub() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE taxi_rides (ride_id STRING, ts DATETIME, latitude FLOAT64, longitude FLOAT64, ride_status STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO taxi_rides VALUES ('ride1', DATETIME '2024-01-15 10:30:00', 40.7128, -74.0060, 'enroute')")
        .unwrap();

    let result = executor.execute_sql(
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
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_to_spanner() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bigquery_table (id INT64, name STRING, value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bigquery_table VALUES (1, 'Alice', 100.5)")
        .unwrap();

    let result = executor.execute_sql(
        r#"EXPORT DATA OPTIONS (
            uri="https://spanner.googleapis.com/projects/my-project/instances/my-instance/databases/my-database",
            format="CLOUD_SPANNER",
            spanner_options="""{ "table": "my_table" }"""
        ) AS SELECT * FROM bigquery_table"#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_avro_with_logical_types() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_avro_types (id INT64, ts TIMESTAMP, dt DATE, tm TIME)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_avro_types VALUES (1, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15', TIME '10:30:00')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*',
            format='AVRO',
            use_avro_logical_types=true
        ) AS SELECT * FROM export_avro_types",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_case_insensitive_format() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_case (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_case VALUES (1)")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.csv',
            format='csv'
        ) AS SELECT * FROM export_case",
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_export_data_parquet_with_overwrite() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE export_par_ow (field1 INT64, field2 STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO export_par_ow VALUES (1, 'test')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/folder/*',
            format='PARQUET',
            overwrite=true
        ) AS SELECT field1, field2 FROM export_par_ow ORDER BY field1 LIMIT 10",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_with_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE orders (order_id INT64, customer_id INT64, amount FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 100, 50.0), (2, 100, 75.0), (3, 200, 100.0)")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.parquet',
            format='PARQUET'
        ) AS
        SELECT customer_id, SUM(amount) AS total_amount
        FROM orders
        GROUP BY customer_id
        HAVING SUM(amount) > 100
        ORDER BY customer_id",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE customers (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE purchases (customer_id INT64, product STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO purchases VALUES (1, 'Laptop'), (1, 'Phone'), (2, 'Tablet')")
        .unwrap();

    let result = executor.execute_sql(
        "EXPORT DATA OPTIONS(
            uri='gs://bucket/export/*.json',
            format='JSON'
        ) AS
        SELECT c.name, p.product
        FROM customers c
        JOIN purchases p ON c.id = p.customer_id
        ORDER BY c.name, p.product",
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_with_cte() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales_data (region STRING, quarter INT64, revenue FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales_data VALUES ('North', 1, 1000), ('North', 2, 1500), ('South', 1, 800)")
        .unwrap();

    let result = executor.execute_sql(
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
    );
    assert!(result.is_ok());
}

#[test]
fn test_export_data_with_window_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE employee_sales (employee STRING, month INT64, sales FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employee_sales VALUES ('Alice', 1, 100), ('Alice', 2, 150), ('Bob', 1, 200)")
        .unwrap();

    let result = executor.execute_sql(
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
    );
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

    #[test]
    fn test_load_csv_basic() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .unwrap();

        let temp_file = create_simple_csv();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
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
    }

    #[test]
    fn test_load_csv_with_file_uri() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE products (id INT64, name STRING, value FLOAT64)")
            .unwrap();

        let temp_file = create_csv_with_header();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO products FROM FILES (FORMAT='CSV', URIS=['file://{}'], skip_leading_rows=1)",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM products")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 3);
    }

    #[test]
    #[ignore]
    fn test_load_csv_no_header() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE data (id INT64, name STRING, value INT64)")
            .unwrap();

        let temp_file = create_csv_no_header();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO data FROM FILES (FORMAT='CSV', URIS=['{}'])",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM data ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
        assert_eq!(get_i64(&result, 2, 0), 100);
    }

    #[test]
    fn test_load_csv_overwrite() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO users VALUES (100, 'Existing', 0.0)")
            .unwrap();

        let temp_file = create_simple_csv();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA OVERWRITE users FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM users ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
    }

    #[test]
    fn test_load_csv_append() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE users (id INT64, name STRING, score FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO users VALUES (0, 'Existing', 50.0)")
            .unwrap();

        let temp_file = create_simple_csv();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO users FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM users")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 4);
    }

    #[test]
    #[ignore]
    fn test_load_csv_with_tab_delimiter() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE tsv_data (id INT64, name STRING, value FLOAT64)")
            .unwrap();

        let temp_file = create_tsv_file();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO tsv_data FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1, field_delimiter='\\t')",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM tsv_data ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alpha");
        assert_eq!(get_f64(&result, 2, 0), 10.5);
    }

    #[test]
    #[ignore]
    fn test_load_csv_with_quoted_fields() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE quoted_csv (id INT64, name STRING, description STRING)")
            .unwrap();

        let temp_file = create_csv_with_quotes();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO quoted_csv FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM quoted_csv ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_string(&result, 1, 0), "Alice");
        assert_eq!(get_string(&result, 2, 2), "Description with, comma");
    }

    #[test]
    #[ignore]
    fn test_load_csv_with_null_marker() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE null_test (id INT64, value INT64, text STRING)")
            .unwrap();

        let temp_file = create_csv_with_nulls();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO null_test FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1, null_marker='NA')",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM null_test ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert!(is_null(&result, 1, 1));
        assert!(is_null(&result, 2, 2));
    }

    #[test]
    fn test_load_csv_with_typed_columns() {
        let mut executor = create_executor();
        executor
            .execute_sql(
                "CREATE TABLE typed_csv (id INT64, active BOOL, created_date DATE, amount FLOAT64)",
            )
            .unwrap();

        let temp_file = create_csv_with_different_types();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO typed_csv FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT id, amount FROM typed_csv ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_f64(&result, 1, 0), 100.50);
    }

    #[test]
    fn test_export_csv_basic() {
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
            "EXPORT DATA OPTIONS(uri='{}', format='CSV', header=true) AS SELECT * FROM products ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        let mut executor2 = create_executor();
        executor2
            .execute_sql("CREATE TABLE imported (id INT64, name STRING, price FLOAT64)")
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        executor2.execute_sql(&load_sql).unwrap();

        let result = executor2
            .execute_sql("SELECT * FROM imported ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Apple");
    }

    #[test]
    #[ignore]
    fn test_export_csv_with_field_delimiter() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE data (id INT64, value STRING)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO data VALUES (1, 'A'), (2, 'B')")
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='CSV', field_delimiter='|') AS SELECT * FROM data ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        let mut executor2 = create_executor();
        executor2
            .execute_sql("CREATE TABLE imported (id INT64, value STRING)")
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported FROM FILES (FORMAT='CSV', URIS=['{}'], field_delimiter='|')",
            path
        );
        executor2.execute_sql(&load_sql).unwrap();

        let result = executor2
            .execute_sql("SELECT * FROM imported ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "A");
    }

    #[test]
    fn test_roundtrip_csv_all_basic_types() {
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
                    (2, FALSE, 200.25, 'sample')",
            )
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='CSV', header=true) AS SELECT * FROM all_types ORDER BY id",
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
            "LOAD DATA INTO imported_types FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
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
    fn test_load_csv_file_not_found() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE test_table (id INT64)")
            .unwrap();

        let result = executor.execute_sql(
            "LOAD DATA INTO test_table FROM FILES (FORMAT='CSV', URIS=['/nonexistent/path/file.csv'])",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_load_csv_column_subset() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE partial (id INT64, name STRING)")
            .unwrap();

        let temp_file = create_simple_csv();
        let path = temp_file.path().to_str().unwrap();

        let load_sql = format!(
            "LOAD DATA INTO partial FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT * FROM partial ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
    }

    #[test]
    fn test_export_csv_with_nulls() {
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
            "EXPORT DATA OPTIONS(uri='{}', format='CSV', header=true) AS SELECT * FROM nullable_data ORDER BY id",
            path
        );
        executor.execute_sql(&export_sql).unwrap();

        let mut executor2 = create_executor();
        executor2
            .execute_sql("CREATE TABLE imported_nullable (id INT64, value STRING)")
            .unwrap();
        let load_sql = format!(
            "LOAD DATA INTO imported_nullable FROM FILES (FORMAT='CSV', URIS=['{}'], skip_leading_rows=1)",
            path
        );
        executor2.execute_sql(&load_sql).unwrap();

        let result = executor2
            .execute_sql("SELECT * FROM imported_nullable ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert!(is_null(&result, 1, 1));
    }

    #[test]
    fn test_export_csv_empty_result() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE empty_source (id INT64, value STRING)")
            .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let export_sql = format!(
            "EXPORT DATA OPTIONS(uri='{}', format='CSV') AS SELECT * FROM empty_source",
            path
        );
        let result = executor.execute_sql(&export_sql);
        assert!(result.is_ok());
    }

    #[test]
    #[ignore]
    fn test_load_csv_multiple_files() {
        let mut executor = create_executor();
        executor
            .execute_sql("CREATE TABLE multi_source (id INT64, name STRING, value INT64)")
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
        executor.execute_sql(&load_sql).unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM multi_source")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 4);
    }
}
