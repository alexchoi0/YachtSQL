use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, Int64Builder, StringBuilder, StructArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::NamedTempFile;
use yachtsql::{DialectType, QueryExecutor};

fn create_parquet_file(num_rows: usize) -> NamedTempFile {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new("value", ArrowDataType::Float64, true),
    ]));

    let mut id_builder = Int64Builder::with_capacity(num_rows);
    let mut name_builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
    let mut value_builder = Float64Builder::with_capacity(num_rows);

    for i in 0..num_rows {
        id_builder.append_value(i as i64);
        if i % 10 == 0 {
            name_builder.append_null();
        } else {
            name_builder.append_value(format!("name_{}", i));
        }
        value_builder.append_value(i as f64 * 1.5);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(name_builder.finish()),
            Arc::new(value_builder.finish()),
        ],
    )
    .unwrap();

    let temp_file = NamedTempFile::new().unwrap();
    let props = WriterProperties::builder().build();

    {
        let file = temp_file.reopen().unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    temp_file
}

fn create_wide_parquet_file(num_rows: usize, num_columns: usize) -> NamedTempFile {
    let fields: Vec<ArrowField> = (0..num_columns)
        .map(|i| ArrowField::new(format!("col_{}", i), ArrowDataType::Int64, true))
        .collect();
    let schema = Arc::new(ArrowSchema::new(fields));

    let arrays: Vec<ArrayRef> = (0..num_columns)
        .map(|col| {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for row in 0..num_rows {
                builder.append_value((row * num_columns + col) as i64);
            }
            Arc::new(builder.finish()) as ArrayRef
        })
        .collect();

    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

    let temp_file = NamedTempFile::new().unwrap();
    {
        let file = temp_file.reopen().unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    temp_file
}

fn create_nested_parquet_file(num_rows: usize) -> NamedTempFile {
    let inner_fields = vec![
        ArrowField::new("x", ArrowDataType::Int64, false),
        ArrowField::new("y", ArrowDataType::Float64, false),
    ];
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new(
            "point",
            ArrowDataType::Struct(inner_fields.clone().into()),
            true,
        ),
    ]));

    let mut id_builder = Int64Builder::with_capacity(num_rows);
    let mut x_builder = Int64Builder::with_capacity(num_rows);
    let mut y_builder = Float64Builder::with_capacity(num_rows);

    for i in 0..num_rows {
        id_builder.append_value(i as i64);
        x_builder.append_value(i as i64 * 10);
        y_builder.append_value(i as f64 * 0.5);
    }

    let struct_array = StructArray::from(vec![
        (
            Arc::new(ArrowField::new("x", ArrowDataType::Int64, false)),
            Arc::new(x_builder.finish()) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new("y", ArrowDataType::Float64, false)),
            Arc::new(y_builder.finish()) as ArrayRef,
        ),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_builder.finish()), Arc::new(struct_array)],
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

fn bench_load_data_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("load_data_rows");

    for num_rows in [100, 1_000, 10_000, 100_000] {
        let temp_file = create_parquet_file(num_rows);
        let path = temp_file.path().to_str().unwrap().to_string();

        group.throughput(Throughput::Elements(num_rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(num_rows), &path, |b, path| {
            b.iter(|| {
                let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
                executor
                    .execute_sql("CREATE TABLE bench_table (id INT64, name STRING, value FLOAT64)")
                    .unwrap();
                let load_sql = format!(
                    "LOAD DATA INTO bench_table FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
                    path
                );
                black_box(executor.execute_sql(&load_sql).unwrap())
            });
        });
    }
    group.finish();
}

fn bench_load_data_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("load_data_columns");
    let num_rows = 10_000;

    for num_columns in [5, 20, 50, 100] {
        let temp_file = create_wide_parquet_file(num_rows, num_columns);
        let path = temp_file.path().to_str().unwrap().to_string();

        let column_defs: String = (0..num_columns)
            .map(|i| format!("col_{} INT64", i))
            .collect::<Vec<_>>()
            .join(", ");

        group.throughput(Throughput::Elements((num_rows * num_columns) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_columns),
            &(path, column_defs),
            |b, (path, column_defs)| {
                b.iter(|| {
                    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
                    executor
                        .execute_sql(&format!("CREATE TABLE wide_table ({})", column_defs))
                        .unwrap();
                    let load_sql = format!(
                        "LOAD DATA INTO wide_table FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
                        path
                    );
                    black_box(executor.execute_sql(&load_sql).unwrap())
                });
            },
        );
    }
    group.finish();
}

fn bench_load_data_nested(c: &mut Criterion) {
    let mut group = c.benchmark_group("load_data_nested");

    for num_rows in [1_000, 10_000, 50_000] {
        let temp_file = create_nested_parquet_file(num_rows);
        let path = temp_file.path().to_str().unwrap().to_string();

        group.throughput(Throughput::Elements(num_rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(num_rows), &path, |b, path| {
            b.iter(|| {
                let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
                executor
                    .execute_sql(
                        "CREATE TABLE nested_table (id INT64, point STRUCT<x INT64, y FLOAT64>)",
                    )
                    .unwrap();
                let load_sql = format!(
                    "LOAD DATA INTO nested_table FROM FILES (FORMAT='PARQUET', URIS=['{}'])",
                    path
                );
                black_box(executor.execute_sql(&load_sql).unwrap())
            });
        });
    }
    group.finish();
}

fn bench_parquet_read_only(c: &mut Criterion) {
    use yachtsql_executor::query_executor::execution::parquet_reader;

    let mut group = c.benchmark_group("parquet_read_only");

    for num_rows in [1_000, 10_000, 100_000] {
        let temp_file = create_parquet_file(num_rows);
        let path = temp_file.path().to_str().unwrap().to_string();

        group.throughput(Throughput::Elements(num_rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(num_rows), &path, |b, path| {
            b.iter(|| black_box(parquet_reader::read_parquet_file(path).unwrap()));
        });
    }
    group.finish();
}

fn bench_export_data_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("export_data_rows");

    for num_rows in [100, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_rows as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);
                        executor
                            .execute_sql(
                                "CREATE TABLE export_bench (id INT64, name STRING, value FLOAT64)",
                            )
                            .unwrap();

                        let insert_values: Vec<String> = (0..num_rows)
                            .map(|i| {
                                format!(
                                    "({}, '{}', {})",
                                    i,
                                    if i % 10 == 0 {
                                        "NULL".to_string()
                                    } else {
                                        format!("name_{}", i)
                                    },
                                    i as f64 * 1.5
                                )
                            })
                            .collect();

                        for chunk in insert_values.chunks(1000) {
                            let insert_sql =
                                format!("INSERT INTO export_bench VALUES {}", chunk.join(", "));
                            executor.execute_sql(&insert_sql).unwrap();
                        }

                        let temp_file = NamedTempFile::new().unwrap();
                        let path = temp_file.path().to_str().unwrap().to_string();
                        (executor, path, temp_file)
                    },
                    |(mut executor, path, _temp_file)| {
                        let export_sql = format!(
                            "EXPORT DATA OPTIONS(uri='{}', format='PARQUET') AS SELECT * FROM export_bench",
                            path
                        );
                        black_box(executor.execute_sql(&export_sql).unwrap())
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_parquet_write_only(c: &mut Criterion) {
    use yachtsql_common::types::{DataType, Value};
    use yachtsql_executor::Table;
    use yachtsql_executor::query_executor::execution::parquet_writer;
    use yachtsql_storage::{Field, Row, Schema};

    let mut group = c.benchmark_group("parquet_write_only");

    for num_rows in [1_000, 10_000, 100_000] {
        let schema = Schema::from_fields(vec![
            Field::required("id".to_string(), DataType::Int64),
            Field::nullable("name".to_string(), DataType::String),
            Field::nullable("value".to_string(), DataType::Float64),
        ]);

        let rows: Vec<Row> = (0..num_rows)
            .map(|i| {
                Row::from_values(vec![
                    Value::int64(i as i64),
                    if i % 10 == 0 {
                        Value::null()
                    } else {
                        Value::string(format!("name_{}", i))
                    },
                    Value::float64(i as f64 * 1.5),
                ])
            })
            .collect();

        let table = Table::from_rows(schema, rows).unwrap();

        group.throughput(Throughput::Elements(num_rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(num_rows), &table, |b, table| {
            b.iter_batched(
                || {
                    let temp_file = NamedTempFile::new().unwrap();
                    let path = temp_file.path().to_str().unwrap().to_string();
                    (path, temp_file)
                },
                |(path, _temp_file)| {
                    black_box(parquet_writer::write_parquet_file(&path, table, None).unwrap())
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_load_data_rows,
    bench_load_data_columns,
    bench_load_data_nested,
    bench_parquet_read_only,
    bench_export_data_rows,
    bench_parquet_write_only,
);
criterion_main!(benches);
