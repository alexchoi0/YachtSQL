#[path = "helpers.rs"]
mod helpers;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use indexmap::IndexMap;
use yachtsql::{DataType, Field, Schema, Table, Value};
use yachtsql_storage::{Row, StorageLayout, TableSchemaOps};

fn create_test_schema() -> Schema {
    Schema::from_fields(vec![
        Field::nullable("id", DataType::Int64),
        Field::nullable("name", DataType::String),
        Field::nullable("email", DataType::String),
        Field::nullable("age", DataType::Int64),
        Field::nullable("score", DataType::Float64),
    ])
}

fn build_row(schema: &Schema, id: i64) -> Row {
    let mut row = Row::for_schema(schema);
    row.set_by_name(schema, "id", Value::int64(id)).unwrap();
    row.set_by_name(schema, "name", Value::string(format!("User{}", id)))
        .unwrap();
    row.set_by_name(
        schema,
        "email",
        Value::string(format!("user{}@example.com", id)),
    )
    .unwrap();
    row.set_by_name(schema, "age", Value::int64(20 + (id % 50)))
        .unwrap();
    row.set_by_name(schema, "score", Value::float64((id as f64) * 1.5))
        .unwrap();
    row
}

fn build_table(layout: StorageLayout, num_rows: usize) -> Table {
    let schema = create_test_schema();
    let mut table = Table::with_layout(schema.clone(), layout);

    for i in 0..num_rows {
        let row = build_row(&schema, i as i64);
        table.insert_row(row).unwrap();
    }

    table
}

fn bench_insert_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_single");
    configure_standard(&mut group);

    let schema = create_test_schema();

    group.bench_function("columnar", |b| {
        b.iter_batched(
            || Table::with_layout(schema.clone(), StorageLayout::Columnar),
            |mut table| {
                let row = build_row(&schema, 1);
                black_box(table.insert_row(row)).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("row", |b| {
        b.iter_batched(
            || Table::with_layout(schema.clone(), StorageLayout::Row),
            |mut table| {
                let row = build_row(&schema, 1);
                black_box(table.insert_row(row)).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_insert_bulk(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_bulk");
    configure_heavy(&mut group);

    let sizes = [100, 1_000, 10_000];

    for &size in &sizes {
        let schema = create_test_schema();

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("columnar", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let mut rows = Vec::with_capacity(size);
                    for i in 0..size {
                        rows.push(build_row(&schema, i as i64));
                    }
                    (
                        Table::with_layout(schema.clone(), StorageLayout::Columnar),
                        rows,
                    )
                },
                |(mut table, rows)| {
                    black_box(table.insert_rows(rows)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("row", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let mut rows = Vec::with_capacity(size);
                    for i in 0..size {
                        rows.push(build_row(&schema, i as i64));
                    }
                    (Table::with_layout(schema.clone(), StorageLayout::Row), rows)
                },
                |(mut table, rows)| {
                    black_box(table.insert_rows(rows)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_select_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_all");
    configure_heavy(&mut group);

    let sizes = [100, 1_000, 10_000];

    for &size in &sizes {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("columnar", size), &size, |b, &size| {
            let table = build_table(StorageLayout::Columnar, size);
            b.iter(|| {
                black_box(table.get_all_rows());
            });
        });

        group.bench_with_input(BenchmarkId::new("row", size), &size, |b, &size| {
            let table = build_table(StorageLayout::Row, size);
            b.iter(|| {
                black_box(table.get_all_rows());
            });
        });
    }

    group.finish();
}

fn bench_select_filtered(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_filtered");
    configure_heavy(&mut group);

    let size = 10_000;
    group.throughput(Throughput::Elements(size as u64));

    group.bench_function("columnar", |b| {
        let table = build_table(StorageLayout::Columnar, size);
        b.iter(|| {
            let filtered = table
                .filter_rows(|row| {
                    let age = row.get_by_name(table.schema(), "age").cloned();
                    Ok(age.and_then(|v| v.as_i64()).is_some_and(|a| a > 40))
                })
                .unwrap();
            black_box(filtered);
        });
    });

    group.bench_function("row", |b| {
        let table = build_table(StorageLayout::Row, size);
        b.iter(|| {
            let filtered = table
                .filter_rows(|row| {
                    let age = row.get_by_name(table.schema(), "age").cloned();
                    Ok(age.and_then(|v| v.as_i64()).is_some_and(|a| a > 40))
                })
                .unwrap();
            black_box(filtered);
        });
    });

    group.finish();
}

fn bench_select_projected(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_projected");
    configure_heavy(&mut group);

    let size = 10_000;
    group.throughput(Throughput::Elements(size as u64));

    let cols = vec!["id".to_string(), "name".to_string()];

    group.bench_function("columnar", |b| {
        let table = build_table(StorageLayout::Columnar, size);
        b.iter(|| {
            let projected = table.project_columns(&cols).unwrap();
            black_box(projected);
        });
    });

    group.bench_function("row", |b| {
        let table = build_table(StorageLayout::Row, size);
        b.iter(|| {
            let projected = table.project_columns(&cols).unwrap();
            black_box(projected);
        });
    });

    group.finish();
}

fn bench_update_single_cell(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_single_cell");
    configure_standard(&mut group);

    let size = 1_000;

    group.bench_function("columnar", |b| {
        b.iter_batched(
            || build_table(StorageLayout::Columnar, size),
            |mut table| {
                black_box(table.update_cell_at_index(500, "age", Value::int64(99))).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("row", |b| {
        b.iter_batched(
            || build_table(StorageLayout::Row, size),
            |mut table| {
                black_box(table.update_cell_at_index(500, "age", Value::int64(99))).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_update_multiple_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_multiple_rows");
    configure_heavy(&mut group);

    let size = 10_000;

    let mut updates = IndexMap::new();
    updates.insert("age".to_string(), Value::int64(100));
    updates.insert("score".to_string(), Value::float64(999.9));

    group.bench_function("columnar", |b| {
        b.iter_batched(
            || build_table(StorageLayout::Columnar, size),
            |mut table| {
                let schema = table.schema().clone();
                let count = table
                    .update_rows(&updates, |row| {
                        let id = row.get_by_name(&schema, "id").cloned();
                        Ok(id.and_then(|v| v.as_i64()).is_some_and(|i| i % 10 == 0))
                    })
                    .unwrap();
                black_box(count);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("row", |b| {
        b.iter_batched(
            || build_table(StorageLayout::Row, size),
            |mut table| {
                let schema = table.schema().clone();
                let count = table
                    .update_rows(&updates, |row| {
                        let id = row.get_by_name(&schema, "id").cloned();
                        Ok(id.and_then(|v| v.as_i64()).is_some_and(|i| i % 10 == 0))
                    })
                    .unwrap();
                black_box(count);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_add_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_column");
    configure_heavy(&mut group);

    let sizes = [100, 1_000, 10_000];

    for &size in &sizes {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("columnar", size), &size, |b, &size| {
            b.iter_batched(
                || build_table(StorageLayout::Columnar, size),
                |mut table| {
                    let field = Field::nullable("new_col", DataType::String);
                    let default = Some(Value::string("default".to_string()));
                    black_box(table.add_column(field, default)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("row", size), &size, |b, &size| {
            b.iter_batched(
                || build_table(StorageLayout::Row, size),
                |mut table| {
                    let field = Field::nullable("new_col", DataType::String);
                    let default = Some(Value::string("default".to_string()));
                    black_box(table.add_column(field, default)).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_drop_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("drop_column");
    configure_heavy(&mut group);

    let sizes = [100, 1_000, 10_000];

    for &size in &sizes {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("columnar", size), &size, |b, &size| {
            b.iter_batched(
                || build_table(StorageLayout::Columnar, size),
                |mut table| {
                    black_box(table.drop_column("email")).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("row", size), &size, |b, &size| {
            b.iter_batched(
                || build_table(StorageLayout::Row, size),
                |mut table| {
                    black_box(table.drop_column("email")).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_rename_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("rename_column");
    configure_standard(&mut group);

    let size = 10_000;

    group.bench_function("columnar", |b| {
        b.iter_batched(
            || build_table(StorageLayout::Columnar, size),
            |mut table| {
                black_box(table.rename_column("email", "email_address")).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("row", |b| {
        b.iter_batched(
            || build_table(StorageLayout::Row, size),
            |mut table| {
                black_box(table.rename_column("email", "email_address")).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_slice(c: &mut Criterion) {
    let mut group = c.benchmark_group("slice");
    configure_heavy(&mut group);

    let size = 10_000;
    let slice_size = 1_000;

    group.throughput(Throughput::Elements(slice_size as u64));

    group.bench_function("columnar", |b| {
        let table = build_table(StorageLayout::Columnar, size);
        b.iter(|| {
            let sliced = table.slice(1000, slice_size).unwrap();
            black_box(sliced);
        });
    });

    group.bench_function("row", |b| {
        let table = build_table(StorageLayout::Row, size);
        b.iter(|| {
            let sliced = table.slice(1000, slice_size).unwrap();
            black_box(sliced);
        });
    });

    group.finish();
}

fn bench_gather(c: &mut Criterion) {
    let mut group = c.benchmark_group("gather");
    configure_heavy(&mut group);

    let size = 10_000;
    let indices: Vec<usize> = (0..size).step_by(10).collect();

    group.throughput(Throughput::Elements(indices.len() as u64));

    group.bench_function("columnar", |b| {
        let table = build_table(StorageLayout::Columnar, size);
        b.iter(|| {
            let gathered = table.gather(&indices).unwrap();
            black_box(gathered);
        });
    });

    group.bench_function("row", |b| {
        let table = build_table(StorageLayout::Row, size);
        b.iter(|| {
            let gathered = table.gather(&indices).unwrap();
            black_box(gathered);
        });
    });

    group.finish();
}

fn bench_statistics(c: &mut Criterion) {
    let mut group = c.benchmark_group("statistics");
    configure_heavy(&mut group);

    let sizes = [1_000, 10_000];

    for &size in &sizes {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("columnar", size), &size, |b, &size| {
            let table = build_table(StorageLayout::Columnar, size);
            b.iter(|| {
                let stats = table.get_statistics();
                black_box(stats);
            });
        });

        group.bench_with_input(BenchmarkId::new("row", size), &size, |b, &size| {
            let table = build_table(StorageLayout::Row, size);
            b.iter(|| {
                let stats = table.get_statistics();
                black_box(stats);
            });
        });
    }

    group.finish();
}

fn bench_delete_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_rows");
    configure_heavy(&mut group);

    let size = 10_000;

    group.bench_function("columnar", |b| {
        b.iter_batched(
            || build_table(StorageLayout::Columnar, size),
            |mut table| {
                let schema = table.schema().clone();
                let count = table
                    .delete_rows(|row| {
                        let id = row.get_by_name(&schema, "id").cloned();
                        Ok(id.and_then(|v| v.as_i64()).is_some_and(|i| i % 2 == 0))
                    })
                    .unwrap();
                black_box(count);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("row", |b| {
        b.iter_batched(
            || build_table(StorageLayout::Row, size),
            |mut table| {
                let schema = table.schema().clone();
                let count = table
                    .delete_rows(|row| {
                        let id = row.get_by_name(&schema, "id").cloned();
                        Ok(id.and_then(|v| v.as_i64()).is_some_and(|i| i % 2 == 0))
                    })
                    .unwrap();
                black_box(count);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    storage_benches,
    bench_insert_single,
    bench_insert_bulk,
    bench_select_all,
    bench_select_filtered,
    bench_select_projected,
    bench_update_single_cell,
    bench_update_multiple_rows,
    bench_add_column,
    bench_drop_column,
    bench_rename_column,
    bench_slice,
    bench_gather,
    bench_statistics,
    bench_delete_rows,
);

criterion_main!(storage_benches);
