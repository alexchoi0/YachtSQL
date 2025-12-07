use criterion::{black_box, BenchmarkId, Criterion};
use yachtsql::QueryExecutor;

use crate::common::{create_executor, ROW_COUNTS};

fn setup_merge_tables(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql("CREATE TABLE target (id INT64, value INT64, status STRING)")
        .expect("Failed to create target table");

    executor
        .execute_sql("CREATE TABLE source (id INT64, value INT64, status STRING)")
        .expect("Failed to create source table");

    for i in 0..rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO target VALUES ({}, {}, 'old')",
                i,
                i * 10
            ))
            .expect("Failed to insert target");

        let source_id = i + rows / 2;
        executor
            .execute_sql(&format!(
                "INSERT INTO source VALUES ({}, {}, 'new')",
                source_id,
                source_id * 10
            ))
            .expect("Failed to insert source");
    }
}

pub fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_merge_insert");
    for &rows in &ROW_COUNTS {
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter(|| {
                let mut executor = create_executor();
                executor
                    .execute_sql("CREATE TABLE items (id INT64, name STRING, value INT64)")
                    .unwrap();
                for i in 0..rows {
                    black_box(
                        executor
                            .execute_sql(&format!(
                                "INSERT INTO items VALUES ({}, 'item{}', {})",
                                i, i, i * 10
                            ))
                            .unwrap(),
                    );
                }
            })
        });
    }
    group.finish();
}

pub fn bench_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_merge_update");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_merge_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("UPDATE target SET value = value + 1 WHERE id < 10")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_merge_delete");
    for &rows in &ROW_COUNTS {
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter(|| {
                let mut executor = create_executor();
                setup_merge_tables(&mut executor, rows);
                black_box(
                    executor
                        .execute_sql("DELETE FROM target WHERE id < 5")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}
