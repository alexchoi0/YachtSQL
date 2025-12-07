use criterion::{black_box, BenchmarkId, Criterion};
use yachtsql::QueryExecutor;

use crate::common::create_executor;

const SCALE_SIZES: [usize; 6] = [10, 50, 100, 200, 500, 1000];

fn setup_large_table(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql("CREATE TABLE large_data (id INT64, value FLOAT64, category STRING, flag INT64)")
        .expect("Failed to create large_data table");

    let categories = ["A", "B", "C", "D", "E"];
    for i in 0..rows {
        let category = categories[i % categories.len()];
        executor
            .execute_sql(&format!(
                "INSERT INTO large_data VALUES ({}, {}, '{}', {})",
                i,
                i as f64 * 1.1,
                category,
                i % 2
            ))
            .expect("Failed to insert large data");
    }
}

pub fn bench_scale_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_scale_select");
    for &rows in &SCALE_SIZES {
        let mut executor = create_executor();
        setup_large_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| black_box(executor.execute_sql("SELECT * FROM large_data").unwrap()))
        });
    }
    group.finish();
}

pub fn bench_scale_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_scale_filter");
    for &rows in &SCALE_SIZES {
        let mut executor = create_executor();
        setup_large_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT * FROM large_data WHERE value > 50.0 AND flag = 1")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_scale_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_scale_aggregation");
    for &rows in &SCALE_SIZES {
        let mut executor = create_executor();
        setup_large_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT category, COUNT(1), SUM(value), AVG(value) FROM large_data GROUP BY category",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_scale_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_scale_sort");
    for &rows in &SCALE_SIZES {
        let mut executor = create_executor();
        setup_large_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT * FROM large_data ORDER BY value DESC")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}
