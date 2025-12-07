use criterion::{black_box, BenchmarkId, Criterion};
use yachtsql::QueryExecutor;

use crate::common::{create_executor, ROW_COUNTS};

fn setup_numeric_table(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql("CREATE TABLE numeric_data (id INT64, int_val INT64, float_val FLOAT64)")
        .expect("Failed to create numeric_data table");

    for i in 0..rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO numeric_data VALUES ({}, {}, {})",
                i,
                i * 100,
                i as f64 * 1.5
            ))
            .expect("Failed to insert numeric data");
    }
}

fn setup_string_table(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql("CREATE TABLE string_data (id INT64, short_str STRING, long_str STRING)")
        .expect("Failed to create string_data table");

    for i in 0..rows {
        let short = format!("s{}", i);
        let long = format!("This is a longer string with more content for row number {}", i);
        executor
            .execute_sql(&format!(
                "INSERT INTO string_data VALUES ({}, '{}', '{}')",
                i, short, long
            ))
            .expect("Failed to insert string data");
    }
}

pub fn bench_int64_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_dtype_int64");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_numeric_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT int_val, int_val + 100, int_val * 2 FROM numeric_data")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_float64_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_dtype_float64");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_numeric_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT float_val, float_val + 1.5, float_val * 2.0 FROM numeric_data")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_string_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_dtype_string");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_string_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT short_str, long_str FROM string_data")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_mixed_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_dtype_mixed");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_numeric_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT id, int_val, float_val, int_val + float_val FROM numeric_data")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_type_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_dtype_comparison");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_numeric_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT * FROM numeric_data WHERE int_val > 50 AND float_val < 100.0")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}
