use criterion::{black_box, BenchmarkId, Criterion};

use crate::common::{create_executor, setup_users_table, ROW_COUNTS};

pub fn bench_string_upper(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_func_upper");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| black_box(executor.execute_sql("SELECT UPPER(name) FROM users").unwrap()))
        });
    }
    group.finish();
}

pub fn bench_string_lower(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_func_lower");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| black_box(executor.execute_sql("SELECT LOWER(name) FROM users").unwrap()))
        });
    }
    group.finish();
}

pub fn bench_string_length(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_func_length");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| black_box(executor.execute_sql("SELECT LENGTH(name) FROM users").unwrap()))
        });
    }
    group.finish();
}

pub fn bench_string_concat(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_func_concat");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT CONCAT(name, ' - ', email) FROM users")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_string_substring(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_func_substring");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT SUBSTRING(name, 1, 3) FROM users")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_math_abs(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_func_abs");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| black_box(executor.execute_sql("SELECT ABS(age - 50) FROM users").unwrap()))
        });
    }
    group.finish();
}

pub fn bench_coalesce(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_func_coalesce");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT COALESCE(name, 'Unknown') FROM users")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}
