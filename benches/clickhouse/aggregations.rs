use criterion::{black_box, BenchmarkId, Criterion};

use crate::common::{create_executor, setup_orders_table, ROW_COUNTS};

pub fn bench_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_agg_count");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_orders_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| black_box(executor.execute_sql("SELECT COUNT(1) FROM orders").unwrap()))
        });
    }
    group.finish();
}

pub fn bench_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_agg_sum");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_orders_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| black_box(executor.execute_sql("SELECT SUM(amount) FROM orders").unwrap()))
        });
    }
    group.finish();
}

pub fn bench_avg(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_agg_avg");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_orders_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| black_box(executor.execute_sql("SELECT AVG(amount) FROM orders").unwrap()))
        });
    }
    group.finish();
}

pub fn bench_min_max(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_agg_min_max");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_orders_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT MIN(amount), MAX(amount) FROM orders")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_group_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_agg_group_by");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_orders_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT status, COUNT(1), SUM(amount) FROM orders GROUP BY status")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_group_by_multiple(c: &mut Criterion) {
    let mut group = c.benchmark_group("ch_agg_group_by_multi");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_orders_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT status, category, COUNT(1), AVG(amount) FROM orders GROUP BY status, category",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}
