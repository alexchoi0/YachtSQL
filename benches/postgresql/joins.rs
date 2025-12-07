use criterion::{black_box, BenchmarkId, Criterion};

use crate::common::{create_executor, setup_join_tables, ROW_COUNTS};

pub fn bench_inner_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_join_inner");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_left_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_join_left");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_right_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_join_right");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT u.name, o.amount FROM users u RIGHT JOIN orders o ON u.id = o.user_id",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_full_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_join_full");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT u.name, o.amount FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_join_with_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_join_with_agg");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT u.name, COUNT(1), SUM(o.amount) FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY u.name",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}
