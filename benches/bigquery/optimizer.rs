use criterion::{black_box, BenchmarkId, Criterion};

use crate::common::{create_executor, setup_join_tables, ROW_COUNTS};

pub fn bench_predicate_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_opt_predicate_pushdown");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 30 AND o.amount > 50",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_projection_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_opt_projection_pushdown");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_constant_folding(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_opt_constant_folding");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT name, age + 10 + 20 + 30 FROM users WHERE 1 = 1 AND age > 20 + 5",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_common_subexpr_elimination(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_opt_cse");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT age * 2, age * 2 + 10, age * 2 - 5 FROM users")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_join_reordering(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_opt_join_reorder");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT u.name, o.status FROM orders o JOIN users u ON u.id = o.user_id WHERE o.amount > 100",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}
