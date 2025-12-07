use criterion::{black_box, BenchmarkId, Criterion};

use crate::common::{create_executor, setup_join_tables, ROW_COUNTS};

pub fn bench_subquery_scalar(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_complex_subquery_scalar");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT name, (SELECT COUNT(1) FROM orders WHERE user_id = users.id) as order_count FROM users",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_subquery_in(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_complex_subquery_in");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_cte_simple(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_complex_cte_simple");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "WITH active_users AS (SELECT * FROM users WHERE age > 25) SELECT * FROM active_users",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_cte_with_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_complex_cte_join");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "WITH order_totals AS (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id) SELECT u.name, ot.total FROM users u JOIN order_totals ot ON u.id = ot.user_id",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_union(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_complex_union");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT name FROM users WHERE age < 30 UNION SELECT name FROM users WHERE age >= 50",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_case_expression(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_complex_case");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_join_tables(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql(
                            "SELECT name, CASE WHEN age < 30 THEN 'young' WHEN age < 50 THEN 'middle' ELSE 'senior' END as age_group FROM users",
                        )
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}
