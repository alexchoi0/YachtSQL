#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

fn setup_test_data(executor: &mut QueryExecutor, outer_rows: usize, inner_rows: usize) {
    executor
        .execute_sql(
            "CREATE TABLE employees (id INT64, name STRING, dept_id INT64, salary FLOAT64)",
        )
        .unwrap();

    executor
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .unwrap();

    for i in 0..outer_rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO employees VALUES ({}, 'emp_{}', {}, {})",
                i,
                i,
                i % 5,
                50000.0 + (i as f64 * 100.0)
            ))
            .unwrap();
    }

    for i in 0..inner_rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO departments VALUES ({}, 'dept_{}')",
                i, i
            ))
            .unwrap();
    }
}

fn bench_uncorrelated_scalar_subquery(c: &mut Criterion) {
    let mut group = c.benchmark_group("uncorrelated_scalar_subquery");
    configure_quick(&mut group);

    for &outer_rows in &[10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("cached", outer_rows),
            &outer_rows,
            |b, &outer_rows| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_test_data(&mut executor, outer_rows, 100);
                        executor
                    },
                    |mut executor| {
                        black_box(
                            executor.execute_sql(
                                "SELECT name, (SELECT AVG(salary) FROM employees) as avg_sal FROM employees"
                            )
                        ).unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_uncorrelated_exists_subquery(c: &mut Criterion) {
    let mut group = c.benchmark_group("uncorrelated_exists_subquery");
    configure_quick(&mut group);

    for &outer_rows in &[10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("cached", outer_rows),
            &outer_rows,
            |b, &outer_rows| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_test_data(&mut executor, outer_rows, 100);
                        executor
                    },
                    |mut executor| {
                        black_box(
                            executor.execute_sql(
                                "SELECT name FROM employees WHERE EXISTS (SELECT 1 FROM departments WHERE id < 3)"
                            )
                        ).unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_uncorrelated_in_subquery(c: &mut Criterion) {
    let mut group = c.benchmark_group("uncorrelated_in_subquery");
    configure_quick(&mut group);

    for &outer_rows in &[10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("cached", outer_rows),
            &outer_rows,
            |b, &outer_rows| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_test_data(&mut executor, outer_rows, 100);
                        executor
                    },
                    |mut executor| {
                        black_box(
                            executor.execute_sql(
                                "SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE id < 3)"
                            )
                        ).unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_comparison_with_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("subquery_vs_join_comparison");
    configure_quick(&mut group);

    for &outer_rows in &[100, 500] {
        group.bench_with_input(
            BenchmarkId::new("scalar_subquery", outer_rows),
            &outer_rows,
            |b, &outer_rows| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_test_data(&mut executor, outer_rows, 100);
                        executor
                    },
                    |mut executor| {
                        black_box(
                            executor.execute_sql(
                                "SELECT name, (SELECT AVG(salary) FROM employees) as avg_sal FROM employees"
                            )
                        ).unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("cross_join_equivalent", outer_rows),
            &outer_rows,
            |b, &outer_rows| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_test_data(&mut executor, outer_rows, 100);
                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql(
                            "SELECT e.name, a.avg_sal FROM employees e CROSS JOIN (SELECT AVG(salary) as avg_sal FROM employees) a"
                        )).unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_uncorrelated_scalar_subquery,
    bench_uncorrelated_exists_subquery,
    bench_uncorrelated_in_subquery,
    bench_comparison_with_join,
);
criterion_main!(benches);
