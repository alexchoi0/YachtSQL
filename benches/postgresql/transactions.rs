use criterion::{black_box, BenchmarkId, Criterion};

use crate::common::{create_executor, setup_users_table, ROW_COUNTS};

pub fn bench_begin_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_tx_begin_commit");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(executor.execute_sql("BEGIN").unwrap());
                black_box(
                    executor
                        .execute_sql("SELECT * FROM users WHERE age > 30")
                        .unwrap(),
                );
                black_box(executor.execute_sql("COMMIT").unwrap());
            })
        });
    }
    group.finish();
}

pub fn bench_begin_rollback(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_tx_begin_rollback");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(executor.execute_sql("BEGIN").unwrap());
                black_box(
                    executor
                        .execute_sql("UPDATE users SET age = age + 1 WHERE id = 1")
                        .unwrap(),
                );
                black_box(executor.execute_sql("ROLLBACK").unwrap());
            })
        });
    }
    group.finish();
}

pub fn bench_transaction_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_tx_insert");
    for &rows in &ROW_COUNTS {
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter(|| {
                let mut executor = create_executor();
                executor
                    .execute_sql("CREATE TABLE items (id INT64, name STRING)")
                    .unwrap();
                black_box(executor.execute_sql("BEGIN").unwrap());
                for i in 0..rows {
                    black_box(
                        executor
                            .execute_sql(&format!("INSERT INTO items VALUES ({}, 'item{}')", i, i))
                            .unwrap(),
                    );
                }
                black_box(executor.execute_sql("COMMIT").unwrap());
            })
        });
    }
    group.finish();
}
