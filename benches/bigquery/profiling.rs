use criterion::{black_box, Criterion};

use crate::common::{create_executor, setup_join_tables};

pub fn bench_parse_simple(c: &mut Criterion) {
    let mut executor = create_executor();
    setup_join_tables(&mut executor, 10);
    c.bench_function("bq_profile_parse_simple", |b| {
        b.iter(|| black_box(executor.execute_sql("SELECT * FROM users").unwrap()))
    });
}

pub fn bench_parse_complex(c: &mut Criterion) {
    let mut executor = create_executor();
    setup_join_tables(&mut executor, 10);
    c.bench_function("bq_profile_parse_complex", |b| {
        b.iter(|| {
            black_box(
                executor
                    .execute_sql(
                        "WITH user_orders AS (
                            SELECT u.id, u.name, COUNT(1) as order_count, SUM(o.amount) as total
                            FROM users u
                            LEFT JOIN orders o ON u.id = o.user_id
                            GROUP BY u.id, u.name
                        )
                        SELECT name, order_count, total
                        FROM user_orders
                        WHERE order_count > 0
                        ORDER BY total DESC
                        LIMIT 10",
                    )
                    .unwrap(),
            )
        })
    });
}

pub fn bench_execution_overhead(c: &mut Criterion) {
    let mut executor = create_executor();
    c.bench_function("bq_profile_exec_overhead", |b| {
        b.iter(|| black_box(executor.execute_sql("SELECT 1").unwrap()))
    });
}

pub fn bench_table_scan(c: &mut Criterion) {
    let mut executor = create_executor();
    setup_join_tables(&mut executor, 100);
    c.bench_function("bq_profile_table_scan", |b| {
        b.iter(|| black_box(executor.execute_sql("SELECT * FROM orders").unwrap()))
    });
}
