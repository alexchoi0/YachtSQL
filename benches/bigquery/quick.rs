use criterion::{black_box, Criterion};

use crate::common::{create_executor, setup_users_table};

pub fn bench_quick(c: &mut Criterion) {
    let mut executor = create_executor();
    setup_users_table(&mut executor, 10);
    c.bench_function("bq_quick", |b| {
        b.iter(|| black_box(executor.execute_sql("SELECT * FROM users").unwrap()))
    });
}
