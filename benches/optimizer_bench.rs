use criterion::{Criterion, black_box, criterion_group, criterion_main};
use yachtsql::YachtSQLEngine;

fn setup_session_with_data(row_count: usize) -> yachtsql::YachtSQLSession {
    let engine = YachtSQLEngine::new();
    let mut session = engine.create_session();

    session
        .execute_sql(
            "CREATE TABLE users (
                id INT64,
                name STRING,
                age INT64,
                salary FLOAT64,
                department STRING
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE orders (
                id INT64,
                user_id INT64,
                amount FLOAT64,
                status STRING
            )",
        )
        .unwrap();

    let batch_size = 100;
    let departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"];
    let statuses = ["pending", "completed", "cancelled"];

    for batch in 0..(row_count / batch_size) {
        let mut values = Vec::new();
        for i in 0..batch_size {
            let id = batch * batch_size + i + 1;
            let dept = departments[id % departments.len()];
            let age = 20 + (id % 50);
            let salary = 50000.0 + (id % 100) as f64 * 1000.0;
            values.push(format!(
                "({}, 'User{}', {}, {}, '{}')",
                id, id, age, salary, dept
            ));
        }
        let sql = format!("INSERT INTO users VALUES {}", values.join(", "));
        session.execute_sql(&sql).unwrap();
    }

    for batch in 0..(row_count / batch_size) {
        let mut values = Vec::new();
        for i in 0..batch_size {
            let id = batch * batch_size + i + 1;
            let user_id = (id % row_count) + 1;
            let amount = 10.0 + (id % 1000) as f64;
            let status = statuses[id % statuses.len()];
            values.push(format!("({}, {}, {}, '{}')", id, user_id, amount, status));
        }
        let sql = format!("INSERT INTO orders VALUES {}", values.join(", "));
        session.execute_sql(&sql).unwrap();
    }

    session
}

fn bench_topn_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("topn");

    for row_count in [1000, 10000, 50000] {
        let mut session = setup_session_with_data(row_count);

        group.bench_function(format!("{}_rows_limit_20", row_count), |b| {
            b.iter(|| {
                black_box(
                    session
                        .execute_sql("SELECT * FROM users ORDER BY salary DESC LIMIT 20")
                        .unwrap(),
                )
            })
        });

        group.bench_function(format!("{}_rows_scan_only", row_count), |b| {
            b.iter(|| black_box(session.execute_sql("SELECT * FROM users").unwrap()))
        });
    }

    group.finish();
}

fn bench_filter_queries(c: &mut Criterion) {
    let mut session = setup_session_with_data(1000);

    let mut group = c.benchmark_group("filter");

    group.bench_function("single_filter", |b| {
        b.iter(|| {
            black_box(
                session
                    .execute_sql("SELECT * FROM users WHERE age > 30")
                    .unwrap(),
            )
        })
    });

    group.bench_function("multiple_filters_and", |b| {
        b.iter(|| {
            black_box(
                session
                    .execute_sql(
                        "SELECT * FROM users WHERE age > 30 AND salary > 60000 AND department = 'Engineering'",
                    )
                    .unwrap(),
            )
        })
    });

    group.bench_function("filter_with_order_limit", |b| {
        b.iter(|| {
            black_box(
                session
                    .execute_sql(
                        "SELECT * FROM users WHERE department = 'Engineering' ORDER BY salary DESC LIMIT 10",
                    )
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_join_queries(c: &mut Criterion) {
    let mut session = setup_session_with_data(500);

    let mut group = c.benchmark_group("join");

    group.bench_function("inner_join_basic", |b| {
        b.iter(|| {
            black_box(
                session
                    .execute_sql(
                        "SELECT u.name, o.amount
                         FROM users u
                         INNER JOIN orders o ON u.id = o.user_id
                         LIMIT 100",
                    )
                    .unwrap(),
            )
        })
    });

    group.bench_function("join_with_filter_on_left", |b| {
        b.iter(|| {
            black_box(
                session
                    .execute_sql(
                        "SELECT u.name, o.amount
                         FROM users u
                         INNER JOIN orders o ON u.id = o.user_id
                         WHERE u.department = 'Engineering'
                         LIMIT 100",
                    )
                    .unwrap(),
            )
        })
    });

    group.bench_function("join_with_filter_on_right", |b| {
        b.iter(|| {
            black_box(
                session
                    .execute_sql(
                        "SELECT u.name, o.amount
                         FROM users u
                         INNER JOIN orders o ON u.id = o.user_id
                         WHERE o.status = 'completed'
                         LIMIT 100",
                    )
                    .unwrap(),
            )
        })
    });

    group.bench_function("join_with_filter_on_both", |b| {
        b.iter(|| {
            black_box(
                session
                    .execute_sql(
                        "SELECT u.name, o.amount
                         FROM users u
                         INNER JOIN orders o ON u.id = o.user_id
                         WHERE u.department = 'Engineering' AND o.status = 'completed'
                         LIMIT 100",
                    )
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_complex_queries(c: &mut Criterion) {
    let mut session = setup_session_with_data(500);

    let mut group = c.benchmark_group("complex");

    group.bench_function("aggregate_with_filter_order_limit", |b| {
        b.iter(|| {
            black_box(
                session
                    .execute_sql(
                        "SELECT department, AVG(salary) as avg_salary, COUNT(*) as cnt
                         FROM users
                         WHERE age > 25
                         GROUP BY department
                         ORDER BY avg_salary DESC
                         LIMIT 3",
                    )
                    .unwrap(),
            )
        })
    });

    group.bench_function("subquery_in_filter", |b| {
        b.iter(|| {
            black_box(
                session
                    .execute_sql(
                        "SELECT * FROM users
                         WHERE salary > (SELECT AVG(salary) FROM users)
                         LIMIT 50",
                    )
                    .unwrap(),
            )
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_topn_queries,
    bench_filter_queries,
    bench_join_queries,
    bench_complex_queries
);

criterion_main!(benches);
