#[path = "helpers.rs"]
mod helpers;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};
use yachtsql_executor::explain::{
    ExplainOptions, MetricValue, ProfilerDisabled, ProfilerEnabled, format_plan,
};
use yachtsql_optimizer::expr::{BinaryOp, Expr, LiteralValue, OrderByExpr};
use yachtsql_optimizer::plan::PlanNode;

fn bench_explain_statement(c: &mut Criterion) {
    let mut group = c.benchmark_group("explain_statement");
    configure_standard(&mut group);

    group.bench_function("baseline_select", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_basic_table(&mut executor, "test_table", 1000);

        b.iter(|| {
            black_box(executor.execute_sql("SELECT * FROM test_table WHERE value > 500")).unwrap();
        });
    });

    group.bench_function("explain_select", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_basic_table(&mut executor, "test_table", 1000);

        b.iter(|| {
            black_box(executor.execute_sql("EXPLAIN SELECT * FROM test_table WHERE value > 500"))
                .unwrap();
        });
    });

    group.bench_function("explain_verbose_select", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_basic_table(&mut executor, "test_table", 1000);

        b.iter(|| {
            black_box(
                executor.execute_sql("EXPLAIN VERBOSE SELECT * FROM test_table WHERE value > 500"),
            )
            .unwrap();
        });
    });

    group.bench_function("explain_analyze_select", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_basic_table(&mut executor, "test_table", 1000);

        b.iter(|| {
            black_box(
                executor.execute_sql("EXPLAIN ANALYZE SELECT * FROM test_table WHERE value > 500"),
            )
            .unwrap();
        });
    });

    group.finish();
}

fn bench_explain_complexity(c: &mut Criterion) {
    let mut group = c.benchmark_group("explain_complexity");
    configure_standard(&mut group);

    group.bench_function("simple_scan", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_basic_table(&mut executor, "test_table", 1000);

        b.iter(|| {
            black_box(executor.execute_sql("EXPLAIN SELECT * FROM test_table")).unwrap();
        });
    });

    group.bench_function("scan_filter", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_basic_table(&mut executor, "test_table", 1000);

        b.iter(|| {
            black_box(executor.execute_sql("EXPLAIN SELECT * FROM test_table WHERE value > 500"))
                .unwrap();
        });
    });

    group.bench_function("scan_filter_sort", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_basic_table(&mut executor, "test_table", 1000);

        b.iter(|| {
            black_box(executor.execute_sql(
                "EXPLAIN SELECT * FROM test_table WHERE value > 500 ORDER BY value DESC",
            ))
            .unwrap();
        });
    });

    group.bench_function("scan_filter_sort_limit", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_basic_table(&mut executor, "test_table", 1000);

        b.iter(|| {
            black_box(executor.execute_sql(
                "EXPLAIN SELECT * FROM test_table WHERE value > 500 ORDER BY value DESC LIMIT 10",
            ))
            .unwrap();
        });
    });

    group.finish();
}

fn bench_profiler_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("profiler_overhead");
    configure_standard(&mut group);

    group.bench_function("no_profiler", |b| {
        b.iter(|| {
            for _ in 0..100 {
                black_box(vec![0u8; 1024]);
            }
        });
    });

    group.bench_function("profiler_disabled", |b| {
        let profiler = ProfilerDisabled::new();

        b.iter(|| {
            for i in 0..100 {
                let mut timer = profiler.start_operator(i);
                black_box(vec![0u8; 1024]);
                timer.record_batch(10);
            }
        });
    });

    group.bench_function("profiler_enabled", |b| {
        let profiler = ProfilerEnabled::new();

        b.iter(|| {
            for i in 0..100 {
                let mut timer = profiler.start_operator(i);
                black_box(vec![0u8; 1024]);
                timer.record_batch(10);
            }
        });
    });

    group.bench_function("profiler_with_custom_metrics", |b| {
        let profiler = ProfilerEnabled::new();

        b.iter(|| {
            for i in 0..100 {
                let mut timer = profiler.start_operator(i);
                black_box(vec![0u8; 1024]);
                timer.record_batch(10);
                timer.record_custom("selectivity".to_string(), MetricValue::Percentage(50.0));
                timer.record_custom("memory".to_string(), MetricValue::Bytes(1024));
            }
        });
    });

    group.finish();
}

fn bench_profiler_scalability(c: &mut Criterion) {
    let mut group = c.benchmark_group("profiler_scalability");
    configure_standard(&mut group);

    for &num_operators in &[10, 50, 100, 500, 1000] {
        group.throughput(Throughput::Elements(num_operators));
        group.bench_with_input(
            BenchmarkId::new("operators", num_operators),
            &num_operators,
            |b, &num_operators| {
                b.iter(|| {
                    let profiler = ProfilerEnabled::new();

                    for i in 0..num_operators {
                        let mut timer = profiler.start_operator(i as usize);
                        black_box(vec![0u8; 128]);
                        timer.record_batch(10);
                    }

                    black_box(profiler.get_all_metrics());
                });
            },
        );
    }

    group.finish();
}

fn bench_plan_formatting(c: &mut Criterion) {
    let mut group = c.benchmark_group("plan_formatting");
    configure_standard(&mut group);

    group.bench_function("format_simple_scan", |b| {
        let plan = PlanNode::Scan {
            table_name: "test_table".to_string(),
            alias: None,
            projection: None,
        };

        let options = ExplainOptions {
            analyze: false,
            verbose: false,
            profiler_metrics: None,
        };

        b.iter(|| {
            black_box(format_plan(&plan, options.clone())).unwrap();
        });
    });

    group.bench_function("format_scan_filter", |b| {
        let scan = PlanNode::Scan {
            table_name: "test_table".to_string(),
            alias: None,
            projection: None,
        };

        let plan = PlanNode::Filter {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: "value".to_string(),
                    table: None,
                }),
                op: BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Int64(500))),
            },
        };

        let options = ExplainOptions {
            analyze: false,
            verbose: false,
            profiler_metrics: None,
        };

        b.iter(|| {
            black_box(format_plan(&plan, options.clone())).unwrap();
        });
    });

    group.bench_function("format_scan_filter_sort", |b| {
        let scan = PlanNode::Scan {
            table_name: "test_table".to_string(),
            alias: None,
            projection: None,
        };

        let filter = PlanNode::Filter {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: "value".to_string(),
                    table: None,
                }),
                op: BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Int64(500))),
            },
        };

        let plan = PlanNode::Sort {
            input: Box::new(filter),
            order_by: vec![OrderByExpr {
                expr: Expr::Column {
                    name: "value".to_string(),
                    table: None,
                },
                asc: Some(false),
                nulls_first: None,
                collation: None,
            }],
        };

        let options = ExplainOptions {
            analyze: false,
            verbose: false,
            profiler_metrics: None,
        };

        b.iter(|| {
            black_box(format_plan(&plan, options.clone())).unwrap();
        });
    });

    group.finish();
}

fn bench_plan_formatting_with_metrics(c: &mut Criterion) {
    let mut group = c.benchmark_group("plan_formatting_with_metrics");
    configure_standard(&mut group);

    let profiler = ProfilerEnabled::new();
    {
        let mut timer = profiler.start_operator(0);
        timer.record_batch(1000);
    }
    {
        let mut timer = profiler.start_operator(1);
        timer.record_batch(500);
        timer.record_custom("selectivity".to_string(), MetricValue::Percentage(50.0));
    }

    let metrics = profiler.get_all_metrics();

    group.bench_function("format_without_metrics", |b| {
        let scan = PlanNode::Scan {
            table_name: "test_table".to_string(),
            alias: None,
            projection: None,
        };

        let plan = PlanNode::Filter {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: "value".to_string(),
                    table: None,
                }),
                op: BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Int64(500))),
            },
        };

        let options = ExplainOptions {
            analyze: true,
            verbose: false,
            profiler_metrics: None,
        };

        b.iter(|| {
            black_box(format_plan(&plan, options.clone())).unwrap();
        });
    });

    group.bench_function("format_with_metrics", |b| {
        let scan = PlanNode::Scan {
            table_name: "test_table".to_string(),
            alias: None,
            projection: None,
        };

        let plan = PlanNode::Filter {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: "value".to_string(),
                    table: None,
                }),
                op: BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Int64(500))),
            },
        };

        let options = ExplainOptions {
            analyze: true,
            verbose: false,
            profiler_metrics: Some(metrics.clone()),
        };

        b.iter(|| {
            black_box(format_plan(&plan, options.clone())).unwrap();
        });
    });

    group.finish();
}

fn bench_explain_analyze_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("explain_analyze_scale");
    configure_standard(&mut group);

    for &size in &[100, 1000, 5000, 10000] {
        group.throughput(Throughput::Elements(size));
        group.bench_with_input(BenchmarkId::new("rows", size), &size, |b, &size| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_basic_table(&mut executor, "test_table", size as usize);

            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("EXPLAIN ANALYZE SELECT * FROM test_table WHERE value > 500"),
                )
                .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_metrics_collection(c: &mut Criterion) {
    let mut group = c.benchmark_group("metrics_collection");
    configure_standard(&mut group);

    group.bench_function("collect_single_operator", |b| {
        b.iter(|| {
            let profiler = ProfilerEnabled::new();
            {
                let mut timer = profiler.start_operator(0);
                timer.record_batch(1000);
            }
            black_box(profiler.get_metrics(0));
        });
    });

    for &num_ops in &[10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("collect_all", num_ops),
            &num_ops,
            |b, &num_ops| {
                b.iter(|| {
                    let profiler = ProfilerEnabled::new();
                    for i in 0..num_ops {
                        let mut timer = profiler.start_operator(i);
                        timer.record_batch(100);
                    }
                    black_box(profiler.get_all_metrics());
                });
            },
        );
    }

    group.finish();
}

fn bench_custom_metrics(c: &mut Criterion) {
    let mut group = c.benchmark_group("custom_metrics");
    configure_standard(&mut group);

    group.bench_function("no_custom_metrics", |b| {
        let profiler = ProfilerEnabled::new();

        b.iter(|| {
            let mut timer = profiler.start_operator(0);
            timer.record_batch(100);
        });
    });

    group.bench_function("one_custom_metric", |b| {
        let profiler = ProfilerEnabled::new();

        b.iter(|| {
            let mut timer = profiler.start_operator(0);
            timer.record_batch(100);
            timer.record_custom("selectivity".to_string(), MetricValue::Percentage(75.0));
        });
    });

    for &num_metrics in &[5, 10, 20] {
        group.bench_with_input(
            BenchmarkId::new("multiple_metrics", num_metrics),
            &num_metrics,
            |b, &num_metrics| {
                let profiler = ProfilerEnabled::new();

                b.iter(|| {
                    let mut timer = profiler.start_operator(0);
                    timer.record_batch(100);
                    for i in 0..num_metrics {
                        timer
                            .record_custom(format!("metric_{}", i), MetricValue::Count(i as usize));
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_explain_statement,
    bench_explain_complexity,
    bench_profiler_overhead,
    bench_profiler_scalability,
    bench_plan_formatting,
    bench_plan_formatting_with_metrics,
    bench_explain_analyze_scale,
    bench_metrics_collection,
    bench_custom_metrics
);
criterion_main!(benches);
