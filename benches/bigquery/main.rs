use criterion::{criterion_group, criterion_main};

mod common;

mod aggregations;
mod basic;
mod complex_queries;
mod data_types;
mod flamegraph;
mod functions;
mod joins;
mod merge;
mod optimizer;
mod profiling;
mod quick;
mod scalability;

criterion_group! {
    name = basic_ops;
    config = flamegraph::profiled_config();
    targets = basic::bench_select_all, basic::bench_select_where
}

criterion_group! {
    name = agg;
    config = flamegraph::profiled_config();
    targets =
        aggregations::bench_count,
        aggregations::bench_sum,
        aggregations::bench_avg,
        aggregations::bench_min_max,
        aggregations::bench_group_by,
        aggregations::bench_group_by_multiple
}

criterion_group! {
    name = join;
    config = flamegraph::profiled_config();
    targets =
        joins::bench_inner_join,
        joins::bench_left_join,
        joins::bench_right_join,
        joins::bench_full_join,
        joins::bench_join_with_aggregation
}

criterion_group! {
    name = func;
    config = flamegraph::profiled_config();
    targets =
        functions::bench_string_upper,
        functions::bench_string_lower,
        functions::bench_string_length,
        functions::bench_string_concat,
        functions::bench_string_substring,
        functions::bench_math_abs,
        functions::bench_coalesce
}

criterion_group! {
    name = complex;
    config = flamegraph::profiled_config();
    targets =
        complex_queries::bench_subquery_scalar,
        complex_queries::bench_subquery_in,
        complex_queries::bench_cte_simple,
        complex_queries::bench_cte_with_join,
        complex_queries::bench_union,
        complex_queries::bench_case_expression
}

criterion_group! {
    name = dtype;
    config = flamegraph::profiled_config();
    targets =
        data_types::bench_int64_operations,
        data_types::bench_float64_operations,
        data_types::bench_string_operations,
        data_types::bench_mixed_types,
        data_types::bench_type_comparison
}

criterion_group! {
    name = opt;
    config = flamegraph::profiled_config();
    targets =
        optimizer::bench_predicate_pushdown,
        optimizer::bench_projection_pushdown,
        optimizer::bench_constant_folding,
        optimizer::bench_common_subexpr_elimination,
        optimizer::bench_join_reordering
}

criterion_group! {
    name = scale;
    config = flamegraph::profiled_config();
    targets =
        scalability::bench_scale_select,
        scalability::bench_scale_filter,
        scalability::bench_scale_aggregation,
        scalability::bench_scale_sort
}

criterion_group! {
    name = dml;
    config = flamegraph::profiled_config();
    targets =
        merge::bench_insert,
        merge::bench_update,
        merge::bench_delete
}

criterion_group! {
    name = profile;
    config = flamegraph::profiled_config();
    targets =
        profiling::bench_parse_simple,
        profiling::bench_parse_complex,
        profiling::bench_execution_overhead,
        profiling::bench_table_scan
}

criterion_group! {
    name = sanity;
    config = flamegraph::profiled_config();
    targets = quick::bench_quick
}

criterion_main!(basic_ops, agg, join, func, complex, dtype, opt, scale, dml, profile, sanity);
