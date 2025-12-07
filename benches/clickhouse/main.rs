use criterion::{criterion_group, criterion_main};

mod common;

mod aggregations;
mod basic;
mod complex_queries;
mod data_types;
mod functions;
mod joins;
mod merge;
mod optimizer;
mod profiling;
mod quick;
mod scalability;

criterion_group!(
    basic_ops,
    basic::bench_select_all,
    basic::bench_select_where,
);

criterion_group!(
    agg,
    aggregations::bench_count,
    aggregations::bench_sum,
    aggregations::bench_avg,
    aggregations::bench_min_max,
    aggregations::bench_group_by,
    aggregations::bench_group_by_multiple,
);

criterion_group!(
    join,
    joins::bench_inner_join,
    joins::bench_left_join,
    joins::bench_right_join,
    joins::bench_full_join,
    joins::bench_join_with_aggregation,
);

criterion_group!(
    func,
    functions::bench_string_upper,
    functions::bench_string_lower,
    functions::bench_string_length,
    functions::bench_string_concat,
    functions::bench_string_substring,
    functions::bench_math_abs,
    functions::bench_coalesce,
);

criterion_group!(
    complex,
    complex_queries::bench_subquery_scalar,
    complex_queries::bench_subquery_in,
    complex_queries::bench_cte_simple,
    complex_queries::bench_cte_with_join,
    complex_queries::bench_union,
    complex_queries::bench_case_expression,
);

criterion_group!(
    dtype,
    data_types::bench_int64_operations,
    data_types::bench_float64_operations,
    data_types::bench_string_operations,
    data_types::bench_mixed_types,
    data_types::bench_type_comparison,
);

criterion_group!(
    opt,
    optimizer::bench_predicate_pushdown,
    optimizer::bench_projection_pushdown,
    optimizer::bench_constant_folding,
    optimizer::bench_common_subexpr_elimination,
    optimizer::bench_join_reordering,
);

criterion_group!(
    scale,
    scalability::bench_scale_select,
    scalability::bench_scale_filter,
    scalability::bench_scale_aggregation,
    scalability::bench_scale_sort,
);

criterion_group!(
    dml,
    merge::bench_insert,
    merge::bench_update,
    merge::bench_delete,
);

criterion_group!(
    profile,
    profiling::bench_parse_simple,
    profiling::bench_parse_complex,
    profiling::bench_execution_overhead,
    profiling::bench_table_scan,
);

criterion_group!(sanity, quick::bench_quick,);

criterion_main!(basic_ops, agg, join, func, complex, dtype, opt, scale, dml, profile, sanity);
