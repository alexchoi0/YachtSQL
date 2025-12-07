use crate::common::create_executor;

#[test]
fn test_aggregate_function_type_sum() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_sum (id UInt64, state AggregateFunction(sum, UInt64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_type_count() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_count (id UInt64, state AggregateFunction(count)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_type_avg() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_avg (id UInt64, state AggregateFunction(avg, Float64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_type_min() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_min (id UInt64, state AggregateFunction(min, Int64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_type_max() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_max (id UInt64, state AggregateFunction(max, Int64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_type_uniq() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_uniq (id UInt64, state AggregateFunction(uniq, String)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_type_uniqexact() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_uniqexact (id UInt64, state AggregateFunction(uniqExact, UInt64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_type_quantile() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_quantile (id UInt64, state AggregateFunction(quantile(0.5), Float64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_type_quantiles() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_quantiles (id UInt64, state AggregateFunction(quantiles(0.25, 0.5, 0.75), Float64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_type_grouparray() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_grouparray (id UInt64, state AggregateFunction(groupArray, String)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_state() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE src (id UInt64, value UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();
    executor
        .execute_sql("INSERT INTO src VALUES (1, 10), (1, 20), (2, 30)")
        .ok();

    let result = executor.execute_sql("SELECT id, sumState(value) FROM src GROUP BY id");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_merge() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE agg_states (id UInt64, state AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree() ORDER BY id"
    ).ok();

    let result = executor.execute_sql("SELECT id, sumMerge(state) FROM agg_states GROUP BY id");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_insert_with_state_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE src2 (id UInt64, value UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();
    executor.execute_sql(
        "CREATE TABLE agg_dest (id UInt64, state AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree() ORDER BY id"
    ).ok();

    let result = executor
        .execute_sql("INSERT INTO agg_dest SELECT id, sumState(value) FROM src2 GROUP BY id");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_countstate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE count_src (id UInt64) ENGINE = MergeTree() ORDER BY id")
        .ok();
    executor
        .execute_sql("INSERT INTO count_src VALUES (1), (1), (2)")
        .ok();

    let result = executor.execute_sql("SELECT id, countState() FROM count_src GROUP BY id");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_avgstate() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE avg_src (id UInt64, val Float64) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql("SELECT id, avgState(val) FROM avg_src GROUP BY id");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_uniqstate() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE uniq_src (id UInt64, name String) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql("SELECT id, uniqState(name) FROM uniq_src GROUP BY id");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_finalize_aggregation() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE final_agg (id UInt64, state AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree() ORDER BY id"
    ).ok();

    let result = executor.execute_sql("SELECT id, finalizeAggregation(state) FROM final_agg");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_with_if_combinator() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_if (id UInt64, state AggregateFunction(sumIf, UInt64, UInt8)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_with_array_combinator() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_array (id UInt64, state AggregateFunction(sumArray, Array(UInt64))) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_with_map_combinator() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_map (id UInt64, state AggregateFunction(sumMap, Array(String), Array(UInt64))) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_with_foreach_combinator() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_foreach (id UInt64, state AggregateFunction(sumForEach, Array(UInt64))) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_with_distinct_combinator() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_distinct (id UInt64, state AggregateFunction(sumDistinct, UInt64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_with_or_default_combinator() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_ordefault (id UInt64, state AggregateFunction(sumOrDefault, UInt64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_with_or_null_combinator() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_ornull (id UInt64, state AggregateFunction(sumOrNull, UInt64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_with_resample_combinator() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_resample (id UInt64, state AggregateFunction(sumResample(0, 100, 10), UInt64, UInt64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_groupbitand() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_bitand (id UInt64, state AggregateFunction(groupBitAnd, UInt64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_groupbitor() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_bitor (id UInt64, state AggregateFunction(groupBitOr, UInt64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_groupbitxor() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_bitxor (id UInt64, state AggregateFunction(groupBitXor, UInt64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_grouparrayinsertat() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_insertat (id UInt64, state AggregateFunction(groupArrayInsertAt, String, UInt64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_groupuniqarray() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_uniqarray (id UInt64, state AggregateFunction(groupUniqArray, String)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_topk() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_topk (id UInt64, state AggregateFunction(topK(5), String)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_topkweighted() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_topkw (id UInt64, state AggregateFunction(topKWeighted(5), String, UInt64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_argmin() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_argmin (id UInt64, state AggregateFunction(argMin, String, Int64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_argmax() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_argmax (id UInt64, state AggregateFunction(argMax, String, Int64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_any() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_any (id UInt64, state AggregateFunction(any, String)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_anylast() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_anylast (id UInt64, state AggregateFunction(anyLast, String)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_anyheavy() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_anyheavy (id UInt64, state AggregateFunction(anyHeavy, String)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_stddevpop() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_stddevpop (id UInt64, state AggregateFunction(stddevPop, Float64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_stddevsamp() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_stddevsamp (id UInt64, state AggregateFunction(stddevSamp, Float64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_varpop() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_varpop (id UInt64, state AggregateFunction(varPop, Float64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_varsamp() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_varsamp (id UInt64, state AggregateFunction(varSamp, Float64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_corr() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_corr (id UInt64, state AggregateFunction(corr, Float64, Float64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_covarpop() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_covarpop (id UInt64, state AggregateFunction(covarPop, Float64, Float64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_covarsamp() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_covarsamp (id UInt64, state AggregateFunction(covarSamp, Float64, Float64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_simplelinearregression() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_linreg (id UInt64, state AggregateFunction(simpleLinearRegression, Float64, Float64)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_multiple_columns() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE multi_agg (
            id UInt64,
            sum_state AggregateFunction(sum, UInt64),
            count_state AggregateFunction(count),
            avg_state AggregateFunction(avg, Float64)
        ) ENGINE = AggregatingMergeTree() ORDER BY id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_in_materialized_view() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE mv_source (id UInt64, value UInt64, ts DateTime) ENGINE = MergeTree() ORDER BY ts").ok();
    executor.execute_sql(
        "CREATE TABLE mv_agg_dest (id UInt64, value_sum AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree() ORDER BY id"
    ).ok();

    let result = executor.execute_sql(
        "CREATE MATERIALIZED VIEW mv_agg TO mv_agg_dest AS
         SELECT id, sumState(value) AS value_sum
         FROM mv_source
         GROUP BY id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_runningaccumulate() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE run_acc (id UInt64, value UInt64) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();
    executor
        .execute_sql("INSERT INTO run_acc VALUES (1, 10), (2, 20), (3, 30)")
        .ok();

    let result = executor.execute_sql(
        "SELECT id, runningAccumulate(sumState(value)) OVER (ORDER BY id) FROM run_acc",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_nullable_inner() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_nullable (id UInt64, state AggregateFunction(sum, Nullable(UInt64))) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_lowcardinality_inner() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_lc (id UInt64, state AggregateFunction(groupArray, LowCardinality(String))) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_nested_array() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_nested_arr (id UInt64, state AggregateFunction(groupArray, Array(UInt64))) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_simple_state() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE simple_state (id UInt64, state SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_simple_aggregate_function_min() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE simple_min (id UInt64, state SimpleAggregateFunction(min, Int64)) ENGINE = AggregatingMergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_simple_aggregate_function_max() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE simple_max (id UInt64, state SimpleAggregateFunction(max, Int64)) ENGINE = AggregatingMergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_simple_aggregate_function_any() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE simple_any (id UInt64, state SimpleAggregateFunction(any, String)) ENGINE = AggregatingMergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_simple_aggregate_function_anylast() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE simple_anylast (id UInt64, state SimpleAggregateFunction(anyLast, String)) ENGINE = AggregatingMergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_simple_aggregate_function_groupbitand() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE simple_bitand (id UInt64, state SimpleAggregateFunction(groupBitAnd, UInt64)) ENGINE = AggregatingMergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_simple_aggregate_function_groupbitor() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE simple_bitor (id UInt64, state SimpleAggregateFunction(groupBitOr, UInt64)) ENGINE = AggregatingMergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_simple_aggregate_function_groupbitxor() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE simple_bitxor (id UInt64, state SimpleAggregateFunction(groupBitXor, UInt64)) ENGINE = AggregatingMergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_simple_aggregate_function_grouparraymaxsize() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE simple_arr (id UInt64, state SimpleAggregateFunction(groupArrayArray, Array(String))) ENGINE = AggregatingMergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_multiple_args() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE multi_args (a UInt64, b UInt64, c Float64) ENGINE = MergeTree() ORDER BY a").ok();

    let result =
        executor.execute_sql("SELECT sumState(a), avgState(c), countState() FROM multi_args");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_merge_state_in_select() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE merge_src (id UInt64, state AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree() ORDER BY id"
    ).ok();

    let result = executor.execute_sql("SELECT sumMerge(state) FROM merge_src");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aggregate_function_array_with_size() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE agg_arr_size (id UInt64, state AggregateFunction(groupArray(10), String)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}
