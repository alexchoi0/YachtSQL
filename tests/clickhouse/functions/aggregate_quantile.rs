use crate::common::create_executor;

#[test]
fn test_quantile() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_test (value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO quantile_test VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantile(0.5)(value) FROM quantile_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_exact() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_exact_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quantile_exact_test VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantileExact(0.5)(value) FROM quantile_exact_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_exact_low() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_exact_low_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quantile_exact_low_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantileExactLow(0.5)(value) FROM quantile_exact_low_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_exact_high() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_exact_high_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quantile_exact_high_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantileExactHigh(0.5)(value) FROM quantile_exact_high_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_exact_weighted() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_weighted_test (value Float64, weight UInt64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO quantile_weighted_test VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantileExactWeighted(0.5)(value, weight) FROM quantile_weighted_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_timing() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_timing_test (response_time UInt64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quantile_timing_test VALUES (100), (150), (200), (250), (300), (500), (1000)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantileTiming(0.95)(response_time) FROM quantile_timing_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_timing_weighted() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_timing_w_test (response_time UInt64, count UInt64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO quantile_timing_w_test VALUES (100, 10), (200, 5), (300, 3), (500, 2)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT quantileTimingWeighted(0.95)(response_time, count) FROM quantile_timing_w_test",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_deterministic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_det_test (value Float64, key UInt64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO quantile_det_test VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantileDeterministic(0.5)(value, key) FROM quantile_det_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_tdigest() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_tdigest_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quantile_tdigest_test VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantileTDigest(0.5)(value) FROM quantile_tdigest_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_tdigest_weighted() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_tdigest_w_test (value Float64, weight UInt64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO quantile_tdigest_w_test VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT quantileTDigestWeighted(0.5)(value, weight) FROM quantile_tdigest_w_test",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_bfloat16() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_bf16_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quantile_bf16_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantileBFloat16(0.5)(value) FROM quantile_bf16_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_bfloat16_weighted() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_bf16_w_test (value Float64, weight UInt64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quantile_bf16_w_test VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT quantileBFloat16Weighted(0.5)(value, weight) FROM quantile_bf16_w_test",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_dd() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_dd_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quantile_dd_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantileDD(0.01, 0.5)(value) FROM quantile_dd_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantiles_multiple() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantiles_test (value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO quantiles_test VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantiles(0.25, 0.5, 0.75)(value) FROM quantiles_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantiles_exact() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantiles_exact_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quantiles_exact_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantilesExact(0.25, 0.5, 0.75)(value) FROM quantiles_exact_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantiles_tdigest() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantiles_tdigest_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quantiles_tdigest_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantilesTDigest(0.25, 0.5, 0.75)(value) FROM quantiles_tdigest_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_gk() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_gk_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quantile_gk_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantileGK(100, 0.5)(value) FROM quantile_gk_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_interpolated_weighted() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_interp_test (value Float64, weight UInt64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO quantile_interp_test VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT quantileInterpolatedWeighted(0.5)(value, weight) FROM quantile_interp_test",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_quantile_grouped() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_grouped (category String, value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO quantile_grouped VALUES
            ('A', 1), ('A', 2), ('A', 3), ('A', 4), ('A', 5),
            ('B', 10), ('B', 20), ('B', 30), ('B', 40), ('B', 50)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, quantile(0.5)(value)
            FROM quantile_grouped
            GROUP BY category
            ORDER BY category",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_quantile_percentile_aliases() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE percentile_test (value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO percentile_test VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantile(0.9)(value) AS p90 FROM percentile_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_quantile_with_if() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quantile_if_test (category String, value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO quantile_if_test VALUES
            ('A', 1), ('A', 2), ('A', 3),
            ('B', 10), ('B', 20), ('B', 30)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT quantileIf(0.5)(value, category = 'A') FROM quantile_if_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_approx_top_k() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE topk_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO topk_test VALUES ('a'), ('a'), ('a'), ('b'), ('b'), ('c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT topK(2)(value) FROM topk_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_topk_weighted() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE topk_w_test (value String, weight UInt64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO topk_w_test VALUES ('a', 10), ('b', 20), ('c', 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT topKWeighted(2)(value, weight) FROM topk_w_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_min_max_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE minmax_by_test (name String, score INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO minmax_by_test VALUES ('alice', 90), ('bob', 85), ('charlie', 95)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT minMap(name, score), maxMap(name, score) FROM minmax_by_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_histogram() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE histogram_test (value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO histogram_test VALUES (1), (2), (2), (3), (3), (3), (4), (4), (5)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT histogram(5)(value) FROM histogram_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
