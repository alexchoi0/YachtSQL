use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_stddev_pop() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stats_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO stats_test VALUES (2), (4), (4), (4), (5), (5), (7), (9)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT stddevPop(value) FROM stats_test")
        .unwrap();
    assert_table_eq!(result, [[2.0]]); // sqrt(variance) = sqrt(4) = 2.0
}

#[ignore = "Implement me!"]
#[test]
fn test_stddev_samp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stddev_samp_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO stddev_samp_test VALUES (10), (20), (30), (40), (50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT stddevSamp(value) FROM stddev_samp_test")
        .unwrap();
    assert_table_eq!(result, [[15.811388300841896]]); // sqrt(250) = 15.811...
}

#[ignore = "Implement me!"]
#[test]
fn test_var_pop() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE var_pop_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO var_pop_test VALUES (2), (4), (4), (4), (5), (5), (7), (9)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT varPop(value) FROM var_pop_test")
        .unwrap();
    assert_table_eq!(result, [[4.0]]); // Variance = 4.0
}

#[ignore = "Implement me!"]
#[test]
fn test_var_samp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE var_samp_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO var_samp_test VALUES (10), (20), (30), (40), (50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT varSamp(value) FROM var_samp_test")
        .unwrap();
    assert_table_eq!(result, [[250.0]]); // Sample variance = 250.0
}

#[ignore = "Implement me!"]
#[test]
fn test_covar_pop() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE covar_test (x Float64, y Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO covar_test VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT covarPop(x, y) FROM covar_test")
        .unwrap();
    assert_table_eq!(result, [[4.0]]); // Covariance = 4.0 (y = 2x)
}

#[ignore = "Implement me!"]
#[test]
fn test_covar_samp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE covar_samp_test (x Float64, y Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO covar_samp_test VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT covarSamp(x, y) FROM covar_samp_test")
        .unwrap();
    assert_table_eq!(result, [[5.0]]); // Sample covariance = 5.0
}

#[test]
fn test_corr() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_test (x Float64, y Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_test VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT corr(x, y) FROM corr_test")
        .unwrap();
    assert_table_eq!(result, [[1.0]]); // Perfect positive correlation (y = 2x)
}

#[ignore = "Implement me!"]
#[test]
fn test_any_heavy() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE heavy_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO heavy_test VALUES ('a'), ('a'), ('a'), ('b'), ('c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT anyHeavy(value) FROM heavy_test")
        .unwrap();
    assert_table_eq!(result, [["a"]]); // 'a' is the heavy hitter (appears 3 times)
}

#[ignore = "Implement me!"]
#[test]
fn test_any_last() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE any_last_test (id INT64, value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO any_last_test VALUES (1, 'first'), (2, 'second'), (3, 'third')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT anyLast(value) FROM any_last_test")
        .unwrap();
    assert_table_eq!(result, [["third"]]); // Last inserted value
}

#[ignore = "Implement me!"]
#[test]
fn test_arg_min() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arg_min_test (name String, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arg_min_test VALUES ('alice', 90), ('bob', 85), ('charlie', 95)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT argMin(name, score) FROM arg_min_test")
        .unwrap();
    assert_table_eq!(result, [["bob"]]); // bob has minimum score (85)
}

#[ignore = "Implement me!"]
#[test]
fn test_arg_max() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arg_max_test (name String, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arg_max_test VALUES ('alice', 90), ('bob', 85), ('charlie', 95)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT argMax(name, score) FROM arg_max_test")
        .unwrap();
    assert_table_eq!(result, [["charlie"]]); // charlie has max score (95)
}

#[ignore = "Implement me!"]
#[test]
fn test_avg_weighted() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE weighted_test (value Float64, weight Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO weighted_test VALUES (10, 1), (20, 2), (30, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT avgWeighted(value, weight) FROM weighted_test")
        .unwrap();
    assert_table_eq!(result, [[23.333333333333332]]); // (10*1 + 20*2 + 30*3) / 6 = 23.33...
}

#[ignore = "Implement me!"]
#[test]
fn test_skew_pop() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE skew_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO skew_test VALUES (1), (2), (3), (4), (5), (10), (20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT skewPop(value) FROM skew_test")
        .unwrap();
    assert!(result.num_rows() == 1); // Skewness value is implementation-specific
}

#[ignore = "Implement me!"]
#[test]
fn test_skew_samp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE skew_samp_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO skew_samp_test VALUES (1), (2), (3), (4), (5), (10), (20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT skewSamp(value) FROM skew_samp_test")
        .unwrap();
    assert!(result.num_rows() == 1); // Skewness value is implementation-specific
}

#[ignore = "Implement me!"]
#[test]
fn test_kurt_pop() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE kurt_test (value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO kurt_test VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT kurtPop(value) FROM kurt_test")
        .unwrap();
    assert!(result.num_rows() == 1); // Kurtosis value is implementation-specific
}

#[ignore = "Implement me!"]
#[test]
fn test_kurt_samp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE kurt_samp_test (value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO kurt_samp_test VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT kurtSamp(value) FROM kurt_samp_test")
        .unwrap();
    assert!(result.num_rows() == 1); // Kurtosis value is implementation-specific
}

#[ignore = "Implement me!"]
#[test]
fn test_simple_linear_regression() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE regression_test (x Float64, y Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO regression_test VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT simpleLinearRegression(x, y) FROM regression_test")
        .unwrap();
    assert!(result.num_rows() == 1); // Returns (slope=2, intercept=0) tuple
}

#[ignore = "Implement me!"]
#[test]
fn test_stochastic_linear_regression() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stoch_reg_test (x Float64, y Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO stoch_reg_test VALUES (1, 2.1), (2, 3.9), (3, 6.1), (4, 8.0), (5, 9.8)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT stochasticLinearRegression(0.1, 0, 1, 'SGD')(y, x) FROM stoch_reg_test",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // SGD result varies
}

#[ignore = "Implement me!"]
#[test]
fn test_entropy() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE entropy_test (category String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO entropy_test VALUES ('a'), ('a'), ('b'), ('b'), ('c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT entropy(category) FROM entropy_test")
        .unwrap();
    assert!(result.num_rows() == 1); // Entropy value depends on probability distribution
}

#[ignore = "Implement me!"]
#[test]
fn test_exponential_moving_average() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ema_test (ts DateTime, value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO ema_test VALUES
            ('2023-01-01 00:00:00', 10),
            ('2023-01-01 01:00:00', 20),
            ('2023-01-01 02:00:00', 15),
            ('2023-01-01 03:00:00', 25)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT exponentialMovingAverage(1)(value, ts) FROM ema_test")
        .unwrap();
    assert!(result.num_rows() == 1); // EMA result depends on algorithm specifics
}

#[ignore = "Implement me!"]
#[test]
fn test_student_t_test() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ttest_data (group_id UInt8, value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO ttest_data VALUES
            (0, 10), (0, 12), (0, 11), (0, 13), (0, 10),
            (1, 15), (1, 17), (1, 14), (1, 16), (1, 18)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT studentTTest(group_id, value) FROM ttest_data")
        .unwrap();
    assert!(result.num_rows() == 1); // t-test result is implementation-specific
}

#[ignore = "Implement me!"]
#[test]
fn test_welch_t_test() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE welch_data (group_id UInt8, value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO welch_data VALUES
            (0, 10), (0, 12), (0, 11),
            (1, 15), (1, 17), (1, 14), (1, 16), (1, 18), (1, 19)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT welchTTest(group_id, value) FROM welch_data")
        .unwrap();
    assert!(result.num_rows() == 1); // Welch t-test result is implementation-specific
}

#[ignore = "Implement me!"]
#[test]
fn test_mann_whitney_u_test() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mw_data (group_id UInt8, value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO mw_data VALUES
            (0, 10), (0, 12), (0, 11), (0, 13),
            (1, 15), (1, 17), (1, 14), (1, 16)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT mannWhitneyUTest(group_id, value) FROM mw_data")
        .unwrap();
    assert!(result.num_rows() == 1); // Mann-Whitney U result is implementation-specific
}

#[ignore = "Implement me!"]
#[test]
fn test_median() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE median_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO median_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT median(value) FROM median_test")
        .unwrap();
    assert_table_eq!(result, [[3.0]]); // Median of (1,2,3,4,5) = 3.0
}

#[ignore = "Implement me!"]
#[test]
fn test_mean_zscore() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE zscore_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO zscore_test VALUES (10), (20), (30), (40), (50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT meanZScore(value) FROM zscore_test")
        .unwrap();
    assert!(result.num_rows() == 1); // Z-score result is implementation-specific
}

#[ignore = "Implement me!"]
#[test]
fn test_rank_corr() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rank_corr_test (x Float64, y Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO rank_corr_test VALUES (1, 1), (2, 3), (3, 2), (4, 5), (5, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT rankCorr(x, y) FROM rank_corr_test")
        .unwrap();
    assert_table_eq!(result, [[0.9]]); // Spearman correlation for near-perfect ordering
}

#[ignore = "Implement me!"]
#[test]
fn test_cramers_v() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cramers_test (x String, y String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO cramers_test VALUES ('a', '1'), ('a', '2'), ('b', '1'), ('b', '2')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT cramersV(x, y) FROM cramers_test")
        .unwrap();
    assert_table_eq!(result, [[0.0]]); // No association between x and y
}

#[ignore = "Implement me!"]
#[test]
fn test_cramers_v_bias_corrected() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cramers_bc_test (x String, y String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO cramers_bc_test VALUES ('a', '1'), ('a', '2'), ('b', '1'), ('b', '2')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT cramersVBiasCorrected(x, y) FROM cramers_bc_test")
        .unwrap();
    assert_table_eq!(result, [[0.0]]); // No association between x and y
}

#[ignore = "Implement me!"]
#[test]
fn test_theils_u() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE theils_test (x String, y String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO theils_test VALUES ('a', '1'), ('a', '1'), ('b', '2'), ('b', '2')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT theilsU(x, y) FROM theils_test")
        .unwrap();
    assert_table_eq!(result, [[1.0]]); // Perfect predictability (x predicts y)
}

#[ignore = "Implement me!"]
#[test]
fn test_contingency() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE contingency_test (x String, y String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO contingency_test VALUES ('a', '1'), ('a', '2'), ('b', '1'), ('b', '2')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT contingency(x, y) FROM contingency_test")
        .unwrap();
    assert_table_eq!(result, [[0.0]]); // No association between x and y
}

#[ignore = "Implement me!"]
#[test]
fn test_statistics_grouped() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE grouped_stats (category String, value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO grouped_stats VALUES
            ('A', 10), ('A', 20), ('A', 30),
            ('B', 15), ('B', 25), ('B', 35)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, stddevPop(value), varPop(value), AVG(value)
            FROM grouped_stats
            GROUP BY category
            ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["A", 8.16496580927726, 66.66666666666667, 20.0], // stddev, var, avg for A
            ["B", 8.16496580927726, 66.66666666666667, 25.0]  // stddev, var, avg for B
        ]
    );
}
