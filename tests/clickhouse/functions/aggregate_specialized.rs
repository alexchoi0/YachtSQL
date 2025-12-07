use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_first_value() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE first_val_test (id INT64, value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO first_val_test VALUES (1, 'first'), (2, 'second'), (3, 'third')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT first_value(value) FROM first_val_test ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_last_value() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE last_val_test (id INT64, value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO last_val_test VALUES (1, 'first'), (2, 'second'), (3, 'third')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT last_value(value) FROM last_val_test ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_any() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE any_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO any_test VALUES ('a'), ('b'), ('c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT any(value) FROM any_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_any_if() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE any_if_test (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO any_if_test VALUES ('A', 10), ('B', 20), ('A', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT anyIf(value, category = 'B') FROM any_if_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_delta_sum() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE delta_sum_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO delta_sum_test VALUES (1), (2), (4), (7), (11)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT deltaSum(value) FROM delta_sum_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_delta_sum_timestamp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE delta_ts_test (ts DateTime, value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO delta_ts_test VALUES
            ('2023-01-01 00:00:00', 100),
            ('2023-01-01 01:00:00', 150),
            ('2023-01-01 02:00:00', 175),
            ('2023-01-01 03:00:00', 200)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT deltaSumTimestamp(value, ts) FROM delta_ts_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_retention() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE retention_test (user_id UInt32, day UInt32, action String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO retention_test VALUES
            (1, 1, 'signup'),
            (1, 3, 'login'),
            (1, 7, 'purchase'),
            (2, 1, 'signup'),
            (2, 2, 'login')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT retention(day = 1, day = 2, day = 7)
            FROM retention_test
            GROUP BY user_id",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sequence_match() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE seq_match_test (user_id UInt32, ts DateTime, event String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO seq_match_test VALUES
            (1, '2023-01-01 10:00:00', 'view'),
            (1, '2023-01-01 11:00:00', 'cart'),
            (1, '2023-01-01 12:00:00', 'purchase'),
            (2, '2023-01-01 10:00:00', 'view'),
            (2, '2023-01-01 11:00:00', 'view')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT user_id, sequenceMatch('(?1)(?2)(?3)')(ts, event = 'view', event = 'cart', event = 'purchase')
            FROM seq_match_test
            GROUP BY user_id
            ORDER BY user_id"
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sequence_count() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE seq_count_test (user_id UInt32, ts DateTime, event String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO seq_count_test VALUES
            (1, '2023-01-01 10:00:00', 'A'),
            (1, '2023-01-01 11:00:00', 'B'),
            (1, '2023-01-01 12:00:00', 'A'),
            (1, '2023-01-01 13:00:00', 'B')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT sequenceCount('(?1)(?2)')(ts, event = 'A', event = 'B')
            FROM seq_count_test",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_window_funnel() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE funnel_test (user_id UInt32, ts DateTime, event String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO funnel_test VALUES
            (1, '2023-01-01 10:00:00', 'visit'),
            (1, '2023-01-01 10:30:00', 'signup'),
            (1, '2023-01-01 11:00:00', 'purchase'),
            (2, '2023-01-01 10:00:00', 'visit'),
            (2, '2023-01-01 10:30:00', 'signup')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT user_id,
                windowFunnel(3600)(ts, event = 'visit', event = 'signup', event = 'purchase') AS steps
            FROM funnel_test
            GROUP BY user_id
            ORDER BY user_id"
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_session_duration_sum() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE session_test (user_id UInt32, ts DateTime)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO session_test VALUES
            (1, '2023-01-01 10:00:00'),
            (1, '2023-01-01 10:05:00'),
            (1, '2023-01-01 10:15:00'),
            (1, '2023-01-01 11:00:00')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT user_id, maxIntersections(ts, ts + INTERVAL 30 MINUTE)
            FROM session_test
            GROUP BY user_id",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_max_intersections() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE intersect_test (start DateTime, end DateTime)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO intersect_test VALUES
            ('2023-01-01 10:00:00', '2023-01-01 11:00:00'),
            ('2023-01-01 10:30:00', '2023-01-01 11:30:00'),
            ('2023-01-01 10:45:00', '2023-01-01 12:00:00')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT maxIntersections(start, end) FROM intersect_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_max_intersections_position() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE intersect_pos_test (start DateTime, end DateTime)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO intersect_pos_test VALUES
            ('2023-01-01 10:00:00', '2023-01-01 11:00:00'),
            ('2023-01-01 10:30:00', '2023-01-01 11:30:00')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT maxIntersectionsPosition(start, end) FROM intersect_pos_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_categories10_overflow_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cat_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cat_test VALUES (1), (2), (3), (100), (1000)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT categoricalInformationValue(value > 10, value > 100) FROM cat_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_init_cap_single_value() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE single_val_test (grp UInt8, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO single_val_test VALUES (1, 10), (1, 20), (2, 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT grp, singleValueOrNull(value)
            FROM single_val_test
            GROUP BY grp
            ORDER BY grp",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_interval_length_sum() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE interval_test (start DateTime, end DateTime)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO interval_test VALUES
            ('2023-01-01 10:00:00', '2023-01-01 11:00:00'),
            ('2023-01-01 12:00:00', '2023-01-01 13:00:00')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT intervalLengthSum(start, end) FROM interval_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_largest_triangle() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ltt_test (x Float64, y Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO ltt_test VALUES
            (1, 1), (2, 4), (3, 2), (4, 6), (5, 3), (6, 8), (7, 5), (8, 9), (9, 7), (10, 10)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT largestTriangleThreeBuckets(4)(x, y) FROM ltt_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_spark_bar() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sparkbar_test (value UInt64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sparkbar_test VALUES (1), (2), (3), (4), (5), (6), (7), (8)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sparkbar(8)(value) FROM sparkbar_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bounded_sample() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bounded_sample_test (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bounded_sample_test VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT boundedSample(0.5)(value) FROM bounded_sample_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sum_with_overflow() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE overflow_test (value Int8)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO overflow_test VALUES (100), (100), (100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumWithOverflow(value) FROM overflow_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sum_kahan() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE kahan_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO kahan_test VALUES (0.1), (0.2), (0.3), (0.4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumKahan(value) FROM kahan_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_nothing() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nothing_test (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nothing_test VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT nothing(value) FROM nothing_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_count_if() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE count_if_test (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO count_if_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT countIf(value > 2) FROM count_if_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sum_if() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sum_if_test (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sum_if_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumIf(value, value > 2) FROM sum_if_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_avg_if() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE avg_if_test (value Float64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO avg_if_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT avgIf(value, value >= 3) FROM avg_if_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_min_if() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE min_if_test (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO min_if_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT minIf(value, value > 2) FROM min_if_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_max_if() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE max_if_test (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO max_if_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT maxIf(value, value < 4) FROM max_if_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_aggregate_combinators_array() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE combinator_arr_test (values Array(INT64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO combinator_arr_test VALUES ([1, 2, 3]), ([4, 5, 6]), ([7, 8, 9])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumArray(values) FROM combinator_arr_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_aggregate_combinators_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE combinator_distinct_test (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO combinator_distinct_test VALUES (1), (2), (1), (3), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumDistinct(value), avgDistinct(value) FROM combinator_distinct_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_aggregate_combinators_or_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE combinator_null_test (value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumOrNull(value) FROM combinator_null_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_aggregate_combinators_or_default() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE combinator_default_test (value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumOrDefault(value) FROM combinator_default_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_aggregate_resample() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE resample_test (key UInt32, value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO resample_test VALUES
            (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumResample(1, 10, 2)(value, key) FROM resample_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
