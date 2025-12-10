use yachtsql::QueryExecutor;
use yachtsql_parser::DialectType;

#[test]
fn test_uniq_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE events (id INT64, user_id INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (1, 100), (2, 200), (3, 100), (4, 300), (5, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT UNIQ(user_id) as unique_users FROM events")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let count = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_uniq_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (NULL), (2), (NULL), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT UNIQ(value) as uniq_count FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let count = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_uniq_exact_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (city STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES ('NYC'), ('LA'), ('NYC'), ('SF'), ('LA')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT UNIQ_EXACT(city) as exact_count FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let count = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_uniq_hll12_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (id INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT UNIQ_HLL12(id) as hll_count FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let count = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_uniq_combined_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (id INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT UNIQ_COMBINED(id) as combined_count FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let count = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_uniq_combined_64_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (id INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT UNIQ_COMBINED_64(id) as combined64_count FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let count = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_uniq_theta_sketch_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (id INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT UNIQ_THETA_SKETCH(id) as theta_count FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let count = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_top_k_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE events (event_type STRING)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO events VALUES
            ('click'), ('click'), ('click'),
            ('view'), ('view'),
            ('purchase')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT TOP_K(event_type) as top_events FROM events")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_top_k_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES ('a'), ('b'), ('a'), (NULL), ('c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT TOP_K(value) as top_values FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_quantile_median() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1.0), (2.0), (3.0), (4.0), (5.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT QUANTILE(value) as median FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let median = result.column(0).unwrap().get(0).unwrap().as_f64().unwrap();
    assert!((median - 3.0).abs() < 0.1);
}

#[test]
fn test_quantile_exact_median() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1.0), (2.0), (3.0), (4.0), (5.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT QUANTILE_EXACT(value) as median FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let median = result.column(0).unwrap().get(0).unwrap().as_f64().unwrap();
    assert_eq!(median, 3.0);
}

#[test]
fn test_quantile_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1.0), (NULL), (3.0), (NULL), (5.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT QUANTILE(value) as median FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_arg_min_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE products (name STRING, price FLOAT64)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO products VALUES
            ('Product A', 100.0),
            ('Product B', 50.0),
            ('Product C', 75.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARG_MIN(name, price) as cheapest FROM products")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let name = result.column(0).unwrap().get(0).unwrap();
    assert_eq!(name.as_str().unwrap(), "Product B");
}

#[test]
fn test_arg_max_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE products (name STRING, price FLOAT64)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO products VALUES
            ('Product A', 100.0),
            ('Product B', 50.0),
            ('Product C', 75.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARG_MAX(name, price) as most_expensive FROM products")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let name = result.column(0).unwrap().get(0).unwrap();
    assert_eq!(name.as_str().unwrap(), "Product A");
}

#[test]
fn test_arg_min_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value STRING, score FLOAT64)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO data VALUES
            ('A', 10.0),
            ('B', NULL),
            ('C', 5.0),
            ('D', 15.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARG_MIN(value, score) as min_val FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let val = result.column(0).unwrap().get(0).unwrap();
    assert_eq!(val.as_str().unwrap(), "C");
}

#[test]
fn test_group_array_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT GROUP_ARRAY(value) as arr FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_group_array_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (NULL), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT GROUP_ARRAY(value) as arr FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_group_uniq_array_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (1), (3), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT GROUP_UNIQ_ARRAY(value) as unique_arr FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_group_uniq_array_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (NULL), (2), (NULL), (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT GROUP_UNIQ_ARRAY(value) as unique_arr FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_any_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ANY(value) as any_val FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);

    let val = result.column(0).unwrap().get(0).unwrap();
    assert!(
        val.as_i64().is_some(),
        "ANY should yield a value convertible to INT64"
    );
}

#[test]
fn test_any_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (NULL), (NULL), (5), (NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ANY(value) as any_val FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let val = result.column(0).unwrap().get(0).unwrap();
    assert_eq!(val.as_i64().unwrap(), 5);
}

#[test]
fn test_any_all_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (NULL), (NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ANY(value) as any_val FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_any_heavy_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('a'), ('b'), ('c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ANY_HEAVY(value) as heavy_val FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let val = result.column(0).unwrap().get(0).unwrap();
    assert_eq!(val.as_str().unwrap(), "a");
}

#[test]
fn test_sum_with_overflow_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM_WITH_OVERFLOW(value) as total FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let sum = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(sum, 15);
}

#[test]
fn test_clickhouse_aggregates_with_group_by() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE events (category STRING, event_type STRING, value FLOAT64)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO events VALUES
            ('A', 'click', 1.0),
            ('A', 'view', 2.0),
            ('A', 'click', 3.0),
            ('B', 'click', 4.0),
            ('B', 'purchase', 5.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                category,
                UNIQ_EXACT(event_type) as unique_events,
                QUANTILE_EXACT(value) as median_value,
                ANY(event_type) as sample_event,
                GROUP_UNIQ_ARRAY(event_type) as event_types
            FROM events
            GROUP BY category
            ORDER BY category",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);

    let cat_a_uniq = result.column(1).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(cat_a_uniq, 2);

    let cat_b_uniq = result.column(1).unwrap().get(1).unwrap().as_i64().unwrap();
    assert_eq!(cat_b_uniq, 2);
}

#[test]
fn test_quantile_timing_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE response_times (latency_ms FLOAT64)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO response_times VALUES (100.0), (200.0), (300.0), (400.0), (500.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT QUANTILE_TIMING(latency_ms) as p50 FROM response_times")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let median = result.column(0).unwrap().get(0).unwrap().as_f64().unwrap();
    assert!((median - 300.0).abs() < 1.0);
}

#[test]
fn test_quantile_tdigest_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1.0), (2.0), (3.0), (4.0), (5.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT QUANTILE_TDIGEST(value) as p50 FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let median = result.column(0).unwrap().get(0).unwrap().as_f64().unwrap();
    assert!((median - 3.0).abs() < 0.5);
}

#[test]
fn test_quantiles_timing_multiple() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (100.0), (200.0), (300.0), (400.0), (500.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT QUANTILES_TIMING(value) as quantiles FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_quantiles_tdigest_multiple() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1.0), (2.0), (3.0), (4.0), (5.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT QUANTILES_TDIGEST(value) as quantiles FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_group_array_moving_avg_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1.0), (2.0), (3.0), (4.0), (5.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT GROUP_ARRAY_MOVING_AVG(value) as moving_avg FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_group_array_moving_sum_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1.0), (2.0), (3.0), (4.0), (5.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT GROUP_ARRAY_MOVING_SUM(value) as moving_sum FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_sum_map_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (keys ARRAY<STRING>, values ARRAY<FLOAT64>)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM_MAP([keys, values]) as result FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_min_map_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (keys ARRAY<STRING>, values ARRAY<FLOAT64>)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT MIN_MAP([keys, values]) as result FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_max_map_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (keys ARRAY<STRING>, values ARRAY<FLOAT64>)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT MAX_MAP([keys, values]) as result FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_group_bitmap_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT GROUP_BITMAP(value) as bitmap FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_group_bitmap_and_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (bitmap ARRAY<INT64>)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT GROUP_BITMAP_AND(bitmap) as result FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_group_bitmap_or_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (bitmap ARRAY<INT64>)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT GROUP_BITMAP_OR(bitmap) as result FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_group_bitmap_xor_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (bitmap ARRAY<INT64>)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT GROUP_BITMAP_XOR(bitmap) as result FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_rank_corr_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (x FLOAT64, y FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT RANK_CORR([x, y]) as corr FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_exponential_moving_average_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE data (value FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1.0), (2.0), (3.0), (4.0), (5.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT EXPONENTIAL_MOVING_AVERAGE(value) as ema FROM data")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let ema = result.column(0).unwrap().get(0).unwrap().as_f64().unwrap();
    assert!(ema > 0.0);
}

#[test]
fn test_interval_length_sum_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE intervals (start_time INT64, end_time INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO intervals VALUES (0, 10), (5, 15), (20, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT INTERVAL_LENGTH_SUM([start_time, end_time]) as total FROM intervals")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let total = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(total, 30);
}

#[test]
fn test_retention_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE events (flags ARRAY<BOOL>)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT RETENTION(flags) as retention FROM events")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_window_funnel_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::ClickHouse);

    executor
        .execute_sql("CREATE TABLE events (timestamp INT64, step INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO events VALUES (1000, 1), (1010, 2), (1020, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT WINDOW_FUNNEL([timestamp, step]) as funnel_level FROM events")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let level = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert!(level >= 0);
}
