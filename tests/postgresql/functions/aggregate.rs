use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE numbers (id INTEGER, value INTEGER, category TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1, 10, 'A'), (2, 20, 'A'), (3, 30, 'B'), (4, 40, 'B'), (5, 50, 'B')")
        .unwrap();
}

#[test]
fn test_count_star() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_count_column() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT COUNT(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_count_with_nulls() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE nullable (value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (3), (NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*), COUNT(value) FROM nullable")
        .unwrap();

    assert_table_eq!(result, [[4, 2]]);
}

#[test]
fn test_sum() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT SUM(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[150]]);
}

#[test]
fn test_avg() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT AVG(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[30.0]]);
}

#[test]
fn test_min() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT MIN(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_max() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT MAX(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[50]]);
}

#[test]
fn test_multiple_aggregates() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT COUNT(*), SUM(value), MIN(value), MAX(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[5, 150, 10, 50]]);
}

#[test]
fn test_aggregate_with_where() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT SUM(value) FROM numbers WHERE value > 20")
        .unwrap();

    assert_table_eq!(result, [[120]]);
}

#[test]
fn test_aggregate_empty_result() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT SUM(value) FROM numbers WHERE value > 100")
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_count_empty_result() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM numbers WHERE value > 100")
        .unwrap();

    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_sum_with_expression() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT SUM(value * 2) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[300]]);
}

#[test]
fn test_count_distinct() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT category) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_sum_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dup_values (value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dup_values VALUES (10), (20), (10), (30), (20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(DISTINCT value) FROM dup_values")
        .unwrap();

    assert_table_eq!(result, [[60]]);
}

#[test]
fn test_avg_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dup_values (value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dup_values VALUES (10), (20), (10), (30), (20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(DISTINCT value) FROM dup_values")
        .unwrap();

    assert_table_eq!(result, [[20.0]]);
}

fn setup_stats_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE stats (id INTEGER, value DOUBLE PRECISION, group_id TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO stats VALUES (1, 2.0, 'A'), (2, 4.0, 'A'), (3, 4.0, 'A'), (4, 4.0, 'B'), (5, 5.0, 'B'), (6, 5.0, 'B'), (7, 7.0, 'B'), (8, 9.0, 'B')")
        .unwrap();
}

#[test]
fn test_variance() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT VARIANCE(value) FROM stats")
        .unwrap();
    assert_table_eq!(result, [[4.571428571428571]]);
}

#[test]
fn test_var_pop() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT VAR_POP(value) FROM stats")
        .unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[test]
fn test_var_samp() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT VAR_SAMP(value) FROM stats")
        .unwrap();
    assert_table_eq!(result, [[4.571428571428571]]);
}

#[test]
fn test_stddev() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT STDDEV(value) FROM stats")
        .unwrap();
    assert_table_eq!(result, [[2.1380899352993947]]);
}

#[test]
fn test_stddev_pop() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT STDDEV_POP(value) FROM stats")
        .unwrap();
    assert_table_eq!(result, [[2.0]]);
}

#[test]
fn test_stddev_samp() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT STDDEV_SAMP(value) FROM stats")
        .unwrap();
    assert_table_eq!(result, [[2.1380899352993947]]);
}

#[test]
fn test_covar_pop() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COVAR_POP(x, y) FROM pairs")
        .unwrap();

    assert_table_eq!(result, [[2.5]]);
}

#[test]
fn test_covar_samp() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COVAR_SAMP(x, y) FROM pairs")
        .unwrap();

    assert_table_eq!(result, [[3.3333333333333335]]);
}

#[test]
fn test_corr() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM pairs")
        .unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_regr_slope() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_SLOPE(y, x) FROM pairs")
        .unwrap();

    assert_table_eq!(result, [[2.0]]);
}

#[test]
fn test_regr_intercept() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_INTERCEPT(y, x) FROM pairs")
        .unwrap();

    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_regr_count() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (NULL, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_COUNT(y, x) FROM pairs")
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_regr_r2() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_R2(y, x) FROM pairs")
        .unwrap();

    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_regr_avgx() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_AVGX(y, x) FROM pairs")
        .unwrap();

    assert_table_eq!(result, [[2.5]]);
}

#[test]
fn test_regr_avgy() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_AVGY(y, x) FROM pairs")
        .unwrap();

    assert_table_eq!(result, [[5.0]]);
}

#[test]
fn test_regr_sxx() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_SXX(y, x) FROM pairs")
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[test]
fn test_regr_syy() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_SYY(y, x) FROM pairs")
        .unwrap();
    assert_table_eq!(result, [[20.0]]);
}

#[test]
fn test_regr_sxy() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pairs (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pairs VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0), (4.0, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_SXY(y, x) FROM pairs")
        .unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[test]
fn test_statistical_with_group_by() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT group_id, STDDEV(value), VARIANCE(value) FROM stats GROUP BY group_id ORDER BY group_id")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["A", 1.1547005383792517, 1.3333333333333333],
            ["B", 2.0, 4.0],
        ]
    );
}

#[test]
fn test_statistical_with_nulls() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nullable_stats (value DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable_stats VALUES (1.0), (NULL), (3.0), (NULL), (5.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STDDEV(value), VARIANCE(value) FROM nullable_stats")
        .unwrap();
    assert_table_eq!(result, [[2.0, 4.0]]);
}

#[test]
fn test_statistical_single_value() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE single_value (value DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO single_value VALUES (5.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STDDEV_SAMP(value) FROM single_value")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_percentile_cont() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) FROM stats")
        .unwrap();
    assert_table_eq!(result, [[4.5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_percentile_cont_multiple() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value), PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) FROM stats")
        .unwrap();
    assert_table_eq!(result, [[4.0, 5.5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_percentile_disc() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) FROM stats")
        .unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_percentile_disc_boundaries() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY value), PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY value) FROM stats")
        .unwrap();
    assert_table_eq!(result, [[2.0, 9.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_percentile_with_group_by() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT group_id, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) FROM stats GROUP BY group_id ORDER BY group_id")
        .unwrap();
    assert_table_eq!(result, [["A", 4.0], ["B", 5.0],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_mode() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT MODE() WITHIN GROUP (ORDER BY value) FROM stats")
        .unwrap();
    assert_table_eq!(result, [[4.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_mode_with_group_by() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor
        .execute_sql("SELECT group_id, MODE() WITHIN GROUP (ORDER BY value) FROM stats GROUP BY group_id ORDER BY group_id")
        .unwrap();
    assert_table_eq!(result, [["A", 4.0], ["B", 5.0],]);
}

#[test]
fn test_array_agg() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[[10, 20, 30, 40, 50]]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_array_agg_with_order() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(value ORDER BY value DESC) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[[50, 40, 30, 20, 10]]]);
}

#[test]
fn test_array_agg_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dup_values (value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dup_values VALUES (1), (2), (1), (3), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_AGG(DISTINCT value ORDER BY value) FROM dup_values")
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[test]
fn test_array_agg_with_group_by() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT category, ARRAY_AGG(value ORDER BY value) FROM numbers GROUP BY category ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", [10, 20]], ["B", [30, 40, 50]],]);
}

#[test]
fn test_string_agg() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT STRING_AGG(category, ',') FROM numbers")
        .unwrap();
    assert_table_eq!(result, [["A,A,B,B,B"]]);
}

#[test]
fn test_string_agg_with_order() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT STRING_AGG(category, ',' ORDER BY id) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [["A,A,B,B,B"]]);
}

#[test]
fn test_string_agg_distinct() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT STRING_AGG(DISTINCT category, ',') FROM numbers")
        .unwrap();
    assert_table_eq!(result, [["A,B"]]);
}

#[test]
fn test_string_agg_with_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (group_name TEXT, item TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('A', 'apple'), ('A', 'apricot'), ('B', 'banana'), ('B', 'blueberry')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT group_name, STRING_AGG(item, ', ' ORDER BY item) FROM items GROUP BY group_name ORDER BY group_name")
        .unwrap();
    assert_table_eq!(
        result,
        [["A", "apple, apricot"], ["B", "banana, blueberry"],]
    );
}

#[test]
fn test_json_agg() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT JSON_AGG(value) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [["[10, 20, 30, 40, 50]"]]);
}

#[test]
fn test_jsonb_agg() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT JSONB_AGG(value) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [["[10, 20, 30, 40, 50]"]]);
}

#[test]
fn test_json_object_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE kv (key TEXT, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO kv VALUES ('a', 1), ('b', 2), ('c', 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT JSON_OBJECT_AGG(key, value) FROM kv")
        .unwrap();
    assert_table_eq!(result, [["{\"a\": 1, \"b\": 2, \"c\": 3}"]]);
}

#[test]
fn test_jsonb_object_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE kv (key TEXT, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO kv VALUES ('a', 1), ('b', 2), ('c', 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT JSONB_OBJECT_AGG(key, value) FROM kv")
        .unwrap();
    assert_table_eq!(result, [["{\"a\": 1, \"b\": 2, \"c\": 3}"]]);
}

#[test]
fn test_bool_and() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE flags (flag BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO flags VALUES (true), (true), (true)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BOOL_AND(flag) FROM flags")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_bool_and_with_false() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE flags (flag BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO flags VALUES (true), (false), (true)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BOOL_AND(flag) FROM flags")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_bool_or() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE flags (flag BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO flags VALUES (false), (false), (false)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BOOL_OR(flag) FROM flags")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_bool_or_with_true() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE flags (flag BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO flags VALUES (false), (true), (false)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BOOL_OR(flag) FROM flags")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_every() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE flags (flag BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO flags VALUES (true), (true), (true)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT EVERY(flag) FROM flags")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_bit_and() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bits (value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bits VALUES (7), (3), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BIT_AND(value) FROM bits")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_bit_or() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bits (value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bits VALUES (1), (2), (4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BIT_OR(value) FROM bits")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
fn test_bit_xor() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bits (value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bits VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT BIT_XOR(value) FROM bits")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
#[ignore = "XMLAGG not available in PostgreSQL dialect"]
fn test_xmlagg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE xml_data (content TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO xml_data VALUES ('<item>1</item>'), ('<item>2</item>'), ('<item>3</item>')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT XMLAGG(content::xml) FROM xml_data")
        .unwrap();
    assert_table_eq!(result, [["<item>1</item><item>2</item><item>3</item>"]]);
}

#[test]
fn test_aggregate_filter() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT COUNT(*) FILTER (WHERE value > 20) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_aggregate_filter_multiple() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT COUNT(*) FILTER (WHERE category = 'A'), COUNT(*) FILTER (WHERE category = 'B') FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[2, 3]]);
}

#[test]
fn test_aggregate_filter_with_group_by() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT category, SUM(value) FILTER (WHERE value > 15) FROM numbers GROUP BY category ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", 20], ["B", 120],]);
}

#[test]
fn test_first_value_aggregate() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT FIRST_VALUE(value) OVER (ORDER BY id) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[10], [10], [10], [10], [10]]);
}

#[test]
fn test_last_value_aggregate() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT LAST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[50], [50], [50], [50], [50]]);
}

#[test]
fn test_nth_value_aggregate() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT NTH_VALUE(value, 2) OVER (ORDER BY id) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[null], [20], [20], [20], [20]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_grouping() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT category, GROUPING(category), SUM(value) FROM numbers GROUP BY ROLLUP(category) ORDER BY category NULLS LAST")
        .unwrap();
    assert_table_eq!(result, [["A", 0, 30], ["B", 0, 120], [null, 1, 150],]);
}

#[test]
fn test_any_value() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor.execute_sql("SELECT ANY_VALUE(value) FROM numbers");
    assert!(result.is_err());
}

#[test]
#[ignore = "LISTAGG not available in PostgreSQL dialect"]
fn test_listagg() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT LISTAGG(category, ',') WITHIN GROUP (ORDER BY id) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [["A,A,B,B,B"]]);
}

#[test]
fn test_aggregate_with_case() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT SUM(CASE WHEN category = 'A' THEN value ELSE 0 END), SUM(CASE WHEN category = 'B' THEN value ELSE 0 END) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[30, 120]]);
}

#[test]
fn test_aggregate_with_coalesce() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nullable (value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(COALESCE(value, 0)) FROM nullable")
        .unwrap();

    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_aggregate_nested() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT MAX(total) FROM (SELECT category, SUM(value) AS total FROM numbers GROUP BY category) subq")
        .unwrap();
    assert_table_eq!(result, [[120]]);
}

#[test]
fn test_having_clause() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT category, SUM(value) FROM numbers GROUP BY category HAVING SUM(value) > 50",
        )
        .unwrap();

    assert_table_eq!(result, [["B", 120]]);
}

#[test]
fn test_having_with_multiple_conditions() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT category, SUM(value), COUNT(*) FROM numbers GROUP BY category HAVING SUM(value) > 20 AND COUNT(*) > 1 ORDER BY category")
        .unwrap();

    assert_table_eq!(result, [["A", 30, 2], ["B", 120, 3],]);
}

#[test]
fn test_aggregate_over_empty_set() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE empty_table (value INTEGER)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM empty_table",
        )
        .unwrap();

    assert_table_eq!(result, [[0, null, null, null, null]]);
}

#[test]
fn test_aggregate_all_nulls() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE all_nulls (value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO all_nulls VALUES (NULL), (NULL), (NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*), COUNT(value), SUM(value), AVG(value) FROM all_nulls")
        .unwrap();

    assert_table_eq!(result, [[3, 0, null, null]]);
}

#[test]
#[ignore = "COUNT_IF not available in PostgreSQL dialect"]
fn test_count_if() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT COUNT_IF(value > 20) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "SUM_IF not available in PostgreSQL dialect"]
fn test_sum_if() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT SUM_IF(value, value > 20) FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[120]]);
}

#[test]
fn test_avg_if() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor.execute_sql("SELECT AVG_IF(value, value > 20) FROM numbers");
    assert!(result.is_err());
}

#[test]
fn test_aggregate_expression_in_order_by() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT category, SUM(value) AS total FROM numbers GROUP BY category ORDER BY total DESC")
        .unwrap();

    assert_table_eq!(result, [["B", 120], ["A", 30],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_aggregate_with_distinct_in_expression() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT category) * 10 FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[20]]);
}

#[test]
fn test_approximate_count_distinct() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor.execute_sql("SELECT APPROX_COUNT_DISTINCT(category) FROM numbers");
    assert!(result.is_err());
}

#[test]
fn test_approx_percentile() {
    let mut executor = create_executor();
    setup_stats_table(&mut executor);

    let result = executor.execute_sql("SELECT APPROX_PERCENTILE(value, 0.5) FROM stats");
    assert!(result.is_err());
}

#[test]
fn test_aggregate_in_cte() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("WITH category_totals AS (SELECT category, SUM(value) AS total FROM numbers GROUP BY category) SELECT MAX(total) FROM category_totals")
        .unwrap();

    assert_table_eq!(result, [[120]]);
}

#[test]
fn test_aggregate_window_vs_aggregate() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT value, SUM(value) OVER (), SUM(value) OVER (PARTITION BY category) FROM numbers ORDER BY value")
        .unwrap();
    assert_table_eq!(
        result,
        [
            [10, 150, 30],
            [20, 150, 30],
            [30, 150, 120],
            [40, 150, 120],
            [50, 150, 120],
        ]
    );
}

fn setup_correlation_data(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE correlation_data (id INTEGER, x DOUBLE PRECISION, y DOUBLE PRECISION, z DOUBLE PRECISION, category TEXT)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO correlation_data VALUES
            (1, 1.0, 2.0, 10.0, 'A'),
            (2, 2.0, 4.0, 20.0, 'A'),
            (3, 3.0, 6.0, 30.0, 'A'),
            (4, 4.0, 8.0, 40.0, 'B'),
            (5, 5.0, 10.0, 50.0, 'B'),
            (6, 6.0, 12.0, 60.0, 'B'),
            (7, 7.0, 14.0, 70.0, 'C'),
            (8, 8.0, 16.0, 80.0, 'C')",
        )
        .unwrap();
}

#[test]
fn test_corr_perfect_positive() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_corr_perfect_negative() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE neg_corr (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO neg_corr VALUES (1.0, 10.0), (2.0, 8.0), (3.0, 6.0), (4.0, 4.0), (5.0, 2.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM neg_corr")
        .unwrap();
    assert_table_eq!(result, [[-1.0]]);
}

#[test]
fn test_corr_no_correlation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE no_corr (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO no_corr VALUES (1.0, 5.0), (2.0, 3.0), (3.0, 7.0), (4.0, 2.0), (5.0, 6.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM no_corr")
        .unwrap();
    assert_table_eq!(result, [[0.0762492851663024]]);
}

#[test]
fn test_corr_with_nulls() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_nulls (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_nulls VALUES (1.0, 2.0), (NULL, 4.0), (3.0, NULL), (4.0, 8.0), (5.0, 10.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM corr_nulls")
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_corr_single_pair() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_single (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_single VALUES (1.0, 2.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM corr_single")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_corr_empty_set() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_empty (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM corr_empty")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_corr_constant_x() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_const_x (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO corr_const_x VALUES (5.0, 1.0), (5.0, 2.0), (5.0, 3.0), (5.0, 4.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM corr_const_x")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_corr_constant_y() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_const_y (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO corr_const_y VALUES (1.0, 5.0), (2.0, 5.0), (3.0, 5.0), (4.0, 5.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM corr_const_y")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_corr_with_group_by() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT category, CORR(x, y) FROM correlation_data GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 1.0], ["B", 1.0], ["C", 1.0],]);
}

#[test]
fn test_corr_with_having() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT category, CORR(x, y) AS correlation FROM correlation_data GROUP BY category HAVING CORR(x, y) > 0.9 ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", 1.0], ["B", 1.0], ["C", 1.0],]);
}

#[test]
fn test_covar_pop_perfect_correlation() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT COVAR_POP(x, y) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[10.5]]);
}

#[test]
fn test_covar_samp_perfect_correlation() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT COVAR_SAMP(x, y) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[12.0]]);
}

#[test]
fn test_covar_pop_vs_samp() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT COVAR_POP(x, y), COVAR_SAMP(x, y) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[10.5, 12.0]]);
}

#[test]
fn test_covar_with_nulls() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE covar_nulls (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO covar_nulls VALUES (1.0, 2.0), (NULL, 4.0), (3.0, NULL), (4.0, 8.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT COVAR_POP(x, y), COVAR_SAMP(x, y) FROM covar_nulls")
        .unwrap();
    assert_table_eq!(result, [[4.5, 9.0]]);
}

#[test]
fn test_covar_single_pair() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE covar_single (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO covar_single VALUES (1.0, 2.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COVAR_POP(x, y), COVAR_SAMP(x, y) FROM covar_single")
        .unwrap();
    assert_table_eq!(result, [[0.0, null]]);
}

#[test]
fn test_covar_with_group_by() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT category, COVAR_POP(x, y), COVAR_SAMP(x, y) FROM correlation_data GROUP BY category ORDER BY category")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["A", 1.3333333333333333, 2.0],
            ["B", 1.3333333333333333, 2.0],
            ["C", 0.5, 1.0],
        ]
    );
}

#[test]
fn test_regr_slope_perfect_linear() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT REGR_SLOPE(y, x) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[2.0]]);
}

#[test]
fn test_regr_intercept_perfect_linear() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT REGR_INTERCEPT(y, x) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_regr_slope_intercept_combined() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT REGR_SLOPE(y, x), REGR_INTERCEPT(y, x) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[2.0, 0.0]]);
}

#[test]
fn test_regr_r2_perfect_fit() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT REGR_R2(y, x) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_regr_count_with_nulls() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE regr_count_nulls (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO regr_count_nulls VALUES (1.0, 2.0), (NULL, 4.0), (3.0, NULL), (4.0, 8.0), (5.0, 10.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_COUNT(y, x) FROM regr_count_nulls")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_regr_avgx_avgy() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT REGR_AVGX(y, x), REGR_AVGY(y, x) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[4.5, 9.0]]);
}

#[test]
fn test_regr_sxx_syy_sxy() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT REGR_SXX(y, x), REGR_SYY(y, x), REGR_SXY(y, x) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[42.0, 168.0, 84.0]]);
}

#[test]
fn test_regr_all_functions() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT REGR_SLOPE(y, x), REGR_INTERCEPT(y, x), REGR_R2(y, x), REGR_COUNT(y, x),
                    REGR_AVGX(y, x), REGR_AVGY(y, x), REGR_SXX(y, x), REGR_SYY(y, x), REGR_SXY(y, x)
             FROM correlation_data",
        )
        .unwrap();
    assert_table_eq!(result, [[2.0, 0.0, 1.0, 8, 4.5, 9.0, 42.0, 168.0, 84.0]]);
}

#[test]
fn test_regr_with_group_by() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT category, REGR_SLOPE(y, x), REGR_INTERCEPT(y, x), REGR_R2(y, x)
             FROM correlation_data GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["A", 2.0, 0.0, 1.0],
            ["B", 2.0, 0.0, 1.0],
            ["C", 2.0, 0.0, 1.0],
        ]
    );
}

#[test]
fn test_regr_empty_set() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE regr_empty (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_SLOPE(y, x), REGR_INTERCEPT(y, x), REGR_R2(y, x) FROM regr_empty")
        .unwrap();
    assert_table_eq!(result, [[null, null, null]]);
}

#[test]
fn test_regr_single_point() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE regr_single (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO regr_single VALUES (1.0, 2.0)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT REGR_SLOPE(y, x), REGR_INTERCEPT(y, x), REGR_R2(y, x) FROM regr_single",
        )
        .unwrap();
    assert_table_eq!(result, [[null, null, null]]);
}

#[test]
fn test_regr_horizontal_line() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE regr_horiz (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO regr_horiz VALUES (1.0, 5.0), (2.0, 5.0), (3.0, 5.0), (4.0, 5.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_SLOPE(y, x), REGR_INTERCEPT(y, x) FROM regr_horiz")
        .unwrap();
    assert_table_eq!(result, [[0.0, 5.0]]);
}

#[test]
fn test_regr_vertical_line() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE regr_vert (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO regr_vert VALUES (5.0, 1.0), (5.0, 2.0), (5.0, 3.0), (5.0, 4.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REGR_SLOPE(y, x), REGR_INTERCEPT(y, x) FROM regr_vert")
        .unwrap();
    assert_table_eq!(result, [[null, null]]);
}

#[test]
fn test_correlation_multiple_variables() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT CORR(x, y), CORR(x, z), CORR(y, z) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[1.0, 1.0, 1.0]]);
}

#[test]
fn test_correlation_with_filter() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT CORR(x, y) FILTER (WHERE category = 'A') FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_correlation_in_subquery() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT category, corr_val FROM (SELECT category, CORR(x, y) AS corr_val FROM correlation_data GROUP BY category) sub WHERE corr_val > 0 ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", 1.0], ["B", 1.0], ["C", 1.0],]);
}

#[test]
fn test_correlation_with_case() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT CASE WHEN CORR(x, y) > 0.8 THEN 'Strong' WHEN CORR(x, y) > 0.5 THEN 'Moderate' ELSE 'Weak' END AS strength FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [["Strong"]]);
}

#[test]
fn test_correlation_window_function() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT id, x, y, CORR(x, y) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_corr FROM correlation_data")
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, 1.0, 2.0, null],
            [2, 2.0, 4.0, 1.0],
            [3, 3.0, 6.0, 1.0],
            [4, 4.0, 8.0, 1.0],
            [5, 5.0, 10.0, 1.0],
            [6, 6.0, 12.0, 1.0],
            [7, 7.0, 14.0, 1.0],
            [8, 8.0, 16.0, 1.0],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_covariance_window_function() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT id, x, y, COVAR_POP(x, y) OVER (PARTITION BY category) AS category_covar FROM correlation_data ORDER BY id")
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, 1.0, 2.0, 2.0],
            [2, 2.0, 4.0, 2.0],
            [3, 3.0, 6.0, 2.0],
            [4, 4.0, 8.0, 2.0],
            [5, 5.0, 10.0, 2.0],
            [6, 6.0, 12.0, 2.0],
            [7, 7.0, 14.0, 1.0],
            [8, 8.0, 16.0, 1.0],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_regr_window_function() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT id, x, y, REGR_SLOPE(y, x) OVER (PARTITION BY category) AS category_slope FROM correlation_data ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, 1.0, 2.0, 2.0],
            [2, 2.0, 4.0, 2.0],
            [3, 3.0, 6.0, 2.0],
            [4, 4.0, 8.0, 2.0],
            [5, 5.0, 10.0, 2.0],
            [6, 6.0, 12.0, 2.0],
            [7, 7.0, 14.0, 2.0],
            [8, 8.0, 16.0, 2.0],
        ]
    );
}

#[test]
fn test_correlation_large_values() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_large (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO corr_large VALUES (1e10, 2e10), (2e10, 4e10), (3e10, 6e10), (4e10, 8e10)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM corr_large")
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_correlation_small_values() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_small (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_small VALUES (1e-10, 2e-10), (2e-10, 4e-10), (3e-10, 6e-10), (4e-10, 8e-10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM corr_small")
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_correlation_mixed_signs() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_mixed (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_mixed VALUES (-2.0, 4.0), (-1.0, 1.0), (0.0, 0.0), (1.0, 1.0), (2.0, 4.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y) FROM corr_mixed")
        .unwrap();
    assert_table_eq!(result, [[0.0]]);
}

#[test]
fn test_correlation_quadratic_relationship() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_quad (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_quad VALUES (-2.0, 4.0), (-1.0, 1.0), (0.0, 0.0), (1.0, 1.0), (2.0, 4.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(x, y), CORR(x, x*x) FROM corr_quad")
        .unwrap();
    assert_table_eq!(result, [[0.0, 0.0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_correlation_with_order_by() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT category, CORR(x, y) AS corr FROM correlation_data GROUP BY category ORDER BY CORR(x, y) DESC",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 1.0], ["B", 1.0], ["C", 1.0]]);
}

#[test]
fn test_correlation_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_dup (x DOUBLE PRECISION, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_dup VALUES (1.0, 2.0), (1.0, 2.0), (2.0, 4.0), (2.0, 4.0), (3.0, 6.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(DISTINCT x, y) FROM corr_dup")
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_correlation_cte() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql(
            "WITH correlations AS (SELECT category, CORR(x, y) AS corr FROM correlation_data GROUP BY category)
         SELECT * FROM correlations WHERE corr > 0.9 ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 1.0], ["B", 1.0], ["C", 1.0]]);
}

#[test]
fn test_correlation_self() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT CORR(x, x) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}

#[test]
fn test_covariance_self() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql("SELECT COVAR_POP(x, x), VAR_POP(x) FROM correlation_data")
        .unwrap();
    assert_table_eq!(result, [[5.25, 5.25]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_regr_prediction() {
    let mut executor = create_executor();
    setup_correlation_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT x, y, REGR_SLOPE(y, x) OVER () * x + REGR_INTERCEPT(y, x) OVER () AS predicted_y FROM correlation_data ORDER BY x",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1.0, 2.0, 2.0],
            [2.0, 4.0, 4.0],
            [3.0, 6.0, 6.0],
            [4.0, 8.0, 8.0],
            [5.0, 10.0, 10.0],
            [6.0, 12.0, 12.0],
            [7.0, 14.0, 14.0],
            [8.0, 16.0, 16.0],
        ]
    );
}

#[test]
fn test_correlation_across_joins() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_left (id INTEGER, x DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE corr_right (id INTEGER, y DOUBLE PRECISION)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_left VALUES (1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_right VALUES (1, 2.0), (2, 4.0), (3, 6.0), (4, 8.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CORR(l.x, r.y) FROM corr_left l JOIN corr_right r ON l.id = r.id")
        .unwrap();
    assert_table_eq!(result, [[1.0]]);
}
