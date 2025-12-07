use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_data(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE data (id INT64, category STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 'A', 10), (2, 'A', 20), (3, 'A', 30), (4, 'B', 15), (5, 'B', 25), (6, 'B', 35), (7, 'C', 5), (8, 'C', 50)")
        .unwrap();
}

#[test]
#[ignore = "Implement me!"]
fn test_approx_count_distinct() {
    let mut executor = create_executor();
    setup_data(&mut executor);

    let result = executor
        .execute_sql("SELECT APPROX_COUNT_DISTINCT(category) = 3 FROM data")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_approx_count_distinct_with_group() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE visits (user_id INT64, page STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO visits VALUES (1, 'home'), (1, 'about'), (2, 'home'), (2, 'home'), (3, 'home'), (3, 'products')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT user_id, APPROX_COUNT_DISTINCT(page) FROM visits GROUP BY user_id ORDER BY user_id")
        .unwrap();
    assert_table_eq!(result, [[1, 2], [2, 1], [3, 2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_approx_quantiles() {
    let mut executor = create_executor();
    setup_data(&mut executor);

    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(APPROX_QUANTILES(value, 4)) FROM data")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_approx_quantiles_with_group() {
    let mut executor = create_executor();
    setup_data(&mut executor);

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM (SELECT category, APPROX_QUANTILES(value, 2) FROM data GROUP BY category ORDER BY category)")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_approx_top_count() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE words (word STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('apple'), ('banana'), ('apple'), ('cherry'), ('apple'), ('banana')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(APPROX_TOP_COUNT(word, 2)) FROM words")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_approx_top_sum() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (product STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO sales VALUES ('A', 100), ('B', 200), ('A', 150), ('C', 50), ('B', 100)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(APPROX_TOP_SUM(product, amount, 2)) FROM sales")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_approx_count_distinct_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES ('a'), (NULL), ('b'), (NULL), ('a')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT APPROX_COUNT_DISTINCT(value) = 2 FROM data")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_approx_quantiles_ignore_nulls() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (10), (NULL), (20), (NULL), (30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(APPROX_QUANTILES(value IGNORE NULLS, 2)) FROM data")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_approx_quantiles_respect_nulls() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (10), (NULL), (20), (NULL), (30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(APPROX_QUANTILES(value RESPECT NULLS, 2)) FROM data")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_hll_count_init() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES ('a'), ('b'), ('c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM (SELECT HLL_COUNT.INIT(value) FROM data)")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_hll_count_merge() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sketches (sketch BYTES)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT HLL_COUNT.MERGE(sketch) IS NULL FROM sketches")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_hll_count_merge_partial() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (category STRING, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES ('A', 'x'), ('A', 'y'), ('B', 'x'), ('B', 'z')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM (SELECT category, HLL_COUNT.MERGE_PARTIAL(HLL_COUNT.INIT(value)) FROM data GROUP BY category ORDER BY category)")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_hll_count_extract() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES ('a'), ('b'), ('c'), ('a')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT HLL_COUNT.EXTRACT(HLL_COUNT.MERGE(HLL_COUNT.INIT(value))) = 3 FROM data",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_approx_count_distinct_large_dataset() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE large_data (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO large_data SELECT n FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS n")
        .unwrap();

    let result = executor
        .execute_sql("SELECT APPROX_COUNT_DISTINCT(id) BETWEEN 950 AND 1050 FROM large_data")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
