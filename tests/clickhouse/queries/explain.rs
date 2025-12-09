use crate::common::create_executor;

#[test]
fn test_explain_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_basic (id INT64, name String)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_basic")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Parser doesn't support EXPLAIN AST syntax"]
#[test]
fn test_explain_ast() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_ast (id INT64)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN AST SELECT id FROM explain_ast WHERE id > 0")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Parser doesn't support EXPLAIN SYNTAX"]
#[test]
fn test_explain_syntax() {
    let mut executor = create_executor();
    let result = executor.execute_sql("EXPLAIN SYNTAX SELECT 1 + 2").unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Parser doesn't support EXPLAIN PLAN syntax"]
#[test]
fn test_explain_plan() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_plan (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN PLAN SELECT id FROM explain_plan WHERE id = 1")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Parser doesn't support EXPLAIN PIPELINE syntax"]
#[test]
fn test_explain_pipeline() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_pipe (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN PIPELINE SELECT id FROM explain_pipe")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[test]
fn test_explain_estimate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_est (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_est VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN ESTIMATE SELECT id FROM explain_est")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Parser doesn't support EXPLAIN indexes = 1 syntax"]
#[test]
fn test_explain_indexes() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_idx (id INT64, INDEX idx_id id TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO explain_idx VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN indexes = 1 SELECT id FROM explain_idx WHERE id = 2")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[test]
fn test_explain_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_left (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE explain_right (id INT64, name String)")
        .unwrap();

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT l.id, r.name FROM explain_left l JOIN explain_right r ON l.id = r.id",
        )
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[test]
fn test_explain_with_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_sub (id INT64, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT * FROM explain_sub WHERE id IN (SELECT id FROM explain_sub WHERE value > 10)")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[test]
fn test_explain_with_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_group (category String, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT category, SUM(value) FROM explain_group GROUP BY category")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[test]
fn test_explain_with_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE explain_order (id INT64, score INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN SELECT id FROM explain_order ORDER BY score DESC")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[test]
fn test_explain_with_window() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_window (id INT64, category String, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql(
            "EXPLAIN SELECT id, SUM(value) OVER (PARTITION BY category) FROM explain_window",
        )
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[test]
fn test_explain_with_cte() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_cte (id INT64)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN WITH cte AS (SELECT id FROM explain_cte) SELECT * FROM cte")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[test]
fn test_explain_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE explain_insert (id INT64)")
        .unwrap();

    let result = executor
        .execute_sql("EXPLAIN INSERT INTO explain_insert SELECT number FROM numbers(10)")
        .unwrap();
    assert!(result.num_rows() > 0);
}
