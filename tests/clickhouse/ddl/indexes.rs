use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE indexed (id INT64, name STRING)")
        .unwrap();

    let result = executor.execute_sql("CREATE INDEX idx_name ON indexed (name)");
    assert!(result.is_ok());
}

#[test]
fn test_create_index_if_not_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE indexed (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_val ON indexed (value)")
        .unwrap();

    let result = executor.execute_sql("CREATE INDEX IF NOT EXISTS idx_val ON indexed (value)");
    assert!(result.is_ok());
}

#[test]
fn test_create_unique_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE unique_test (id INT64, email STRING)")
        .unwrap();

    let result = executor.execute_sql("CREATE UNIQUE INDEX idx_email ON unique_test (email)");
    assert!(result.is_ok());
}

#[test]
fn test_create_index_multiple_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE multi_idx (a INT64, b INT64, c INT64)")
        .unwrap();

    let result = executor.execute_sql("CREATE INDEX idx_ab ON multi_idx (a, b)");
    assert!(result.is_ok());
}

#[test]
fn test_drop_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE drop_idx_test (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_val ON drop_idx_test (value)")
        .unwrap();

    let result = executor.execute_sql("DROP INDEX idx_val ON drop_idx_test");
    assert!(result.is_ok());
}

#[test]
fn test_drop_index_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE drop_idx_test2 (id INT64)")
        .unwrap();

    let result = executor.execute_sql("DROP INDEX IF EXISTS non_existent ON drop_idx_test2");
    assert!(result.is_ok());
}

#[test]
fn test_create_index_minmax() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE minmax_idx (id INT64, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("CREATE INDEX idx_minmax ON minmax_idx (value) TYPE minmax GRANULARITY 4");
    assert!(result.is_ok());
}

#[test]
fn test_create_index_set() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE set_idx (id INT64, value INT64)")
        .unwrap();

    let result = executor.execute_sql("CREATE INDEX idx_set ON set_idx (value) TYPE set(100)");
    assert!(result.is_ok());
}

#[test]
fn test_create_index_bloom_filter() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bloom_idx (id INT64, value STRING)")
        .unwrap();

    let result =
        executor.execute_sql("CREATE INDEX idx_bloom ON bloom_idx (value) TYPE bloom_filter(0.01)");
    assert!(result.is_ok());
}

#[test]
fn test_create_index_ngrambf() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ngram_idx (id INT64, text STRING)")
        .unwrap();

    let result = executor
        .execute_sql("CREATE INDEX idx_ngram ON ngram_idx (text) TYPE ngrambf_v1(4, 256, 2, 0)");
    assert!(result.is_ok());
}

#[test]
fn test_create_index_tokenbf() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE token_idx (id INT64, text STRING)")
        .unwrap();

    let result = executor
        .execute_sql("CREATE INDEX idx_token ON token_idx (text) TYPE tokenbf_v1(256, 2, 0)");
    assert!(result.is_ok());
}

#[ignore = "numbers() function not supported"]
#[test]
fn test_index_performance() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE perf_test (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO perf_test SELECT number, number * 10 FROM numbers(1000)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_perf ON perf_test (value) TYPE minmax")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM perf_test WHERE value = 500 LIMIT 1")
        .unwrap();
    assert_table_eq!(result, [[50]]);
}

#[ignore = "ALTER TABLE MATERIALIZE INDEX not supported"]
#[test]
fn test_materialize_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mat_idx (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mat_idx VALUES (1, 10), (2, 20)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_mat ON mat_idx (value) TYPE minmax")
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE mat_idx MATERIALIZE INDEX idx_mat");
    assert!(result.is_ok());
}

#[ignore = "ALTER TABLE CLEAR INDEX not supported"]
#[test]
fn test_clear_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE clear_idx (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO clear_idx VALUES (1, 10), (2, 20)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_clear ON clear_idx (value) TYPE minmax")
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE clear_idx CLEAR INDEX idx_clear");
    assert!(result.is_ok());
}

#[test]
fn test_create_index_inverted() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE inverted_idx (id INT64, content STRING)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE INDEX idx_inverted ON inverted_idx (content) TYPE inverted GRANULARITY 1",
    );
    assert!(result.is_ok());
}

#[test]
fn test_create_index_full_text() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fulltext_idx (id INT64, document STRING)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE INDEX idx_fulltext ON fulltext_idx (document) TYPE full_text GRANULARITY 1",
    );
    assert!(result.is_ok());
}

#[ignore = "Float32 data type not supported"]
#[test]
fn test_create_index_annoy() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE annoy_idx (id INT64, embedding Array(Float32))")
        .unwrap();

    let result = executor.execute_sql("CREATE INDEX idx_annoy ON annoy_idx (embedding) TYPE annoy('L2Distance', 100) GRANULARITY 1");
    assert!(result.is_ok());
}

#[ignore = "Float32 data type not supported"]
#[test]
fn test_create_index_usearch() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE usearch_idx (id INT64, vector Array(Float32))")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE INDEX idx_usearch ON usearch_idx (vector) TYPE usearch('L2Distance') GRANULARITY 1",
    );
    assert!(result.is_ok());
}

#[test]
fn test_create_index_hypothesis() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE hypothesis_idx (id INT64, value Float64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE INDEX idx_hyp ON hypothesis_idx (value) TYPE hypothesis GRANULARITY 8",
    );
    assert!(result.is_ok());
}

#[ignore = "Parser doesn't support inline INDEX definition in CREATE TABLE"]
#[test]
fn test_inline_minmax_index() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE inline_minmax (
                id UInt64,
                value Int64,
                INDEX idx_value value TYPE minmax GRANULARITY 4
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Parser doesn't support inline INDEX definition in CREATE TABLE"]
#[test]
fn test_inline_bloom_filter_index() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE inline_bloom (
                id UInt64,
                email String,
                INDEX idx_email email TYPE bloom_filter(0.01) GRANULARITY 1
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Parser doesn't support inline INDEX definition in CREATE TABLE"]
#[test]
fn test_inline_multiple_indexes() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE inline_multi (
                id UInt64,
                name String,
                value Int64,
                timestamp DateTime,
                INDEX idx_name name TYPE bloom_filter GRANULARITY 1,
                INDEX idx_value value TYPE minmax GRANULARITY 4,
                INDEX idx_ts timestamp TYPE minmax GRANULARITY 1
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_index_with_expression() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE expr_idx (id INT64, value INT64)")
        .unwrap();

    let result = executor
        .execute_sql("CREATE INDEX idx_double ON expr_idx ((value * 2)) TYPE minmax GRANULARITY 1");
    assert!(result.is_ok());
}

#[test]
fn test_index_with_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE func_idx (id INT64, name STRING)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE INDEX idx_lower ON func_idx (lower(name)) TYPE set(100) GRANULARITY 1",
    );
    assert!(result.is_ok());
}

#[ignore = "Parser doesn't support inline INDEX definition in CREATE TABLE"]
#[test]
fn test_bloom_filter_query() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE bloom_query (
                id UInt64,
                email String,
                INDEX idx_email email TYPE bloom_filter GRANULARITY 1
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO bloom_query VALUES
            (1, 'alice@example.com'),
            (2, 'bob@example.com'),
            (3, 'charlie@example.com')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM bloom_query WHERE email = 'bob@example.com'")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Parser doesn't support inline INDEX definition in CREATE TABLE"]
#[test]
fn test_set_index_in_query() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE set_query (
                id UInt64,
                category String,
                INDEX idx_cat category TYPE set(10) GRANULARITY 1
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO set_query VALUES
            (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT count() FROM set_query WHERE category IN ('A', 'C')")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[ignore = "Parser doesn't support inline INDEX definition in CREATE TABLE"]
#[test]
fn test_fulltext_has_token() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE fulltext_search (
                id UInt64,
                content String,
                INDEX idx_content content TYPE inverted GRANULARITY 1
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO fulltext_search VALUES
            (1, 'The quick brown fox'),
            (2, 'jumps over the lazy dog'),
            (3, 'A quick brown rabbit')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT count() FROM fulltext_search WHERE hasToken(content, 'quick')")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Parser doesn't support inline INDEX definition in CREATE TABLE"]
#[test]
fn test_vector_l2_distance() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE vector_test (
                id UInt64,
                embedding Array(Float32),
                INDEX idx_emb embedding TYPE annoy('L2Distance', 100) GRANULARITY 1
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO vector_test VALUES
            (1, [1.0, 0.0, 0.0]),
            (2, [0.0, 1.0, 0.0]),
            (3, [0.0, 0.0, 1.0])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM vector_test
            ORDER BY L2Distance(embedding, [1.0, 0.0, 0.0])
            LIMIT 1",
        )
        .unwrap();
    assert_table_eq!(result, [[1]]);
}
