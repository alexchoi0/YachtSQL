use yachtsql_executor::plan_cache::{CachedPlan, PlanCache, PlanCacheKey};
use yachtsql_executor::{QueryExecutor, hash_sql};
use yachtsql_optimizer::plan::PlanNode;
use yachtsql_parser::DialectType;

#[test]
fn test_cache_basic_access() {
    let cache = PlanCache::new();

    let stats = cache.stats();
    assert_eq!(stats.entry_count, 0);
    assert_eq!(stats.hit_count, 0);
    assert_eq!(stats.miss_count, 0);
}

#[test]
fn test_cache_statistics() {
    let mut cache = PlanCache::new();

    let sql = "SELECT * FROM cache_stats_test_unique_12345";
    let hash = hash_sql(sql, DialectType::PostgreSQL);
    let key = PlanCacheKey::new(hash, DialectType::PostgreSQL);

    let plan = PlanNode::Scan {
        table_name: "cache_stats_test".to_string(),
        alias: None,
        projection: None,
        only: false,
    };

    let cached = CachedPlan::new(plan, sql.to_string());

    cache.insert(key, cached);

    let result = cache.get(&key);
    assert!(result.is_some(), "Should find cached entry");

    let stats_after_hit = cache.stats();
    assert_eq!(stats_after_hit.hit_count, 1, "Should have 1 hit");

    let hash2 = hash_sql(
        "SELECT * FROM cache_stats_nonexistent_99999",
        DialectType::PostgreSQL,
    );
    let key2 = PlanCacheKey::new(hash2, DialectType::PostgreSQL);
    let result2 = cache.get(&key2);
    assert!(result2.is_none(), "Should not find non-existent entry");

    let stats_after_miss = cache.stats();
    assert_eq!(stats_after_miss.miss_count, 1, "Should have 1 miss");
}

#[test]
fn test_create_table_invalidates_cache() {
    let mut executor = QueryExecutor::new();

    let stats_before = executor.plan_cache_stats();
    let initial_invalidations = stats_before.invalidation_count;

    let result =
        executor.execute_sql("CREATE TABLE test_create_invalidate (id INT64, name STRING)");
    assert!(result.is_ok(), "CREATE TABLE should succeed");

    let stats_after = executor.plan_cache_stats();
    assert!(
        stats_after.invalidation_count > initial_invalidations,
        "Should have incremented invalidation count after CREATE TABLE"
    );
}

#[test]
fn test_drop_table_invalidates_cache() {
    let mut executor = QueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE test_drop_table (id INT64)")
        .unwrap();

    let stats_before = executor.plan_cache_stats();
    let initial_invalidations = stats_before.invalidation_count;

    let result = executor.execute_sql("DROP TABLE test_drop_table");
    assert!(result.is_ok(), "DROP TABLE should succeed");

    let stats_after = executor.plan_cache_stats();
    assert!(
        stats_after.invalidation_count > initial_invalidations,
        "Should have incremented invalidation count after DROP TABLE"
    );
}

#[test]
fn test_create_view_invalidates_cache() {
    let mut executor = QueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE test_create_view_base (id INT64, value STRING)")
        .unwrap();

    let stats_before = executor.plan_cache_stats();
    let initial_invalidations = stats_before.invalidation_count;

    let result =
        executor.execute_sql("CREATE VIEW test_create_view AS SELECT * FROM test_create_view_base");
    assert!(result.is_ok(), "CREATE VIEW should succeed");

    let stats_after = executor.plan_cache_stats();
    assert!(
        stats_after.invalidation_count > initial_invalidations,
        "Should have incremented invalidation count after CREATE VIEW"
    );
}

#[test]
fn test_drop_view_invalidates_cache() {
    let mut executor = QueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE test_drop_view_tbl (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW test_view_to_drop AS SELECT * FROM test_drop_view_tbl")
        .unwrap();

    let stats_before = executor.plan_cache_stats();
    let initial_invalidations = stats_before.invalidation_count;

    let result = executor.execute_sql("DROP VIEW test_view_to_drop");
    assert!(result.is_ok(), "DROP VIEW should succeed");

    let stats_after = executor.plan_cache_stats();
    assert!(
        stats_after.invalidation_count > initial_invalidations,
        "Should have incremented invalidation count after DROP VIEW"
    );
}

#[test]
fn test_create_index_invalidates_cache() {
    let mut executor = QueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE test_create_index_tbl (id INT64, name STRING)")
        .unwrap();

    let stats_before = executor.plan_cache_stats();
    let initial_invalidations = stats_before.invalidation_count;

    let result = executor.execute_sql("CREATE INDEX idx_create_test ON test_create_index_tbl (id)");
    assert!(result.is_ok(), "CREATE INDEX should succeed");

    let stats_after = executor.plan_cache_stats();
    assert!(
        stats_after.invalidation_count > initial_invalidations,
        "Should have incremented invalidation count after CREATE INDEX"
    );
}

#[test]
fn test_drop_index_invalidates_cache() {
    let mut executor = QueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE test_drop_index_tbl (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_to_drop ON test_drop_index_tbl (id)")
        .unwrap();

    let stats_before = executor.plan_cache_stats();
    let initial_invalidations = stats_before.invalidation_count;

    let result = executor.execute_sql("DROP INDEX idx_to_drop");
    assert!(result.is_ok(), "DROP INDEX should succeed");

    let stats_after = executor.plan_cache_stats();
    assert!(
        stats_after.invalidation_count > initial_invalidations,
        "Should have incremented invalidation count after DROP INDEX"
    );
}

#[test]
fn test_dialect_isolation() {
    let sql = "SELECT * FROM dialect_test_unique";

    let pg_hash = hash_sql(sql, DialectType::PostgreSQL);
    let bq_hash = hash_sql(sql, DialectType::BigQuery);
    let ch_hash = hash_sql(sql, DialectType::ClickHouse);

    assert_ne!(
        pg_hash, bq_hash,
        "PostgreSQL and BigQuery should have different hashes"
    );
    assert_ne!(
        pg_hash, ch_hash,
        "PostgreSQL and ClickHouse should have different hashes"
    );
    assert_ne!(
        bq_hash, ch_hash,
        "BigQuery and ClickHouse should have different hashes"
    );
}

#[test]
fn test_dml_operations() {
    let mut executor = QueryExecutor::new();

    executor
        .execute_sql("CREATE TABLE dml_test_table (id INT64, value STRING)")
        .unwrap();

    let insert_result =
        executor.execute_sql("INSERT INTO dml_test_table (id, value) VALUES (1, 'test')");
    assert!(insert_result.is_ok(), "INSERT should succeed");

    let select_result = executor.execute_sql("SELECT * FROM dml_test_table");
    assert!(select_result.is_ok(), "SELECT should succeed");
}

#[test]
fn test_if_exists_invalidates() {
    let mut executor = QueryExecutor::new();

    let stats_before = executor.plan_cache_stats();
    let initial_invalidations = stats_before.invalidation_count;

    let result = executor.execute_sql("DROP TABLE IF EXISTS nonexistent_table_xyz");
    assert!(result.is_ok(), "DROP TABLE IF EXISTS should succeed");

    let stats_after = executor.plan_cache_stats();

    assert!(
        stats_after.invalidation_count > initial_invalidations,
        "Cache is invalidated even with IF EXISTS (current behavior)"
    );
}
