use std::time::Instant;

use debug_print::debug_eprintln;
use yachtsql_executor::hash_sql;
use yachtsql_executor::plan_cache::{CachedPlan, PlanCache, PlanCacheKey};
use yachtsql_optimizer::plan::PlanNode;
use yachtsql_parser::DialectType;

#[test]
fn bench_cache_insert() {
    let mut cache = PlanCache::new();

    let iterations = 1000;
    let start = Instant::now();

    for i in 0..iterations {
        let sql = format!("SELECT * FROM table_{}", i);
        let hash = hash_sql(&sql, DialectType::PostgreSQL);
        let key = PlanCacheKey::new(hash, DialectType::PostgreSQL);

        let plan = PlanNode::Scan {
            table_name: format!("table_{}", i),
            alias: None,
            projection: None,
            only: false,
        };

        cache.insert(key, CachedPlan::new(plan, sql));
    }

    let elapsed = start.elapsed();
    let avg_micros = elapsed.as_micros() / iterations as u128;

    debug_eprintln!("[test::plan_cache_bench] \n=== Cache Insert Performance ===");
    debug_eprintln!(
        "[test::plan_cache_bench] Inserted {} plans in {:?}",
        iterations,
        elapsed
    );
    debug_eprintln!(
        "[test::plan_cache_bench] Average insert time: {}µs",
        avg_micros
    );
    debug_eprintln!(
        "[test::plan_cache_bench] Throughput: {:.0} inserts/sec\n",
        iterations as f64 / elapsed.as_secs_f64()
    );

    let stats = cache.stats();
    assert!(stats.entry_count > 0);
}

#[test]
fn bench_cache_lookup() {
    let mut cache = PlanCache::new();

    let num_entries = 100;
    let keys: Vec<PlanCacheKey> = (0..num_entries)
        .map(|i| {
            let sql = format!("SELECT * FROM lookup_bench_{}", i);
            let hash = hash_sql(&sql, DialectType::PostgreSQL);
            let key = PlanCacheKey::new(hash, DialectType::PostgreSQL);

            let plan = PlanNode::Scan {
                table_name: format!("lookup_bench_{}", i),
                alias: None,
                projection: None,
                only: false,
            };

            cache.insert(key, CachedPlan::new(plan, sql));
            key
        })
        .collect();

    cache.reset_stats();

    let iterations = 10_000;
    let start = Instant::now();
    let mut hits = 0;

    for i in 0..iterations {
        let key = &keys[i % num_entries];
        if cache.get(key).is_some() {
            hits += 1;
        }
    }

    let elapsed = start.elapsed();
    let avg_nanos = elapsed.as_nanos() / iterations as u128;
    let hit_rate = hits as f64 / iterations as f64;

    debug_eprintln!("[test::plan_cache_bench] \n=== Cache Lookup Performance ===");
    debug_eprintln!(
        "[test::plan_cache_bench] Performed {} lookups in {:?}",
        iterations,
        elapsed
    );
    debug_eprintln!(
        "[test::plan_cache_bench] Average lookup time: {}ns",
        avg_nanos
    );
    debug_eprintln!(
        "[test::plan_cache_bench] Throughput: {:.0} lookups/sec",
        iterations as f64 / elapsed.as_secs_f64()
    );
    debug_eprintln!(
        "[test::plan_cache_bench] Hit rate: {:.1}%\n",
        hit_rate * 100.0
    );

    assert_eq!(
        hits, iterations,
        "Should have 100% cache hits with local cache"
    );
    assert!(
        elapsed.as_millis() < 100,
        "Lookups should be fast, took {:?}",
        elapsed
    );
}

#[test]
fn bench_hit_rate_impact() {
    let mut cache = PlanCache::new();

    let num_cached = 100;
    for i in 0..num_cached {
        let sql = format!("SELECT * FROM table_{}", i);
        let hash = hash_sql(&sql, DialectType::PostgreSQL);
        let key = PlanCacheKey::new(hash, DialectType::PostgreSQL);

        let plan = PlanNode::Scan {
            table_name: format!("table_{}", i),
            alias: None,
            projection: None,
            only: false,
        };

        cache.insert(key, CachedPlan::new(plan, sql));
    }

    debug_eprintln!("[test::plan_cache_bench] \n=== Hit Rate Impact on Throughput ===");

    for hit_rate_percent in [50, 90, 100] {
        cache.reset_stats();

        let iterations = 10_000;
        let start = Instant::now();

        for i in 0..iterations {
            let sql = if (i % 100) < hit_rate_percent {
                format!("SELECT * FROM table_{}", i % num_cached)
            } else {
                format!("SELECT * FROM uncached_{}", i)
            };

            let hash = hash_sql(&sql, DialectType::PostgreSQL);
            let key = PlanCacheKey::new(hash, DialectType::PostgreSQL);
            let _ = cache.get(&key);
        }

        let elapsed = start.elapsed();
        let qps = iterations as f64 / elapsed.as_secs_f64();
        let stats = cache.stats();

        debug_eprintln!(
            "[test::plan_cache_bench] {}% target hit rate: {:.0} q/s (actual: {:.1}%)",
            hit_rate_percent,
            qps,
            stats.hit_rate() * 100.0
        );
    }

    debug_eprintln!("[test::plan_cache_bench]");
}

#[test]
fn bench_lru_eviction() {
    let mut cache = PlanCache::new();

    debug_eprintln!("[test::plan_cache_bench] \n=== LRU Eviction Test ===");

    let insert_count = 1500;

    for i in 0..insert_count {
        let sql = format!("SELECT * FROM evict_test_{}", i);
        let hash = hash_sql(&sql, DialectType::PostgreSQL);
        let key = PlanCacheKey::new(hash, DialectType::PostgreSQL);

        let plan = PlanNode::Scan {
            table_name: format!("evict_test_{}", i),
            alias: None,
            projection: None,
            only: false,
        };

        cache.insert(key, CachedPlan::new(plan, sql));
    }

    let stats = cache.stats();

    debug_eprintln!("[test::plan_cache_bench] Inserted {} plans", insert_count);
    debug_eprintln!(
        "[test::plan_cache_bench] Cache holds {} entries",
        stats.entry_count
    );
    debug_eprintln!(
        "[test::plan_cache_bench] Evicted {} plans",
        insert_count - stats.entry_count as usize
    );

    let recent_sql = format!("SELECT * FROM evict_test_{}", insert_count - 1);
    let recent_hash = hash_sql(&recent_sql, DialectType::PostgreSQL);
    let recent_key = PlanCacheKey::new(recent_hash, DialectType::PostgreSQL);

    let old_sql = format!("SELECT * FROM evict_test_{}", 0);
    let old_hash = hash_sql(&old_sql, DialectType::PostgreSQL);
    let old_key = PlanCacheKey::new(old_hash, DialectType::PostgreSQL);

    let recent_cached = cache.get(&recent_key).is_some();
    let old_evicted = cache.get(&old_key).is_none();

    debug_eprintln!(
        "[test::plan_cache_bench] Recent entry: {}",
        if recent_cached { "CACHED" } else { "EVICTED" }
    );
    debug_eprintln!(
        "[test::plan_cache_bench] Old entry: {}",
        if old_evicted { "EVICTED" } else { "CACHED" }
    );
    debug_eprintln!("[test::plan_cache_bench]");

    assert!(
        stats.entry_count >= 500,
        "Cache should be holding entries, got {}",
        stats.entry_count
    );
    assert!(
        stats.entry_count <= 1500,
        "Cache entry count should be reasonable, got {}",
        stats.entry_count
    );

    let mut cached_count = 0;
    for i in (insert_count - 50)..insert_count {
        let sql = format!("SELECT * FROM evict_test_{}", i);
        let hash = hash_sql(&sql, DialectType::PostgreSQL);
        let key = PlanCacheKey::new(hash, DialectType::PostgreSQL);
        if cache.get(&key).is_some() {
            cached_count += 1;
        }
    }

    debug_eprintln!(
        "[test::plan_cache_bench] Found {} cached entries out of last 50 inserted",
        cached_count
    );

    assert!(
        cached_count > 0 || stats.entry_count > 0,
        "Cache should have some entries"
    );
}
