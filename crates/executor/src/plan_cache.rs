use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::rc::Rc;

use debug_print::debug_eprintln;
use lru::LruCache;
use yachtsql_optimizer::plan::PlanNode;

#[derive(Debug, Clone)]
pub struct PlanCacheConfig {
    pub max_capacity: usize,
}

impl Default for PlanCacheConfig {
    fn default() -> Self {
        Self {
            max_capacity: 1_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlanCacheKey {
    pub sql_hash: u64,
    pub dialect: u8,
}

impl PlanCacheKey {
    pub fn new(sql_hash: u64, dialect: yachtsql_parser::DialectType) -> Self {
        let dialect_id = match dialect {
            yachtsql_parser::DialectType::PostgreSQL => 0,
            yachtsql_parser::DialectType::BigQuery => 1,
            yachtsql_parser::DialectType::ClickHouse => 2,
        };

        Self {
            sql_hash,
            dialect: dialect_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CachedPlan {
    pub plan: Rc<PlanNode>,
    pub sql: Rc<String>,
    pub cached_at: std::time::Instant,
    pub hit_count: Rc<RefCell<u64>>,
    pub referenced_tables: Rc<HashSet<String>>,
}

impl CachedPlan {
    pub fn new(plan: PlanNode, sql: String) -> Self {
        let referenced_tables = extract_referenced_tables(&plan);
        Self {
            plan: Rc::new(plan),
            sql: Rc::new(sql),
            cached_at: std::time::Instant::now(),
            hit_count: Rc::new(RefCell::new(0)),
            referenced_tables: Rc::new(referenced_tables),
        }
    }

    pub fn record_hit(&self) {
        *self.hit_count.borrow_mut() += 1;
    }

    pub fn hits(&self) -> u64 {
        *self.hit_count.borrow()
    }
}

pub struct PlanCache {
    cache: LruCache<PlanCacheKey, Rc<CachedPlan>>,
    hit_count: u64,
    miss_count: u64,
    invalidation_count: u64,
    table_to_keys: HashMap<String, HashSet<PlanCacheKey>>,
}

impl PlanCache {
    pub fn new() -> Self {
        Self::with_config(PlanCacheConfig::default())
    }

    pub fn with_config(config: PlanCacheConfig) -> Self {
        let cap = NonZeroUsize::new(config.max_capacity).unwrap_or(NonZeroUsize::MIN);
        Self {
            cache: LruCache::new(cap),
            hit_count: 0,
            miss_count: 0,
            invalidation_count: 0,
            table_to_keys: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &PlanCacheKey) -> Option<Rc<CachedPlan>> {
        match self.cache.get(key) {
            Some(cached) => {
                cached.record_hit();
                self.hit_count += 1;
                Some(Rc::clone(cached))
            }
            None => {
                self.miss_count += 1;
                None
            }
        }
    }

    pub fn insert(&mut self, key: PlanCacheKey, cached_plan: CachedPlan) {
        let cached_plan = Rc::new(cached_plan);

        for table in cached_plan.referenced_tables.iter() {
            self.table_to_keys
                .entry(table.clone())
                .or_insert_with(HashSet::new)
                .insert(key);
        }

        self.cache.put(key, cached_plan);
    }

    pub fn invalidate_all(&mut self) {
        self.cache.clear();
        self.table_to_keys.clear();
        self.invalidation_count += 1;
    }

    pub fn invalidate_table(&mut self, _dataset: &str, table: &str) {
        if let Some(keys_to_invalidate) = self.table_to_keys.remove(table) {
            for key in keys_to_invalidate {
                self.cache.pop(&key);
            }
            self.invalidation_count += 1;
        }
    }

    pub fn clear(&mut self) {
        self.cache.clear();
        self.table_to_keys.clear();
        self.hit_count = 0;
        self.miss_count = 0;
        self.invalidation_count = 0;
    }

    pub fn stats(&self) -> PlanCacheStats {
        PlanCacheStats {
            entry_count: self.cache.len() as u64,
            hit_count: self.hit_count,
            miss_count: self.miss_count,
            invalidation_count: self.invalidation_count,
        }
    }

    pub fn hit_rate(&self) -> f64 {
        let stats = self.stats();
        let total = stats.hit_count + stats.miss_count;
        if total == 0 {
            0.0
        } else {
            stats.hit_count as f64 / total as f64
        }
    }

    pub fn get_cache_entries(&self) -> Vec<CacheEntryInfo> {
        let mut entries = Vec::new();
        let mut seen_keys = HashSet::new();

        for (_table_name, keys) in self.table_to_keys.iter() {
            for key in keys {
                if seen_keys.insert(*key) {
                    if let Some(cached) = self.cache.peek(key) {
                        entries.push(CacheEntryInfo {
                            key: *key,
                            sql: (*cached.sql).clone(),
                            referenced_tables: cached.referenced_tables.iter().cloned().collect(),
                            hits: cached.hits(),
                            age_seconds: cached.cached_at.elapsed().as_secs(),
                        });
                    }
                }
            }
        }

        entries
    }

    pub fn get_plans_for_table(&self, table: &str) -> Vec<CacheEntryInfo> {
        let mut entries = Vec::new();

        if let Some(keys) = self.table_to_keys.get(table) {
            for key in keys {
                if let Some(cached) = self.cache.peek(key) {
                    entries.push(CacheEntryInfo {
                        key: *key,
                        sql: (*cached.sql).clone(),
                        referenced_tables: cached.referenced_tables.iter().cloned().collect(),
                        hits: cached.hits(),
                        age_seconds: cached.cached_at.elapsed().as_secs(),
                    });
                }
            }
        }

        entries
    }

    pub fn reset_stats(&mut self) {
        self.hit_count = 0;
        self.miss_count = 0;
    }

    pub fn get_and_reset_stats(&mut self) -> PlanCacheStats {
        let stats = self.stats();
        self.reset_stats();
        stats
    }

    pub fn log_stats(&self) {
        let stats = self.stats();
        debug_eprintln!("[executor::plan_cache] {}", stats.format_report());
    }

    pub fn estimated_memory_bytes(&self) -> usize {
        const AVG_PLAN_SIZE_BYTES: usize = 1024;
        self.cache.len() * AVG_PLAN_SIZE_BYTES
    }

    pub fn is_healthy(&self, min_hit_rate: f64) -> bool {
        let total = self.hit_count + self.miss_count;
        if total < 100 {
            return true;
        }

        self.hit_rate() >= min_hit_rate
    }

    pub fn get_hot_queries(&self, limit: usize) -> Vec<CacheEntryInfo> {
        let mut entries = self.get_cache_entries();
        entries.sort_by(|a, b| b.hits.cmp(&a.hits));
        entries.truncate(limit);
        entries
    }

    pub fn get_cold_queries(&self, limit: usize) -> Vec<CacheEntryInfo> {
        let mut entries = self.get_cache_entries();
        entries.sort_by(|a, b| b.age_seconds.cmp(&a.age_seconds));
        entries.truncate(limit);
        entries
    }

    pub fn health_report(&self) -> HealthReport {
        let stats = self.stats();
        let entries = self.get_cache_entries();

        let total_hits: u64 = entries.iter().map(|e| e.hits).sum();
        let avg_hits = if !entries.is_empty() {
            total_hits as f64 / entries.len() as f64
        } else {
            0.0
        };

        let avg_age = if !entries.is_empty() {
            entries.iter().map(|e| e.age_seconds).sum::<u64>() as f64 / entries.len() as f64
        } else {
            0.0
        };

        HealthReport {
            stats,
            total_entries: entries.len(),
            avg_hits_per_entry: avg_hits,
            avg_age_seconds: avg_age,
            memory_estimate_mb: self.estimated_memory_bytes() as f64 / 1_048_576.0,
            is_healthy: self.is_healthy(0.5),
        }
    }

    pub fn export_metadata(&self) -> CacheMetadata {
        let entries = self.get_cache_entries();
        let stats = self.stats();

        CacheMetadata {
            version: 1,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            entry_count: entries.len(),
            stats,
            entries: entries
                .into_iter()
                .map(|e| CacheEntryMetadata {
                    sql_hash: e.key.sql_hash,
                    dialect: e.key.dialect,
                    sql: e.sql,
                    referenced_tables: e.referenced_tables,
                    hits: e.hits,
                    age_seconds: e.age_seconds,
                })
                .collect(),
        }
    }

    pub fn diff(&self, previous: &CacheMetadata) -> CacheDiff {
        let current_entries = self.get_cache_entries();
        let current_map: HashSet<_> = current_entries.iter().map(|e| e.key.sql_hash).collect();

        let previous_map: HashSet<_> = previous.entries.iter().map(|e| e.sql_hash).collect();

        let added: Vec<_> = current_entries
            .iter()
            .filter(|e| !previous_map.contains(&e.key.sql_hash))
            .map(|e| e.sql.clone())
            .collect();

        let removed: Vec<_> = previous
            .entries
            .iter()
            .filter(|e| !current_map.contains(&e.sql_hash))
            .map(|e| e.sql.clone())
            .collect();

        let hit_changes: Vec<_> = current_entries
            .iter()
            .filter_map(|curr| {
                previous
                    .entries
                    .iter()
                    .find(|prev| prev.sql_hash == curr.key.sql_hash)
                    .map(|prev| {
                        (
                            curr.sql.clone(),
                            prev.hits,
                            curr.hits,
                            curr.hits as i64 - prev.hits as i64,
                        )
                    })
            })
            .filter(|(_, _, _, delta)| *delta != 0)
            .collect();

        CacheDiff {
            added_count: added.len(),
            removed_count: removed.len(),
            changed_count: hit_changes.len(),
            added,
            removed,
            hit_changes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HealthReport {
    pub stats: PlanCacheStats,
    pub total_entries: usize,
    pub avg_hits_per_entry: f64,
    pub avg_age_seconds: f64,
    pub memory_estimate_mb: f64,
    pub is_healthy: bool,
}

#[derive(Debug, Clone)]
pub struct CacheMetadata {
    pub version: u32,
    pub timestamp: u64,
    pub entry_count: usize,
    pub stats: PlanCacheStats,
    pub entries: Vec<CacheEntryMetadata>,
}

#[derive(Debug, Clone)]
pub struct CacheEntryMetadata {
    pub sql_hash: u64,
    pub dialect: u8,
    pub sql: String,
    pub referenced_tables: Vec<String>,
    pub hits: u64,
    pub age_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct CacheDiff {
    pub added_count: usize,
    pub removed_count: usize,
    pub changed_count: usize,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub hit_changes: Vec<(String, u64, u64, i64)>,
}

impl CacheDiff {
    pub fn format_report(&self) -> String {
        let mut report = String::new();
        report.push_str("Cache Diff Report:\n");
        report.push_str(&format!("├─ Added: {} queries\n", self.added_count));
        report.push_str(&format!("├─ Removed: {} queries\n", self.removed_count));
        report.push_str(&format!("└─ Changed: {} queries\n", self.changed_count));

        if !self.added.is_empty() {
            report.push_str("\nAdded queries:\n");
            for sql in self.added.iter().take(5) {
                report.push_str(&format!("  + {}\n", sql));
            }
            if self.added.len() > 5 {
                report.push_str(&format!("  ... and {} more\n", self.added.len() - 5));
            }
        }

        if !self.removed.is_empty() {
            report.push_str("\nRemoved queries:\n");
            for sql in self.removed.iter().take(5) {
                report.push_str(&format!("  - {}\n", sql));
            }
            if self.removed.len() > 5 {
                report.push_str(&format!("  ... and {} more\n", self.removed.len() - 5));
            }
        }

        if !self.hit_changes.is_empty() {
            report.push_str("\nTop hit changes:\n");
            let mut sorted = self.hit_changes.clone();
            sorted.sort_by(|a, b| b.3.abs().cmp(&a.3.abs()));
            for (sql, old, new, delta) in sorted.iter().take(5) {
                let sign = if *delta > 0 { "+" } else { "" };
                report.push_str(&format!(
                    "  {} → {} ({}{}) - {}\n",
                    old, new, sign, delta, sql
                ));
            }
        }

        report
    }
}

impl HealthReport {
    pub fn format_report(&self) -> String {
        format!(
            "Plan Cache Health Report:\n\
             ├─ Status: {}\n\
             ├─ Entries: {}\n\
             ├─ Hit Rate: {:.1}%\n\
             ├─ Avg Hits/Entry: {:.1}\n\
             ├─ Avg Age: {:.1}s\n\
             ├─ Memory: {:.2} MB\n\
             ├─ Total Queries: {}\n\
             ├─ Cache Hits: {}\n\
             ├─ Cache Misses: {}\n\
             ├─ Invalidations: {}\n\
             └─ Effective Speedup: {:.1}x",
            if self.is_healthy {
                "HEALTHY ✓"
            } else {
                "UNHEALTHY ✗"
            },
            self.total_entries,
            self.stats.hit_rate() * 100.0,
            self.avg_hits_per_entry,
            self.avg_age_seconds,
            self.memory_estimate_mb,
            self.stats.total_queries(),
            self.stats.hit_count,
            self.stats.miss_count,
            self.stats.invalidation_count,
            self.stats.effective_speedup()
        )
    }
}

impl Default for PlanCache {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct CacheEntryInfo {
    pub key: PlanCacheKey,
    pub sql: String,
    pub referenced_tables: Vec<String>,
    pub hits: u64,
    pub age_seconds: u64,
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PlanCacheStats {
    pub entry_count: u64,
    pub hit_count: u64,
    pub miss_count: u64,
    pub invalidation_count: u64,
}

impl PlanCacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            0.0
        } else {
            self.hit_count as f64 / total as f64
        }
    }

    pub fn estimated_time_saved_ms(&self) -> f64 {
        const MISS_TIME_MS: f64 = 1.0;
        const HIT_TIME_MS: f64 = 0.05;
        const TIME_SAVED_PER_HIT_MS: f64 = MISS_TIME_MS - HIT_TIME_MS;

        self.hit_count as f64 * TIME_SAVED_PER_HIT_MS
    }

    pub fn total_queries(&self) -> u64 {
        self.hit_count + self.miss_count
    }

    pub fn miss_rate(&self) -> f64 {
        1.0 - self.hit_rate()
    }

    pub fn effective_speedup(&self) -> f64 {
        const SPEEDUP_PER_HIT: f64 = 20.0;
        let hit_rate = self.hit_rate();

        if hit_rate == 0.0 {
            1.0
        } else {
            (hit_rate * SPEEDUP_PER_HIT) + ((1.0 - hit_rate) * 1.0)
        }
    }

    pub fn format_report(&self) -> String {
        format!(
            "Plan Cache Statistics:\n\
             ├─ Entries: {}\n\
             ├─ Total Queries: {}\n\
             ├─ Cache Hits: {} ({:.1}%)\n\
             ├─ Cache Misses: {} ({:.1}%)\n\
             ├─ Invalidations: {}\n\
             ├─ Time Saved: {:.2}ms\n\
             └─ Effective Speedup: {:.1}x",
            self.entry_count,
            self.total_queries(),
            self.hit_count,
            self.hit_rate() * 100.0,
            self.miss_count,
            self.miss_rate() * 100.0,
            self.invalidation_count,
            self.estimated_time_saved_ms(),
            self.effective_speedup()
        )
    }

    #[cfg(feature = "serde")]
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self)
    }

    #[cfg(not(feature = "serde"))]
    pub fn to_json(&self) -> Result<String, &'static str> {
        Ok(format!(
            r#"{{
  "entry_count": {},
  "hit_count": {},
  "miss_count": {},
  "invalidation_count": {},
  "hit_rate": {:.4},
  "miss_rate": {:.4},
  "total_queries": {},
  "estimated_time_saved_ms": {:.2},
  "effective_speedup": {:.2}
}}"#,
            self.entry_count,
            self.hit_count,
            self.miss_count,
            self.invalidation_count,
            self.hit_rate(),
            self.miss_rate(),
            self.total_queries(),
            self.estimated_time_saved_ms(),
            self.effective_speedup()
        ))
    }
}

fn extract_referenced_tables(plan: &PlanNode) -> HashSet<String> {
    let mut tables = HashSet::new();

    match plan {
        PlanNode::Scan { table_name, .. }
        | PlanNode::IndexScan { table_name, .. }
        | PlanNode::Update { table_name, .. }
        | PlanNode::Delete { table_name, .. }
        | PlanNode::Truncate { table_name } => {
            tables.insert(table_name.clone());
        }
        PlanNode::InsertOnConflict { table_name, .. } => {
            tables.insert(table_name.clone());
        }
        PlanNode::Merge { target_table, .. } => {
            tables.insert(target_table.clone());
        }
        PlanNode::AlterTable { table_name, .. } => {
            tables.insert(table_name.clone());
        }
        _ => {}
    }

    for child in plan.children() {
        tables.extend(extract_referenced_tables(child));
    }

    tables
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_creation() {
        let key1 = PlanCacheKey::new(12345, yachtsql_parser::DialectType::PostgreSQL);
        assert_eq!(key1.sql_hash, 12345);
        assert_eq!(key1.dialect, 0);

        let key2 = PlanCacheKey::new(12345, yachtsql_parser::DialectType::BigQuery);
        assert_eq!(key2.dialect, 1);

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_cache_insert_and_get() {
        let mut cache = PlanCache::new();
        let key = PlanCacheKey::new(12345, yachtsql_parser::DialectType::PostgreSQL);

        let plan = PlanNode::Scan {
            table_name: "users".to_string(),
            alias: None,
            projection: None,
            only: false,
        };

        let cached = CachedPlan::new(plan, "SELECT * FROM users".to_string());
        cache.insert(key, cached);

        let result = cache.get(&key);
        assert!(result.is_some());

        let plan_ref = result.unwrap();
        assert_eq!(plan_ref.hits(), 1);
    }

    #[test]
    fn test_cache_miss() {
        let mut cache = PlanCache::new();
        let key = PlanCacheKey::new(99999, yachtsql_parser::DialectType::PostgreSQL);

        let result = cache.get(&key);
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = PlanCache::new();
        let key = PlanCacheKey::new(11111, yachtsql_parser::DialectType::PostgreSQL);

        let plan = PlanNode::Scan {
            table_name: "users".to_string(),
            alias: None,
            projection: None,
            only: false,
        };

        let cached = CachedPlan::new(plan, "SELECT * FROM users".to_string());
        cache.insert(key, cached);

        cache.get(&key);

        let miss_key = PlanCacheKey::new(22222, yachtsql_parser::DialectType::PostgreSQL);
        cache.get(&miss_key);

        let stats = cache.stats();
        assert_eq!(stats.hit_count, 1);
        assert_eq!(stats.miss_count, 1);
        assert_eq!(cache.hit_rate(), 0.5);
    }

    #[test]
    fn test_cache_invalidation() {
        let mut cache = PlanCache::new();
        let key = PlanCacheKey::new(12345, yachtsql_parser::DialectType::PostgreSQL);

        let plan = PlanNode::Scan {
            table_name: "users".to_string(),
            alias: None,
            projection: None,
            only: false,
        };

        let cached = CachedPlan::new(plan, "SELECT * FROM users".to_string());
        cache.insert(key, cached);

        assert!(cache.get(&key).is_some());

        cache.invalidate_all();
        assert!(cache.get(&key).is_none());

        let stats = cache.stats();
        assert_eq!(stats.entry_count, 0);
        assert!(stats.invalidation_count > 0);
    }

    #[test]
    fn test_cached_plan_hit_tracking() {
        let plan = PlanNode::Scan {
            table_name: "users".to_string(),
            alias: None,
            projection: None,
            only: false,
        };

        let cached = CachedPlan::new(plan, "SELECT * FROM users".to_string());

        assert_eq!(cached.hits(), 0);

        cached.record_hit();
        assert_eq!(cached.hits(), 1);

        cached.record_hit();
        cached.record_hit();
        assert_eq!(cached.hits(), 3);
    }

    #[test]
    fn test_stats_time_saved_estimation() {
        let stats = PlanCacheStats {
            entry_count: 100,
            hit_count: 1000,
            miss_count: 10,
            invalidation_count: 5,
        };

        let time_saved = stats.estimated_time_saved_ms();
        assert!(time_saved > 900.0 && time_saved < 1000.0);

        assert_eq!(stats.hit_rate(), 1000.0 / 1010.0);
    }

    #[test]
    fn test_selective_table_invalidation() {
        let mut cache = PlanCache::new();

        let users_key = PlanCacheKey::new(11111, yachtsql_parser::DialectType::PostgreSQL);
        let users_plan = PlanNode::Scan {
            table_name: "users".to_string(),
            alias: None,
            projection: None,
            only: false,
        };
        cache.insert(
            users_key,
            CachedPlan::new(users_plan, "SELECT * FROM users".to_string()),
        );

        let orders_key = PlanCacheKey::new(22222, yachtsql_parser::DialectType::PostgreSQL);
        let orders_plan = PlanNode::Scan {
            table_name: "orders".to_string(),
            alias: None,
            projection: None,
            only: false,
        };
        cache.insert(
            orders_key,
            CachedPlan::new(orders_plan, "SELECT * FROM orders".to_string()),
        );

        assert!(cache.get(&users_key).is_some());
        assert!(cache.get(&orders_key).is_some());

        cache.invalidate_table("", "users");

        assert!(cache.get(&users_key).is_none());
        assert!(cache.get(&orders_key).is_some());

        let stats = cache.stats();
        assert_eq!(stats.invalidation_count, 1);
    }

    #[test]
    fn test_selective_invalidation_with_join() {
        let mut cache = PlanCache::new();

        let join_key = PlanCacheKey::new(33333, yachtsql_parser::DialectType::PostgreSQL);
        let join_plan = PlanNode::Join {
            left: Box::new(PlanNode::Scan {
                table_name: "users".to_string(),
                alias: None,
                projection: None,
                only: false,
            }),
            right: Box::new(PlanNode::Scan {
                table_name: "orders".to_string(),
                alias: None,
                projection: None,
                only: false,
            }),
            on: yachtsql_ir::expr::Expr::Literal(yachtsql_ir::expr::LiteralValue::Boolean(true)),
            join_type: yachtsql_ir::plan::JoinType::Inner,
        };
        cache.insert(
            join_key,
            CachedPlan::new(join_plan, "SELECT * FROM users JOIN orders".to_string()),
        );

        let products_key = PlanCacheKey::new(44444, yachtsql_parser::DialectType::PostgreSQL);
        let products_plan = PlanNode::Scan {
            table_name: "products".to_string(),
            alias: None,
            projection: None,
            only: false,
        };
        cache.insert(
            products_key,
            CachedPlan::new(products_plan, "SELECT * FROM products".to_string()),
        );

        assert!(cache.get(&join_key).is_some());
        assert!(cache.get(&products_key).is_some());

        cache.invalidate_table("", "users");

        assert!(cache.get(&join_key).is_none());
        assert!(cache.get(&products_key).is_some());
    }

    #[test]
    fn test_extract_referenced_tables() {
        let scan_plan = PlanNode::Scan {
            table_name: "users".to_string(),
            alias: None,
            projection: None,
            only: false,
        };
        let tables = extract_referenced_tables(&scan_plan);
        assert_eq!(tables.len(), 1);
        assert!(tables.contains("users"));

        let join_plan = PlanNode::Join {
            left: Box::new(PlanNode::Scan {
                table_name: "users".to_string(),
                alias: None,
                projection: None,
                only: false,
            }),
            right: Box::new(PlanNode::Scan {
                table_name: "orders".to_string(),
                alias: None,
                projection: None,
                only: false,
            }),
            on: yachtsql_ir::expr::Expr::Literal(yachtsql_ir::expr::LiteralValue::Boolean(true)),
            join_type: yachtsql_ir::plan::JoinType::Inner,
        };
        let tables = extract_referenced_tables(&join_plan);
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));

        let complex_plan = PlanNode::Projection {
            expressions: vec![],
            input: Box::new(PlanNode::Filter {
                predicate: yachtsql_ir::expr::Expr::Literal(
                    yachtsql_ir::expr::LiteralValue::Boolean(true),
                ),
                input: Box::new(PlanNode::Scan {
                    table_name: "products".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                }),
            }),
        };
        let tables = extract_referenced_tables(&complex_plan);
        assert_eq!(tables.len(), 1);
        assert!(tables.contains("products"));
    }

    #[test]
    fn test_invalidate_nonexistent_table() {
        let mut cache = PlanCache::new();

        let users_key = PlanCacheKey::new(11111, yachtsql_parser::DialectType::PostgreSQL);
        let users_plan = PlanNode::Scan {
            table_name: "users".to_string(),
            alias: None,
            projection: None,
            only: false,
        };
        cache.insert(
            users_key,
            CachedPlan::new(users_plan, "SELECT * FROM users".to_string()),
        );

        cache.invalidate_table("", "nonexistent");

        assert!(cache.get(&users_key).is_some());
    }

    #[test]
    fn test_get_cache_entries() {
        let mut cache = PlanCache::new();

        let key1 = PlanCacheKey::new(11111, yachtsql_parser::DialectType::PostgreSQL);
        cache.insert(
            key1,
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM users".to_string(),
            ),
        );

        let key2 = PlanCacheKey::new(22222, yachtsql_parser::DialectType::PostgreSQL);
        cache.insert(
            key2,
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "orders".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM orders".to_string(),
            ),
        );

        let entries = cache.get_cache_entries();
        assert_eq!(entries.len(), 2);

        let users_entry = entries.iter().find(|e| e.sql.contains("users")).unwrap();
        assert_eq!(users_entry.referenced_tables.len(), 1);
        assert!(users_entry.referenced_tables.contains(&"users".to_string()));
    }

    #[test]
    fn test_get_plans_for_table() {
        let mut cache = PlanCache::new();

        cache.insert(
            PlanCacheKey::new(1, yachtsql_parser::DialectType::PostgreSQL),
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM users".to_string(),
            ),
        );

        cache.insert(
            PlanCacheKey::new(2, yachtsql_parser::DialectType::PostgreSQL),
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "orders".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM orders".to_string(),
            ),
        );

        cache.insert(
            PlanCacheKey::new(3, yachtsql_parser::DialectType::PostgreSQL),
            CachedPlan::new(
                PlanNode::Join {
                    left: Box::new(PlanNode::Scan {
                        table_name: "users".to_string(),
                        alias: None,
                        projection: None,
                        only: false,
                    }),
                    right: Box::new(PlanNode::Scan {
                        table_name: "orders".to_string(),
                        alias: None,
                        projection: None,
                        only: false,
                    }),
                    on: yachtsql_ir::expr::Expr::Literal(yachtsql_ir::expr::LiteralValue::Boolean(
                        true,
                    )),
                    join_type: yachtsql_ir::plan::JoinType::Inner,
                },
                "SELECT * FROM users JOIN orders".to_string(),
            ),
        );

        let users_plans = cache.get_plans_for_table("users");
        assert_eq!(users_plans.len(), 2);

        let orders_plans = cache.get_plans_for_table("orders");
        assert_eq!(orders_plans.len(), 2);

        let products_plans = cache.get_plans_for_table("products");
        assert_eq!(products_plans.len(), 0);
    }

    #[test]
    fn test_hot_queries() {
        let mut cache = PlanCache::new();

        let k1 = PlanCacheKey::new(1, yachtsql_parser::DialectType::PostgreSQL);
        let k2 = PlanCacheKey::new(2, yachtsql_parser::DialectType::PostgreSQL);
        let k3 = PlanCacheKey::new(3, yachtsql_parser::DialectType::PostgreSQL);

        cache.insert(
            k1,
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM users".to_string(),
            ),
        );
        cache.insert(
            k2,
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "orders".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM orders".to_string(),
            ),
        );
        cache.insert(
            k3,
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "products".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM products".to_string(),
            ),
        );

        cache.get(&k1);
        cache.get(&k2);
        cache.get(&k2);
        cache.get(&k3);
        cache.get(&k3);
        cache.get(&k3);

        let hot = cache.get_hot_queries(2);
        assert_eq!(hot.len(), 2);
        assert_eq!(hot[0].hits, 3);
        assert_eq!(hot[1].hits, 2);
    }

    #[test]
    fn test_health_report() {
        let mut cache = PlanCache::new();

        cache.insert(
            PlanCacheKey::new(1, yachtsql_parser::DialectType::PostgreSQL),
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM users".to_string(),
            ),
        );

        cache.get(&PlanCacheKey::new(
            1,
            yachtsql_parser::DialectType::PostgreSQL,
        ));
        cache.get(&PlanCacheKey::new(
            1,
            yachtsql_parser::DialectType::PostgreSQL,
        ));
        cache.get(&PlanCacheKey::new(
            999,
            yachtsql_parser::DialectType::PostgreSQL,
        ));

        let report = cache.health_report();
        assert_eq!(report.total_entries, 1);
        assert_eq!(report.stats.hit_count, 2);
        assert_eq!(report.stats.miss_count, 1);
        assert!(report.is_healthy);
    }

    #[test]
    fn test_export_metadata() {
        let mut cache = PlanCache::new();

        cache.insert(
            PlanCacheKey::new(1, yachtsql_parser::DialectType::PostgreSQL),
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM users".to_string(),
            ),
        );

        cache.insert(
            PlanCacheKey::new(2, yachtsql_parser::DialectType::PostgreSQL),
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "orders".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM orders".to_string(),
            ),
        );

        let metadata = cache.export_metadata();
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.entry_count, 2);
        assert_eq!(metadata.entries.len(), 2);
    }

    #[test]
    fn test_cache_diff() {
        let mut cache = PlanCache::new();

        cache.insert(
            PlanCacheKey::new(1, yachtsql_parser::DialectType::PostgreSQL),
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM users".to_string(),
            ),
        );

        cache.insert(
            PlanCacheKey::new(2, yachtsql_parser::DialectType::PostgreSQL),
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "orders".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM orders".to_string(),
            ),
        );

        let snapshot1 = cache.export_metadata();

        cache.invalidate_table("", "users");

        cache.insert(
            PlanCacheKey::new(3, yachtsql_parser::DialectType::PostgreSQL),
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "products".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM products".to_string(),
            ),
        );

        for _ in 0..5 {
            cache.get(&PlanCacheKey::new(
                2,
                yachtsql_parser::DialectType::PostgreSQL,
            ));
        }

        let diff = cache.diff(&snapshot1);
        assert_eq!(diff.added_count, 1);
        assert_eq!(diff.removed_count, 1);
        assert_eq!(diff.changed_count, 1);
    }

    #[test]
    fn test_diff_format() {
        let mut cache = PlanCache::new();

        cache.insert(
            PlanCacheKey::new(1, yachtsql_parser::DialectType::PostgreSQL),
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM users".to_string(),
            ),
        );

        let snapshot = cache.export_metadata();

        cache.insert(
            PlanCacheKey::new(2, yachtsql_parser::DialectType::PostgreSQL),
            CachedPlan::new(
                PlanNode::Scan {
                    table_name: "orders".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                },
                "SELECT * FROM orders".to_string(),
            ),
        );

        let diff = cache.diff(&snapshot);
        let report = diff.format_report();

        assert!(report.contains("Added: 1"));
        assert!(report.contains("SELECT * FROM orders"));
    }

    #[test]
    fn test_local_cache_basic() {
        let mut local = PlanCache::new();
        let key = PlanCacheKey::new(1, yachtsql_parser::DialectType::PostgreSQL);

        let plan = PlanNode::Scan {
            table_name: "users".to_string(),
            alias: None,
            projection: None,
            only: false,
        };

        local.insert(
            key,
            CachedPlan::new(plan, "SELECT * FROM users".to_string()),
        );

        let result = local.get(&key);
        assert!(result.is_some());

        let stats = local.stats();
        assert_eq!(stats.entry_count, 1);
        assert_eq!(stats.hit_count, 1);
        assert_eq!(stats.miss_count, 0);
    }
}
