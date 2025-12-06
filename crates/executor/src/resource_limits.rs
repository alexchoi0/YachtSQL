pub mod memory_manager;
pub mod memory_pool;
pub mod monitoring;
pub mod priority;
pub mod query_registry;

use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use debug_print::debug_eprintln;
pub use memory_manager::{
    MemoryManager, MemoryManagerConfig, MemoryManagerStatsSnapshot, MemoryPressure, QueryQueue,
    QueryQueueStats,
};
pub use memory_pool::{
    AllocationHandle, MemoryPool, PoolStats, TieredAllocationHandle, TieredMemoryPool,
    TieredPoolStats,
};
pub use monitoring::{ResourceMonitor, ResourceReport, SystemHealth};
pub use priority::{PriorityConfig, QueryPriority};
pub use query_registry::{QueryId, QueryInfo, QueryRegistry, QueryStatus, RegistryStats};
use yachtsql_core::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct ResourceLimitsConfig {
    pub max_memory_bytes: Option<usize>,

    pub timeout: Option<Duration>,

    pub enable_memory_tracking: bool,
}

impl Default for ResourceLimitsConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: Some(1024 * 1024 * 1024),
            timeout: Some(Duration::from_secs(60)),
            enable_memory_tracking: true,
        }
    }
}

impl ResourceLimitsConfig {
    pub fn unlimited() -> Self {
        Self {
            max_memory_bytes: None,
            timeout: None,
            enable_memory_tracking: false,
        }
    }

    pub fn new(max_memory_mb: usize, timeout_seconds: u64) -> Self {
        Self {
            max_memory_bytes: Some(max_memory_mb * 1024 * 1024),
            timeout: Some(Duration::from_secs(timeout_seconds)),
            enable_memory_tracking: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CancellationToken {
    cancelled: Rc<Cell<bool>>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            cancelled: Rc::new(Cell::new(false)),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.set(true);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.get()
    }

    pub fn reset(&self) {
        self.cancelled.set(false);
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct ResourceTrackerInner {
    memory_bytes: Cell<usize>,
    peak_memory_bytes: Cell<usize>,
    batch_count: Cell<usize>,
    row_count: Cell<usize>,
}

#[derive(Debug)]
pub struct ResourceTracker {
    config: ResourceLimitsConfig,

    inner: Rc<ResourceTrackerInner>,

    start_time: Instant,

    cancellation_token: CancellationToken,

    memory_pool: Option<Rc<MemoryPool>>,

    pool_allocations: std::rc::Rc<std::cell::RefCell<Vec<AllocationHandle>>>,

    query_registry: Option<Rc<QueryRegistry>>,

    query_id: Option<QueryId>,
}

impl ResourceTracker {
    pub fn new(config: ResourceLimitsConfig) -> Self {
        Self {
            config,
            inner: Rc::new(ResourceTrackerInner {
                memory_bytes: Cell::new(0),
                peak_memory_bytes: Cell::new(0),
                batch_count: Cell::new(0),
                row_count: Cell::new(0),
            }),
            start_time: Instant::now(),
            cancellation_token: CancellationToken::new(),
            memory_pool: None,
            pool_allocations: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            query_registry: None,
            query_id: None,
        }
    }

    pub fn with_pool(config: ResourceLimitsConfig, pool: Rc<MemoryPool>) -> Self {
        Self {
            config,
            inner: Rc::new(ResourceTrackerInner {
                memory_bytes: Cell::new(0),
                peak_memory_bytes: Cell::new(0),
                batch_count: Cell::new(0),
                row_count: Cell::new(0),
            }),
            start_time: Instant::now(),
            cancellation_token: CancellationToken::new(),
            memory_pool: Some(pool),
            pool_allocations: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            query_registry: None,
            query_id: None,
        }
    }

    pub fn with_registry(
        config: ResourceLimitsConfig,
        registry: Rc<QueryRegistry>,
        sql: &str,
        priority: QueryPriority,
    ) -> Result<Self> {
        let (query_id, cancellation_token) = registry.register(sql, priority)?;

        Ok(Self {
            config,
            inner: Rc::new(ResourceTrackerInner {
                memory_bytes: Cell::new(0),
                peak_memory_bytes: Cell::new(0),
                batch_count: Cell::new(0),
                row_count: Cell::new(0),
            }),
            start_time: Instant::now(),
            cancellation_token,
            memory_pool: None,
            pool_allocations: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            query_registry: Some(registry),
            query_id: Some(query_id),
        })
    }

    pub fn with_pool_and_registry(
        config: ResourceLimitsConfig,
        pool: Rc<MemoryPool>,
        registry: Rc<QueryRegistry>,
        sql: &str,
        priority: QueryPriority,
    ) -> Result<Self> {
        let (query_id, cancellation_token) = registry.register(sql, priority)?;

        Ok(Self {
            config,
            inner: Rc::new(ResourceTrackerInner {
                memory_bytes: Cell::new(0),
                peak_memory_bytes: Cell::new(0),
                batch_count: Cell::new(0),
                row_count: Cell::new(0),
            }),
            start_time: Instant::now(),
            cancellation_token,
            memory_pool: Some(pool),
            pool_allocations: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            query_registry: Some(registry),
            query_id: Some(query_id),
        })
    }

    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    pub fn query_id(&self) -> Option<QueryId> {
        self.query_id
    }

    pub fn mark_started(&self) -> Result<()> {
        if let (Some(registry), Some(id)) = (&self.query_registry, self.query_id) {
            registry.mark_started(id)?;
        }
        Ok(())
    }

    pub fn mark_completed(&self) -> Result<()> {
        if let (Some(registry), Some(id)) = (&self.query_registry, self.query_id) {
            registry.mark_completed(id)?;
        }
        Ok(())
    }

    pub fn mark_failed(&self) -> Result<()> {
        if let (Some(registry), Some(id)) = (&self.query_registry, self.query_id) {
            registry.mark_failed(id)?;
        }
        Ok(())
    }

    fn update_registry_stats(&self) {
        if let (Some(registry), Some(id)) = (&self.query_registry, self.query_id) {
            let _ = registry.update_stats(
                id,
                self.memory_bytes(),
                self.peak_memory_bytes(),
                self.inner.row_count.get(),
            );
        }
    }

    pub fn check_cancellation(&self) -> Result<()> {
        if self.cancellation_token.is_cancelled() {
            return Err(Error::ExecutionError(
                "Query was cancelled by user".to_string(),
            ));
        }
        Ok(())
    }

    pub fn check_limits(&self) -> Result<()> {
        self.check_cancellation()?;
        self.check_timeout()?;
        Ok(())
    }

    pub fn allocate(&self, bytes: usize) -> Result<()> {
        if !self.config.enable_memory_tracking {
            return Ok(());
        }

        if let Some(pool) = &self.memory_pool {
            let handle = pool.try_allocate(bytes)?;

            self.pool_allocations.borrow_mut().push(handle);

            let current = self.inner.memory_bytes.get();
            let new_usage = current + bytes;
            self.inner.memory_bytes.set(new_usage);

            if new_usage > self.inner.peak_memory_bytes.get() {
                self.inner.peak_memory_bytes.set(new_usage);
            }

            return Ok(());
        }

        let current = self.inner.memory_bytes.get();
        let new_usage = current + bytes;
        self.inner.memory_bytes.set(new_usage);

        if new_usage > self.inner.peak_memory_bytes.get() {
            self.inner.peak_memory_bytes.set(new_usage);
        }

        if let Some(limit) = self.config.max_memory_bytes {
            if new_usage > limit {
                self.inner.memory_bytes.set(current);

                return Err(Error::ExecutionError(format!(
                    "Query exceeded memory limit: {} MB used (limit: {} MB)",
                    new_usage / (1024 * 1024),
                    limit / (1024 * 1024)
                )));
            }
        }

        Ok(())
    }

    pub fn deallocate(&self, bytes: usize) {
        if !self.config.enable_memory_tracking {
            return;
        }

        let current = self.inner.memory_bytes.get();
        self.inner.memory_bytes.set(current.saturating_sub(bytes));
    }

    pub fn check_timeout(&self) -> Result<()> {
        if let Some(timeout) = self.config.timeout {
            let elapsed = self.start_time.elapsed();
            if elapsed > timeout {
                return Err(Error::ExecutionError(format!(
                    "Query timeout: exceeded {} seconds (elapsed: {:.2}s)",
                    timeout.as_secs(),
                    elapsed.as_secs_f64()
                )));
            }
        }
        Ok(())
    }

    pub fn record_batch(&self, num_rows: usize) {
        let batch = self.inner.batch_count.get();
        self.inner.batch_count.set(batch + 1);

        let rows = self.inner.row_count.get();
        self.inner.row_count.set(rows + num_rows);

        if (batch + 1) % 10 == 0 {
            self.update_registry_stats();
        }
    }

    pub fn memory_bytes(&self) -> usize {
        self.inner.memory_bytes.get()
    }

    pub fn peak_memory_bytes(&self) -> usize {
        self.inner.peak_memory_bytes.get()
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn stats(&self) -> ResourceStats {
        ResourceStats {
            memory_bytes: self.memory_bytes(),
            peak_memory_bytes: self.peak_memory_bytes(),
            elapsed: self.elapsed(),
            batch_count: self.inner.batch_count.get(),
            row_count: self.inner.row_count.get(),
        }
    }
}

impl Drop for ResourceTracker {
    fn drop(&mut self) {
        self.update_registry_stats();

        if self.config.enable_memory_tracking {
            let stats = self.stats();
            debug_eprintln!(
                "[executor::resource_limits] Query completed: {:.2}s, {} rows, peak memory {} MB",
                stats.elapsed.as_secs_f64(),
                stats.row_count,
                stats.peak_memory_bytes / (1024 * 1024)
            );
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResourceStats {
    pub memory_bytes: usize,

    pub peak_memory_bytes: usize,

    pub elapsed: Duration,

    pub batch_count: usize,

    pub row_count: usize,
}

impl ResourceStats {
    pub fn format(&self) -> String {
        format!(
            "Memory: {:.2} MB (peak: {:.2} MB) | Time: {:.2}s | Rows: {} ({} batches)",
            self.memory_bytes as f64 / (1024.0 * 1024.0),
            self.peak_memory_bytes as f64 / (1024.0 * 1024.0),
            self.elapsed.as_secs_f64(),
            self.row_count,
            self.batch_count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_tracking() {
        let config = ResourceLimitsConfig::new(10, 60);
        let tracker = ResourceTracker::new(config);

        assert!(tracker.allocate(5 * 1024 * 1024).is_ok());
        assert_eq!(tracker.memory_bytes(), 5 * 1024 * 1024);

        assert!(tracker.allocate(3 * 1024 * 1024).is_ok());
        assert_eq!(tracker.memory_bytes(), 8 * 1024 * 1024);

        assert!(tracker.allocate(5 * 1024 * 1024).is_err());
        assert_eq!(tracker.memory_bytes(), 8 * 1024 * 1024);

        tracker.deallocate(3 * 1024 * 1024);
        assert_eq!(tracker.memory_bytes(), 5 * 1024 * 1024);

        assert!(tracker.allocate(5 * 1024 * 1024).is_ok());
        assert_eq!(tracker.memory_bytes(), 10 * 1024 * 1024);
    }

    #[test]
    fn test_peak_memory() {
        let config = ResourceLimitsConfig::new(100, 60);
        let tracker = ResourceTracker::new(config);

        tracker.allocate(10 * 1024 * 1024).unwrap();
        assert_eq!(tracker.peak_memory_bytes(), 10 * 1024 * 1024);

        tracker.allocate(20 * 1024 * 1024).unwrap();
        assert_eq!(tracker.peak_memory_bytes(), 30 * 1024 * 1024);

        tracker.deallocate(15 * 1024 * 1024);
        assert_eq!(tracker.memory_bytes(), 15 * 1024 * 1024);

        assert_eq!(tracker.peak_memory_bytes(), 30 * 1024 * 1024);
    }

    #[test]
    fn test_timeout_check() {
        let config = ResourceLimitsConfig {
            max_memory_bytes: None,
            timeout: Some(Duration::from_millis(100)),
            enable_memory_tracking: false,
        };
        let tracker = ResourceTracker::new(config);

        assert!(tracker.check_timeout().is_ok());

        std::thread::sleep(Duration::from_millis(150));

        assert!(tracker.check_timeout().is_err());
    }

    #[test]
    fn test_batch_tracking() {
        let config = ResourceLimitsConfig::unlimited();
        let tracker = ResourceTracker::new(config);

        tracker.record_batch(100);
        tracker.record_batch(200);
        tracker.record_batch(300);

        let stats = tracker.stats();
        assert_eq!(stats.batch_count, 3);
        assert_eq!(stats.row_count, 600);
    }

    #[test]
    fn test_unlimited_config() {
        let config = ResourceLimitsConfig::unlimited();
        let tracker = ResourceTracker::new(config);

        assert!(tracker.allocate(1_000_000_000).is_ok());
        assert!(tracker.allocate(1_000_000_000).is_ok());

        std::thread::sleep(Duration::from_millis(10));
        assert!(tracker.check_timeout().is_ok());
    }

    #[test]
    fn test_cancellation_token() {
        let token = CancellationToken::new();

        assert!(!token.is_cancelled());

        token.cancel();
        assert!(token.is_cancelled());

        token.reset();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn test_cancellation_check() {
        let config = ResourceLimitsConfig::unlimited();
        let tracker = ResourceTracker::new(config);

        assert!(tracker.check_cancellation().is_ok());
        assert!(tracker.check_limits().is_ok());

        tracker.cancellation_token().cancel();

        assert!(tracker.check_cancellation().is_err());
        assert!(tracker.check_limits().is_err());
    }

    #[test]
    fn test_cancellation_token_clone() {
        let token = CancellationToken::new();
        let clone = token.clone();

        token.cancel();

        assert!(clone.is_cancelled());
    }
}
