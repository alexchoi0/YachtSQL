use std::cell::Cell;
use std::rc::Rc;
use std::time::Instant;

use debug_print::debug_eprintln;
use yachtsql_common::error::{Error, Result};

use super::memory_pool::MemoryPool;
use super::priority::QueryPriority;
use super::query_registry::{QueryRegistry, QueryStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MemoryPressure {
    Normal,
    Moderate,
    High,
    Critical,
}

impl MemoryPressure {
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryPressure::Normal => "normal",
            MemoryPressure::Moderate => "moderate",
            MemoryPressure::High => "high",
            MemoryPressure::Critical => "critical",
        }
    }

    pub fn from_usage_percent(percent: f64) -> Self {
        if percent >= 0.95 {
            MemoryPressure::Critical
        } else if percent >= 0.85 {
            MemoryPressure::High
        } else if percent >= 0.70 {
            MemoryPressure::Moderate
        } else {
            MemoryPressure::Normal
        }
    }

    pub fn should_queue_queries(&self) -> bool {
        matches!(self, MemoryPressure::High | MemoryPressure::Critical)
    }

    pub fn should_cancel_low_priority(&self) -> bool {
        matches!(self, MemoryPressure::Critical)
    }
}

#[derive(Debug, Clone)]
pub struct MemoryManagerConfig {
    pub enable_queuing: bool,
    pub enable_auto_cancel: bool,
    pub enable_adaptive_sizing: bool,
    pub pressure_check_interval_secs: u64,
    pub max_queued_queries: usize,
    pub min_pool_size_bytes: usize,
    pub max_pool_size_bytes: usize,
}

impl Default for MemoryManagerConfig {
    fn default() -> Self {
        Self {
            enable_queuing: true,
            enable_auto_cancel: false,
            enable_adaptive_sizing: false,
            pressure_check_interval_secs: 5,
            max_queued_queries: 100,
            min_pool_size_bytes: 1024 * 1024 * 1024,
            max_pool_size_bytes: 16 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug)]
struct MemoryManagerInner {
    current_pressure: Cell<MemoryPressure>,
    last_pressure_check: Cell<Instant>,
    running: Cell<bool>,
    queries_queued: Cell<u64>,
    queries_cancelled: Cell<u64>,
    peak_pressure: Cell<MemoryPressure>,
    time_in_normal: Cell<u64>,
    time_in_moderate: Cell<u64>,
    time_in_high: Cell<u64>,
    time_in_critical: Cell<u64>,
}

pub struct MemoryManager {
    pool: Rc<MemoryPool>,
    registry: Option<Rc<QueryRegistry>>,
    config: MemoryManagerConfig,
    inner: Rc<MemoryManagerInner>,
}

impl MemoryManager {
    pub fn new(
        pool: Rc<MemoryPool>,
        registry: Option<Rc<QueryRegistry>>,
        config: MemoryManagerConfig,
    ) -> Self {
        Self {
            pool,
            registry,
            config,
            inner: Rc::new(MemoryManagerInner {
                current_pressure: Cell::new(MemoryPressure::Normal),
                last_pressure_check: Cell::new(Instant::now()),
                running: Cell::new(false),
                queries_queued: Cell::new(0),
                queries_cancelled: Cell::new(0),
                peak_pressure: Cell::new(MemoryPressure::Normal),
                time_in_normal: Cell::new(0),
                time_in_moderate: Cell::new(0),
                time_in_high: Cell::new(0),
                time_in_critical: Cell::new(0),
            }),
        }
    }

    pub fn current_pressure(&self) -> MemoryPressure {
        self.inner.current_pressure.get()
    }

    pub fn check_pressure(&self) -> MemoryPressure {
        let pool_stats = self.pool.stats();
        let pressure = MemoryPressure::from_usage_percent(pool_stats.utilization);

        self.inner.current_pressure.set(pressure);

        let peak = self.inner.peak_pressure.get();
        if pressure > peak {
            self.inner.peak_pressure.set(pressure);
        }

        self.inner.last_pressure_check.set(Instant::now());

        pressure
    }

    pub fn handle_pressure(&self, pressure: MemoryPressure) -> Result<()> {
        match pressure {
            MemoryPressure::Normal | MemoryPressure::Moderate => Ok(()),
            MemoryPressure::High => {
                if self.config.enable_queuing {
                    debug_eprintln!(
                        "[executor::memory_manager] Memory pressure HIGH: would queue new queries"
                    );
                }
                Ok(())
            }
            MemoryPressure::Critical => {
                if self.config.enable_auto_cancel {
                    if let Some(registry) = &self.registry {
                        let cancelled = registry.cancel_all(|q| {
                            q.status == QueryStatus::Running && q.priority == QueryPriority::Low
                        });

                        let current = self.inner.queries_cancelled.get();
                        self.inner.queries_cancelled.set(current + cancelled as u64);

                        debug_eprintln!(
                            "[executor::memory_manager] Memory pressure CRITICAL: cancelled {} low-priority queries",
                            cancelled
                        );
                    }
                }
                Ok(())
            }
        }
    }

    pub fn start_monitoring(&self) {
        self.inner.running.set(true);
    }

    pub fn stop_monitoring(&self) {
        self.inner.running.set(false);
    }

    pub fn stats(&self) -> MemoryManagerStatsSnapshot {
        MemoryManagerStatsSnapshot {
            queries_queued: self.inner.queries_queued.get(),
            queries_cancelled: self.inner.queries_cancelled.get(),
            current_pressure: self.current_pressure(),
            peak_pressure: self.inner.peak_pressure.get(),
            time_in_normal_secs: self.inner.time_in_normal.get(),
            time_in_moderate_secs: self.inner.time_in_moderate.get(),
            time_in_high_secs: self.inner.time_in_high.get(),
            time_in_critical_secs: self.inner.time_in_critical.get(),
        }
    }

    pub fn pool(&self) -> &Rc<MemoryPool> {
        &self.pool
    }

    pub fn suggest_pool_adjustment(&self) -> Option<usize> {
        if !self.config.enable_adaptive_sizing {
            return None;
        }

        let pool_stats = self.pool.stats();
        let peak_usage_percent = pool_stats.peak_allocated as f64 / pool_stats.total_budget as f64;

        if peak_usage_percent > 0.90 {
            let suggested = (pool_stats.total_budget as f64 * 1.5) as usize;
            if suggested <= self.config.max_pool_size_bytes {
                return Some(suggested.min(self.config.max_pool_size_bytes));
            }
        }

        if peak_usage_percent < 0.40 && pool_stats.total_budget > self.config.min_pool_size_bytes {
            let suggested = (pool_stats.total_budget as f64 * 0.75) as usize;
            return Some(suggested.max(self.config.min_pool_size_bytes));
        }

        None
    }
}

#[derive(Debug, Clone)]
pub struct MemoryManagerStatsSnapshot {
    pub queries_queued: u64,
    pub queries_cancelled: u64,
    pub current_pressure: MemoryPressure,
    pub peak_pressure: MemoryPressure,
    pub time_in_normal_secs: u64,
    pub time_in_moderate_secs: u64,
    pub time_in_high_secs: u64,
    pub time_in_critical_secs: u64,
}

impl MemoryManagerStatsSnapshot {
    pub fn format(&self) -> String {
        format!(
            "Memory Manager Stats:\n\
             Current Pressure: {} | Peak: {}\n\
             Queries Queued: {} | Cancelled: {}\n\
             Time Distribution: {}s normal, {}s moderate, {}s high, {}s critical",
            self.current_pressure.as_str(),
            self.peak_pressure.as_str(),
            self.queries_queued,
            self.queries_cancelled,
            self.time_in_normal_secs,
            self.time_in_moderate_secs,
            self.time_in_high_secs,
            self.time_in_critical_secs
        )
    }
}

#[derive(Debug)]
struct QueryQueueInner {
    total_queued: Cell<u64>,
    total_dequeued: Cell<u64>,
    total_rejected: Cell<u64>,
}

pub struct QueryQueue {
    queue: std::rc::Rc<std::cell::RefCell<Vec<QueuedQuery>>>,
    max_size: usize,
    inner: Rc<QueryQueueInner>,
}

#[derive(Debug, Clone)]
struct QueuedQuery {
    query_id: u64,
    priority: QueryPriority,
    queued_at: Instant,
    estimated_memory_bytes: usize,
}

impl QueryQueue {
    pub fn new(max_size: usize) -> Self {
        Self {
            queue: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            max_size,
            inner: Rc::new(QueryQueueInner {
                total_queued: Cell::new(0),
                total_dequeued: Cell::new(0),
                total_rejected: Cell::new(0),
            }),
        }
    }

    pub fn enqueue(
        &self,
        query_id: u64,
        priority: QueryPriority,
        estimated_memory_bytes: usize,
    ) -> Result<()> {
        let mut queue = self.queue.borrow_mut();

        if queue.len() >= self.max_size {
            let rejected = self.inner.total_rejected.get();
            self.inner.total_rejected.set(rejected + 1);
            return Err(Error::ExecutionError(format!(
                "Query queue full ({} queries waiting)",
                queue.len()
            )));
        }

        let entry = QueuedQuery {
            query_id,
            priority,
            queued_at: Instant::now(),
            estimated_memory_bytes,
        };

        queue.push(entry);

        queue.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.queued_at.cmp(&b.queued_at))
        });

        let queued = self.inner.total_queued.get();
        self.inner.total_queued.set(queued + 1);

        Ok(())
    }

    pub fn try_dequeue(&self, available_memory_bytes: usize) -> Option<(u64, usize)> {
        let mut queue = self.queue.try_borrow_mut().ok()?;

        if let Some(pos) = queue
            .iter()
            .position(|q| q.estimated_memory_bytes <= available_memory_bytes)
        {
            let entry = queue.remove(pos);
            let dequeued = self.inner.total_dequeued.get();
            self.inner.total_dequeued.set(dequeued + 1);
            return Some((entry.query_id, entry.estimated_memory_bytes));
        }

        None
    }

    pub fn len(&self) -> usize {
        self.queue.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.borrow().is_empty()
    }

    pub fn stats(&self) -> QueryQueueStats {
        QueryQueueStats {
            current_size: self.len(),
            max_size: self.max_size,
            total_queued: self.inner.total_queued.get(),
            total_dequeued: self.inner.total_dequeued.get(),
            total_rejected: self.inner.total_rejected.get(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryQueueStats {
    pub current_size: usize,
    pub max_size: usize,
    pub total_queued: u64,
    pub total_dequeued: u64,
    pub total_rejected: u64,
}

impl QueryQueueStats {
    pub fn format(&self) -> String {
        format!(
            "Queue: {}/{} | Total: {} queued, {} dequeued, {} rejected",
            self.current_size,
            self.max_size,
            self.total_queued,
            self.total_dequeued,
            self.total_rejected
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pressure_levels() {
        assert_eq!(
            MemoryPressure::from_usage_percent(0.5),
            MemoryPressure::Normal
        );
        assert_eq!(
            MemoryPressure::from_usage_percent(0.75),
            MemoryPressure::Moderate
        );
        assert_eq!(
            MemoryPressure::from_usage_percent(0.90),
            MemoryPressure::High
        );
        assert_eq!(
            MemoryPressure::from_usage_percent(0.98),
            MemoryPressure::Critical
        );
    }

    #[test]
    fn test_pressure_should_queue() {
        assert!(!MemoryPressure::Normal.should_queue_queries());
        assert!(!MemoryPressure::Moderate.should_queue_queries());
        assert!(MemoryPressure::High.should_queue_queries());
        assert!(MemoryPressure::Critical.should_queue_queries());
    }

    #[test]
    fn test_memory_manager_creation() {
        let pool = Rc::new(MemoryPool::new(1024 * 1024 * 1024));
        let config = MemoryManagerConfig::default();
        let manager = MemoryManager::new(pool, None, config);

        assert_eq!(manager.current_pressure(), MemoryPressure::Normal);
    }

    #[test]
    fn test_pressure_detection() {
        let pool = Rc::new(MemoryPool::new(1000));
        let config = MemoryManagerConfig::default();
        let manager = MemoryManager::new(pool.clone(), None, config);

        let pressure = manager.check_pressure();
        assert_eq!(pressure, MemoryPressure::Normal);

        let _h1 = pool.try_allocate(750).unwrap();
        let pressure = manager.check_pressure();
        assert_eq!(pressure, MemoryPressure::Moderate);

        let _h2 = pool.try_allocate(150).unwrap();
        let pressure = manager.check_pressure();
        assert_eq!(pressure, MemoryPressure::High);
    }

    #[test]
    fn test_query_queue_basic() {
        let queue = QueryQueue::new(10);

        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);

        queue.enqueue(1, QueryPriority::Normal, 1000).unwrap();
        assert_eq!(queue.len(), 1);

        let result = queue.try_dequeue(2000);
        assert!(result.is_some());
        let (id, mem) = result.unwrap();
        assert_eq!(id, 1);
        assert_eq!(mem, 1000);

        assert!(queue.is_empty());
    }

    #[test]
    fn test_query_queue_priority() {
        let queue = QueryQueue::new(10);

        queue.enqueue(1, QueryPriority::Low, 1000).unwrap();
        queue.enqueue(2, QueryPriority::High, 1000).unwrap();
        queue.enqueue(3, QueryPriority::Normal, 1000).unwrap();
        queue.enqueue(4, QueryPriority::Critical, 1000).unwrap();

        assert_eq!(queue.try_dequeue(5000).unwrap().0, 4);
        assert_eq!(queue.try_dequeue(5000).unwrap().0, 2);
        assert_eq!(queue.try_dequeue(5000).unwrap().0, 3);
        assert_eq!(queue.try_dequeue(5000).unwrap().0, 1);
    }

    #[test]
    fn test_query_queue_memory_fit() {
        let queue = QueryQueue::new(10);

        queue.enqueue(1, QueryPriority::Normal, 1000).unwrap();
        queue.enqueue(2, QueryPriority::Normal, 500).unwrap();

        let result = queue.try_dequeue(600);
        assert!(result.is_some());
        assert_eq!(result.unwrap().0, 2);

        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_query_queue_full() {
        let queue = QueryQueue::new(2);

        queue.enqueue(1, QueryPriority::Normal, 1000).unwrap();
        queue.enqueue(2, QueryPriority::Normal, 1000).unwrap();

        let result = queue.enqueue(3, QueryPriority::Normal, 1000);
        assert!(result.is_err());

        let stats = queue.stats();
        assert_eq!(stats.total_rejected, 1);
    }

    #[test]
    fn test_adaptive_sizing_suggestion() {
        let pool = Rc::new(MemoryPool::new(1000));
        let config = MemoryManagerConfig {
            enable_adaptive_sizing: true,
            min_pool_size_bytes: 500,
            max_pool_size_bytes: 5000,
            ..Default::default()
        };

        let manager = MemoryManager::new(pool.clone(), None, config);

        let _h = pool.try_allocate(950).unwrap();

        let suggestion = manager.suggest_pool_adjustment();
        assert!(suggestion.is_some());
        let new_size = suggestion.unwrap();
        assert!(new_size > 1000);
        assert!(new_size <= 5000);
    }
}
