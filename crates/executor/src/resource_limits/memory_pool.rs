use std::cell::Cell;
use std::rc::Rc;

use yachtsql_common::error::{Error, Result};

use super::priority::{PriorityConfig, QueryPriority};

#[derive(Debug)]
pub struct AllocationHandle {
    bytes: usize,

    pool: Rc<MemoryPoolInner>,
}

impl Drop for AllocationHandle {
    fn drop(&mut self) {
        self.pool.deallocate(self.bytes);
    }
}

#[derive(Debug)]
struct MemoryPoolInner {
    total_budget: usize,
    allocated: Cell<usize>,
    peak_allocated: Cell<usize>,
    total_allocations: Cell<u64>,
    successful_allocations: Cell<u64>,
    failed_allocations: Cell<u64>,
    total_deallocations: Cell<u64>,
}

impl MemoryPoolInner {
    fn deallocate(&self, bytes: usize) {
        self.allocated.set(self.allocated.get() - bytes);
        self.total_deallocations
            .set(self.total_deallocations.get() + 1);
    }
}

#[derive(Debug, Clone)]
pub struct MemoryPool {
    inner: Rc<MemoryPoolInner>,
}

impl MemoryPool {
    pub fn new(total_budget_bytes: usize) -> Self {
        Self {
            inner: Rc::new(MemoryPoolInner {
                total_budget: total_budget_bytes,
                allocated: Cell::new(0),
                peak_allocated: Cell::new(0),
                total_allocations: Cell::new(0),
                successful_allocations: Cell::new(0),
                failed_allocations: Cell::new(0),
                total_deallocations: Cell::new(0),
            }),
        }
    }

    pub fn unlimited() -> Self {
        Self::new(usize::MAX)
    }

    pub fn try_allocate(&self, bytes: usize) -> Result<AllocationHandle> {
        self.inner
            .total_allocations
            .set(self.inner.total_allocations.get() + 1);

        let current = self.inner.allocated.get();
        let new_allocated = current + bytes;

        if new_allocated > self.inner.total_budget {
            self.inner
                .failed_allocations
                .set(self.inner.failed_allocations.get() + 1);

            return Err(Error::ExecutionError(format!(
                "Memory pool exhausted: {} MB requested, {} MB available (total budget: {} MB, currently allocated: {} MB)",
                bytes / (1024 * 1024),
                self.available() / (1024 * 1024),
                self.inner.total_budget / (1024 * 1024),
                current / (1024 * 1024)
            )));
        }

        self.inner.allocated.set(new_allocated);
        self.inner
            .successful_allocations
            .set(self.inner.successful_allocations.get() + 1);

        if new_allocated > self.inner.peak_allocated.get() {
            self.inner.peak_allocated.set(new_allocated);
        }

        Ok(AllocationHandle {
            bytes,
            pool: Rc::clone(&self.inner),
        })
    }

    pub fn total_budget(&self) -> usize {
        self.inner.total_budget
    }

    pub fn allocated(&self) -> usize {
        self.inner.allocated.get()
    }

    pub fn available(&self) -> usize {
        let allocated = self.allocated();
        self.inner.total_budget.saturating_sub(allocated)
    }

    pub fn peak_allocated(&self) -> usize {
        self.inner.peak_allocated.get()
    }

    pub fn utilization(&self) -> f64 {
        if self.inner.total_budget == 0 {
            return 0.0;
        }
        self.allocated() as f64 / self.inner.total_budget as f64
    }

    pub fn stats(&self) -> PoolStats {
        PoolStats {
            total_budget: self.inner.total_budget,
            allocated: self.allocated(),
            available: self.available(),
            peak_allocated: self.peak_allocated(),
            utilization: self.utilization(),
            total_allocations: self.inner.total_allocations.get(),
            successful_allocations: self.inner.successful_allocations.get(),
            failed_allocations: self.inner.failed_allocations.get(),
            total_deallocations: self.inner.total_deallocations.get(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_budget: usize,
    pub allocated: usize,
    pub available: usize,
    pub peak_allocated: usize,
    pub utilization: f64,
    pub total_allocations: u64,
    pub successful_allocations: u64,
    pub failed_allocations: u64,
    pub total_deallocations: u64,
}

impl PoolStats {
    pub fn format(&self) -> String {
        format!(
            "Pool: {:.2} MB / {:.2} MB ({:.1}% used) | Peak: {:.2} MB | Allocs: {} ok, {} failed, {} freed",
            self.allocated as f64 / (1024.0 * 1024.0),
            self.total_budget as f64 / (1024.0 * 1024.0),
            self.utilization * 100.0,
            self.peak_allocated as f64 / (1024.0 * 1024.0),
            self.successful_allocations,
            self.failed_allocations,
            self.total_deallocations
        )
    }
}

#[derive(Debug)]
struct TieredMemoryPoolInner {
    total_budget: usize,

    priority_config: PriorityConfig,

    critical_allocated: Cell<usize>,
    high_allocated: Cell<usize>,
    normal_allocated: Cell<usize>,
    low_allocated: Cell<usize>,

    critical_peak: Cell<usize>,
    high_peak: Cell<usize>,
    normal_peak: Cell<usize>,
    low_peak: Cell<usize>,

    global_allocated: Cell<usize>,

    global_peak: Cell<usize>,

    total_allocations: Cell<u64>,
    successful_allocations: Cell<u64>,
    failed_allocations: Cell<u64>,
    total_deallocations: Cell<u64>,
}

#[derive(Debug, Clone)]
pub struct TieredMemoryPool {
    inner: Rc<TieredMemoryPoolInner>,
}

impl TieredMemoryPool {
    pub fn new(total_budget: usize, config: PriorityConfig) -> Self {
        Self {
            inner: Rc::new(TieredMemoryPoolInner {
                total_budget,
                priority_config: config,
                critical_allocated: Cell::new(0),
                high_allocated: Cell::new(0),
                normal_allocated: Cell::new(0),
                low_allocated: Cell::new(0),
                critical_peak: Cell::new(0),
                high_peak: Cell::new(0),
                normal_peak: Cell::new(0),
                low_peak: Cell::new(0),
                global_allocated: Cell::new(0),
                global_peak: Cell::new(0),
                total_allocations: Cell::new(0),
                successful_allocations: Cell::new(0),
                failed_allocations: Cell::new(0),
                total_deallocations: Cell::new(0),
            }),
        }
    }

    fn tier_allocated(&self, priority: QueryPriority) -> &Cell<usize> {
        match priority {
            QueryPriority::Critical => &self.inner.critical_allocated,
            QueryPriority::High => &self.inner.high_allocated,
            QueryPriority::Normal => &self.inner.normal_allocated,
            QueryPriority::Low => &self.inner.low_allocated,
        }
    }

    fn tier_peak(&self, priority: QueryPriority) -> &Cell<usize> {
        match priority {
            QueryPriority::Critical => &self.inner.critical_peak,
            QueryPriority::High => &self.inner.high_peak,
            QueryPriority::Normal => &self.inner.normal_peak,
            QueryPriority::Low => &self.inner.low_peak,
        }
    }

    pub fn try_allocate(
        &self,
        bytes: usize,
        priority: QueryPriority,
    ) -> Result<TieredAllocationHandle> {
        self.inner
            .total_allocations
            .set(self.inner.total_allocations.get() + 1);

        let tier_limit = self
            .inner
            .priority_config
            .tier_limit(priority, self.inner.total_budget);
        let tier_allocated = self.tier_allocated(priority);
        let tier_peak = self.tier_peak(priority);

        let current_tier = tier_allocated.get();
        let current_global = self.inner.global_allocated.get();

        let new_tier = current_tier + bytes;
        let new_global = current_global + bytes;

        if new_tier > tier_limit && !self.inner.priority_config.allow_borrowing {
            self.inner
                .failed_allocations
                .set(self.inner.failed_allocations.get() + 1);
            return Err(Error::ExecutionError(format!(
                "Priority tier exhausted: {} MB requested, {} MB available in {} tier (tier limit: {} MB)",
                bytes / (1024 * 1024),
                tier_limit.saturating_sub(current_tier) / (1024 * 1024),
                priority.as_str(),
                tier_limit / (1024 * 1024)
            )));
        }

        if new_global > self.inner.total_budget {
            self.inner
                .failed_allocations
                .set(self.inner.failed_allocations.get() + 1);
            return Err(Error::ExecutionError(format!(
                "Memory pool exhausted: {} MB requested, {} MB available (total budget: {} MB)",
                bytes / (1024 * 1024),
                self.inner.total_budget.saturating_sub(current_global) / (1024 * 1024),
                self.inner.total_budget / (1024 * 1024)
            )));
        }

        tier_allocated.set(new_tier);
        self.inner.global_allocated.set(new_global);

        if new_tier > tier_peak.get() {
            tier_peak.set(new_tier);
        }
        if new_global > self.inner.global_peak.get() {
            self.inner.global_peak.set(new_global);
        }

        self.inner
            .successful_allocations
            .set(self.inner.successful_allocations.get() + 1);

        Ok(TieredAllocationHandle {
            bytes,
            priority,
            pool: self.clone(),
        })
    }

    pub fn total_budget(&self) -> usize {
        self.inner.total_budget
    }

    pub fn allocated(&self) -> usize {
        self.inner.global_allocated.get()
    }

    pub fn available(&self) -> usize {
        self.inner.total_budget.saturating_sub(self.allocated())
    }

    pub fn tier_allocated_bytes(&self, priority: QueryPriority) -> usize {
        self.tier_allocated(priority).get()
    }

    pub fn stats(&self) -> TieredPoolStats {
        TieredPoolStats {
            total_budget: self.inner.total_budget,
            global_allocated: self.allocated(),
            global_available: self.available(),
            global_peak: self.inner.global_peak.get(),
            critical_allocated: self.inner.critical_allocated.get(),
            high_allocated: self.inner.high_allocated.get(),
            normal_allocated: self.inner.normal_allocated.get(),
            low_allocated: self.inner.low_allocated.get(),
            critical_peak: self.inner.critical_peak.get(),
            high_peak: self.inner.high_peak.get(),
            normal_peak: self.inner.normal_peak.get(),
            low_peak: self.inner.low_peak.get(),
            total_allocations: self.inner.total_allocations.get(),
            successful_allocations: self.inner.successful_allocations.get(),
            failed_allocations: self.inner.failed_allocations.get(),
            total_deallocations: self.inner.total_deallocations.get(),
        }
    }
}

#[derive(Debug)]
pub struct TieredAllocationHandle {
    bytes: usize,
    priority: QueryPriority,
    pool: TieredMemoryPool,
}

impl Drop for TieredAllocationHandle {
    fn drop(&mut self) {
        let tier_allocated = self.pool.tier_allocated(self.priority);
        tier_allocated.set(tier_allocated.get() - self.bytes);
        self.pool
            .inner
            .global_allocated
            .set(self.pool.inner.global_allocated.get() - self.bytes);
        self.pool
            .inner
            .total_deallocations
            .set(self.pool.inner.total_deallocations.get() + 1);
    }
}

#[derive(Debug, Clone)]
pub struct TieredPoolStats {
    pub total_budget: usize,
    pub global_allocated: usize,
    pub global_available: usize,
    pub global_peak: usize,

    pub critical_allocated: usize,
    pub high_allocated: usize,
    pub normal_allocated: usize,
    pub low_allocated: usize,

    pub critical_peak: usize,
    pub high_peak: usize,
    pub normal_peak: usize,
    pub low_peak: usize,

    pub total_allocations: u64,
    pub successful_allocations: u64,
    pub failed_allocations: u64,
    pub total_deallocations: u64,
}

impl TieredPoolStats {
    pub fn format(&self) -> String {
        format!(
            "Tiered Pool: {:.2} MB / {:.2} MB | Critical: {:.2} MB | High: {:.2} MB | Normal: {:.2} MB | Low: {:.2} MB",
            self.global_allocated as f64 / (1024.0 * 1024.0),
            self.total_budget as f64 / (1024.0 * 1024.0),
            self.critical_allocated as f64 / (1024.0 * 1024.0),
            self.high_allocated as f64 / (1024.0 * 1024.0),
            self.normal_allocated as f64 / (1024.0 * 1024.0),
            self.low_allocated as f64 / (1024.0 * 1024.0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_allocation() {
        let pool = MemoryPool::new(1024 * 1024);

        let handle = pool.try_allocate(512 * 1024).unwrap();
        assert_eq!(pool.allocated(), 512 * 1024);
        assert_eq!(pool.available(), 512 * 1024);

        drop(handle);
        assert_eq!(pool.allocated(), 0);
        assert_eq!(pool.available(), 1024 * 1024);
    }

    #[test]
    fn test_pool_exhaustion() {
        let pool = MemoryPool::new(1024 * 1024);

        let _handle1 = pool.try_allocate(512 * 1024).unwrap();

        let _handle2 = pool.try_allocate(512 * 1024).unwrap();

        let result = pool.try_allocate(1);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exhausted"));
    }

    #[test]
    fn test_peak_tracking() {
        let pool = MemoryPool::new(1024 * 1024);

        let handle1 = pool.try_allocate(300 * 1024).unwrap();
        assert_eq!(pool.peak_allocated(), 300 * 1024);

        let handle2 = pool.try_allocate(400 * 1024).unwrap();
        assert_eq!(pool.peak_allocated(), 700 * 1024);

        drop(handle1);
        assert_eq!(pool.allocated(), 400 * 1024);

        assert_eq!(pool.peak_allocated(), 700 * 1024);

        drop(handle2);
        assert_eq!(pool.allocated(), 0);

        assert_eq!(pool.peak_allocated(), 700 * 1024);
    }

    #[test]
    fn test_utilization() {
        let pool = MemoryPool::new(1000);

        assert_eq!(pool.utilization(), 0.0);

        let _handle = pool.try_allocate(500).unwrap();
        assert_eq!(pool.utilization(), 0.5);

        let _handle2 = pool.try_allocate(250).unwrap();
        assert_eq!(pool.utilization(), 0.75);
    }

    #[test]
    fn test_stats() {
        let pool = MemoryPool::new(1024 * 1024);

        let handle1 = pool.try_allocate(512 * 1024).unwrap();
        let _handle2 = pool.try_allocate(256 * 1024).unwrap();

        let _ = pool.try_allocate(512 * 1024);

        drop(handle1);

        let stats = pool.stats();
        assert_eq!(stats.total_budget, 1024 * 1024);
        assert_eq!(stats.allocated, 256 * 1024);
        assert_eq!(stats.available, 768 * 1024);
        assert_eq!(stats.peak_allocated, 768 * 1024);
        assert_eq!(stats.total_allocations, 3);
        assert_eq!(stats.successful_allocations, 2);
        assert_eq!(stats.failed_allocations, 1);
        assert_eq!(stats.total_deallocations, 1);
    }

    #[test]
    fn test_unlimited_pool() {
        let pool = MemoryPool::unlimited();

        let _handle = pool.try_allocate(1024 * 1024 * 1024 * 1024).unwrap();
    }

    #[test]
    fn test_sequential_allocations() {
        let pool = MemoryPool::new(10 * 1024 * 1024);

        let mut handles1 = Vec::new();
        for _ in 0..10 {
            if let Ok(h) = pool.try_allocate(100 * 1024) {
                handles1.push(h);
            }
        }

        let mut handles2 = Vec::new();
        for _ in 0..10 {
            if let Ok(h) = pool.try_allocate(100 * 1024) {
                handles2.push(h);
            }
        }

        assert_eq!(handles1.len(), 10);
        assert_eq!(handles2.len(), 10);
        assert_eq!(pool.allocated(), 20 * 100 * 1024);

        drop(handles1);
        drop(handles2);

        assert_eq!(pool.allocated(), 0);
    }

    #[test]
    fn test_format_stats() {
        let pool = MemoryPool::new(10 * 1024 * 1024);
        let _handle = pool.try_allocate(5 * 1024 * 1024).unwrap();

        let stats = pool.stats();
        let formatted = stats.format();

        assert!(formatted.contains("5.00 MB"));
        assert!(formatted.contains("10.00 MB"));
        assert!(formatted.contains("50.0% used"));
    }
}
