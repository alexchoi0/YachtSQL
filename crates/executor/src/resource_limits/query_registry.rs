use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use yachtsql_core::error::{Error, Result};

use super::CancellationToken;
use super::priority::QueryPriority;

pub type QueryId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryStatus {
    Running,
    Waiting,
    Completed,
    Failed,
    Cancelled,
}

impl QueryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryStatus::Running => "running",
            QueryStatus::Waiting => "waiting",
            QueryStatus::Completed => "completed",
            QueryStatus::Failed => "failed",
            QueryStatus::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            QueryStatus::Completed | QueryStatus::Failed | QueryStatus::Cancelled
        )
    }

    pub fn is_active(&self) -> bool {
        matches!(self, QueryStatus::Running | QueryStatus::Waiting)
    }
}

#[derive(Debug, Clone)]
pub struct QueryInfo {
    pub id: QueryId,
    pub sql: String,
    pub priority: QueryPriority,
    pub status: QueryStatus,
    pub submitted_at: Instant,
    pub started_at: Option<Instant>,
    pub finished_at: Option<Instant>,
    pub memory_bytes: usize,
    pub peak_memory_bytes: usize,
    pub row_count: usize,
    pub(crate) cancellation_token: CancellationToken,
}

impl QueryInfo {
    pub fn elapsed(&self) -> Option<Duration> {
        self.started_at.map(|start| start.elapsed())
    }

    pub fn duration(&self) -> Option<Duration> {
        match (self.started_at, self.finished_at) {
            (Some(start), Some(end)) => Some(end.duration_since(start)),
            _ => None,
        }
    }

    pub fn wait_time(&self) -> Option<Duration> {
        self.started_at
            .map(|start| start.duration_since(self.submitted_at))
    }
}

#[derive(Debug, Clone)]
pub struct QueryRegistry {
    inner: Rc<RefCell<QueryRegistryInner>>,
}

#[derive(Debug)]
struct QueryRegistryInner {
    next_id: QueryId,
    queries: HashMap<QueryId, QueryInfo>,
    max_history: usize,
}

impl QueryRegistry {
    pub fn new(max_history: usize) -> Self {
        Self {
            inner: Rc::new(RefCell::new(QueryRegistryInner {
                next_id: 1,
                queries: HashMap::new(),
                max_history,
            })),
        }
    }

    pub fn register(
        &self,
        sql: &str,
        priority: QueryPriority,
    ) -> Result<(QueryId, CancellationToken)> {
        let mut inner = self.inner.borrow_mut();

        let id = inner.next_id;
        inner.next_id += 1;

        let sql_truncated = if sql.len() > 500 {
            format!("{}...", &sql[..497])
        } else {
            sql.to_string()
        };

        let cancellation_token = CancellationToken::new();

        let info = QueryInfo {
            id,
            sql: sql_truncated,
            priority,
            status: QueryStatus::Waiting,
            submitted_at: Instant::now(),
            started_at: None,
            finished_at: None,
            memory_bytes: 0,
            peak_memory_bytes: 0,
            row_count: 0,
            cancellation_token: cancellation_token.clone(),
        };

        inner.queries.insert(id, info);

        Ok((id, cancellation_token))
    }

    pub fn mark_started(&self, id: QueryId) -> Result<()> {
        let mut inner = self.inner.borrow_mut();

        let query = inner
            .queries
            .get_mut(&id)
            .ok_or_else(|| Error::ExecutionError(format!("Query {} not found", id)))?;

        query.status = QueryStatus::Running;
        query.started_at = Some(Instant::now());

        Ok(())
    }

    pub fn mark_completed(&self, id: QueryId) -> Result<()> {
        self.mark_finished(id, QueryStatus::Completed)
    }

    pub fn mark_failed(&self, id: QueryId) -> Result<()> {
        self.mark_finished(id, QueryStatus::Failed)
    }

    pub fn mark_cancelled(&self, id: QueryId) -> Result<()> {
        self.mark_finished(id, QueryStatus::Cancelled)
    }

    fn mark_finished(&self, id: QueryId, status: QueryStatus) -> Result<()> {
        let mut inner = self.inner.borrow_mut();

        let query = inner
            .queries
            .get_mut(&id)
            .ok_or_else(|| Error::ExecutionError(format!("Query {} not found", id)))?;

        query.status = status;
        query.finished_at = Some(Instant::now());

        if inner.max_history > 0 {
            self.cleanup_history(&mut inner);
        }

        Ok(())
    }

    fn cleanup_history(&self, inner: &mut QueryRegistryInner) {
        let terminal_queries: Vec<_> = inner
            .queries
            .iter()
            .filter(|(_, q)| q.status.is_terminal())
            .map(|(id, q)| (*id, q.finished_at.unwrap_or(q.submitted_at)))
            .collect();

        if terminal_queries.len() > inner.max_history {
            let mut sorted = terminal_queries;
            sorted.sort_by_key(|(_, time)| *time);

            let to_remove = sorted.len() - inner.max_history;
            for (id, _) in sorted.iter().take(to_remove) {
                inner.queries.remove(id);
            }
        }
    }

    pub fn update_stats(
        &self,
        id: QueryId,
        memory_bytes: usize,
        peak_memory_bytes: usize,
        row_count: usize,
    ) -> Result<()> {
        let mut inner = self.inner.borrow_mut();

        let query = inner
            .queries
            .get_mut(&id)
            .ok_or_else(|| Error::ExecutionError(format!("Query {} not found", id)))?;

        query.memory_bytes = memory_bytes;
        query.peak_memory_bytes = peak_memory_bytes;
        query.row_count = row_count;

        Ok(())
    }

    pub fn get(&self, id: QueryId) -> Option<QueryInfo> {
        let inner = self.inner.borrow();
        inner.queries.get(&id).cloned()
    }

    pub fn list(&self, status_filter: Option<QueryStatus>) -> Vec<QueryInfo> {
        let inner = self.inner.borrow();

        let mut queries: Vec<_> = inner
            .queries
            .values()
            .filter(|q| status_filter.is_none() || Some(q.status) == status_filter)
            .cloned()
            .collect();

        queries.sort_by(|a, b| b.submitted_at.cmp(&a.submitted_at));

        queries
    }

    pub fn list_active(&self) -> Vec<QueryInfo> {
        let inner = self.inner.borrow();

        let mut queries: Vec<_> = inner
            .queries
            .values()
            .filter(|q| q.status.is_active())
            .cloned()
            .collect();

        queries.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.submitted_at.cmp(&b.submitted_at))
        });

        queries
    }

    pub fn cancel(&self, id: QueryId) -> Result<bool> {
        let inner = self.inner.borrow();

        let query = inner
            .queries
            .get(&id)
            .ok_or_else(|| Error::ExecutionError(format!("Query {} not found", id)))?;

        if query.status.is_terminal() {
            return Ok(false);
        }

        query.cancellation_token.cancel();

        Ok(true)
    }

    pub fn cancel_all<F>(&self, filter: F) -> usize
    where
        F: Fn(&QueryInfo) -> bool,
    {
        let inner = self.inner.borrow();

        let mut cancelled = 0;

        for query in inner.queries.values() {
            if !query.status.is_terminal() && filter(query) {
                query.cancellation_token.cancel();
                cancelled += 1;
            }
        }

        cancelled
    }

    pub fn stats(&self) -> RegistryStats {
        let inner = self.inner.borrow();

        let mut stats = RegistryStats {
            total_queries: inner.queries.len(),
            running: 0,
            waiting: 0,
            completed: 0,
            failed: 0,
            cancelled: 0,
            total_memory_bytes: 0,
            peak_memory_bytes: 0,
        };

        for query in inner.queries.values() {
            match query.status {
                QueryStatus::Running => stats.running += 1,
                QueryStatus::Waiting => stats.waiting += 1,
                QueryStatus::Completed => stats.completed += 1,
                QueryStatus::Failed => stats.failed += 1,
                QueryStatus::Cancelled => stats.cancelled += 1,
            }

            if query.status.is_active() {
                stats.total_memory_bytes += query.memory_bytes;
                stats.peak_memory_bytes = stats.peak_memory_bytes.max(query.peak_memory_bytes);
            }
        }

        stats
    }

    pub fn remove(&self, id: QueryId) -> Result<()> {
        let mut inner = self.inner.borrow_mut();

        inner
            .queries
            .remove(&id)
            .ok_or_else(|| Error::ExecutionError(format!("Query {} not found", id)))?;

        Ok(())
    }

    pub fn clear_history(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.queries.retain(|_, q| q.status.is_active());
    }
}

#[derive(Debug, Clone, Default)]
pub struct RegistryStats {
    pub total_queries: usize,
    pub running: usize,
    pub waiting: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub total_memory_bytes: usize,
    pub peak_memory_bytes: usize,
}

impl RegistryStats {
    pub fn format(&self) -> String {
        format!(
            "Queries: {} total ({} running, {} waiting, {} completed, {} failed, {} cancelled) | Memory: {:.2} MB (peak: {:.2} MB)",
            self.total_queries,
            self.running,
            self.waiting,
            self.completed,
            self.failed,
            self.cancelled,
            self.total_memory_bytes as f64 / (1024.0 * 1024.0),
            self.peak_memory_bytes as f64 / (1024.0 * 1024.0)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_registration() {
        let registry = QueryRegistry::new(100);

        let (id1, _) = registry
            .register("SELECT * FROM users", QueryPriority::Normal)
            .unwrap();
        let (id2, _) = registry
            .register("SELECT * FROM orders", QueryPriority::High)
            .unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);

        let query1 = registry.get(id1).unwrap();
        assert_eq!(query1.sql, "SELECT * FROM users");
        assert_eq!(query1.priority, QueryPriority::Normal);
        assert_eq!(query1.status, QueryStatus::Waiting);
    }

    #[test]
    fn test_query_lifecycle() {
        let registry = QueryRegistry::new(100);

        let (id, _) = registry
            .register("SELECT 1", QueryPriority::Normal)
            .unwrap();

        assert_eq!(registry.get(id).unwrap().status, QueryStatus::Waiting);

        registry.mark_started(id).unwrap();
        assert_eq!(registry.get(id).unwrap().status, QueryStatus::Running);
        assert!(registry.get(id).unwrap().started_at.is_some());

        registry.mark_completed(id).unwrap();
        assert_eq!(registry.get(id).unwrap().status, QueryStatus::Completed);
        assert!(registry.get(id).unwrap().finished_at.is_some());
    }

    #[test]
    fn test_query_cancellation() {
        let registry = QueryRegistry::new(100);

        let (id, token) = registry
            .register("SELECT 1", QueryPriority::Normal)
            .unwrap();

        assert!(!token.is_cancelled());

        assert!(registry.cancel(id).unwrap());
        assert!(token.is_cancelled());

        registry.mark_cancelled(id).unwrap();

        assert!(!registry.cancel(id).unwrap());
    }

    #[test]
    fn test_list_active_queries() {
        let registry = QueryRegistry::new(100);

        let (id1, _) = registry.register("SELECT 1", QueryPriority::Low).unwrap();
        let (id2, _) = registry.register("SELECT 2", QueryPriority::High).unwrap();
        let (id3, _) = registry
            .register("SELECT 3", QueryPriority::Critical)
            .unwrap();

        registry.mark_started(id1).unwrap();
        registry.mark_started(id2).unwrap();
        registry.mark_started(id3).unwrap();

        registry.mark_completed(id1).unwrap();

        let active = registry.list_active();
        assert_eq!(active.len(), 2);

        assert_eq!(active[0].id, id3);
        assert_eq!(active[1].id, id2);
    }

    #[test]
    fn test_stats_update() {
        let registry = QueryRegistry::new(100);

        let (id, _) = registry
            .register("SELECT 1", QueryPriority::Normal)
            .unwrap();
        registry.mark_started(id).unwrap();

        registry.update_stats(id, 1000, 2000, 100).unwrap();

        let query = registry.get(id).unwrap();
        assert_eq!(query.memory_bytes, 1000);
        assert_eq!(query.peak_memory_bytes, 2000);
        assert_eq!(query.row_count, 100);
    }

    #[test]
    fn test_registry_stats() {
        let registry = QueryRegistry::new(100);

        let (id1, _) = registry
            .register("SELECT 1", QueryPriority::Normal)
            .unwrap();
        let (id2, _) = registry.register("SELECT 2", QueryPriority::High).unwrap();
        let (id3, _) = registry.register("SELECT 3", QueryPriority::Low).unwrap();

        registry.mark_started(id1).unwrap();
        registry.mark_started(id2).unwrap();
        registry.mark_completed(id3).unwrap();

        let stats = registry.stats();
        assert_eq!(stats.total_queries, 3);
        assert_eq!(stats.running, 2);
        assert_eq!(stats.waiting, 0);
        assert_eq!(stats.completed, 1);
    }

    #[test]
    fn test_history_cleanup() {
        let registry = QueryRegistry::new(2);

        for i in 0..5 {
            let (id, _) = registry
                .register(&format!("SELECT {}", i), QueryPriority::Normal)
                .unwrap();
            registry.mark_started(id).unwrap();
            registry.mark_completed(id).unwrap();
        }

        let all = registry.list(None);
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_cancel_all() {
        let registry = QueryRegistry::new(100);

        let (id1, _) = registry.register("SELECT 1", QueryPriority::Low).unwrap();
        let (id2, _) = registry.register("SELECT 2", QueryPriority::Low).unwrap();
        let (id3, _) = registry.register("SELECT 3", QueryPriority::High).unwrap();

        registry.mark_started(id1).unwrap();
        registry.mark_started(id2).unwrap();
        registry.mark_started(id3).unwrap();

        let cancelled = registry.cancel_all(|q| q.priority == QueryPriority::Low);
        assert_eq!(cancelled, 2);

        let query3 = registry.get(id3).unwrap();
        assert!(!query3.cancellation_token.is_cancelled());
    }
}
