use std::collections::{HashMap, HashSet};

use crate::table::Row;
use crate::transaction::{IsolationLevel, Transaction};

type VersionKey = (String, usize);

#[derive(Debug, Clone)]
pub struct RowVersion {
    pub txn_created: u64,
    pub txn_deleted: Option<u64>,
    pub data: Row,
}

impl RowVersion {
    pub fn new(txn_id: u64, data: Row) -> Self {
        Self {
            txn_created: txn_id,
            txn_deleted: None,
            data,
        }
    }

    pub fn mark_deleted(&mut self, txn_id: u64) {
        self.txn_deleted = Some(txn_id);
    }

    pub fn is_deleted(&self) -> bool {
        self.txn_deleted.is_some()
    }

    pub fn is_visible_to(
        &self,
        txn: &Transaction,
        committed_txns: &HashSet<u64>,
        commit_timestamps: &HashMap<u64, u64>,
    ) -> bool {
        match txn.isolation_level {
            IsolationLevel::ReadUncommitted => self.is_visible_read_uncommitted(),
            IsolationLevel::ReadCommitted => self.is_visible_read_committed(committed_txns),
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                self.is_visible_snapshot(txn.start_timestamp, commit_timestamps)
            }
        }
    }

    fn is_visible_read_uncommitted(&self) -> bool {
        !self.is_deleted()
    }

    fn is_visible_read_committed(&self, committed_txns: &HashSet<u64>) -> bool {
        let created_by_committed = committed_txns.contains(&self.txn_created);

        let deleted_by_uncommitted = self
            .txn_deleted
            .map(|del_txn| !committed_txns.contains(&del_txn))
            .unwrap_or(false);

        created_by_committed && (self.txn_deleted.is_none() || deleted_by_uncommitted)
    }

    fn is_visible_snapshot(
        &self,
        snapshot_timestamp: u64,
        commit_timestamps: &HashMap<u64, u64>,
    ) -> bool {
        self.was_committed_before(snapshot_timestamp, commit_timestamps)
            && self.was_not_deleted_before(snapshot_timestamp, commit_timestamps)
    }

    fn was_committed_before(
        &self,
        snapshot_timestamp: u64,
        commit_timestamps: &HashMap<u64, u64>,
    ) -> bool {
        if let Some(&commit_ts) = commit_timestamps.get(&self.txn_created) {
            commit_ts <= snapshot_timestamp
        } else {
            false
        }
    }

    fn was_not_deleted_before(
        &self,
        snapshot_timestamp: u64,
        commit_timestamps: &HashMap<u64, u64>,
    ) -> bool {
        match self.txn_deleted {
            None => true,
            Some(del_txn) => {
                if let Some(&del_commit_ts) = commit_timestamps.get(&del_txn) {
                    del_commit_ts > snapshot_timestamp
                } else {
                    true
                }
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct VersionStore {
    versions: HashMap<VersionKey, Vec<RowVersion>>,
}

impl VersionStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn make_key(table: &str, row_id: usize) -> VersionKey {
        (table.to_string(), row_id)
    }

    pub fn insert_version(&mut self, table: &str, row_id: usize, version: RowVersion) {
        self.versions
            .entry(Self::make_key(table, row_id))
            .or_default()
            .push(version);
    }

    pub fn get_visible_version(
        &self,
        table: &str,
        row_id: usize,
        txn: &Transaction,
        committed_txns: &HashSet<u64>,
        commit_timestamps: &HashMap<u64, u64>,
    ) -> Option<&RowVersion> {
        let versions = self.get_versions(table, row_id)?;
        self.find_visible_version(versions, txn, committed_txns, commit_timestamps)
    }

    pub fn get_all_versions(&self, table: &str, row_id: usize) -> Option<&Vec<RowVersion>> {
        self.get_versions(table, row_id)
    }

    pub fn mark_deleted(&mut self, table: &str, row_id: usize, txn_id: u64) {
        if let Some(versions) = self.get_versions_mut(table, row_id) {
            Self::mark_all_versions_deleted(versions, txn_id);
        }
    }

    fn mark_all_versions_deleted(versions: &mut [RowVersion], txn_id: u64) {
        for version in versions.iter_mut() {
            if Self::should_update_deletion(version, txn_id) {
                version.mark_deleted(txn_id);
            }
        }
    }

    fn should_update_deletion(version: &RowVersion, new_deletion_txn: u64) -> bool {
        version.txn_deleted.is_none() || version.txn_deleted.unwrap() > new_deletion_txn
    }

    fn get_versions(&self, table: &str, row_id: usize) -> Option<&Vec<RowVersion>> {
        self.versions.get(&Self::make_key(table, row_id))
    }

    fn get_versions_mut(&mut self, table: &str, row_id: usize) -> Option<&mut Vec<RowVersion>> {
        self.versions.get_mut(&Self::make_key(table, row_id))
    }

    fn find_visible_version<'a>(
        &self,
        versions: &'a [RowVersion],
        txn: &Transaction,
        committed_txns: &HashSet<u64>,
        commit_timestamps: &HashMap<u64, u64>,
    ) -> Option<&'a RowVersion> {
        versions
            .iter()
            .rev()
            .find(|v| v.is_visible_to(txn, committed_txns, commit_timestamps))
    }

    pub fn vacuum(&mut self, oldest_active_txn: u64) {
        for versions in self.versions.values_mut() {
            Self::vacuum_version_chain(versions, oldest_active_txn);
        }
    }

    const VACUUM_THRESHOLD: u64 = 50;

    fn vacuum_version_chain(versions: &mut Vec<RowVersion>, oldest_active_txn: u64) {
        if versions.len() <= 1 {
            return;
        }

        let keep_from = Self::find_vacuum_cutoff(versions, oldest_active_txn);
        if keep_from > 0 {
            versions.drain(0..keep_from);
        }
    }

    fn find_vacuum_cutoff(versions: &[RowVersion], oldest_active_txn: u64) -> usize {
        let first_active_idx = Self::find_first_active_version(versions, oldest_active_txn);

        match first_active_idx {
            Some(idx) if idx > 0 => Self::find_last_deleted_before(versions, idx).unwrap_or(0),
            _ => Self::vacuum_old_versions(versions, oldest_active_txn),
        }
    }

    fn find_first_active_version(versions: &[RowVersion], oldest_active_txn: u64) -> Option<usize> {
        versions
            .iter()
            .position(|v| Self::is_needed_by_active_txn(v, oldest_active_txn))
    }

    fn find_last_deleted_before(versions: &[RowVersion], before_idx: usize) -> Option<usize> {
        (0..before_idx)
            .rev()
            .find(|&i| versions[i].is_deleted())
            .filter(|&idx| idx > 0)
    }

    fn vacuum_old_versions(versions: &[RowVersion], oldest_active_txn: u64) -> usize {
        if Self::are_all_versions_ancient(versions, oldest_active_txn) {
            return Self::find_last_non_deleted(versions).unwrap_or(0);
        }

        if Self::has_deleted_and_non_deleted(versions) {
            return Self::find_first_non_deleted(versions).unwrap_or(0);
        }

        0
    }

    fn are_all_versions_ancient(versions: &[RowVersion], oldest_active_txn: u64) -> bool {
        versions
            .iter()
            .filter(|v| !v.is_deleted())
            .all(|v| oldest_active_txn.saturating_sub(v.txn_created) > Self::VACUUM_THRESHOLD)
    }

    fn find_last_non_deleted(versions: &[RowVersion]) -> Option<usize> {
        versions.iter().rposition(|v| !v.is_deleted())
    }

    fn find_first_non_deleted(versions: &[RowVersion]) -> Option<usize> {
        versions.iter().position(|v| !v.is_deleted())
    }

    fn has_deleted_and_non_deleted(versions: &[RowVersion]) -> bool {
        let has_deleted = versions.iter().any(|v| v.is_deleted());
        let has_non_deleted = versions.iter().any(|v| !v.is_deleted());
        has_deleted && has_non_deleted
    }

    fn is_needed_by_active_txn(version: &RowVersion, oldest_active_txn: u64) -> bool {
        if version.txn_created >= oldest_active_txn {
            return true;
        }

        if let Some(deleted) = version.txn_deleted
            && deleted >= oldest_active_txn
        {
            return true;
        }

        false
    }

    pub fn version_count(&self) -> usize {
        self.versions.values().map(|v| v.len()).sum()
    }

    #[cfg(test)]
    pub fn clear(&mut self) {
        self.versions.clear();
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use yachtsql_common::types::Value;

    use super::*;

    fn make_row(id: i64, name: &str) -> Row {
        let mut values = IndexMap::new();
        values.insert("id".to_string(), Value::int64(id));
        values.insert("name".to_string(), Value::string(name.to_string()));
        Row::from_named_values(values)
    }

    fn make_test_txn(txn_id: u64, start_timestamp: u64, isolation: IsolationLevel) -> Transaction {
        let storage = crate::Storage::new();
        Transaction::new(txn_id, start_timestamp, &storage, isolation)
    }

    fn commit_map(entries: &[(u64, u64)]) -> HashMap<u64, u64> {
        let mut map = HashMap::new();
        for (txn, ts) in entries {
            map.insert(*txn, *ts);
        }
        map
    }

    #[test]
    fn test_version_store_insert_and_get() {
        let mut store = VersionStore::new();
        let row = make_row(1, "Alice");

        let version = RowVersion::new(1, row.clone());
        store.insert_version("users", 0, version);

        let txn = make_test_txn(2, 1, IsolationLevel::RepeatableRead);
        let committed = HashSet::new();
        let commit_timestamps = commit_map(&[(1, 1)]);
        let visible = store.get_visible_version("users", 0, &txn, &committed, &commit_timestamps);

        assert!(visible.is_some());
        assert_eq!(
            visible.unwrap().data["name"],
            Value::string("Alice".to_string())
        );
    }

    #[test]
    fn test_version_visibility_repeatable_read() {
        let mut store = VersionStore::new();

        store.insert_version("users", 0, RowVersion::new(1, make_row(1, "v1")));

        let txn2 = make_test_txn(2, 5, IsolationLevel::RepeatableRead);
        let committed = HashSet::new();
        let mut commit_timestamps = commit_map(&[(1, 1)]);

        assert!(
            store
                .get_visible_version("users", 0, &txn2, &committed, &commit_timestamps)
                .is_some()
        );

        store.insert_version("users", 1, RowVersion::new(10, make_row(2, "v2")));
        commit_timestamps.insert(10, 10);

        assert!(
            store
                .get_visible_version("users", 1, &txn2, &committed, &commit_timestamps)
                .is_none()
        );
    }

    #[test]
    fn test_mark_deleted() {
        let mut store = VersionStore::new();
        store.insert_version("users", 0, RowVersion::new(1, make_row(1, "Alice")));

        store.mark_deleted("users", 0, 5);
        let committed = HashSet::new();
        let mut commit_timestamps = commit_map(&[(1, 1)]);
        commit_timestamps.insert(5, 5);

        let txn3 = make_test_txn(3, 3, IsolationLevel::RepeatableRead);
        assert!(
            store
                .get_visible_version("users", 0, &txn3, &committed, &commit_timestamps)
                .is_some()
        );

        let txn10 = make_test_txn(10, 10, IsolationLevel::RepeatableRead);
        assert!(
            store
                .get_visible_version("users", 0, &txn10, &committed, &commit_timestamps)
                .is_none()
        );
    }

    #[test]
    fn test_vacuum() {
        let mut store = VersionStore::new();

        store.insert_version("users", 0, RowVersion::new(1, make_row(1, "v1")));
        store.insert_version("users", 0, RowVersion::new(2, make_row(1, "v2")));
        store.insert_version("users", 0, RowVersion::new(3, make_row(1, "v3")));

        assert_eq!(store.version_count(), 3);

        store.vacuum(3);

        let versions = store.get_all_versions("users", 0).unwrap();

        assert!(!versions.is_empty());
    }

    #[test]
    fn test_read_committed_prevents_dirty_reads() {
        let mut store = VersionStore::new();

        store.insert_version("accounts", 0, RowVersion::new(1, make_row(1, "Alice")));

        let mut committed = HashSet::new();
        committed.insert(1);

        let txn2 = make_test_txn(2, 2, IsolationLevel::ReadCommitted);
        let visible = store.get_visible_version("accounts", 0, &txn2, &committed, &HashMap::new());
        assert!(visible.is_some(), "READ COMMITTED should see committed row");

        store.insert_version("accounts", 1, RowVersion::new(3, make_row(2, "Bob")));

        let txn4 = make_test_txn(4, 4, IsolationLevel::ReadCommitted);
        let visible = store.get_visible_version("accounts", 1, &txn4, &committed, &HashMap::new());
        assert!(
            visible.is_none(),
            "READ COMMITTED must NOT see uncommitted row (dirty read prevention)"
        );

        committed.insert(3);

        let txn5 = make_test_txn(5, 5, IsolationLevel::ReadCommitted);
        let visible = store.get_visible_version("accounts", 1, &txn5, &committed, &HashMap::new());
        assert!(
            visible.is_some(),
            "READ COMMITTED should see committed row after commit"
        );
    }

    #[test]
    fn test_read_committed_with_uncommitted_delete() {
        let mut store = VersionStore::new();

        store.insert_version("accounts", 0, RowVersion::new(1, make_row(1, "Alice")));
        let mut committed = HashSet::new();
        committed.insert(1);

        store.mark_deleted("accounts", 0, 2);

        let txn3 = make_test_txn(3, 3, IsolationLevel::ReadCommitted);
        let visible = store.get_visible_version("accounts", 0, &txn3, &committed, &HashMap::new());
        assert!(
            visible.is_some(),
            "READ COMMITTED should see row when delete is uncommitted"
        );

        committed.insert(2);

        let txn4 = make_test_txn(4, 4, IsolationLevel::ReadCommitted);
        let visible = store.get_visible_version("accounts", 0, &txn4, &committed, &HashMap::new());
        assert!(
            visible.is_none(),
            "READ COMMITTED should NOT see row after committed delete"
        );
    }

    #[test]
    fn test_read_uncommitted_sees_dirty_data() {
        let mut store = VersionStore::new();
        let committed = HashSet::new();

        store.insert_version("accounts", 0, RowVersion::new(1, make_row(1, "Alice")));

        let txn2 = make_test_txn(2, 2, IsolationLevel::ReadUncommitted);
        let visible = store.get_visible_version("accounts", 0, &txn2, &committed, &HashMap::new());
        assert!(
            visible.is_some(),
            "READ UNCOMMITTED should see uncommitted data (dirty read allowed)"
        );
    }
}
