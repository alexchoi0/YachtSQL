use std::collections::{HashMap, HashSet};
use std::time::Instant;

use yachtsql_core::error::Result;
use yachtsql_core::types::Value;

use crate::{Row, Schema, Storage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintTiming {
    Immediate,
    Deferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DMLOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct DeferredFKCheck {
    pub constraint_name: String,

    pub child_table: String,

    pub child_columns: Vec<String>,

    pub parent_table: String,

    pub fk_values: Vec<Value>,

    pub parent_columns: Vec<String>,

    pub operation: DMLOperation,
}

#[derive(Debug, Clone, Default)]
pub struct DeferredFKState {
    pub pending_checks: Vec<DeferredFKCheck>,

    pub constraint_timings: HashMap<String, ConstraintTiming>,

    pub default_mode: Option<ConstraintTiming>,
}

impl DeferredFKState {
    pub fn get_timing(&self, constraint_name: &str, initial: ConstraintTiming) -> ConstraintTiming {
        if let Some(timing) = self.constraint_timings.get(constraint_name) {
            return *timing;
        }
        if let Some(default) = self.default_mode {
            return default;
        }
        initial
    }

    pub fn set_timing(&mut self, constraint_name: String, timing: ConstraintTiming) {
        self.constraint_timings.insert(constraint_name, timing);
    }

    pub fn set_default_mode(&mut self, timing: ConstraintTiming) {
        self.default_mode = Some(timing);
    }

    pub fn pending_checks(&self) -> &[DeferredFKCheck] {
        &self.pending_checks
    }

    pub fn defer_check(&mut self, check: DeferredFKCheck) {
        self.pending_checks.push(check);
    }

    pub fn has_pending_checks(&self) -> bool {
        !self.pending_checks.is_empty()
    }

    pub fn pending_check_count(&self) -> usize {
        self.pending_checks.len()
    }

    pub fn clear(&mut self) {
        self.pending_checks.clear();
        self.constraint_timings.clear();
        self.default_mode = None;
    }

    pub fn clear_pending_checks(&mut self) {
        self.pending_checks.clear();
    }

    pub fn remove_checks_for_constraints<S: AsRef<str>>(&mut self, constraint_names: &[S]) {
        if constraint_names.is_empty() {
            return;
        }

        use std::collections::HashSet;
        let targets: HashSet<String> = constraint_names
            .iter()
            .map(|name| name.as_ref().to_ascii_lowercase())
            .collect();

        self.pending_checks
            .retain(|check| !targets.contains(&check.constraint_name.to_ascii_lowercase()));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    ReadUncommitted,
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TransactionCharacteristics {
    pub isolation_level: Option<IsolationLevel>,
    pub read_only: Option<bool>,
    pub deferrable: Option<bool>,
}

impl TransactionCharacteristics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_isolation_level(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = Some(level);
        self
    }

    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = Some(read_only);
        self
    }

    pub fn with_deferrable(mut self, deferrable: bool) -> Self {
        self.deferrable = Some(deferrable);
        self
    }
}

#[derive(Debug, Clone)]
pub struct Savepoint {
    pub name: String,

    pub storage_snapshot: Storage,

    pub pending_changes_snapshot: Option<PendingChanges>,

    pub deferred_fk_state: DeferredFKState,
}

#[derive(Debug, Clone, Default)]
pub struct TableDelta {
    pub inserted_rows: Vec<Row>,

    pub updated_rows: HashMap<usize, Row>,

    pub deleted_rows: HashSet<usize>,
}

impl TableDelta {
    pub fn is_empty(&self) -> bool {
        self.inserted_rows.is_empty()
            && self.updated_rows.is_empty()
            && self.deleted_rows.is_empty()
    }

    pub fn change_count(&self) -> usize {
        self.inserted_rows.len() + self.updated_rows.len() + self.deleted_rows.len()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SequenceDelta {
    pub incremented_values: Vec<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct PendingChanges {
    modified_tables: HashMap<String, TableDelta>,
    created_tables: HashMap<String, Schema>,
    dropped_tables: HashSet<String>,
    sequence_changes: HashMap<String, SequenceDelta>,
}

impl PendingChanges {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.modified_tables.is_empty()
            && self.created_tables.is_empty()
            && self.dropped_tables.is_empty()
            && self.sequence_changes.is_empty()
    }

    pub fn get_table_delta_mut(&mut self, table_name: &str) -> &mut TableDelta {
        self.modified_tables
            .entry(table_name.to_string())
            .or_default()
    }

    pub fn get_table_delta(&self, table_name: &str) -> Option<&TableDelta> {
        self.modified_tables.get(table_name)
    }

    pub fn track_create_table(&mut self, table_name: String, schema: Schema) {
        self.created_tables.insert(table_name, schema);
    }

    pub fn track_drop_table(&mut self, table_name: String) {
        self.dropped_tables.insert(table_name);
    }

    pub fn track_insert(&mut self, table_name: &str, row: Row) {
        self.get_table_delta_mut(table_name).inserted_rows.push(row);
    }

    pub fn track_update(&mut self, table_name: &str, row_index: usize, new_row: Row) {
        self.get_table_delta_mut(table_name)
            .updated_rows
            .insert(row_index, new_row);
    }

    pub fn track_delete(&mut self, table_name: &str, row_index: usize) {
        self.get_table_delta_mut(table_name)
            .deleted_rows
            .insert(row_index);
    }

    pub fn modified_table_names(&self) -> impl Iterator<Item = &String> {
        self.modified_tables.keys()
    }

    pub fn created_tables(&self) -> &HashMap<String, Schema> {
        &self.created_tables
    }

    pub fn dropped_tables(&self) -> &HashSet<String> {
        &self.dropped_tables
    }
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub txn_id: u64,

    pub snapshot: Storage,

    pub pending_changes: Option<PendingChanges>,

    pub savepoints: Vec<Savepoint>,

    pub isolation_level: IsolationLevel,

    pub read_only: bool,

    pub deferrable: bool,

    pub started_at: Instant,

    pub deferred_fk_state: DeferredFKState,

    pub start_timestamp: u64,

    pub has_written: bool,

    pub snapshot_timestamp: Option<u64>,

    pub locks_held: HashSet<(String, usize)>,

    pub read_set: HashSet<(String, usize)>,

    pub write_set: HashSet<(String, usize)>,

    pub timeout_at: Option<Instant>,
    status: TransactionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionStatus {
    Active,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionScope {
    ImplicitAutocommit,
    ImplicitSession,
    Explicit,
}

#[derive(Debug)]
struct ActiveTransactionContext {
    scope: TransactionScope,
    transaction: Transaction,
}

impl Transaction {
    pub fn new(
        txn_id: u64,
        start_timestamp: u64,
        storage: &Storage,
        isolation_level: IsolationLevel,
    ) -> Self {
        Self::with_characteristics(
            txn_id,
            start_timestamp,
            storage,
            isolation_level,
            false,
            false,
        )
    }

    pub fn with_characteristics(
        txn_id: u64,
        start_timestamp: u64,
        storage: &Storage,
        isolation_level: IsolationLevel,
        read_only: bool,
        deferrable: bool,
    ) -> Self {
        Self {
            txn_id,
            snapshot: storage.clone(),
            pending_changes: Some(PendingChanges::new()),
            savepoints: Vec::new(),
            isolation_level,
            read_only,
            deferrable,
            started_at: Instant::now(),
            deferred_fk_state: DeferredFKState::default(),
            start_timestamp,
            has_written: false,
            snapshot_timestamp: None,
            locks_held: HashSet::new(),
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            timeout_at: None,
            status: TransactionStatus::Active,
        }
    }

    pub fn get_snapshot(&self) -> &Storage {
        &self.snapshot
    }

    pub fn get_snapshot_mut(&mut self) -> &mut Storage {
        &mut self.snapshot
    }

    pub fn mark_written(&mut self) {
        self.has_written = true;
    }

    pub fn is_aborted(&self) -> bool {
        matches!(self.status, TransactionStatus::Aborted)
    }

    pub fn mark_aborted(&mut self) {
        self.status = TransactionStatus::Aborted;
    }

    pub fn mark_active(&mut self) {
        self.status = TransactionStatus::Active;
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    pub fn is_deferrable(&self) -> bool {
        self.deferrable
    }

    pub fn set_deferrable(&mut self, deferrable: bool) {
        self.deferrable = deferrable;
    }

    pub fn set_isolation_level(&mut self, level: IsolationLevel) {
        self.isolation_level = level;
    }

    pub fn track_insert(&mut self, table_name: &str, row: Row) {
        if let Some(changes) = &mut self.pending_changes {
            changes.track_insert(table_name, row);
            self.mark_written();
        }
    }

    pub fn track_update(&mut self, table_name: &str, row_index: usize, new_row: Row) {
        if let Some(changes) = &mut self.pending_changes {
            changes.track_update(table_name, row_index, new_row);
            self.mark_written();
        }
    }

    pub fn track_delete(&mut self, table_name: &str, row_index: usize) {
        if let Some(changes) = &mut self.pending_changes {
            changes.track_delete(table_name, row_index);
            self.mark_written();
        }
    }

    pub fn track_create_table(&mut self, table_name: String, schema: Schema) {
        if let Some(changes) = &mut self.pending_changes {
            changes.track_create_table(table_name, schema);
            self.mark_written();
        }
    }

    pub fn track_drop_table(&mut self, table_name: String) {
        if let Some(changes) = &mut self.pending_changes {
            changes.track_drop_table(table_name);
            self.mark_written();
        }
    }

    pub fn pending_changes(&self) -> Option<&PendingChanges> {
        self.pending_changes.as_ref()
    }

    pub fn pending_changes_mut(&mut self) -> Option<&mut PendingChanges> {
        self.pending_changes.as_mut()
    }

    pub fn has_pending_changes(&self) -> bool {
        self.pending_changes
            .as_ref()
            .map(|c| !c.is_empty())
            .unwrap_or(false)
    }

    pub fn create_savepoint(&mut self, name: String) {
        self.savepoints.retain(|sp| sp.name != name);

        let savepoint = Savepoint {
            name,
            storage_snapshot: self.snapshot.clone(),
            pending_changes_snapshot: self.pending_changes.clone(),
            deferred_fk_state: self.deferred_fk_state.clone(),
        };

        self.savepoints.push(savepoint);
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let sp_idx = self
            .savepoints
            .iter()
            .rposition(|sp| sp.name == name)
            .ok_or_else(|| yachtsql_core::error::Error::SavepointNotFound {
                name: name.to_string(),
            })?;

        self.snapshot = self.savepoints[sp_idx].storage_snapshot.clone();

        self.pending_changes = self.savepoints[sp_idx].pending_changes_snapshot.clone();

        self.deferred_fk_state = self.savepoints[sp_idx].deferred_fk_state.clone();

        self.savepoints.truncate(sp_idx + 1);

        Ok(())
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        let sp_idx = self
            .savepoints
            .iter()
            .rposition(|sp| sp.name == name)
            .ok_or_else(|| yachtsql_core::error::Error::SavepointNotFound {
                name: name.to_string(),
            })?;

        self.savepoints.truncate(sp_idx);

        Ok(())
    }

    pub fn deferred_fk_state(&self) -> &DeferredFKState {
        &self.deferred_fk_state
    }

    pub fn deferred_fk_state_mut(&mut self) -> &mut DeferredFKState {
        &mut self.deferred_fk_state
    }

    pub fn elapsed(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }
}

#[derive(Debug)]
pub struct TransactionManager {
    active_transaction: Option<ActiveTransactionContext>,
    next_txn_id: u64,
    global_timestamp: u64,
    default_isolation_level: IsolationLevel,
    committed_txns: HashSet<u64>,
    commit_timestamps: HashMap<u64, u64>,
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active_transaction: None,
            next_txn_id: 1,
            global_timestamp: 1,
            default_isolation_level: IsolationLevel::default(),
            committed_txns: HashSet::new(),
            commit_timestamps: HashMap::new(),
        }
    }

    fn next_timestamp(&mut self) -> u64 {
        let ts = self.global_timestamp;
        self.global_timestamp += 1;
        ts
    }

    pub fn is_active(&self) -> bool {
        self.active_transaction.is_some()
    }

    pub fn is_committed(&self, txn_id: u64) -> bool {
        self.committed_txns.contains(&txn_id)
    }

    pub fn get_commit_timestamp(&self, txn_id: u64) -> Option<u64> {
        self.commit_timestamps.get(&txn_id).copied()
    }

    pub fn commit_timestamps(&self) -> &HashMap<u64, u64> {
        &self.commit_timestamps
    }

    pub fn has_savepoints(&self) -> bool {
        self.active_transaction
            .as_ref()
            .map(|ctx| !ctx.transaction.savepoints.is_empty())
            .unwrap_or(false)
    }

    pub fn get_active_transaction(&self) -> Option<&Transaction> {
        self.active_transaction.as_ref().map(|ctx| &ctx.transaction)
    }

    pub fn get_active_transaction_mut(&mut self) -> Option<&mut Transaction> {
        self.active_transaction
            .as_mut()
            .map(|ctx| &mut ctx.transaction)
    }

    pub fn set_default_isolation_level(&mut self, level: IsolationLevel) {
        self.default_isolation_level = level;
    }

    pub fn mark_transaction_written(&mut self) {
        if let Some(ctx) = &mut self.active_transaction {
            ctx.transaction.mark_written();
        }
    }

    pub fn default_isolation_level(&self) -> IsolationLevel {
        self.default_isolation_level
    }

    fn no_transaction_error(operation: &str) -> yachtsql_core::error::Error {
        yachtsql_core::error::Error::NoActiveTransaction {
            operation: operation.to_string(),
        }
    }

    fn aborted_transaction_error(operation: &str) -> yachtsql_core::error::Error {
        yachtsql_core::error::Error::TransactionAborted {
            operation: operation.to_string(),
        }
    }

    pub fn begin(&mut self, storage: &mut Storage) -> Result<u64> {
        self.begin_scoped(storage, TransactionScope::Explicit)
    }

    pub fn begin_scoped(&mut self, storage: &mut Storage, scope: TransactionScope) -> Result<u64> {
        if self.active_transaction.is_some() {
            return Err(yachtsql_core::error::Error::InvalidOperation(
                "Cannot BEGIN TRANSACTION: a transaction is already active. \
                 Use COMMIT or ROLLBACK to end the current transaction first."
                    .to_string(),
            ));
        }

        let txn_id = self.next_txn_id;
        let start_timestamp = self.next_timestamp();

        let transaction = Transaction::new(
            txn_id,
            start_timestamp,
            storage,
            self.default_isolation_level,
        );

        self.next_txn_id += 1;
        self.active_transaction = Some(ActiveTransactionContext { scope, transaction });

        Ok(txn_id)
    }

    pub fn begin_with_isolation(
        &mut self,
        storage: &mut Storage,
        isolation_level: IsolationLevel,
    ) -> Result<u64> {
        self.begin_with_isolation_scoped(storage, isolation_level, TransactionScope::Explicit)
    }

    pub fn begin_with_isolation_scoped(
        &mut self,
        storage: &mut Storage,
        isolation_level: IsolationLevel,
        scope: TransactionScope,
    ) -> Result<u64> {
        if self.active_transaction.is_some() {
            return Err(yachtsql_core::error::Error::InvalidOperation(
                "Cannot BEGIN: transaction already active".to_string(),
            ));
        }

        let txn_id = self.next_txn_id;
        let start_timestamp = self.next_timestamp();
        let transaction = Transaction::new(txn_id, start_timestamp, storage, isolation_level);

        self.next_txn_id += 1;
        self.active_transaction = Some(ActiveTransactionContext { scope, transaction });

        Ok(txn_id)
    }

    pub fn begin_with_characteristics(
        &mut self,
        storage: &mut Storage,
        characteristics: TransactionCharacteristics,
        scope: TransactionScope,
    ) -> Result<u64> {
        if self.active_transaction.is_some() {
            return Err(yachtsql_core::error::Error::InvalidOperation(
                "Cannot BEGIN: transaction already active".to_string(),
            ));
        }

        let txn_id = self.next_txn_id;
        let start_timestamp = self.next_timestamp();

        let isolation = characteristics
            .isolation_level
            .unwrap_or(self.default_isolation_level);
        let read_only = characteristics.read_only.unwrap_or(false);
        let deferrable = characteristics.deferrable.unwrap_or(false);

        let transaction = Transaction::with_characteristics(
            txn_id,
            start_timestamp,
            storage,
            isolation,
            read_only,
            deferrable,
        );

        self.next_txn_id += 1;
        self.active_transaction = Some(ActiveTransactionContext { scope, transaction });

        Ok(txn_id)
    }

    pub fn get_current_characteristics(&self) -> Option<TransactionCharacteristics> {
        self.active_transaction
            .as_ref()
            .map(|ctx| TransactionCharacteristics {
                isolation_level: Some(ctx.transaction.isolation_level),
                read_only: Some(ctx.transaction.read_only),
                deferrable: Some(ctx.transaction.deferrable),
            })
    }

    fn apply_deltas(&self, storage: &mut Storage, changes: &PendingChanges) -> Result<()> {
        for table_name in changes.dropped_tables() {
            if let Some(dot_pos) = table_name.find('.') {
                let dataset_name = &table_name[..dot_pos];
                let table_id = &table_name[dot_pos + 1..];

                if let Some(dataset) = storage.get_dataset_mut(dataset_name) {
                    dataset.delete_table(table_id)?;
                }
            }
        }

        for (table_name, schema) in changes.created_tables() {
            if let Some(dot_pos) = table_name.find('.') {
                let dataset_name = &table_name[..dot_pos];
                let table_id = &table_name[dot_pos + 1..];

                if storage.get_dataset(dataset_name).is_none() {
                    storage.create_dataset(dataset_name.to_string())?;
                }

                let dataset = storage.get_dataset_mut(dataset_name).ok_or_else(|| {
                    yachtsql_core::error::Error::InvalidOperation(format!(
                        "Dataset '{}' not found",
                        dataset_name
                    ))
                })?;

                dataset.create_table(table_id.to_string(), schema.clone())?;
            }
        }

        for table_name in changes.modified_table_names() {
            if let Some(delta) = changes.get_table_delta(table_name) {
                self.apply_table_delta(storage, table_name, delta)?;
            }
        }

        Ok(())
    }

    fn apply_table_delta(
        &self,
        storage: &mut Storage,
        table_name: &str,
        delta: &TableDelta,
    ) -> Result<()> {
        let (dataset_name, table_id) = if let Some(dot_pos) = table_name.find('.') {
            let dataset = &table_name[..dot_pos];
            let table = &table_name[dot_pos + 1..];
            (dataset, table)
        } else {
            ("default", table_name)
        };

        let current_rows = {
            let dataset = storage.get_dataset(dataset_name).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found",
                    dataset_name
                ))
            })?;
            let table = dataset.get_table(table_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}' not found in dataset '{}'",
                    table_id, dataset_name
                ))
            })?;
            table.get_all_rows()
        };

        let mut new_rows = Vec::new();

        for (idx, row) in current_rows.into_iter().enumerate() {
            if delta.deleted_rows.contains(&idx) {
                continue;
            }

            if let Some(updated_row) = delta.updated_rows.get(&idx) {
                new_rows.push(updated_row.clone());
            } else {
                new_rows.push(row);
            }
        }

        new_rows.extend(delta.inserted_rows.iter().cloned());

        let dataset = storage.get_dataset_mut(dataset_name).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Dataset '{}' not found",
                dataset_name
            ))
        })?;
        let table = dataset.get_table_mut(table_id).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Table '{}' not found in dataset '{}'",
                table_id, dataset_name
            ))
        })?;

        table.clear_rows()?;
        for row in new_rows {
            table.insert_row(row)?;
        }

        Ok(())
    }

    pub fn commit(&mut self, storage: &mut Storage) -> Result<()> {
        if self.active_transaction.is_none() {
            return Ok(());
        }

        let is_aborted = self
            .active_transaction
            .as_ref()
            .map(|ctx| ctx.transaction.is_aborted())
            .unwrap_or(false);

        let context = self
            .active_transaction
            .take()
            .ok_or_else(|| Self::no_transaction_error("COMMIT"))?;

        if is_aborted {
            return Err(Self::aborted_transaction_error("COMMIT"));
        }

        let txn_id = context.transaction.txn_id;
        self.committed_txns.insert(txn_id);

        let commit_timestamp = self.next_timestamp();
        self.commit_timestamps.insert(txn_id, commit_timestamp);

        if context.transaction.has_written {
            if let Some(changes) = &context.transaction.pending_changes {
                if !changes.is_empty() {
                    self.apply_deltas(storage, changes)?;
                }
            } else {
                *storage = context.transaction.snapshot;
            }
        }

        Ok(())
    }

    pub fn rollback(&mut self, storage: &mut Storage) -> Result<()> {
        if self.active_transaction.is_none() {
            return Ok(());
        }

        let context = self
            .active_transaction
            .take()
            .ok_or_else(|| Self::no_transaction_error("ROLLBACK"))?;

        if let Some(changes) = context.transaction.pending_changes() {
            for table_name in changes.created_tables().keys() {
                if let Some(dot_pos) = table_name.find('.') {
                    let dataset_name = &table_name[..dot_pos];
                    let table_id = &table_name[dot_pos + 1..];

                    if let Some(dataset) = storage.get_dataset_mut(dataset_name) {
                        let _ = dataset.delete_table(table_id);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn savepoint(&mut self, name: String) -> Result<()> {
        let context = self
            .active_transaction
            .as_mut()
            .ok_or_else(|| Self::no_transaction_error("SAVEPOINT"))?;

        if context.transaction.is_aborted() {
            return Err(Self::aborted_transaction_error("SAVEPOINT"));
        }

        context.transaction.create_savepoint(name);
        Ok(())
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let context = self
            .active_transaction
            .as_mut()
            .ok_or_else(|| Self::no_transaction_error("ROLLBACK TO SAVEPOINT"))?;

        context.transaction.rollback_to_savepoint(name)?;
        context.transaction.mark_active();
        Ok(())
    }

    pub fn rollback_to_latest_savepoint(&mut self) -> Result<()> {
        let context = self
            .active_transaction
            .as_mut()
            .ok_or_else(|| Self::no_transaction_error("ROLLBACK TO SAVEPOINT"))?;

        let latest_savepoint_name = context
            .transaction
            .savepoints
            .last()
            .map(|sp| sp.name.clone())
            .ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(
                    "No savepoints to rollback to".to_string(),
                )
            })?;

        context
            .transaction
            .rollback_to_savepoint(&latest_savepoint_name)?;
        context.transaction.mark_active();
        Ok(())
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        let context = self
            .active_transaction
            .as_mut()
            .ok_or_else(|| Self::no_transaction_error("RELEASE SAVEPOINT"))?;

        if context.transaction.is_aborted() {
            return Err(Self::aborted_transaction_error("RELEASE SAVEPOINT"));
        }

        context.transaction.release_savepoint(name)
    }

    pub fn get_snapshot(&self) -> Option<&Storage> {
        self.active_transaction
            .as_ref()
            .map(|ctx| ctx.transaction.get_snapshot())
    }

    pub fn force_abort(&mut self) {
        self.active_transaction = None;
    }

    pub fn mark_aborted(&mut self) {
        if let Some(ctx) = self.active_transaction.as_mut() {
            ctx.transaction.mark_aborted();
        }
    }

    pub fn is_aborted(&self) -> bool {
        self.active_transaction
            .as_ref()
            .map(|ctx| ctx.transaction.is_aborted())
            .unwrap_or(false)
    }

    pub fn active_scope(&self) -> Option<TransactionScope> {
        self.active_transaction.as_ref().map(|ctx| ctx.scope)
    }

    pub fn set_active_scope(&mut self, scope: TransactionScope) {
        if let Some(ctx) = self.active_transaction.as_mut() {
            ctx.scope = scope;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_manager_begin_commit() {
        let mut storage = Storage::new();
        let mut tm = TransactionManager::new();

        assert!(!tm.is_active());

        let txn_id = tm.begin(&mut storage).unwrap();
        assert_eq!(txn_id, 1);
        assert!(tm.is_active());

        tm.commit(&mut storage).unwrap();
        assert!(!tm.is_active());
    }

    #[test]
    fn test_transaction_manager_begin_rollback() {
        let mut storage = Storage::new();
        let mut tm = TransactionManager::new();

        tm.begin(&mut storage).unwrap();
        assert!(tm.is_active());

        tm.rollback(&mut storage).unwrap();
        assert!(!tm.is_active());
    }

    #[test]
    fn test_transaction_manager_double_begin_fails() {
        let mut storage = Storage::new();
        let mut tm = TransactionManager::new();

        tm.begin(&mut storage).unwrap();
        let result = tm.begin(&mut storage);
        assert!(result.is_err());
    }

    #[test]
    fn test_savepoint_operations() {
        let storage = Storage::new();
        let mut txn = Transaction::new(1, 1, &storage, IsolationLevel::ReadCommitted);

        txn.create_savepoint("sp1".to_string());
        assert_eq!(txn.savepoints.len(), 1);

        txn.create_savepoint("sp2".to_string());
        assert_eq!(txn.savepoints.len(), 2);

        txn.rollback_to_savepoint("sp1").unwrap();
        assert_eq!(txn.savepoints.len(), 1);

        txn.release_savepoint("sp1").unwrap();
        assert_eq!(txn.savepoints.len(), 0);
    }

    #[test]
    fn test_commit_timestamp_ordering() {
        let mut storage1 = Storage::new();
        let mut storage2 = Storage::new();
        let mut tm = TransactionManager::new();

        let txn1_id = tm.begin(&mut storage1).unwrap();
        assert_eq!(txn1_id, 1, "T1 should have ID 1");

        tm.rollback(&mut storage1).unwrap();

        let txn2_id = tm.begin(&mut storage2).unwrap();
        assert_eq!(txn2_id, 2, "T2 should have ID 2");

        tm.commit(&mut storage2).unwrap();

        assert!(tm.is_committed(txn2_id), "T2 should be committed");

        let t2_commit_ts = tm.get_commit_timestamp(txn2_id);
        assert!(t2_commit_ts.is_some(), "T2 should have commit timestamp");

        let txn1_new_id = tm.begin(&mut storage1).unwrap();
        assert_eq!(txn1_new_id, 3);

        tm.commit(&mut storage1).unwrap();

        let t1_commit_ts = tm.get_commit_timestamp(txn1_new_id);
        assert!(t1_commit_ts.is_some(), "T1 should have commit timestamp");

        assert!(
            t2_commit_ts.unwrap() < t1_commit_ts.unwrap(),
            "T2's commit timestamp should be less than T1's commit timestamp"
        );

        assert!(tm.is_committed(txn2_id), "T2 should remain committed");
        assert!(tm.is_committed(txn1_new_id), "T1 should be committed");
    }

    #[test]
    fn test_commit_timestamps_map_access() {
        let mut storage = Storage::new();
        let mut tm = TransactionManager::new();

        for _ in 0..3 {
            let txn_id = tm.begin(&mut storage).unwrap();
            tm.commit(&mut storage).unwrap();

            let commit_ts = tm.get_commit_timestamp(txn_id);
            assert!(
                commit_ts.is_some(),
                "Transaction {} should have commit timestamp",
                txn_id
            );
        }

        let commit_timestamps = tm.commit_timestamps();
        assert_eq!(
            commit_timestamps.len(),
            3,
            "Should have 3 commit timestamps"
        );

        let mut timestamps: Vec<_> = commit_timestamps.values().copied().collect();
        timestamps.sort();
        for pair in timestamps.windows(2) {
            assert!(
                pair[1] > pair[0],
                "Timestamps should be monotonically increasing"
            );
        }
    }
}
