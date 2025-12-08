use debug_print::debug_eprintln;
use yachtsql_core::error::{Error, Result};
use yachtsql_storage::TransactionScope;

use super::QueryExecutor;

#[derive(Debug, Clone)]
pub struct SessionTransactionController {
    autocommit: bool,
}

impl SessionTransactionController {
    pub fn new() -> Self {
        Self { autocommit: true }
    }

    pub fn with_autocommit(autocommit: bool) -> Self {
        Self { autocommit }
    }

    pub fn autocommit(&self) -> bool {
        self.autocommit
    }

    pub fn set_autocommit(&mut self, value: bool) {
        self.autocommit = value;
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementMetadata {
    requires_transaction: bool,
    allowed_when_aborted: bool,
    savepoint_management: bool,
}

#[allow(dead_code)]
impl StatementMetadata {
    pub fn new(requires_transaction: bool) -> Self {
        Self {
            requires_transaction,
            allowed_when_aborted: false,
            savepoint_management: false,
        }
    }

    pub fn builder() -> StatementMetadataBuilder {
        StatementMetadataBuilder::new()
    }

    pub fn transactional() -> Self {
        Self::new(true)
    }

    pub fn transaction_control() -> Self {
        Self {
            requires_transaction: false,
            allowed_when_aborted: true,
            savepoint_management: false,
        }
    }

    pub fn savepoint_management() -> Self {
        Self {
            requires_transaction: false,
            allowed_when_aborted: true,
            savepoint_management: true,
        }
    }

    pub fn requires_transaction(&self) -> bool {
        self.requires_transaction
    }

    pub fn allowed_when_aborted(&self) -> bool {
        self.allowed_when_aborted
    }

    pub fn is_savepoint_management(&self) -> bool {
        self.savepoint_management
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct StatementMetadataBuilder {
    requires_transaction: bool,
    allowed_when_aborted: bool,
    savepoint_management: bool,
}

#[allow(dead_code)]
impl StatementMetadataBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn requires_transaction(mut self, value: bool) -> Self {
        self.requires_transaction = value;
        self
    }

    pub fn allowed_when_aborted(mut self, value: bool) -> Self {
        self.allowed_when_aborted = value;
        self
    }

    pub fn savepoint_management(mut self, value: bool) -> Self {
        self.savepoint_management = value;
        self
    }

    pub fn build(self) -> StatementMetadata {
        StatementMetadata {
            requires_transaction: self.requires_transaction,
            allowed_when_aborted: self.allowed_when_aborted,
            savepoint_management: self.savepoint_management,
        }
    }
}

impl QueryExecutor {
    #[allow(dead_code)]
    pub(crate) fn session_autocommit(&self) -> bool {
        self.session_tx.autocommit()
    }

    pub(crate) fn begin_transaction_with_scope(&mut self, scope: TransactionScope) -> Result<()> {
        let mut storage = self.storage.borrow_mut();
        let mut manager = self.transaction_manager.borrow_mut();
        manager.begin_scoped(&mut storage, scope).map(|_| ())
    }

    pub(crate) fn ensure_autocommit_off_transaction(&mut self) -> Result<()> {
        if self.session_tx.autocommit() {
            return Ok(());
        }

        let needs_new_transaction = {
            let mut manager = self.transaction_manager.borrow_mut();
            match manager.active_scope() {
                Some(TransactionScope::ImplicitSession) | Some(TransactionScope::Explicit) => false,
                Some(TransactionScope::ImplicitAutocommit) => {
                    manager.set_active_scope(TransactionScope::ImplicitSession);
                    false
                }
                None => true,
            }
        };

        if needs_new_transaction {
            self.begin_transaction_with_scope(TransactionScope::ImplicitSession)?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn begin_implicit_transaction_if_needed(
        &mut self,
        metadata: Option<&StatementMetadata>,
    ) -> Result<()> {
        {
            let manager = self.transaction_manager.borrow_mut();
            if manager.is_active() {
                return Ok(());
            }
        }

        let requires_transaction = metadata
            .map(StatementMetadata::requires_transaction)
            .unwrap_or(true);

        if !requires_transaction {
            return Ok(());
        }

        let scope = if self.session_tx.autocommit() {
            TransactionScope::ImplicitAutocommit
        } else {
            TransactionScope::ImplicitSession
        };
        self.begin_transaction_with_scope(scope)
    }

    #[allow(dead_code)]
    pub(crate) fn handle_statement_error(
        &mut self,
        metadata: Option<&StatementMetadata>,
        preserve_diagnostics: bool,
    ) {
        if metadata
            .map(StatementMetadata::is_savepoint_management)
            .unwrap_or(false)
        {
            return;
        }

        if preserve_diagnostics {
            return;
        }

        let mut needs_session_txn = false;

        {
            let mut manager = self.transaction_manager.borrow_mut();

            if !manager.is_active() {
                return;
            }

            if manager.has_savepoints()
                && let Err(err) = manager.rollback_to_latest_savepoint()
            {
                debug_eprintln!(
                    "[executor::transaction] Warning: savepoint rollback failed ({}), marking transaction aborted",
                    err
                );
            }

            match manager.active_scope() {
                Some(TransactionScope::ImplicitAutocommit) => {
                    manager.force_abort();
                }
                Some(TransactionScope::ImplicitSession) | Some(TransactionScope::Explicit) => {
                    manager.mark_aborted();
                }
                None => {
                    if self.session_tx.autocommit() {
                        manager.force_abort();
                    } else {
                        manager.mark_aborted();
                    }
                }
            }

            if !manager.is_active() && !self.session_tx.autocommit() {
                needs_session_txn = true;
            }
        }

        if needs_session_txn {
            let _ = self.ensure_autocommit_off_transaction();
        }
    }

    #[allow(dead_code)]
    pub(crate) fn implicit_commit_for_ddl(&mut self) -> Result<()> {
        self.commit_active_implicit_transaction()
    }

    #[allow(dead_code)]
    pub(crate) fn finalize_ddl_transaction(&mut self) -> Result<()> {
        self.commit_active_implicit_transaction()?;
        if !self.session_tx.autocommit() {
            self.ensure_autocommit_off_transaction()?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn commit_active_implicit_transaction(&mut self) -> Result<()> {
        let needs_commit = {
            let manager = self.transaction_manager.borrow_mut();
            matches!(
                manager.active_scope(),
                Some(TransactionScope::ImplicitSession | TransactionScope::ImplicitAutocommit)
            )
        };

        if needs_commit {
            self.commit_active_transaction()
        } else {
            Ok(())
        }
    }

    pub(crate) fn commit_active_transaction(&mut self) -> Result<()> {
        let mut storage = self.storage.borrow_mut();
        let mut manager = self.transaction_manager.borrow_mut();
        manager.commit(&mut storage)
    }

    pub(crate) fn rollback_active_transaction(&mut self) -> Result<()> {
        let mut storage = self.storage.borrow_mut();
        let mut manager = self.transaction_manager.borrow_mut();
        manager.rollback(&mut storage)
    }

    pub(crate) fn explicit_transaction_active(&self) -> bool {
        let manager = self.transaction_manager.borrow_mut();
        manager.is_active() && matches!(manager.active_scope(), Some(TransactionScope::Explicit))
    }

    pub(crate) fn enable_autocommit(&mut self) -> Result<()> {
        {
            let manager = self.transaction_manager.borrow_mut();
            if manager.is_aborted() {
                return Err(Error::TransactionAborted {
                    operation: Self::autocommit_operation(true).to_string(),
                });
            }
        }

        if self.explicit_transaction_active() {
            self.session_tx.set_autocommit(true);
            return Ok(());
        }

        let should_rollback = {
            let manager = self.transaction_manager.borrow_mut();
            manager.is_active()
                && matches!(
                    manager.active_scope(),
                    Some(TransactionScope::ImplicitAutocommit | TransactionScope::ImplicitSession)
                )
        };

        if should_rollback {
            self.rollback_active_transaction()?;
        }

        self.session_tx.set_autocommit(true);
        Ok(())
    }

    pub(crate) fn disable_autocommit(&mut self) -> Result<()> {
        self.session_tx.set_autocommit(false);

        let needs_new_transaction = {
            let mut manager = self.transaction_manager.borrow_mut();
            match manager.active_scope() {
                Some(TransactionScope::Explicit) | Some(TransactionScope::ImplicitSession) => false,
                Some(TransactionScope::ImplicitAutocommit) => {
                    manager.set_active_scope(TransactionScope::ImplicitSession);
                    false
                }
                None => true,
            }
        };

        if needs_new_transaction {
            self.begin_transaction_with_scope(TransactionScope::ImplicitSession)?;
        }

        Ok(())
    }

    fn autocommit_operation(value: bool) -> &'static str {
        if value {
            "SET SESSION AUTOCOMMIT = ON"
        } else {
            "SET SESSION AUTOCOMMIT = OFF"
        }
    }

    pub fn execute_begin_transaction(&mut self) -> Result<()> {
        if self.explicit_transaction_active() {
            return Err(Error::InvalidOperation(
                "Transaction is already active. Use COMMIT or ROLLBACK before starting a new one."
                    .to_string(),
            ));
        }

        self.begin_transaction_with_scope(TransactionScope::Explicit)?;

        self.session.snapshot_feature_registry();

        Ok(())
    }

    pub fn execute_commit_transaction(&mut self) -> Result<()> {
        {
            let manager = self.transaction_manager.borrow_mut();
            if !manager.is_active() {
                return Err(Error::InvalidOperation(
                    "No active transaction to commit".to_string(),
                ));
            }
        }

        self.validate_deferred_fk_constraints()?;

        self.commit_active_transaction()?;

        self.ensure_autocommit_off_transaction()?;

        self.apply_on_commit_actions()?;

        self.session.clear_feature_registry_snapshot();

        Ok(())
    }

    pub fn execute_rollback_transaction(&mut self) -> Result<()> {
        {
            let manager = self.transaction_manager.borrow_mut();
            if !manager.is_active() {
                return Err(Error::InvalidOperation(
                    "No active transaction to rollback".to_string(),
                ));
            }
        }

        self.rollback_active_transaction()?;

        self.ensure_autocommit_off_transaction()?;

        self.session.restore_feature_registry_snapshot();

        Ok(())
    }

    pub fn execute_savepoint(&mut self, name: String) -> Result<()> {
        let mut manager = self.transaction_manager.borrow_mut();

        if !manager.is_active() {
            return Err(Error::InvalidOperation(
                "SAVEPOINT requires an active transaction. Use BEGIN first.".to_string(),
            ));
        }

        manager.savepoint(name)?;
        Ok(())
    }

    pub fn execute_release_savepoint(&mut self, name: String) -> Result<()> {
        let mut manager = self.transaction_manager.borrow_mut();

        if !manager.is_active() {
            return Err(Error::InvalidOperation(format!(
                "Cannot release savepoint '{}': no active transaction",
                name
            )));
        }

        manager.release_savepoint(&name).map_err(|e| match e {
            Error::SavepointNotFound { .. } => {
                Error::InvalidOperation(format!("Savepoint '{}' does not exist", name))
            }
            other => other,
        })?;

        Ok(())
    }

    pub fn execute_rollback_to_savepoint(&mut self, name: String) -> Result<()> {
        let mut manager = self.transaction_manager.borrow_mut();

        if !manager.is_active() {
            return Err(Error::InvalidOperation(format!(
                "Cannot rollback to savepoint '{}': no active transaction",
                name
            )));
        }

        manager.rollback_to_savepoint(&name)?;
        Ok(())
    }

    pub fn execute_set_session_autocommit(&mut self, enabled: bool) -> Result<()> {
        self.apply_autocommit_setting(enabled)?;
        self.clear_exception_diagnostic();
        self.record_row_count(0);
        Ok(())
    }

    fn apply_autocommit_setting(&mut self, value: bool) -> Result<()> {
        {
            let manager = self.transaction_manager.borrow_mut();
            if manager.is_aborted() {
                return Err(Error::TransactionAborted {
                    operation: Self::autocommit_operation(value).to_string(),
                });
            }
        }

        let current = self.session_tx.autocommit();
        if value == current {
            return self.project_autocommit_noop(self.explicit_transaction_active(), value);
        }

        if value {
            self.enable_autocommit()
        } else {
            self.disable_autocommit()
        }
    }

    fn project_autocommit_noop(
        &self,
        explicit_txn_active: bool,
        autocommit_on: bool,
    ) -> Result<()> {
        if autocommit_on && explicit_txn_active {
            Err(Error::active_sql_transaction(Self::autocommit_operation(
                true,
            )))
        } else {
            Ok(())
        }
    }

    fn validate_deferred_fk_constraints(&self) -> Result<()> {
        let pending_checks: Vec<yachtsql_storage::DeferredFKCheck> = {
            let manager = self.transaction_manager.borrow();
            match manager.get_active_transaction() {
                Some(txn) => txn.deferred_fk_state().pending_checks().to_vec(),
                None => Vec::new(),
            }
        };

        for check in &pending_checks {
            self.validate_single_deferred_fk_check(check)?;
        }

        Ok(())
    }

    fn apply_on_commit_actions(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn execute_set_constraints(
        &mut self,
        custom_stmt: &yachtsql_parser::validator::CustomStatement,
    ) -> Result<crate::Table> {
        use yachtsql_parser::validator::{
            CustomStatement, SetConstraintsMode, SetConstraintsTarget,
        };
        use yachtsql_storage::ConstraintTiming;

        let (mode, constraints) = match custom_stmt {
            CustomStatement::SetConstraints { mode, constraints } => (mode, constraints),
            _ => {
                return Err(Error::internal(
                    "execute_set_constraints called with wrong statement type",
                ));
            }
        };

        let timing = match mode {
            SetConstraintsMode::Immediate => ConstraintTiming::Immediate,
            SetConstraintsMode::Deferred => ConstraintTiming::Deferred,
        };

        let mut manager = self.transaction_manager.borrow_mut();

        if !manager.is_active() {
            drop(manager);
            return Self::empty_result();
        }

        let txn = match manager.get_active_transaction_mut() {
            Some(t) => t,
            None => {
                drop(manager);
                return Self::empty_result();
            }
        };

        match constraints {
            SetConstraintsTarget::All => {
                txn.deferred_fk_state_mut().set_default_mode(timing);
            }
            SetConstraintsTarget::Named(names) => {
                for name in names {
                    txn.deferred_fk_state_mut().set_timing(name.clone(), timing);
                }
            }
        }

        if timing == ConstraintTiming::Immediate {
            let pending_checks: Vec<_> = txn.deferred_fk_state().pending_checks().to_vec();
            let checks_to_run: Vec<_> = match constraints {
                SetConstraintsTarget::All => pending_checks,
                SetConstraintsTarget::Named(names) => pending_checks
                    .into_iter()
                    .filter(|c| {
                        names
                            .iter()
                            .any(|n| n.eq_ignore_ascii_case(&c.constraint_name))
                    })
                    .collect(),
            };

            drop(manager);

            for check in &checks_to_run {
                self.validate_single_deferred_fk_check(check)?;
            }

            let mut manager = self.transaction_manager.borrow_mut();
            if let Some(txn) = manager.get_active_transaction_mut() {
                match constraints {
                    SetConstraintsTarget::All => {
                        txn.deferred_fk_state_mut().clear_pending_checks();
                    }
                    SetConstraintsTarget::Named(names) => {
                        txn.deferred_fk_state_mut()
                            .remove_checks_for_constraints(names);
                    }
                }
            }
        }

        Self::empty_result()
    }

    fn validate_single_deferred_fk_check(
        &self,
        check: &yachtsql_storage::DeferredFKCheck,
    ) -> Result<()> {
        use yachtsql_storage::DMLOperation;

        let storage = self.storage.borrow();

        let (parent_dataset, parent_table_name) = self.parse_table_name(&check.parent_table);
        let parent_dataset = parent_dataset.unwrap_or_else(|| "default".to_string());

        let parent_dataset_obj = storage.get_dataset(&parent_dataset).ok_or_else(|| {
            Error::DatasetNotFound(format!("Dataset '{}' not found", parent_dataset))
        })?;

        let parent_table = parent_dataset_obj
            .get_table(&parent_table_name)
            .ok_or_else(|| {
                Error::TableNotFound(format!(
                    "Table '{}.{}' not found",
                    parent_dataset, parent_table_name
                ))
            })?;

        let parent_full_name = format!("{}.{}", parent_dataset, parent_table_name);

        let pending_inserts: Vec<yachtsql_storage::Row> = {
            let manager = self.transaction_manager.borrow();
            manager
                .get_active_transaction()
                .and_then(|txn| txn.pending_changes())
                .and_then(|changes| changes.get_table_delta(&parent_full_name))
                .map(|delta| delta.inserted_rows.clone())
                .unwrap_or_default()
        };

        match check.operation {
            DMLOperation::Insert | DMLOperation::Update => {
                let committed_rows = parent_table.get_all_rows();

                let all_rows: Vec<_> = committed_rows.into_iter().chain(pending_inserts).collect();

                let mut found = false;
                for row in all_rows {
                    let mut all_match = true;
                    for (i, parent_col) in check.parent_columns.iter().enumerate() {
                        let parent_idx = parent_table
                            .schema()
                            .fields()
                            .iter()
                            .position(|f| f.name.eq_ignore_ascii_case(parent_col));

                        if let Some(idx) = parent_idx {
                            let parent_val = row.values().get(idx);
                            let fk_val = check.fk_values.get(i);

                            if parent_val != fk_val {
                                all_match = false;
                                break;
                            }
                        } else {
                            all_match = false;
                            break;
                        }
                    }

                    if all_match {
                        found = true;
                        break;
                    }
                }

                if !found {
                    let fk_display: Vec<String> =
                        check.fk_values.iter().map(|v| format!("{}", v)).collect();
                    return Err(Error::constraint_violation(format!(
                        "Foreign key constraint '{}' violated: key ({}) is not present in table '{}'",
                        check.constraint_name,
                        fk_display.join(", "),
                        check.parent_table
                    )));
                }
            }
            DMLOperation::Delete => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DialectType;
    use crate::query_executor::execution::QueryExecutor;

    #[test]
    fn disable_autocommit_opens_session_transaction() {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        executor.disable_autocommit().expect("autocommit toggle");

        let manager = executor.transaction_manager.borrow_mut();
        assert!(manager.is_active());
        assert_eq!(
            manager.active_scope(),
            Some(TransactionScope::ImplicitSession)
        );
    }

    #[test]
    fn begin_implicit_transaction_when_required() {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        let metadata = StatementMetadata::builder()
            .requires_transaction(true)
            .build();

        executor
            .begin_implicit_transaction_if_needed(Some(&metadata))
            .expect("implicit transaction");

        let manager = executor.transaction_manager.borrow_mut();
        assert!(manager.is_active());
        assert_eq!(
            manager.active_scope(),
            Some(TransactionScope::ImplicitAutocommit)
        );
    }

    #[test]
    fn handle_error_rolls_back_savepoint_and_marks_aborted() {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        executor
            .begin_transaction_with_scope(TransactionScope::Explicit)
            .expect("explicit begin");

        {
            let mut manager = executor.transaction_manager.borrow_mut();
            manager.savepoint("sp1".to_string()).expect("savepoint");
        }

        executor.handle_statement_error(None, false);

        let manager = executor.transaction_manager.borrow_mut();
        assert!(manager.is_active());
        assert!(manager.is_aborted());
    }

    #[test]
    fn implicit_autocommit_error_aborts_transaction() {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        let metadata = StatementMetadata::transactional();
        executor
            .begin_implicit_transaction_if_needed(Some(&metadata))
            .expect("implicit begin");

        executor.handle_statement_error(None, false);

        let manager = executor.transaction_manager.borrow_mut();
        assert!(!manager.is_active());
    }

    #[test]
    fn ensure_autocommit_off_transaction_recreates_session_scope() {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        executor.disable_autocommit().expect("disable");
        executor
            .commit_active_transaction()
            .expect("commit existing session txn");

        executor
            .ensure_autocommit_off_transaction()
            .expect("ensure session txn");

        let manager = executor.transaction_manager.borrow_mut();
        assert!(manager.is_active());
        assert_eq!(
            manager.active_scope(),
            Some(TransactionScope::ImplicitSession)
        );
    }
}
