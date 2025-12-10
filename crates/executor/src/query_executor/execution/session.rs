use std::collections::HashMap;
use std::rc::Rc;

use sqlparser::ast::Query;
use yachtsql_capability::FeatureRegistry;
use yachtsql_core::diagnostics::DiagnosticArea;
use yachtsql_core::diagnostics::sqlstate::SqlState;
use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};
use yachtsql_parser::DialectType;
use yachtsql_storage::ExtensionRegistry;

use crate::Table;

const NO_EXCEPTION_DIAGNOSTIC_MESSAGE: &str =
    "No exception diagnostic available; the most recent statement completed successfully";

#[derive(Debug, Clone)]
pub struct SessionVariable {
    pub data_type: DataType,
    pub value: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct UdfDefinition {
    pub parameters: Vec<String>,
    pub body: sqlparser::ast::Expr,
}

#[derive(Debug, Clone)]
pub struct CursorState {
    pub name: String,
    pub query: Box<Query>,
    pub result_set: Option<Table>,
    pub position: i64,
    pub is_scrollable: bool,
    pub with_hold: bool,
    pub is_binary: bool,
}

#[derive(Debug)]
pub struct SessionState {
    dialect: DialectType,
    diagnostics: SessionDiagnostics,
    feature_registry: Rc<FeatureRegistry>,
    function_registry: Rc<crate::functions::FunctionRegistry>,
    feature_registry_snapshot: Option<Rc<FeatureRegistry>>,
    extension_registry: ExtensionRegistry,
    search_path: Vec<String>,
    variables: HashMap<String, SessionVariable>,
    udfs: HashMap<String, UdfDefinition>,
    cursors: HashMap<String, CursorState>,
}

impl SessionState {
    pub fn new(dialect: DialectType) -> Self {
        let registry = Rc::new(FeatureRegistry::with_default_features(dialect));
        Self::with_registry(dialect, registry)
    }

    pub fn with_registry(dialect: DialectType, feature_registry: Rc<FeatureRegistry>) -> Self {
        Self {
            dialect,
            diagnostics: SessionDiagnostics::new(),
            feature_registry,
            function_registry: Rc::new(crate::functions::FunctionRegistry::new()),
            feature_registry_snapshot: None,
            extension_registry: ExtensionRegistry::new(),
            search_path: vec!["default".to_string()],
            variables: HashMap::new(),
            udfs: HashMap::new(),
            cursors: HashMap::new(),
        }
    }

    pub fn dialect(&self) -> DialectType {
        self.dialect
    }

    pub fn feature_registry(&self) -> &Rc<FeatureRegistry> {
        &self.feature_registry
    }

    #[allow(dead_code)]
    pub fn feature_registry_mut(&mut self) -> &mut Rc<FeatureRegistry> {
        &mut self.feature_registry
    }

    pub fn set_feature_registry(&mut self, registry: Rc<FeatureRegistry>) {
        self.feature_registry = registry;
    }

    pub fn function_registry(&self) -> &Rc<crate::functions::FunctionRegistry> {
        &self.function_registry
    }

    #[allow(dead_code)]
    pub fn diagnostics(&self) -> &SessionDiagnostics {
        &self.diagnostics
    }

    pub fn diagnostics_mut(&mut self) -> &mut SessionDiagnostics {
        &mut self.diagnostics
    }

    pub fn diagnostics_snapshot(&self) -> DiagnosticsSnapshot {
        self.diagnostics.snapshot()
    }

    pub fn reset_diagnostics(&mut self) {
        self.diagnostics = SessionDiagnostics::new();
    }

    pub fn snapshot_feature_registry(&mut self) {
        self.feature_registry_snapshot = Some(Rc::clone(&self.feature_registry));
    }

    pub fn restore_feature_registry_snapshot(&mut self) {
        if let Some(snapshot) = self.feature_registry_snapshot.take() {
            self.feature_registry = snapshot;
        }
    }

    pub fn clear_feature_registry_snapshot(&mut self) {
        self.feature_registry_snapshot = None;
    }

    pub fn extension_registry(&self) -> &ExtensionRegistry {
        &self.extension_registry
    }

    pub fn extension_registry_mut(&mut self) -> &mut ExtensionRegistry {
        &mut self.extension_registry
    }

    pub fn search_path(&self) -> &[String] {
        &self.search_path
    }

    pub fn set_search_path(&mut self, schemas: Vec<String>) {
        self.search_path = schemas;
    }

    pub fn current_schema(&self) -> &str {
        self.search_path
            .first()
            .map(|s| s.as_str())
            .unwrap_or("default")
    }

    pub fn declare_variable(
        &mut self,
        name: String,
        data_type: DataType,
        default_value: Option<Value>,
    ) {
        self.variables.insert(
            name,
            SessionVariable {
                data_type,
                value: default_value,
            },
        );
    }

    pub fn set_variable(&mut self, name: &str, value: Value) -> Result<(), Error> {
        if let Some(var) = self.variables.get_mut(name) {
            var.value = Some(value);
        } else {
            self.variables.insert(
                name.to_string(),
                SessionVariable {
                    data_type: value.data_type(),
                    value: Some(value),
                },
            );
        }
        Ok(())
    }

    pub fn get_variable(&self, name: &str) -> Option<&SessionVariable> {
        self.variables.get(name)
    }

    pub fn variables(&self) -> &HashMap<String, SessionVariable> {
        &self.variables
    }

    pub fn push_variable_scope(&mut self) {}

    pub fn pop_variable_scope(&mut self) {}

    pub fn register_udf(&mut self, name: String, definition: UdfDefinition) {
        debug_print::debug_eprintln!("[session] Registering UDF: {}", name.to_uppercase());
        self.udfs.insert(name.to_uppercase(), definition);
    }

    pub fn get_udf(&self, name: &str) -> Option<&UdfDefinition> {
        self.udfs.get(&name.to_uppercase())
    }

    pub fn has_udf(&self, name: &str) -> bool {
        self.udfs.contains_key(&name.to_uppercase())
    }

    pub fn drop_udf(&mut self, name: &str) -> bool {
        self.udfs.remove(&name.to_uppercase()).is_some()
    }

    pub fn udf_names(&self) -> std::collections::HashSet<String> {
        let names: std::collections::HashSet<String> = self.udfs.keys().cloned().collect();
        debug_print::debug_eprintln!("[session] Getting UDF names: {:?}", names);
        names
    }

    pub fn udfs(&self) -> &HashMap<String, UdfDefinition> {
        &self.udfs
    }

    pub fn udfs_for_parser(&self) -> HashMap<String, yachtsql_parser::UdfDefinition> {
        self.udfs
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    yachtsql_parser::UdfDefinition {
                        parameters: v.parameters.clone(),
                        body: v.body.clone(),
                    },
                )
            })
            .collect()
    }

    pub fn declare_cursor(&mut self, cursor: CursorState) -> Result<(), Error> {
        let name = cursor.name.to_uppercase();
        if self.cursors.contains_key(&name) {
            return Err(Error::duplicate_cursor(format!(
                "cursor \"{}\" already exists",
                cursor.name
            )));
        }
        self.cursors.insert(name, cursor);
        Ok(())
    }

    pub fn get_cursor(&self, name: &str) -> Option<&CursorState> {
        self.cursors.get(&name.to_uppercase())
    }

    pub fn get_cursor_mut(&mut self, name: &str) -> Option<&mut CursorState> {
        self.cursors.get_mut(&name.to_uppercase())
    }

    pub fn close_cursor(&mut self, name: &str) -> Result<(), Error> {
        let key = name.to_uppercase();
        if self.cursors.remove(&key).is_none() {
            return Err(Error::invalid_cursor_name(format!(
                "cursor \"{}\" does not exist",
                name
            )));
        }
        Ok(())
    }

    pub fn close_all_cursors(&mut self) {
        self.cursors.clear();
    }

    pub fn cursors(&self) -> &HashMap<String, CursorState> {
        &self.cursors
    }

    pub fn clear_non_holdable_cursors(&mut self) {
        self.cursors.retain(|_, cursor| cursor.with_hold);
    }
}

#[derive(Debug, Default)]
pub struct SessionDiagnostics {
    #[allow(dead_code)]
    diagnostic_area: DiagnosticArea,
    pending_row_count: Option<usize>,
    exception_diagnostic: Option<DiagnosticArea>,
}

impl SessionDiagnostics {
    pub fn new() -> Self {
        Self {
            diagnostic_area: DiagnosticArea::success(),
            pending_row_count: None,
            exception_diagnostic: None,
        }
    }

    pub fn diagnostic_area(&self) -> &DiagnosticArea {
        &self.diagnostic_area
    }

    pub fn pending_row_count(&self) -> Option<usize> {
        self.pending_row_count
    }

    pub fn exception_diagnostic(&self) -> Option<&DiagnosticArea> {
        self.exception_diagnostic.as_ref()
    }

    pub fn clear_exception(&mut self) {
        self.exception_diagnostic = None;
    }

    pub fn record_row_count(&mut self, count: usize) {
        self.pending_row_count = Some(count);
    }

    pub fn record_success(&mut self, batch: Option<&Table>, clear_exception: bool) {
        let mut diag = DiagnosticArea::success();
        let mut applied_row_count = false;

        if let Some(count) = self.pending_row_count.take() {
            diag = diag.with_row_count(count);
            applied_row_count = true;
        } else if let Some(batch) = batch
            && let Some(count) = Self::rows_affected_from_batch(batch)
        {
            diag = diag.with_row_count(count);
            applied_row_count = true;
        }

        if !applied_row_count {
            diag = diag.with_row_count(0);
        }

        if clear_exception {
            self.clear_exception();
        }

        self.diagnostic_area = diag;
    }

    pub fn record_error(&mut self, error: &Error) {
        let mut diag = DiagnosticArea::error(SqlState::from(error.sqlstate()), error.to_string());
        if let Some(detail) = error.details() {
            diag = diag.with_detail(detail);
        }

        if Self::should_preserve_exception(error) {
            self.diagnostic_area = diag;
            self.pending_row_count = None;
            return;
        }

        self.exception_diagnostic = Some(diag.clone());
        self.diagnostic_area = diag;
        self.pending_row_count = None;
    }

    pub fn snapshot(&self) -> DiagnosticsSnapshot {
        DiagnosticsSnapshot {
            diagnostic_area: self.diagnostic_area.clone(),
            pending_row_count: self.pending_row_count,
            exception_diagnostic: self.exception_diagnostic.clone(),
        }
    }

    fn rows_affected_from_batch(batch: &Table) -> Option<usize> {
        let column = batch.column_by_name("rows_affected")?;
        let value = column.get(0).ok()?;
        if let Some(i) = value.as_i64() {
            if i >= 0 {
                return Some(i as usize);
            }
        }
        None
    }

    fn should_preserve_exception(error: &Error) -> bool {
        matches!(
            error,
            Error::InvalidOperation(message)
                if message.contains(NO_EXCEPTION_DIAGNOSTIC_MESSAGE)
        )
    }
}

#[derive(Debug, Clone)]
pub struct DiagnosticsSnapshot {
    diagnostic_area: DiagnosticArea,
    pending_row_count: Option<usize>,
    exception_diagnostic: Option<DiagnosticArea>,
}

impl DiagnosticsSnapshot {
    pub fn diagnostic_area(&self) -> &DiagnosticArea {
        &self.diagnostic_area
    }

    pub fn pending_row_count(&self) -> Option<usize> {
        self.pending_row_count
    }

    pub fn exception_diagnostic(&self) -> Option<&DiagnosticArea> {
        self.exception_diagnostic.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_storage::{Field, Schema};

    use super::*;

    #[test]
    fn session_state_initializes_with_defaults() {
        let state = SessionState::new(DialectType::PostgreSQL);
        assert_eq!(state.dialect(), DialectType::PostgreSQL);
        assert!(state.diagnostics().pending_row_count().is_none());
        assert!(state.diagnostics().exception_diagnostic().is_none());
    }

    #[test]
    fn diagnostics_record_and_clear_row_counts() {
        let mut state = SessionState::new(DialectType::BigQuery);
        state.diagnostics_mut().record_row_count(42);
        assert_eq!(state.diagnostics().pending_row_count(), Some(42));
        state.diagnostics_mut().record_success(None, true);
        assert_eq!(
            state
                .diagnostics()
                .diagnostic_area()
                .row_count()
                .expect("row count"),
            42
        );
        assert!(state.diagnostics().pending_row_count().is_none());
    }

    #[test]
    fn rows_affected_column_updates_row_count() {
        let mut diagnostics = SessionDiagnostics::new();
        let schema = Schema::from_fields(vec![Field::required(
            "rows_affected".to_string(),
            DataType::Int64,
        )]);
        let batch = Table::from_values(schema, vec![vec![Value::int64(5)]]).expect("batch build");

        diagnostics.record_success(Some(&batch), false);
        assert_eq!(
            diagnostics
                .diagnostic_area()
                .row_count()
                .expect("row count"),
            5
        );
    }

    #[test]
    fn record_error_sets_exception_diagnostic() {
        let mut diagnostics = SessionDiagnostics::new();
        diagnostics.record_row_count(10);
        diagnostics.record_error(&Error::invalid_query("syntax error"));

        assert!(diagnostics.pending_row_count().is_none());
        let snapshot = diagnostics.snapshot();
        assert!(snapshot.exception_diagnostic().is_some());
        assert!(snapshot.diagnostic_area().sqlstate().is_error());
    }
}
