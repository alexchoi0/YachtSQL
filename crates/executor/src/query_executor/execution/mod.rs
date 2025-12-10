mod cursor;
mod ddl;
mod dispatcher;
mod dml;
mod query;
mod session;
mod transaction;
mod utility;

use std::cell::RefCell;
use std::rc::Rc;
use std::str::FromStr;

use chrono::Datelike;
pub use ddl::{
    AlterTableExecutor, DdlDropExecutor, DdlExecutor, DomainExecutor, ExtensionExecutor,
    FunctionExecutor, MaterializedViewExecutor, ProcedureExecutor, SchemaExecutor,
    SequenceExecutor, SnapshotExecutor, TriggerExecutor, TypeExecutor,
};
use debug_print::debug_eprintln;
pub use dispatcher::{
    CopyOperation, CursorOperation, DdlOperation, Dispatcher, DmlOperation, MergeOperation,
    ScriptingOperation, StatementJob, TxOperation, UtilityOperation,
};
pub use dml::{
    DmlDeleteExecutor, DmlInsertExecutor, DmlMergeExecutor, DmlTruncateExecutor, DmlUpdateExecutor,
};
pub use query::QueryExecutorTrait;
use rust_decimal::prelude::ToPrimitive;
pub use session::{
    CursorState, DiagnosticsSnapshot, ProcedureDefinition, ProcedureParameter,
    ProcedureParameterMode, SessionDiagnostics, SessionVariable, UdfDefinition,
};
pub use utility::{
    add_interval_to_date, apply_interval_to_date, apply_numeric_precision_scale,
    calculate_date_diff, decode_values_match, evaluate_condition_as_bool, evaluate_numeric_op,
    evaluate_vector_cosine_distance, evaluate_vector_inner_product, evaluate_vector_l2_distance,
    infer_scalar_subquery_type_static, perform_cast, safe_add, safe_divide, safe_multiply,
    safe_negate, safe_subtract, sub_interval_from_date,
};
use yachtsql_capability::FeatureId;
use yachtsql_capability::error::CapabilityError;
use yachtsql_capability::feature_ids::{
    F781_SELF_REFERENCING_OPERATIONS, F782_COMMIT_STATEMENT, F783_ROLLBACK_STATEMENT,
    F784_SAVEPOINT_STATEMENT, F785_ROLLBACK_TO_SAVEPOINT_STATEMENT, F786_SAVEPOINTS,
};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_optimizer::rules::{
    IndexSelectionRule, SubqueryFlattening, UnionOptimization, WindowOptimization,
};
use yachtsql_parser::parser::StandardStatement;
use yachtsql_parser::{DialectType, Parser, Statement};
use yachtsql_storage::Schema;

type SqlStatement = sqlparser::ast::Statement;

use self::session::SessionState;
use self::transaction::SessionTransactionController;
use crate::Table;
use crate::catalog_adapter::SnapshotCatalog;

fn create_default_optimizer() -> yachtsql_optimizer::Optimizer {
    yachtsql_optimizer::Optimizer::new()
        .with_rule(Box::new(IndexSelectionRule::disabled()))
        .with_rule(Box::new(SubqueryFlattening::new()))
        .with_rule(Box::new(WindowOptimization::new()))
        .with_rule(Box::new(UnionOptimization::new()))
}

fn create_optimizer_with_catalog(
    catalog: std::sync::Arc<SnapshotCatalog>,
) -> yachtsql_optimizer::Optimizer {
    yachtsql_optimizer::Optimizer::new()
        .with_rule(Box::new(IndexSelectionRule::with_catalog(catalog)))
        .with_rule(Box::new(SubqueryFlattening::new()))
        .with_rule(Box::new(WindowOptimization::new()))
        .with_rule(Box::new(UnionOptimization::new()))
}

pub struct QueryExecutor {
    pub storage: Rc<RefCell<yachtsql_storage::Storage>>,
    pub transaction_manager: Rc<RefCell<yachtsql_storage::TransactionManager>>,
    pub temporary_storage: Rc<RefCell<yachtsql_storage::TempStorage>>,
    session: SessionState,
    session_tx: SessionTransactionController,
    resource_limits: crate::resource_limits::ResourceLimitsConfig,
    optimizer: yachtsql_optimizer::Optimizer,
    plan_cache: Rc<RefCell<crate::plan_cache::PlanCache>>,
    memory_pool: Option<Rc<crate::resource_limits::MemoryPool>>,
    query_registry: Option<Rc<crate::resource_limits::QueryRegistry>>,
}

impl QueryExecutor {
    pub fn new() -> Self {
        let storage = Rc::new(RefCell::new(yachtsql_storage::Storage::new()));
        let session = SessionState::new(DialectType::PostgreSQL);

        let executor = Self {
            storage,
            transaction_manager: Rc::new(RefCell::new(yachtsql_storage::TransactionManager::new())),
            temporary_storage: Rc::new(RefCell::new(yachtsql_storage::TempStorage::new())),
            session,
            session_tx: SessionTransactionController::new(),
            resource_limits: crate::resource_limits::ResourceLimitsConfig::default(),
            optimizer: create_default_optimizer(),
            plan_cache: Rc::new(RefCell::new(crate::plan_cache::PlanCache::new())),
            memory_pool: None,
            query_registry: None,
        };

        executor
    }

    pub fn with_resource_limits(
        mut self,
        config: crate::resource_limits::ResourceLimitsConfig,
    ) -> Self {
        self.resource_limits = config;
        self
    }

    pub fn with_memory_pool(mut self, pool: Rc<crate::resource_limits::MemoryPool>) -> Self {
        self.memory_pool = Some(pool);
        self
    }

    pub fn memory_pool(&self) -> Option<&Rc<crate::resource_limits::MemoryPool>> {
        self.memory_pool.as_ref()
    }

    pub fn with_query_registry(
        mut self,
        registry: Rc<crate::resource_limits::QueryRegistry>,
    ) -> Self {
        self.query_registry = Some(registry);
        self
    }

    pub fn query_registry(&self) -> Option<&Rc<crate::resource_limits::QueryRegistry>> {
        self.query_registry.as_ref()
    }

    pub fn with_dialect(dialect: DialectType) -> Self {
        let mut executor = Self::new();
        executor.session = SessionState::new(dialect);
        executor.session_tx = SessionTransactionController::new();
        executor
    }

    pub fn dialect(&self) -> DialectType {
        self.session.dialect()
    }

    pub fn enable_index_selection(&mut self, dataset_name: &str) {
        let storage = self.storage.borrow();
        if let Some(dataset) = storage.get_dataset(dataset_name) {
            let catalog = SnapshotCatalog::from_dataset(dataset).into_arc();
            self.optimizer = create_optimizer_with_catalog(catalog);
        }
    }

    pub fn disable_index_selection(&mut self) {
        self.optimizer = create_default_optimizer();
    }

    pub fn with_storage_and_transaction(
        storage: Rc<RefCell<yachtsql_storage::Storage>>,
        transaction_manager: Rc<RefCell<yachtsql_storage::TransactionManager>>,
    ) -> Self {
        Self {
            storage,
            transaction_manager,
            temporary_storage: Rc::new(RefCell::new(yachtsql_storage::TempStorage::new())),
            session: SessionState::new(DialectType::PostgreSQL),
            session_tx: SessionTransactionController::new(),
            resource_limits: crate::resource_limits::ResourceLimitsConfig::default(),
            optimizer: create_default_optimizer(),
            plan_cache: Rc::new(RefCell::new(crate::plan_cache::PlanCache::new())),
            memory_pool: None,
            query_registry: None,
        }
    }

    pub fn new_without_optimizer() -> Self {
        let storage = Rc::new(RefCell::new(yachtsql_storage::Storage::new()));
        let session = SessionState::new(DialectType::PostgreSQL);

        Self {
            storage,
            transaction_manager: Rc::new(RefCell::new(yachtsql_storage::TransactionManager::new())),
            temporary_storage: Rc::new(RefCell::new(yachtsql_storage::TempStorage::new())),
            session,
            session_tx: SessionTransactionController::new(),
            resource_limits: crate::resource_limits::ResourceLimitsConfig::default(),
            optimizer: yachtsql_optimizer::Optimizer::disabled(),
            plan_cache: Rc::new(RefCell::new(crate::plan_cache::PlanCache::new())),
            memory_pool: None,
            query_registry: None,
        }
    }

    pub fn with_feature_registry(
        mut self,
        registry: Rc<yachtsql_capability::FeatureRegistry>,
    ) -> Self {
        self.session.set_feature_registry(registry);
        self
    }

    pub fn feature_registry(&self) -> Rc<yachtsql_capability::FeatureRegistry> {
        Rc::clone(self.session.feature_registry())
    }

    #[allow(dead_code)]
    pub(crate) fn feature_registry_arc(&self) -> &Rc<yachtsql_capability::FeatureRegistry> {
        self.session.feature_registry()
    }

    #[allow(dead_code)]
    pub(crate) fn feature_registry_arc_mut(
        &mut self,
    ) -> &mut Rc<yachtsql_capability::FeatureRegistry> {
        self.session.feature_registry_mut()
    }

    pub fn require_feature(
        &self,
        feature_id: yachtsql_capability::FeatureId,
        feature_name: &str,
    ) -> Result<()> {
        if self.session.feature_registry().is_enabled(feature_id) {
            Ok(())
        } else {
            Err(Error::unsupported_feature(format!(
                "Feature {} ({}) is not enabled",
                feature_id, feature_name
            )))
        }
    }

    pub fn clear_exception_diagnostic(&mut self) {
        self.session.diagnostics_mut().clear_exception();
    }

    pub fn record_row_count(&mut self, count: usize) {
        self.session.diagnostics_mut().record_row_count(count);
    }

    pub fn diagnostics_snapshot(&self) -> DiagnosticsSnapshot {
        self.session.diagnostics_snapshot()
    }

    fn reset_session_diagnostics(&mut self) {
        self.session.reset_diagnostics();
    }

    fn create_subquery_executor(&self) -> crate::query_executor::evaluator::SubqueryExecutorImpl {
        crate::query_executor::evaluator::SubqueryExecutorImpl::new(
            Rc::clone(&self.storage),
            Rc::clone(&self.transaction_manager),
            Rc::clone(&self.temporary_storage),
            Rc::clone(self.session.feature_registry()),
            self.session.dialect(),
        )
    }

    pub fn parse_table_name(&self, table_name: &str) -> (Option<String>, String) {
        if let Some(dot_pos) = table_name.find('.') {
            let dataset = table_name[..dot_pos].to_string();
            let table = table_name[dot_pos + 1..].to_string();
            (Some(dataset), table)
        } else {
            (None, table_name.to_string())
        }
    }

    pub fn should_defer_fk_check(&self) -> bool {
        false
    }

    pub(super) fn empty_result() -> Result<Table> {
        Ok(Table::empty(Schema::from_fields(vec![])))
    }

    pub fn qualify_table_name(dataset_id: &str, table_id: &str) -> String {
        format!("{}.{}", dataset_id, table_id)
    }

    fn check_exclude_not_supported(sql: &str) -> Result<()> {
        let patterns = [
            "EXCLUDE CURRENT ROW",
            "EXCLUDE CURRENT_ROW",
            "exclude current row",
            "exclude current_row",
            "Exclude Current Row",
            "EXCLUDE NO OTHERS",
            "EXCLUDE NO_OTHERS",
            "exclude no others",
            "exclude no_others",
            "Exclude No Others",
            "EXCLUDE GROUP",
            "exclude group",
            "Exclude Group",
            "EXCLUDE TIES",
            "exclude ties",
            "Exclude Ties",
        ];

        for pattern in &patterns {
            if sql.contains(pattern) {
                return Err(Error::unsupported_feature(format!(
                    "EXCLUDE clauses in window frames are not supported. \
                    This is an intentional limitation because sqlparser-rs (the underlying SQL parser) \
                    does not support EXCLUDE syntax. The parser will fail before we can convert to IR. \
                    \n\nFound: {}\n\nNote: YachtSQL's IR fully supports EXCLUDE, but we cannot parse it yet. \
                    This limitation will be removed when sqlparser-rs adds EXCLUDE support.",
                    pattern
                )));
            }
        }

        Ok(())
    }

    fn validate_sql_before_parse(sql: &str, dialect: DialectType) -> Result<()> {
        match dialect {
            DialectType::BigQuery => {
                Self::check_exclude_not_supported(sql)?;
            }
            DialectType::ClickHouse | DialectType::PostgreSQL => {}
        }
        Ok(())
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<Table> {
        use yachtsql_parser::validator::{CustomStatement, ExportFormat};
        use yachtsql_parser::{Parser, Statement};

        Self::validate_sql_before_parse(sql, self.dialect())?;

        let _registry_guard =
            crate::query_executor::evaluator::physical_plan::ProjectionWithExprExec::enter_feature_registry_context(
                Rc::clone(self.session.feature_registry()),
            );

        let subquery_executor = self.create_subquery_executor();
        let _subquery_guard =
            crate::query_executor::evaluator::physical_plan::SubqueryExecutorContextGuard::set(
                Rc::new(subquery_executor),
            );

        let _storage_guard =
            crate::query_executor::evaluator::physical_plan::StorageContextGuard::set(Rc::clone(
                &self.storage,
            ));

        let parser = Parser::with_dialect(self.dialect());
        let statements = parser
            .parse_sql(sql)
            .map_err(|e| Error::parse_error(format!("Failed to parse SQL: {}", e)))?;

        if statements.is_empty() {
            return Err(Error::parse_error("No SQL statement provided".to_string()));
        }

        if statements.len() > 1 {
            return Err(Error::parse_error(format!(
                "Multiple statements not supported (found {})",
                statements.len()
            )));
        }

        let statement = &statements[0];
        let udf_names = self.session.udf_names();
        crate::query_executor::validate_statement_with_udfs(
            statement,
            self.session.feature_registry(),
            Some(&udf_names),
        )?;

        if let Statement::Custom(custom_stmt) = statement {
            return match custom_stmt {
                CustomStatement::CreateSequence { .. } => self.execute_create_sequence(custom_stmt),
                CustomStatement::AlterSequence { .. } => self.execute_alter_sequence(custom_stmt),
                CustomStatement::DropSequence { .. } => self.execute_drop_sequence(custom_stmt),
                CustomStatement::RefreshMaterializedView { .. } => {
                    self.execute_refresh_materialized_view(custom_stmt)
                }
                CustomStatement::DropMaterializedView { .. } => {
                    self.execute_drop_materialized_view(custom_stmt)
                }
                CustomStatement::CreateDomain { .. } => self.execute_create_domain(custom_stmt),
                CustomStatement::AlterDomain { .. } => self.execute_alter_domain(custom_stmt),
                CustomStatement::DropDomain { .. } => self.execute_drop_domain(custom_stmt),
                CustomStatement::CreateType { .. } => {
                    self.execute_create_composite_type(custom_stmt)
                }
                CustomStatement::DropType { .. } => self.execute_drop_composite_type(custom_stmt),
                CustomStatement::SetConstraints { .. } => self.execute_set_constraints(custom_stmt),
                CustomStatement::ExistsTable { name } => self.execute_exists_table(name),
                CustomStatement::ExistsDatabase { name } => self.execute_exists_database(name),
                CustomStatement::Abort => {
                    self.execute_rollback_transaction()?;
                    Self::empty_result()
                }
                CustomStatement::BeginTransaction {
                    isolation_level,
                    read_only,
                    deferrable,
                } => {
                    self.execute_begin_transaction_with_options(
                        isolation_level.clone(),
                        *read_only,
                        *deferrable,
                    )?;
                    Self::empty_result()
                }
                CustomStatement::ClickHouseCreateIndex { .. } => Self::empty_result(),
                CustomStatement::ClickHouseAlterColumnCodec { .. } => Self::empty_result(),
                CustomStatement::ClickHouseAlterTableTtl { .. } => Self::empty_result(),
                CustomStatement::ClickHouseQuota { statement } => {
                    return self.execute_clickhouse_quota(statement);
                }
                CustomStatement::ClickHouseRowPolicy { statement } => {
                    return self.execute_clickhouse_row_policy(statement);
                }
                CustomStatement::ClickHouseSettingsProfile { statement } => {
                    return self.execute_clickhouse_settings_profile(statement);
                }
                CustomStatement::ClickHouseDictionary { .. } => Self::empty_result(),
                CustomStatement::ClickHouseShow { statement } => {
                    return self.execute_clickhouse_show(statement);
                }
                CustomStatement::ClickHouseFunction { statement } => {
                    return self.execute_clickhouse_function(statement);
                }
                CustomStatement::ClickHouseMaterializedView { .. } => Self::empty_result(),
                CustomStatement::ClickHouseProjection { .. } => Self::empty_result(),
                CustomStatement::ClickHouseAlterUser { .. } => Self::empty_result(),
                CustomStatement::ClickHouseGrant { .. } => Self::empty_result(),
                CustomStatement::ClickHouseSystem { .. } => Self::empty_result(),
                CustomStatement::ClickHouseCreateDictionary {
                    name,
                    columns,
                    primary_key,
                    source,
                    layout,
                    lifetime,
                } => {
                    return self.execute_create_dictionary(
                        name,
                        columns,
                        primary_key,
                        source,
                        *layout,
                        lifetime,
                    );
                }
                CustomStatement::ClickHouseDatabase { .. } => Self::empty_result(),
                CustomStatement::ClickHouseRenameDatabase { .. } => Self::empty_result(),
                CustomStatement::ClickHouseUse { .. } => Self::empty_result(),
                CustomStatement::ClickHouseCreateTableWithProjection { stripped, .. } => {
                    return self.execute_sql(stripped);
                }
                CustomStatement::ClickHouseCreateTablePassthrough { stripped, .. } => {
                    return self.execute_sql(stripped);
                }
                CustomStatement::ClickHouseCreateTableAs {
                    new_table,
                    source_table,
                    engine_clause,
                } => {
                    return self.execute_create_table_as(new_table, source_table, engine_clause);
                }
                CustomStatement::ClickHouseAlterTable { .. } => Self::empty_result(),
                CustomStatement::Loop { label, body } => {
                    self.execute_loop_custom(label.clone(), body)
                }
                CustomStatement::Repeat {
                    label,
                    body,
                    until_condition,
                } => self.execute_repeat_custom(label.clone(), body, until_condition),
                CustomStatement::For {
                    label,
                    variable,
                    query,
                    body,
                } => self.execute_for_custom(label.clone(), variable, query, body),
                CustomStatement::Leave { label } => {
                    let msg = match label {
                        Some(lbl) => format!("LEAVE {}", lbl),
                        None => "LEAVE".to_string(),
                    };
                    Err(Error::invalid_query(msg))
                }
                CustomStatement::Continue { label } => {
                    let msg = match label {
                        Some(lbl) => format!("CONTINUE {}", lbl),
                        None => "CONTINUE".to_string(),
                    };
                    Err(Error::invalid_query(msg))
                }
                CustomStatement::Break { label } => {
                    let msg = match label {
                        Some(lbl) => format!("BREAK {}", lbl),
                        None => "BREAK".to_string(),
                    };
                    Err(Error::invalid_query(msg))
                }
                CustomStatement::While {
                    label,
                    condition,
                    body,
                } => self.execute_while_custom(label.clone(), condition, body),
                CustomStatement::ExportData {
                    uri,
                    format,
                    overwrite,
                    header,
                    field_delimiter,
                    compression,
                    query,
                } => self.execute_export_data(
                    uri,
                    *format,
                    *overwrite,
                    *header,
                    *field_delimiter,
                    compression.clone(),
                    query,
                ),
                CustomStatement::LoadData {
                    table_name,
                    overwrite,
                    is_temp,
                    temp_table_schema,
                    format,
                    uris,
                    allow_schema_update,
                } => self.execute_load_data(
                    table_name,
                    *overwrite,
                    *is_temp,
                    temp_table_schema.clone(),
                    *format,
                    uris,
                    *allow_schema_update,
                ),
                CustomStatement::AlterTableRestartIdentity { .. }
                | CustomStatement::GetDiagnostics { .. } => Err(Error::unsupported_feature(
                    format!("Custom statement not yet supported: {:?}", custom_stmt),
                )),
                CustomStatement::CreateSnapshotTable { .. } => {
                    self.execute_create_snapshot_table(custom_stmt)
                }
                CustomStatement::DropSnapshotTable { .. } => {
                    self.execute_drop_snapshot_table(custom_stmt)
                }
            };
        }

        let dispatcher =
            Dispatcher::with_capabilities(self.session.feature_registry().snapshot_view());
        let job = dispatcher.classify_statement(statement)?;

        match job {
            StatementJob::Transaction { operation } => {
                self.execute_transaction_control(operation)?;
                Self::empty_result()
            }

            StatementJob::DDL { operation, stmt } => {
                let result = match operation {
                    DdlOperation::CreateTable => {
                        self.execute_create_table(&stmt, sql)?;
                        Self::empty_result()
                    }
                    DdlOperation::DropTable => {
                        self.execute_drop_table(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::CreateView => {
                        self.execute_create_view(&stmt, sql)?;
                        Self::empty_result()
                    }
                    DdlOperation::CreateMaterializedView => {
                        self.execute_create_view(&stmt, sql)?;
                        Self::empty_result()
                    }
                    DdlOperation::DropView => {
                        self.execute_drop_view(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::CreateIndex => {
                        self.execute_create_index(&stmt, sql)?;
                        Self::empty_result()
                    }
                    DdlOperation::DropIndex => {
                        self.execute_drop_index(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::CreateTrigger => {
                        self.execute_create_trigger(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::DropTrigger => {
                        self.execute_drop_trigger(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::CreateType => {
                        self.execute_create_type(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::DropType => {
                        self.execute_drop_type(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::AlterTable => {
                        self.execute_alter_table(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::CreateExtension => {
                        self.execute_create_extension(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::DropExtension => {
                        self.execute_drop_extension(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::CreateSchema => {
                        self.execute_create_schema(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::DropSchema => {
                        self.execute_drop_schema(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::CreateFunction => {
                        debug_eprintln!("[mod] Executing CREATE FUNCTION");
                        self.execute_create_function(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::DropFunction => {
                        self.execute_drop_function(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::CreateProcedure => {
                        debug_eprintln!("[mod] Executing CREATE PROCEDURE");
                        self.execute_create_procedure(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::DropProcedure => {
                        self.execute_drop_procedure(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::CreateDatabase {
                        name,
                        if_not_exists,
                    } => {
                        self.execute_create_database(&name, if_not_exists)?;
                        Self::empty_result()
                    }

                    DdlOperation::DropDatabase => {
                        self.execute_drop_database(&stmt)?;
                        Self::empty_result()
                    }
                    DdlOperation::CreateUser => Self::empty_result(),
                    DdlOperation::DropUser => Self::empty_result(),
                    DdlOperation::AlterUser => Self::empty_result(),
                    DdlOperation::CreateRole => Self::empty_result(),
                    DdlOperation::DropRole => Self::empty_result(),
                    DdlOperation::AlterRole => Self::empty_result(),
                    DdlOperation::Grant => Self::empty_result(),
                    DdlOperation::Revoke => Self::empty_result(),
                    DdlOperation::SetRole => Self::empty_result(),
                    DdlOperation::SetDefaultRole => Self::empty_result(),

                    DdlOperation::CreateSequence
                    | DdlOperation::AlterSequence
                    | DdlOperation::DropSequence => {
                        panic!("Sequence operations should be handled via CustomStatement path")
                    }

                    DdlOperation::CreateSnapshotTable | DdlOperation::DropSnapshotTable => {
                        panic!("Snapshot operations should be handled via CustomStatement path")
                    }
                };

                if result.is_ok() {
                    self.plan_cache.borrow_mut().invalidate_all();
                }

                result
            }

            StatementJob::DML { operation, stmt } => {
                if self.is_transaction_read_only() {
                    let op_name = match operation {
                        DmlOperation::Insert => "INSERT",
                        DmlOperation::Update => "UPDATE",
                        DmlOperation::Delete => "DELETE",
                        DmlOperation::Truncate => "TRUNCATE",
                    };
                    return Err(Error::InvalidOperation(format!(
                        "cannot execute {} in a read-only transaction",
                        op_name
                    )));
                }
                match operation {
                    DmlOperation::Insert => self.execute_insert(&stmt, sql),
                    DmlOperation::Update => self.execute_update(&stmt, sql),
                    DmlOperation::Delete => self.execute_delete(&stmt, sql),
                    DmlOperation::Truncate => {
                        let _rows_affected = self.execute_truncate(&stmt)?;
                        Self::empty_result()
                    }
                }
            }

            StatementJob::CteDml { operation, stmt } => {
                if self.is_transaction_read_only() {
                    let op_name = match operation {
                        DmlOperation::Insert => "INSERT",
                        DmlOperation::Update => "UPDATE",
                        DmlOperation::Delete => "DELETE",
                        DmlOperation::Truncate => "TRUNCATE",
                    };
                    return Err(Error::InvalidOperation(format!(
                        "cannot execute {} in a read-only transaction",
                        op_name
                    )));
                }
                self.execute_cte_dml(&stmt, operation, sql)
            }

            StatementJob::Query { stmt } => self.execute_select(&stmt, sql),

            StatementJob::Merge { operation } => {
                self.execute_merge(&operation.stmt, sql, operation.merge_returning.clone())
            }

            StatementJob::Utility { operation } => match operation {
                UtilityOperation::SetCapabilities { enable, features } => {
                    self.execute_set_capabilities(enable, &features)
                }
                UtilityOperation::Explain {
                    stmt,
                    analyze,
                    verbose,
                } => self.execute_explain(&stmt, analyze, verbose),
                UtilityOperation::SetSearchPath { schemas } => {
                    self.execute_set_search_path(&schemas)
                }
                UtilityOperation::Show { variable } => self.execute_show(variable.as_deref()),
                UtilityOperation::DescribeTable { table_name } => {
                    self.execute_describe_table(&table_name)
                }
                UtilityOperation::ShowCreateTable { table_name } => {
                    self.execute_show_create_table(&table_name)
                }
                UtilityOperation::ShowTables { filter } => {
                    self.execute_show_tables(filter.as_deref())
                }
                UtilityOperation::ShowColumns { table_name } => {
                    self.execute_show_columns(&table_name)
                }
                UtilityOperation::ExistsTable { table_name } => {
                    self.execute_exists_table(&table_name)
                }
                UtilityOperation::ExistsDatabase { db_name } => {
                    self.execute_exists_database(&db_name)
                }
                UtilityOperation::ShowUsers => self.execute_show_users(),
                UtilityOperation::ShowRoles => self.execute_show_roles(),
                UtilityOperation::ShowGrants { user_name } => {
                    self.execute_show_grants(user_name.as_deref())
                }
                UtilityOperation::OptimizeTable { table_name } => {
                    self.execute_optimize_table(&table_name)
                }
            },

            StatementJob::Procedure { name, args } => self.execute_procedure(&name, &args),

            StatementJob::Copy { operation } => self.execute_copy(&operation.stmt),

            StatementJob::Scripting { operation } => self.execute_scripting(operation),

            StatementJob::Cursor { operation } => self.execute_cursor_operation(operation),
        }
    }

    fn execute_transaction_control(&mut self, operation: TxOperation) -> Result<()> {
        match operation {
            TxOperation::Begin => {
                self.require_feature(F781_SELF_REFERENCING_OPERATIONS, "BEGIN statement")?;
                self.execute_begin_transaction()
            }

            TxOperation::Commit { chain } => {
                self.require_feature(F782_COMMIT_STATEMENT, "COMMIT statement")?;
                let characteristics = if chain {
                    self.get_current_transaction_characteristics()
                } else {
                    None
                };
                self.execute_commit_transaction()?;
                if chain {
                    if let Some(chars) = characteristics {
                        self.begin_transaction_with_characteristics(
                            chars,
                            yachtsql_storage::TransactionScope::Explicit,
                        )?;
                    } else {
                        self.execute_begin_transaction()?;
                    }
                }
                Ok(())
            }

            TxOperation::Rollback { chain } => {
                self.require_feature(F783_ROLLBACK_STATEMENT, "ROLLBACK statement")?;
                let characteristics = if chain {
                    self.get_current_transaction_characteristics()
                } else {
                    None
                };
                self.execute_rollback_transaction()?;
                if chain {
                    if let Some(chars) = characteristics {
                        self.begin_transaction_with_characteristics(
                            chars,
                            yachtsql_storage::TransactionScope::Explicit,
                        )?;
                    } else {
                        self.execute_begin_transaction()?;
                    }
                }
                Ok(())
            }

            TxOperation::Savepoint { name } => {
                self.require_feature(F784_SAVEPOINT_STATEMENT, "SAVEPOINT statement")?;
                self.execute_savepoint(name)
            }

            TxOperation::ReleaseSavepoint { name } => {
                self.require_feature(F786_SAVEPOINTS, "RELEASE SAVEPOINT statement")?;
                self.execute_release_savepoint(name)
            }

            TxOperation::RollbackToSavepoint { name } => {
                self.require_feature(
                    F785_ROLLBACK_TO_SAVEPOINT_STATEMENT,
                    "ROLLBACK TO SAVEPOINT statement",
                )?;
                self.execute_rollback_to_savepoint(name)
            }

            TxOperation::SetAutocommit { enabled } => self.execute_set_session_autocommit(enabled),

            TxOperation::SetTransactionIsolation { .. } => Err(Error::unsupported_feature(
                "SET TRANSACTION ISOLATION LEVEL not yet implemented".to_string(),
            )),

            TxOperation::SetTransaction { modes } => self.execute_set_transaction(modes),
        }
    }

    fn execute_set_capabilities(&mut self, enable: bool, features: &[String]) -> Result<Table> {
        if features.is_empty() {
            return Err(Error::invalid_query(
                "SET yachtsql.capability requires at least one feature identifier".to_string(),
            ));
        }

        let registry_arc = self.feature_registry_arc_mut();
        let registry = Rc::make_mut(registry_arc);
        let mut resolved: Vec<FeatureId> = Vec::with_capacity(features.len());

        for feature_name in features {
            if let Some(feature) = registry
                .all_features()
                .find(|candidate| candidate.id.as_str().eq_ignore_ascii_case(feature_name))
            {
                resolved.push(feature.id);
            } else {
                return Err(Error::unsupported_feature(format!(
                    "Unknown SQL feature identifier '{}'",
                    feature_name
                )));
            }
        }

        if enable {
            let ids = resolved.clone();
            registry
                .enable_features_strict(ids.into_iter())
                .map_err(Self::map_capability_error)?;
        } else {
            for feature_id in &resolved {
                if let Some(feature) = registry.all_features().find(|f| f.id == *feature_id)
                    && feature.is_core
                {
                    return Err(Error::unsupported_feature(format!(
                        "Cannot disable core feature '{}' ({})",
                        feature_id.as_str(),
                        feature.name
                    )));
                }
            }

            for feature_id in resolved {
                registry
                    .disable(feature_id)
                    .map_err(Self::map_capability_error)?;
            }
        }

        self.record_row_count(0);
        Self::empty_result()
    }

    fn map_capability_error(err: CapabilityError) -> Error {
        Error::unsupported_feature(format!("Capability update failed: {}", err))
    }

    fn execute_set_search_path(&mut self, schemas: &[String]) -> Result<Table> {
        self.session.set_search_path(schemas.to_vec());
        self.record_row_count(0);
        Self::empty_result()
    }

    fn execute_show(&mut self, variable: Option<&str>) -> Result<Table> {
        let var_name = variable.unwrap_or("all");

        match var_name.to_lowercase().as_str() {
            "search_path" => {
                let path = self.session.search_path();
                let path_str = path.join(", ");
                let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
                    "search_path".to_string(),
                    DataType::String,
                )]);
                let rows = vec![vec![Value::string(path_str)]];
                Table::from_values(schema, rows)
            }
            _ => Err(Error::unsupported_feature(format!(
                "SHOW {} is not supported",
                var_name
            ))),
        }
    }

    fn execute_describe_table(&mut self, table_name: &sqlparser::ast::ObjectName) -> Result<Table> {
        let table_str = table_name.to_string();
        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_str)?;

        let storage = self.storage.borrow();
        let dataset = storage
            .get_dataset(&dataset_id)
            .ok_or_else(|| Error::invalid_query(format!("Table '{}' does not exist", table_str)))?;
        let table = dataset
            .get_table(&table_id)
            .ok_or_else(|| Error::invalid_query(format!("Table '{}' does not exist", table_str)))?;

        let schema = Schema::from_fields(vec![
            yachtsql_storage::Field::required("name".to_string(), DataType::String),
            yachtsql_storage::Field::required("type".to_string(), DataType::String),
            yachtsql_storage::Field::required("default_type".to_string(), DataType::String),
            yachtsql_storage::Field::required("default_expression".to_string(), DataType::String),
            yachtsql_storage::Field::required("comment".to_string(), DataType::String),
            yachtsql_storage::Field::required("codec_expression".to_string(), DataType::String),
            yachtsql_storage::Field::required("ttl_expression".to_string(), DataType::String),
        ]);

        let mut rows = Vec::new();
        for field in table.schema().fields() {
            rows.push(vec![
                Value::string(field.name.clone()),
                Value::string(field.data_type.to_string()),
                Value::string("".to_string()),
                Value::string(
                    field
                        .default_value
                        .as_ref()
                        .map(|v| format_default_value(v))
                        .unwrap_or_default(),
                ),
                Value::string(field.description.clone().unwrap_or_default()),
                Value::string("".to_string()),
                Value::string("".to_string()),
            ]);
        }

        Table::from_values(schema, rows)
    }

    fn execute_show_create_table(
        &mut self,
        table_name: &sqlparser::ast::ObjectName,
    ) -> Result<Table> {
        let table_str = table_name.to_string();
        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_str)?;

        let storage = self.storage.borrow();
        let dataset = storage
            .get_dataset(&dataset_id)
            .ok_or_else(|| Error::invalid_query(format!("Table '{}' does not exist", table_str)))?;
        let table = dataset
            .get_table(&table_id)
            .ok_or_else(|| Error::invalid_query(format!("Table '{}' does not exist", table_str)))?;

        let mut columns = Vec::new();
        for field in table.schema().fields() {
            columns.push(format!("    {} {}", field.name, field.data_type));
        }

        let create_stmt = format!(
            "CREATE TABLE {} (\n{}\n) ENGINE = MergeTree",
            table_id,
            columns.join(",\n")
        );

        let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
            "statement".to_string(),
            DataType::String,
        )]);
        let rows = vec![vec![Value::string(create_stmt)]];
        Table::from_values(schema, rows)
    }

    fn execute_show_tables(&mut self, filter: Option<&str>) -> Result<Table> {
        let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
            "name".to_string(),
            DataType::String,
        )]);

        let mut rows = Vec::new();
        let storage = self.storage.borrow();

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(&dataset_id) {
                for table_id in dataset.tables().keys() {
                    let matches = match filter {
                        Some(pattern) => {
                            let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
                            regex::Regex::new(&format!("^{}$", regex_pattern))
                                .map(|re| re.is_match(table_id))
                                .unwrap_or(false)
                        }
                        None => true,
                    };
                    if matches {
                        rows.push(vec![Value::string(table_id.clone())]);
                    }
                }
            }
        }

        Table::from_values(schema, rows)
    }

    fn execute_show_columns(&mut self, table_name: &sqlparser::ast::ObjectName) -> Result<Table> {
        let table_str = table_name.to_string();
        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_str)?;

        let storage = self.storage.borrow();
        let dataset = storage
            .get_dataset(&dataset_id)
            .ok_or_else(|| Error::invalid_query(format!("Table '{}' does not exist", table_str)))?;
        let table = dataset
            .get_table(&table_id)
            .ok_or_else(|| Error::invalid_query(format!("Table '{}' does not exist", table_str)))?;

        let schema = Schema::from_fields(vec![
            yachtsql_storage::Field::required("field".to_string(), DataType::String),
            yachtsql_storage::Field::required("type".to_string(), DataType::String),
            yachtsql_storage::Field::required("null".to_string(), DataType::String),
            yachtsql_storage::Field::required("key".to_string(), DataType::String),
            yachtsql_storage::Field::required("default".to_string(), DataType::String),
            yachtsql_storage::Field::required("extra".to_string(), DataType::String),
        ]);

        let mut rows = Vec::new();
        for field in table.schema().fields() {
            let is_nullable = matches!(field.mode, yachtsql_storage::FieldMode::Nullable);
            rows.push(vec![
                Value::string(field.name.clone()),
                Value::string(field.data_type.to_string()),
                Value::string(if is_nullable { "YES" } else { "NO" }.to_string()),
                Value::string("".to_string()),
                Value::string(
                    field
                        .default_value
                        .as_ref()
                        .map(|v| format_default_value(v))
                        .unwrap_or_default(),
                ),
                Value::string("".to_string()),
            ]);
        }

        Table::from_values(schema, rows)
    }

    fn execute_clickhouse_show(&mut self, statement: &str) -> Result<Table> {
        let statement_upper = statement.to_uppercase();

        if statement_upper.starts_with("SHOW DATABASES") {
            return self.execute_show_databases();
        }
        if statement_upper.starts_with("SHOW QUOTAS") {
            return self.execute_show_quotas();
        }
        if statement_upper.starts_with("SHOW ROW POLICIES") {
            return self.execute_show_row_policies();
        }
        if statement_upper.starts_with("SHOW SETTINGS PROFILES") {
            return self.execute_show_settings_profiles();
        }
        if statement_upper.starts_with("SHOW DICTIONARIES") {
            return self.execute_show_dictionaries();
        }
        if statement_upper.starts_with("SHOW CREATE TABLE") {
            let prefix_len = "SHOW CREATE TABLE".len();
            let table_name = statement[prefix_len..].trim();
            let obj_name =
                sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    sqlparser::ast::Ident::new(table_name),
                )]);
            return self.execute_show_create_table(&obj_name);
        }
        if statement_upper.starts_with("SHOW TABLES") {
            let prefix_len = "SHOW TABLES".len();
            let rest = statement[prefix_len..].trim();
            let rest_upper = rest.to_uppercase();
            let filter = if let Some(pos) = rest_upper.find("LIKE") {
                let pattern = rest[pos + 4..].trim().trim_matches('\'');
                Some(pattern.to_string())
            } else {
                None
            };
            return self.execute_show_tables(filter.as_deref());
        }
        if statement_upper.starts_with("SHOW COLUMNS FROM") {
            let prefix_len = "SHOW COLUMNS FROM".len();
            let table_name = statement[prefix_len..].trim();
            let obj_name =
                sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    sqlparser::ast::Ident::new(table_name),
                )]);
            return self.execute_show_columns(&obj_name);
        }
        if statement_upper.starts_with("SHOW COLUMNS IN") {
            let prefix_len = "SHOW COLUMNS IN".len();
            let table_name = statement[prefix_len..].trim();
            let obj_name =
                sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    sqlparser::ast::Ident::new(table_name),
                )]);
            return self.execute_show_columns(&obj_name);
        }
        Self::empty_result()
    }

    fn execute_clickhouse_function(&mut self, statement: &str) -> Result<Table> {
        let statement_upper = statement.to_uppercase();

        if statement_upper.starts_with("CREATE") {
            return self.execute_clickhouse_create_function(statement);
        }
        if statement_upper.starts_with("DROP") {
            return self.execute_clickhouse_drop_function(statement);
        }

        Err(Error::unsupported_feature(format!(
            "Unsupported ClickHouse function statement: {}",
            statement
        )))
    }

    fn execute_clickhouse_create_function(&mut self, statement: &str) -> Result<Table> {
        use regex::Regex;

        let re = Regex::new(
            r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+AS\s*\(([^)]*)\)\s*->\s*(.*)",
        )
        .unwrap();

        let captures = re.captures(statement).ok_or_else(|| {
            Error::invalid_query(format!("Invalid CREATE FUNCTION syntax: {}", statement))
        })?;

        let func_name = captures.get(1).unwrap().as_str().to_string();
        let params_str = captures.get(2).unwrap().as_str();
        let body_str = captures.get(3).unwrap().as_str().trim();

        let parameters: Vec<String> = if params_str.trim().is_empty() {
            vec![]
        } else {
            params_str
                .split(',')
                .map(|s| s.trim().to_string())
                .collect()
        };

        let dialect = sqlparser::dialect::ClickHouseDialect {};
        let body_expr = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(body_str)
            .map_err(|e| Error::parse_error(format!("Failed to parse function body: {}", e)))?
            .parse_expr()
            .map_err(|e| Error::parse_error(format!("Failed to parse function body: {}", e)))?;

        let is_or_replace = statement.to_uppercase().contains("OR REPLACE");
        let is_if_not_exists = statement.to_uppercase().contains("IF NOT EXISTS");

        if is_or_replace {
            self.session.drop_udf(&func_name);
        } else if !is_if_not_exists && self.session.has_udf(&func_name) {
            return Err(Error::invalid_query(format!(
                "Function '{}' already exists",
                func_name
            )));
        }

        if !self.session.has_udf(&func_name) {
            let udf_def = UdfDefinition {
                parameters,
                body: body_expr,
            };
            self.session.register_udf(func_name, udf_def);
        }

        Self::empty_result()
    }

    fn execute_clickhouse_drop_function(&mut self, statement: &str) -> Result<Table> {
        use regex::Regex;

        let re = Regex::new(r"(?i)DROP\s+FUNCTION\s+(?:IF\s+EXISTS\s+)?(\w+)").unwrap();

        let captures = re.captures(statement).ok_or_else(|| {
            Error::invalid_query(format!("Invalid DROP FUNCTION syntax: {}", statement))
        })?;

        let func_name = captures.get(1).unwrap().as_str();
        let is_if_exists = statement.to_uppercase().contains("IF EXISTS");

        if !self.session.drop_udf(func_name) && !is_if_exists {
            return Err(Error::invalid_query(format!(
                "Function '{}' does not exist",
                func_name
            )));
        }

        Self::empty_result()
    }

    fn execute_show_databases(&mut self) -> Result<Table> {
        let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
            "name".to_string(),
            DataType::String,
        )]);

        let mut rows = Vec::new();
        let storage = self.storage.borrow();

        for db_id in storage.list_datasets() {
            rows.push(vec![Value::string(db_id.to_string())]);
        }

        Table::from_values(schema, rows)
    }

    fn execute_show_quotas(&mut self) -> Result<Table> {
        let schema = Schema::from_fields(vec![
            yachtsql_storage::Field::required("name".to_string(), DataType::String),
            yachtsql_storage::Field::required("id".to_string(), DataType::String),
        ]);

        let storage = self.storage.borrow();
        let rows: Vec<Vec<Value>> = storage
            .quotas()
            .map(|q| vec![Value::string(q.name.clone()), Value::string(q.name.clone())])
            .collect();

        Table::from_values(schema, rows)
    }

    fn execute_show_row_policies(&mut self) -> Result<Table> {
        let schema = Schema::from_fields(vec![
            yachtsql_storage::Field::required("name".to_string(), DataType::String),
            yachtsql_storage::Field::required("database".to_string(), DataType::String),
            yachtsql_storage::Field::required("table".to_string(), DataType::String),
        ]);

        let storage = self.storage.borrow();
        let rows: Vec<Vec<Value>> = storage
            .row_policies()
            .map(|p| {
                vec![
                    Value::string(p.name.clone()),
                    Value::string(p.database.clone()),
                    Value::string(p.table.clone()),
                ]
            })
            .collect();

        Table::from_values(schema, rows)
    }

    fn execute_show_settings_profiles(&mut self) -> Result<Table> {
        let schema = Schema::from_fields(vec![
            yachtsql_storage::Field::required("name".to_string(), DataType::String),
            yachtsql_storage::Field::required("id".to_string(), DataType::String),
        ]);

        let storage = self.storage.borrow();
        let rows: Vec<Vec<Value>> = storage
            .settings_profiles()
            .map(|p| vec![Value::string(p.name.clone()), Value::string(p.name.clone())])
            .collect();

        Table::from_values(schema, rows)
    }

    fn execute_clickhouse_quota(&mut self, statement: &str) -> Result<Table> {
        let statement_upper = statement.to_uppercase();
        if statement_upper.starts_with("CREATE QUOTA") {
            let name = Self::extract_object_name(statement, "CREATE QUOTA");
            if !name.is_empty() {
                self.storage.borrow_mut().add_quota(name);
            }
        } else if statement_upper.starts_with("DROP QUOTA") {
            let name = Self::extract_object_name(statement, "DROP QUOTA");
            if !name.is_empty() {
                self.storage.borrow_mut().remove_quota(&name);
            }
        }
        Self::empty_result()
    }

    fn execute_clickhouse_row_policy(&mut self, statement: &str) -> Result<Table> {
        let statement_upper = statement.to_uppercase();
        if statement_upper.starts_with("CREATE ROW POLICY") {
            let trimmed = statement["CREATE ROW POLICY".len()..].trim();
            let (name, remainder) = Self::extract_first_word(trimmed);
            let table_part = remainder.to_uppercase().find("ON ").map(|pos| {
                let after_on = remainder[pos + 3..].trim();
                Self::extract_first_word(after_on).0
            });
            let table_name = table_part.unwrap_or_default();
            if !name.is_empty() {
                self.storage
                    .borrow_mut()
                    .add_row_policy(name, "default".to_string(), table_name);
            }
        } else if statement_upper.starts_with("DROP ROW POLICY") {
            let name = Self::extract_object_name(statement, "DROP ROW POLICY");
            if !name.is_empty() {
                self.storage.borrow_mut().remove_row_policy(&name);
            }
        }
        Self::empty_result()
    }

    fn execute_clickhouse_settings_profile(&mut self, statement: &str) -> Result<Table> {
        let statement_upper = statement.to_uppercase();
        if statement_upper.starts_with("CREATE SETTINGS PROFILE") {
            let name = Self::extract_object_name(statement, "CREATE SETTINGS PROFILE");
            if !name.is_empty() {
                self.storage.borrow_mut().add_settings_profile(name);
            }
        } else if statement_upper.starts_with("DROP SETTINGS PROFILE") {
            let name = Self::extract_object_name(statement, "DROP SETTINGS PROFILE");
            if !name.is_empty() {
                self.storage.borrow_mut().remove_settings_profile(&name);
            }
        } else if statement_upper.starts_with("ALTER SETTINGS PROFILE") {
        }
        Self::empty_result()
    }

    fn extract_object_name(statement: &str, prefix: &str) -> String {
        let rest = statement[prefix.len()..].trim();
        let (name, _) = Self::extract_first_word(rest);
        name.trim_end_matches([',', ';']).to_string()
    }

    fn extract_first_word(s: &str) -> (String, &str) {
        let s = s.trim();
        let mut in_quotes = false;
        let mut quote_char = ' ';

        for (i, c) in s.char_indices() {
            if !in_quotes && (c == '\'' || c == '"' || c == '`') {
                in_quotes = true;
                quote_char = c;
            } else if in_quotes && c == quote_char {
                in_quotes = false;
            } else if !in_quotes && c.is_whitespace() {
                let word = s[..i].trim_matches(|c| c == '\'' || c == '"' || c == '`');
                return (word.to_string(), &s[i..]);
            }
        }

        let word = s.trim_matches(|c| c == '\'' || c == '"' || c == '`');
        (word.to_string(), "")
    }

    fn execute_show_dictionaries(&mut self) -> Result<Table> {
        let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
            "name".to_string(),
            DataType::String,
        )]);

        let rows = Vec::new();

        Table::from_values(schema, rows)
    }

    fn execute_exists_table(&mut self, table_name: &sqlparser::ast::ObjectName) -> Result<Table> {
        let table_str = table_name.to_string();
        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_str)?;

        let storage = self.storage.borrow();
        let exists = storage
            .get_dataset(&dataset_id)
            .and_then(|d| d.get_table(&table_id))
            .is_some();

        let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
            "result".to_string(),
            DataType::Int64,
        )]);
        let rows = vec![vec![Value::int64(if exists { 1 } else { 0 })]];
        Table::from_values(schema, rows)
    }

    fn execute_exists_database(&mut self, db_name: &sqlparser::ast::ObjectName) -> Result<Table> {
        let db_str = db_name.to_string();

        let storage = self.storage.borrow();
        let exists = storage.get_dataset(&db_str).is_some();

        let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
            "result".to_string(),
            DataType::Int64,
        )]);
        let rows = vec![vec![Value::int64(if exists { 1 } else { 0 })]];
        Table::from_values(schema, rows)
    }

    fn execute_create_database(
        &mut self,
        name: &sqlparser::ast::ObjectName,
        if_not_exists: bool,
    ) -> Result<()> {
        let db_id = name.to_string();

        let mut storage = self.storage.borrow_mut();

        if storage.get_dataset(&db_id).is_some() {
            if if_not_exists {
                return Ok(());
            } else {
                return Err(Error::invalid_query(format!(
                    "database \"{}\" already exists",
                    db_id
                )));
            }
        }

        storage
            .create_dataset(db_id.clone())
            .map_err(|e| Error::invalid_query(format!("Failed to create database: {}", e)))?;

        Ok(())
    }

    fn execute_drop_database(&mut self, stmt: &sqlparser::ast::Statement) -> Result<()> {
        use sqlparser::ast::Statement;

        let Statement::Drop {
            names, if_exists, ..
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a DROP DATABASE statement".to_string(),
            ));
        };

        for db_name_obj in names {
            let db_id = db_name_obj.to_string();

            let mut storage = self.storage.borrow_mut();

            match storage.get_dataset(&db_id) {
                Some(_) => {
                    storage.delete_dataset(&db_id)?;
                }
                None => {
                    if !*if_exists {
                        return Err(Error::DatasetNotFound(format!(
                            "Database '{}' not found",
                            db_id
                        )));
                    }
                }
            }
        }

        self.plan_cache.borrow_mut().invalidate_all();

        Ok(())
    }
    fn execute_show_users(&mut self) -> Result<Table> {
        let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
            "name".to_string(),
            DataType::String,
        )]);
        Table::from_values(schema, vec![])
    }

    fn execute_show_roles(&mut self) -> Result<Table> {
        let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
            "name".to_string(),
            DataType::String,
        )]);
        Table::from_values(schema, vec![])
    }

    fn execute_show_grants(&mut self, _user_name: Option<&str>) -> Result<Table> {
        let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
            "grants".to_string(),
            DataType::String,
        )]);
        Table::from_values(schema, vec![])
    }

    fn execute_optimize_table(&mut self, table_name: &sqlparser::ast::ObjectName) -> Result<Table> {
        use std::collections::HashMap;

        use yachtsql_core::types::Value;
        use yachtsql_storage::TableEngine;

        let name = table_name.to_string();
        let (dataset_id, table_id) = self.parse_ddl_table_name(&name)?;

        let mut storage = self.storage.borrow_mut();
        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;
        let table = dataset
            .get_table_mut(&table_id)
            .ok_or_else(|| Error::TableNotFound(format!("Table '{}' not found", table_id)))?;

        let engine = table.engine().clone();
        let schema = table.schema().clone();
        let rows = table.get_all_rows();

        let get_key_indices = |order_by: &[String]| -> Vec<usize> {
            order_by
                .iter()
                .filter_map(|col| {
                    schema
                        .fields()
                        .iter()
                        .position(|f| f.name.eq_ignore_ascii_case(col))
                })
                .collect()
        };

        let get_col_idx = |col_name: &str| -> Option<usize> {
            schema
                .fields()
                .iter()
                .position(|f| f.name.eq_ignore_ascii_case(col_name))
        };

        let get_numeric_indices = |exclude: &[usize]| -> Vec<usize> {
            schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(i, f)| {
                    !exclude.contains(i)
                        && matches!(
                            f.data_type,
                            DataType::Int64 | DataType::Float64 | DataType::Numeric(_)
                        )
                })
                .map(|(i, _)| i)
                .collect()
        };

        let optimized_rows = match &engine {
            TableEngine::ReplacingMergeTree {
                order_by,
                version_column,
            } => {
                let key_indices = get_key_indices(order_by);
                let version_idx = version_column.as_ref().and_then(|vc| get_col_idx(vc));
                let mut groups: HashMap<Vec<Value>, yachtsql_storage::Row> = HashMap::new();
                for row in rows {
                    let key: Vec<Value> = key_indices
                        .iter()
                        .map(|&i| row.get(i).cloned().unwrap_or(Value::null()))
                        .collect();
                    let should_replace = if let Some(ver_idx) = version_idx {
                        groups.get(&key).is_none_or(|existing| {
                            let existing_ver =
                                existing.get(ver_idx).and_then(|v| v.as_i64()).unwrap_or(0);
                            let new_ver = row.get(ver_idx).and_then(|v| v.as_i64()).unwrap_or(0);
                            new_ver >= existing_ver
                        })
                    } else {
                        true
                    };
                    if should_replace {
                        groups.insert(key, row);
                    }
                }
                groups.into_values().collect()
            }
            TableEngine::SummingMergeTree {
                order_by,
                sum_columns,
            } => {
                let key_indices = get_key_indices(order_by);
                let sum_indices = if sum_columns.is_empty() {
                    get_numeric_indices(&key_indices)
                } else {
                    sum_columns.iter().filter_map(|c| get_col_idx(c)).collect()
                };
                let mut groups: HashMap<Vec<Value>, yachtsql_storage::Row> = HashMap::new();
                for row in rows {
                    let key: Vec<Value> = key_indices
                        .iter()
                        .map(|&i| row.get(i).cloned().unwrap_or(Value::null()))
                        .collect();
                    groups
                        .entry(key)
                        .and_modify(|existing| {
                            for &idx in &sum_indices {
                                let existing_val =
                                    existing.get(idx).cloned().unwrap_or(Value::null());
                                let new_val = row.get(idx).cloned().unwrap_or(Value::null());
                                if let (Some(e), Some(n)) =
                                    (existing_val.as_i64(), new_val.as_i64())
                                {
                                    let _ = existing.set(idx, Value::int64(e + n));
                                } else if let (Some(e), Some(n)) =
                                    (existing_val.as_f64(), new_val.as_f64())
                                {
                                    let _ = existing.set(idx, Value::float64(e + n));
                                }
                            }
                        })
                        .or_insert(row);
                }
                groups.into_values().collect()
            }
            TableEngine::CollapsingMergeTree {
                order_by,
                sign_column,
            } => {
                let key_indices = get_key_indices(order_by);
                let sign_idx = get_col_idx(sign_column).unwrap_or(0);
                let mut groups: HashMap<Vec<Value>, Vec<yachtsql_storage::Row>> = HashMap::new();
                for row in rows {
                    let key: Vec<Value> = key_indices
                        .iter()
                        .map(|&i| row.get(i).cloned().unwrap_or(Value::null()))
                        .collect();
                    groups.entry(key).or_default().push(row);
                }
                let mut result = Vec::new();
                for (_key, group_rows) in groups {
                    let mut sum_sign: i64 = 0;
                    let mut last_positive: Option<yachtsql_storage::Row> = None;
                    let mut last_negative: Option<yachtsql_storage::Row> = None;
                    for row in group_rows {
                        let sign = row.get(sign_idx).and_then(|v| v.as_i64()).unwrap_or(1);
                        sum_sign += sign;
                        if sign > 0 {
                            last_positive = Some(row);
                        } else {
                            last_negative = Some(row);
                        }
                    }
                    if sum_sign > 0 {
                        if let Some(r) = last_positive {
                            result.push(r);
                        }
                    } else if sum_sign < 0 {
                        if let Some(r) = last_negative {
                            result.push(r);
                        }
                    }
                }
                result
            }
            _ => rows,
        };

        table.clear_rows()?;
        table.insert_rows(optimized_rows)?;

        drop(storage);
        Self::empty_result()
    }

    fn execute_procedure(&mut self, name: &str, args: &[sqlparser::ast::Value]) -> Result<Table> {
        match name.to_lowercase().as_str() {
            "enable_feature" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "enable_feature() requires at least one argument".to_string(),
                    ));
                }

                let features: Vec<String> = args
                    .iter()
                    .filter_map(|v| match v {
                        sqlparser::ast::Value::SingleQuotedString(s)
                        | sqlparser::ast::Value::DoubleQuotedString(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();

                if features.is_empty() {
                    return Err(Error::invalid_query(
                        "enable_feature() requires string arguments".to_string(),
                    ));
                }

                self.execute_set_capabilities(true, &features)
            }

            "disable_feature" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "disable_feature() requires at least one argument".to_string(),
                    ));
                }

                let features: Vec<String> = args
                    .iter()
                    .filter_map(|v| match v {
                        sqlparser::ast::Value::SingleQuotedString(s)
                        | sqlparser::ast::Value::DoubleQuotedString(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();

                if features.is_empty() {
                    return Err(Error::invalid_query(
                        "disable_feature() requires string arguments".to_string(),
                    ));
                }

                self.execute_set_capabilities(false, &features)
            }

            _ => {
                if let Some(proc_def) = self.session.get_procedure(name).cloned() {
                    debug_eprintln!("[procedure] Executing user-defined procedure: {}", name);
                    let mut last_result = Self::empty_result()?;
                    for stmt in &proc_def.body {
                        let sql = stmt.to_string();
                        last_result = self.execute_sql(&sql)?;
                    }
                    Ok(last_result)
                } else {
                    Err(Error::unsupported_feature(format!(
                        "Unknown procedure: {}",
                        name
                    )))
                }
            }
        }
    }

    fn execute_explain(
        &mut self,
        stmt: &sqlparser::ast::Statement,
        analyze: bool,
        verbose: bool,
    ) -> Result<Table> {
        use sqlparser::ast::Statement as SqlStatement;

        match stmt {
            SqlStatement::Query(query) => {
                let session_udfs = self.session.udfs_for_parser();
                let plan_builder = yachtsql_parser::LogicalPlanBuilder::new()
                    .with_storage(Rc::clone(&self.storage))
                    .with_dialect(self.dialect())
                    .with_udfs(session_udfs);
                let logical_plan = plan_builder.query_to_plan(query)?;

                let optimized = self.optimizer.optimize(logical_plan)?;
                let query_plan = optimized.root().clone();

                let mut output = String::new();
                if analyze {
                    output.push_str("EXPLAIN ANALYZE:\n");

                    let start = crate::explain::FastTimestamp::now();
                    let exec_result = self.execute_select(stmt, "")?;
                    let elapsed = start.elapsed();

                    let actual_rows = exec_result.num_rows();

                    output.push_str(&format!(
                        "  Execution time: {:.2}ms\n",
                        elapsed.as_secs_f64() * 1000.0
                    ));
                    output.push_str(&format!("  Actual rows: {}\n\n", actual_rows));
                }

                let options = crate::explain::ExplainOptions {
                    analyze,
                    verbose,
                    profiler_metrics: None,
                };

                let plan_output = crate::explain::explain_query(&query_plan, options)?;

                if analyze {
                    let mut combined_rows: Vec<Vec<Value>> = vec![vec![Value::string(output)]];
                    if let Ok(plan_rows) = plan_output.rows() {
                        for row in plan_rows {
                            combined_rows.push(row.values().to_vec());
                        }
                    }

                    let schema = plan_output.schema().clone();
                    Table::from_values(schema, combined_rows)
                } else {
                    Ok(plan_output)
                }
            }

            SqlStatement::Insert { .. } => {
                let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
                    "QUERY PLAN".to_string(),
                    DataType::String,
                )]);
                let rows = vec![vec![Value::string(format!(
                    "Insert Statement:\n  {}",
                    stmt
                ))]];
                Table::from_values(schema, rows)
            }

            SqlStatement::Update { .. } => {
                let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
                    "QUERY PLAN".to_string(),
                    DataType::String,
                )]);
                let rows = vec![vec![Value::string(format!(
                    "Update Statement:\n  {}",
                    stmt
                ))]];
                Table::from_values(schema, rows)
            }

            SqlStatement::Delete { .. } => {
                let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
                    "QUERY PLAN".to_string(),
                    DataType::String,
                )]);
                let rows = vec![vec![Value::string(format!(
                    "Delete Statement:\n  {}",
                    stmt
                ))]];
                Table::from_values(schema, rows)
            }

            _ => {
                let schema = Schema::from_fields(vec![yachtsql_storage::Field::required(
                    "QUERY PLAN".to_string(),
                    DataType::String,
                )]);
                let rows = vec![vec![Value::string(format!("Statement:\n  {}", stmt))]];
                Table::from_values(schema, rows)
            }
        }
    }

    pub(crate) fn parse_returning_clause(
        &self,
        returning: &[sqlparser::ast::SelectItem],
        _schema: &Schema,
    ) -> Result<crate::query_executor::returning::ReturningSpec> {
        use sqlparser::ast::SelectItem;

        use crate::query_executor::returning::{
            ReturningAstItem, ReturningColumn, ReturningColumnOrigin, ReturningSpec,
        };

        if returning.is_empty() {
            return Ok(ReturningSpec::None);
        }

        if returning.len() == 1 && matches!(returning[0], SelectItem::Wildcard(_)) {
            return Ok(ReturningSpec::AllColumns);
        }

        let all_simple_columns = returning.iter().all(|item| match item {
            SelectItem::UnnamedExpr(expr) => matches!(expr, sqlparser::ast::Expr::Identifier(_)),
            SelectItem::ExprWithAlias { expr, .. } => {
                matches!(expr, sqlparser::ast::Expr::Identifier(_))
            }
            _ => false,
        });

        if all_simple_columns {
            let mut columns = Vec::new();
            for item in returning {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        if let sqlparser::ast::Expr::Identifier(ident) = expr {
                            columns.push(ReturningColumn {
                                source_name: ident.value.clone(),
                                output_name: ident.value.clone(),
                                origin: ReturningColumnOrigin::Target,
                            });
                        }
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        if let sqlparser::ast::Expr::Identifier(ident) = expr {
                            columns.push(ReturningColumn {
                                source_name: ident.value.clone(),
                                output_name: alias.value.clone(),
                                origin: ReturningColumnOrigin::Target,
                            });
                        }
                    }
                    SelectItem::Wildcard(_) => {
                        return Err(Error::InvalidQuery(
                            "RETURNING * cannot be combined with other columns".to_string(),
                        ));
                    }
                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "Unsupported RETURNING item".to_string(),
                        ));
                    }
                }
            }
            return Ok(ReturningSpec::Columns(columns));
        }

        let mut ast_items = Vec::new();
        for item in returning {
            let output_name = match item {
                SelectItem::UnnamedExpr(expr) => derive_output_name_from_expr(expr),
                SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                SelectItem::Wildcard(_) => {
                    return Err(Error::InvalidQuery(
                        "RETURNING * cannot be combined with other columns".to_string(),
                    ));
                }

                SelectItem::QualifiedWildcard(kind, _) => match kind {
                    sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        format!("{}.*", name)
                    }
                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "Expression-qualified wildcard not supported in RETURNING".to_string(),
                        ));
                    }
                },
            };

            ast_items.push(ReturningAstItem {
                item: item.clone(),
                output_name,
            });
        }

        Ok(ReturningSpec::AstItems(ast_items))
    }

    pub fn plan_cache_stats(&self) -> crate::plan_cache::PlanCacheStats {
        self.plan_cache.borrow().stats()
    }

    pub fn invalidate_plan_cache(&mut self) {
        self.plan_cache.borrow_mut().invalidate_all();
    }

    pub fn refresh_materialized_view(&mut self, dataset_id: &str, view_id: &str) -> Result<usize> {
        let view_sql = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;

            let view = dataset.views().get_view(view_id).ok_or_else(|| {
                Error::InvalidQuery(format!("View '{}.{}' not found", dataset_id, view_id))
            })?;

            if !view.is_materialized() {
                return Err(Error::InvalidQuery(format!(
                    "View '{}.{}' is not a materialized view",
                    dataset_id, view_id
                )));
            }

            view.sql.clone()
        };

        let result = self.execute_sql(&view_sql)?;
        let result_schema = result.schema().clone();
        let result_rows: Vec<yachtsql_storage::Row> =
            result.rows().map(|rows| rows.to_vec()).unwrap_or_default();

        let row_count = result_rows.len();

        let mut storage = self.storage.borrow_mut();
        let dataset = storage
            .get_dataset_mut(dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        let view = dataset.views_mut().get_view_mut(view_id).ok_or_else(|| {
            Error::InvalidQuery(format!("View '{}.{}' not found", dataset_id, view_id))
        })?;

        view.refresh_materialized_data(result_rows, result_schema);

        debug_eprintln!(
            "[executor::execution] Refreshed materialized view '{}.{}' with {} rows",
            dataset_id,
            view_id,
            row_count
        );

        Ok(row_count)
    }

    fn execute_copy(&mut self, stmt: &sqlparser::ast::Statement) -> Result<Table> {
        use sqlparser::ast::{CopyOption, Statement as SqlStatement};

        let (source, to, target, options) = match stmt {
            SqlStatement::Copy {
                source,
                to,
                target,
                options,
                ..
            } => (source, *to, target, options),
            _ => {
                return Err(Error::InternalError(
                    "execute_copy called with non-COPY statement".to_string(),
                ));
            }
        };

        let mut delimiter = ',';
        let mut has_header = false;
        let mut null_string = String::new();
        let mut _quote_char = '"';
        let mut _escape_char = '\\';

        for opt in options {
            match opt {
                CopyOption::Delimiter(c) => delimiter = *c,
                CopyOption::Header(h) => has_header = *h,
                CopyOption::Null(s) => null_string = s.clone(),
                CopyOption::Quote(c) => _quote_char = *c,
                CopyOption::Escape(c) => _escape_char = *c,
                CopyOption::Format(ident) => {
                    let format_name = ident.value.to_uppercase();
                    if format_name == "BINARY" {
                        return Err(Error::unsupported_feature(
                            "COPY with BINARY format is not yet supported".to_string(),
                        ));
                    }

                    if format_name == "TEXT" && delimiter == ',' {
                        delimiter = '\t';
                    }
                }
                _ => {}
            }
        }

        if to {
            self.execute_copy_to(source, target, delimiter, has_header, null_string)
        } else {
            self.execute_copy_from(source, target, delimiter, has_header, null_string)
        }
    }

    fn execute_copy_from(
        &mut self,
        source: &sqlparser::ast::CopySource,
        target: &sqlparser::ast::CopyTarget,
        delimiter: char,
        has_header: bool,
        null_string: String,
    ) -> Result<Table> {
        use sqlparser::ast::{CopySource, CopyTarget};
        use yachtsql_storage::Row;

        let (table_name, columns) = match source {
            CopySource::Table {
                table_name,
                columns,
            } => {
                let name = table_name.to_string();
                let cols: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
                (name, if cols.is_empty() { None } else { Some(cols) })
            }
            CopySource::Query(_) => {
                return Err(Error::invalid_query(
                    "COPY (query) FROM is not supported - use COPY table FROM".to_string(),
                ));
            }
        };

        let file_path = match target {
            CopyTarget::File { filename } => filename.clone(),
            CopyTarget::Stdin => {
                return Err(Error::unsupported_feature(
                    "COPY FROM STDIN is not yet supported".to_string(),
                ));
            }
            CopyTarget::Stdout => {
                return Err(Error::invalid_query(
                    "COPY FROM STDOUT is not valid - use COPY TO STDOUT".to_string(),
                ));
            }
            CopyTarget::Program { .. } => {
                return Err(Error::unsupported_feature(
                    "COPY FROM PROGRAM is not yet supported".to_string(),
                ));
            }
        };

        let content = std::fs::read_to_string(&file_path).map_err(|e| {
            Error::invalid_query(format!(
                "Could not open file '{}' for reading: {}",
                file_path, e
            ))
        })?;

        let (dataset_id, table_id) = self.parse_table_name(&table_name);
        let dataset_id = dataset_id.unwrap_or_else(|| "default".to_string());

        let schema = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;
            let table = dataset.get_table(&table_id).ok_or_else(|| {
                Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
            })?;
            table.schema().clone()
        };

        let target_columns: Vec<String> = match &columns {
            Some(cols) => cols.clone(),
            None => schema.fields().iter().map(|f| f.name.clone()).collect(),
        };

        let mut rows_imported = 0;
        let lines: Vec<&str> = content.lines().collect();
        let start_line = if has_header { 1 } else { 0 };

        for line in lines.iter().skip(start_line) {
            if line.trim().is_empty() {
                continue;
            }

            let values = self.parse_csv_line(line, delimiter)?;

            if values.len() != target_columns.len() {
                return Err(Error::invalid_query(format!(
                    "CSV row has {} columns but expected {} columns",
                    values.len(),
                    target_columns.len()
                )));
            }

            let mut row_values = Vec::with_capacity(schema.fields().len());
            for field in schema.fields() {
                if let Some(idx) = target_columns.iter().position(|c| c == &field.name) {
                    let val = &values[idx];
                    if val.is_empty() || val == &null_string {
                        row_values.push(Value::null());
                    } else {
                        let typed_value = self.parse_value_for_type(val, &field.data_type)?;
                        row_values.push(typed_value);
                    }
                } else {
                    row_values.push(Value::null());
                }
            }

            {
                let mut storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
                    Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
                })?;
                let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
                    Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
                })?;
                table.insert_row(Row::from_values(row_values))?;
            }

            rows_imported += 1;
        }

        self.record_row_count(rows_imported);
        Self::empty_result()
    }

    fn execute_copy_to(
        &mut self,
        source: &sqlparser::ast::CopySource,
        target: &sqlparser::ast::CopyTarget,
        delimiter: char,
        has_header: bool,
        null_string: String,
    ) -> Result<Table> {
        use sqlparser::ast::{CopySource, CopyTarget};

        let file_path = match target {
            CopyTarget::File { filename } => Some(filename.clone()),
            CopyTarget::Stdout => None,
            CopyTarget::Stdin => {
                return Err(Error::invalid_query(
                    "COPY TO STDIN is not valid - use COPY FROM STDIN".to_string(),
                ));
            }
            CopyTarget::Program { .. } => {
                return Err(Error::unsupported_feature(
                    "COPY TO PROGRAM is not yet supported".to_string(),
                ));
            }
        };

        let (data, column_names): (Vec<Vec<Value>>, Vec<String>) = match source {
            CopySource::Table {
                table_name,
                columns,
            } => {
                let name = table_name.to_string();
                let col_filter: Option<Vec<String>> = if columns.is_empty() {
                    None
                } else {
                    Some(columns.iter().map(|c| c.value.clone()).collect())
                };

                let (dataset_id, table_id) = self.parse_table_name(&name);
                let dataset_id = dataset_id.unwrap_or_else(|| "default".to_string());

                let storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                    Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
                })?;
                let table = dataset.get_table(&table_id).ok_or_else(|| {
                    Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
                })?;

                let schema = table.schema();
                let cols = match col_filter {
                    Some(ref filter) => filter.clone(),
                    None => schema.fields().iter().map(|f| f.name.clone()).collect(),
                };

                let all_rows = table.get_all_rows();
                let rows: Vec<Vec<Value>> = all_rows
                    .iter()
                    .map(|row| {
                        cols.iter()
                            .map(|col_name| {
                                if let Some(idx) =
                                    schema.fields().iter().position(|f| &f.name == col_name)
                                {
                                    row.values().get(idx).cloned().unwrap_or(Value::null())
                                } else {
                                    Value::null()
                                }
                            })
                            .collect()
                    })
                    .collect();

                (rows, cols)
            }
            CopySource::Query(query) => {
                let sql = format!("{}", query);
                let result = self.execute_sql(&sql)?;
                let cols: Vec<String> = result
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name.clone())
                    .collect();
                let rows: Vec<Vec<Value>> = result
                    .rows()
                    .map(|rs| rs.iter().map(|r| r.values().to_vec()).collect())
                    .unwrap_or_default();
                (rows, cols)
            }
        };

        let mut output = String::new();

        if has_header {
            output.push_str(&column_names.join(&delimiter.to_string()));
            output.push('\n');
        }

        for row in &data {
            let line: Vec<String> = row
                .iter()
                .map(|v| self.value_to_csv_string(v, &null_string))
                .collect();
            output.push_str(&line.join(&delimiter.to_string()));
            output.push('\n');
        }

        if let Some(path) = file_path {
            std::fs::write(&path, &output).map_err(|e| {
                Error::invalid_query(format!("Could not write to file '{}': {}", path, e))
            })?;
        } else {
            print!("{}", output);
        }

        self.record_row_count(data.len());
        Self::empty_result()
    }

    fn parse_csv_line(&self, line: &str, delimiter: char) -> Result<Vec<String>> {
        let mut values = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(c) = chars.next() {
            if in_quotes {
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        current.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                } else {
                    current.push(c);
                }
            } else if c == '"' {
                in_quotes = true;
            } else if c == delimiter {
                values.push(current.clone());
                current.clear();
            } else {
                current.push(c);
            }
        }
        values.push(current);

        Ok(values)
    }

    fn parse_value_for_type(&self, val: &str, data_type: &DataType) -> Result<Value> {
        match data_type {
            DataType::Int64 => val
                .parse::<i64>()
                .map(Value::int64)
                .map_err(|_| Error::invalid_query(format!("Cannot parse '{}' as INT64", val))),
            DataType::Float64 => val
                .parse::<f64>()
                .map(Value::float64)
                .map_err(|_| Error::invalid_query(format!("Cannot parse '{}' as FLOAT64", val))),
            DataType::String => Ok(Value::string(val.to_string())),
            DataType::Bool => match val.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" => Ok(Value::bool_val(true)),
                "false" | "f" | "0" | "no" => Ok(Value::bool_val(false)),
                _ => Err(Error::invalid_query(format!(
                    "Cannot parse '{}' as BOOL",
                    val
                ))),
            },
            DataType::Date => {
                let date = chrono::NaiveDate::parse_from_str(val, "%Y-%m-%d")
                    .map_err(|_| Error::invalid_query(format!("Cannot parse '{}' as DATE", val)))?;
                Ok(Value::date(date))
            }
            DataType::Timestamp => {
                use chrono::TimeZone;

                if let Ok(naive_ts) =
                    chrono::NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S%.f")
                {
                    let ts = chrono::Utc.from_utc_datetime(&naive_ts);
                    return Ok(Value::timestamp(ts));
                }
                if let Ok(naive_ts) =
                    chrono::NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S")
                {
                    let ts = chrono::Utc.from_utc_datetime(&naive_ts);
                    return Ok(Value::timestamp(ts));
                }
                if let Ok(naive_ts) =
                    chrono::NaiveDateTime::parse_from_str(val, "%Y-%m-%dT%H:%M:%S%.f")
                {
                    let ts = chrono::Utc.from_utc_datetime(&naive_ts);
                    return Ok(Value::timestamp(ts));
                }
                if let Ok(naive_ts) =
                    chrono::NaiveDateTime::parse_from_str(val, "%Y-%m-%dT%H:%M:%S")
                {
                    let ts = chrono::Utc.from_utc_datetime(&naive_ts);
                    return Ok(Value::timestamp(ts));
                }
                Err(Error::invalid_query(format!(
                    "Cannot parse '{}' as TIMESTAMP",
                    val
                )))
            }
            _ => Ok(Value::string(val.to_string())),
        }
    }

    fn value_to_csv_string(&self, value: &Value, null_string: &str) -> String {
        if value.is_null() {
            return null_string.to_string();
        }

        if value.data_type() == DataType::String {
            if let Some(s) = value.as_str() {
                if s.contains(',') || s.contains('"') || s.contains('\n') {
                    return format!("\"{}\"", s.replace('"', "\"\""));
                } else {
                    return s.to_string();
                }
            }
        }

        value.to_string()
    }

    fn execute_scripting(&mut self, operation: ScriptingOperation) -> Result<Table> {
        match operation {
            ScriptingOperation::Declare {
                names,
                data_type,
                default_expr,
            } => {
                let data_type = self.convert_sql_data_type(data_type)?;
                let default_value = if let Some(expr) = default_expr {
                    Some(self.evaluate_constant_expr(&expr)?)
                } else {
                    None
                };

                for name in names {
                    self.session
                        .declare_variable(name, data_type.clone(), default_value.clone());
                }

                Self::empty_result()
            }
            ScriptingOperation::SetVariable { name, value } => {
                let evaluated = self.evaluate_constant_expr(&value)?;
                self.session.set_variable(&name, evaluated)?;
                Self::empty_result()
            }
            ScriptingOperation::If { stmt } => self.execute_if_statement(&stmt),
            ScriptingOperation::While { stmt } => self.execute_while_statement(&stmt),
            ScriptingOperation::Loop { stmt } => self.execute_loop_statement(&stmt),
            ScriptingOperation::Repeat { stmt } => self.execute_repeat_statement(&stmt),
            ScriptingOperation::BeginEnd { stmt } => self.execute_begin_end_statement(&stmt),
            ScriptingOperation::Case { stmt } => self.execute_case_statement(&stmt),
            ScriptingOperation::Leave { label } => Err(Error::invalid_query(
                "LEAVE without enclosing LOOP".to_string(),
            )),
            ScriptingOperation::Continue { label } => Err(Error::invalid_query(
                "CONTINUE without enclosing LOOP".to_string(),
            )),
            ScriptingOperation::Return { value: _ } => {
                Err(Error::invalid_query("RETURN".to_string()))
            }
            ScriptingOperation::ExecuteImmediate { stmt } => self.execute_execute_immediate(&stmt),
        }
    }

    fn execute_if_statement(&mut self, stmt: &SqlStatement) -> Result<Table> {
        use sqlparser::ast::Statement as Stmt;

        let Stmt::If(if_stmt) = stmt else {
            return Err(Error::internal("Expected IF statement".to_string()));
        };

        let condition =
            if_stmt.if_block.condition.as_ref().ok_or_else(|| {
                Error::invalid_query("IF statement requires a condition".to_string())
            })?;

        let cond_result = self.evaluate_constant_expr(condition)?;
        let cond_bool = cond_result.as_bool().ok_or_else(|| {
            Error::invalid_query("IF condition must evaluate to boolean".to_string())
        })?;

        if cond_bool {
            for inner_stmt in if_stmt.if_block.statements() {
                self.execute_inner_statement(inner_stmt)?;
            }
            return Self::empty_result();
        }

        for elseif_block in &if_stmt.elseif_blocks {
            if let Some(condition) = &elseif_block.condition {
                let cond_result = self.evaluate_constant_expr(condition)?;
                let cond_bool = cond_result.as_bool().ok_or_else(|| {
                    Error::invalid_query("ELSEIF condition must evaluate to boolean".to_string())
                })?;

                if cond_bool {
                    for inner_stmt in elseif_block.statements() {
                        self.execute_inner_statement(inner_stmt)?;
                    }
                    return Self::empty_result();
                }
            }
        }

        if let Some(else_block) = &if_stmt.else_block {
            for inner_stmt in else_block.statements() {
                self.execute_inner_statement(inner_stmt)?;
            }
        }

        Self::empty_result()
    }

    fn execute_while_statement(&mut self, stmt: &SqlStatement) -> Result<Table> {
        use sqlparser::ast::Statement as Stmt;

        let Stmt::While(while_stmt) = stmt else {
            return Err(Error::internal("Expected WHILE statement".to_string()));
        };

        let condition = while_stmt.while_block.condition.as_ref().ok_or_else(|| {
            Error::invalid_query("WHILE statement requires a condition".to_string())
        })?;

        const MAX_ITERATIONS: usize = 100000;
        let mut iterations = 0;

        loop {
            iterations += 1;
            if iterations > MAX_ITERATIONS {
                return Err(Error::invalid_query(format!(
                    "WHILE loop exceeded maximum iterations ({})",
                    MAX_ITERATIONS
                )));
            }

            let cond_result = self.evaluate_constant_expr(condition)?;
            let cond_bool = cond_result.as_bool().ok_or_else(|| {
                Error::invalid_query("WHILE condition must evaluate to boolean".to_string())
            })?;

            if !cond_bool {
                break;
            }

            match self.execute_statement_block(while_stmt.while_block.statements()) {
                Ok(_) => continue,
                Err(e) if Self::is_leave_error(&e) => break,
                Err(e) if Self::is_continue_error(&e) => continue,
                Err(e) => return Err(e),
            }
        }

        Self::empty_result()
    }

    fn execute_loop_statement(&mut self, _stmt: &SqlStatement) -> Result<Table> {
        Err(Error::unsupported_feature("LOOP statement".to_string()))
    }

    fn execute_repeat_statement(&mut self, _stmt: &SqlStatement) -> Result<Table> {
        Err(Error::unsupported_feature("REPEAT statement".to_string()))
    }

    fn execute_loop_custom(&mut self, label: Option<String>, body: &str) -> Result<Table> {
        const MAX_ITERATIONS: usize = 100000;
        let mut iterations = 0;

        loop {
            iterations += 1;
            if iterations > MAX_ITERATIONS {
                return Err(Error::invalid_query(format!(
                    "LOOP exceeded maximum iterations ({})",
                    MAX_ITERATIONS
                )));
            }

            match self.execute_body_statements(body) {
                Ok(_) => continue,
                Err(e) if Self::is_leave_error(&e) => {
                    if Self::label_matches_error(&e, &label) {
                        break;
                    }
                    return Err(e);
                }
                Err(e) if Self::is_continue_error(&e) => {
                    if Self::label_matches_error(&e, &label) {
                        continue;
                    }
                    return Err(e);
                }
                Err(e) if Self::is_break_error(&e) => break,
                Err(e) => return Err(e),
            }
        }

        Self::empty_result()
    }

    fn execute_repeat_custom(
        &mut self,
        label: Option<String>,
        body: &str,
        until_condition: &str,
    ) -> Result<Table> {
        const MAX_ITERATIONS: usize = 100000;
        let mut iterations = 0;

        loop {
            iterations += 1;
            if iterations > MAX_ITERATIONS {
                return Err(Error::invalid_query(format!(
                    "REPEAT exceeded maximum iterations ({})",
                    MAX_ITERATIONS
                )));
            }

            match self.execute_body_statements(body) {
                Ok(_) => {}
                Err(e) if Self::is_leave_error(&e) => {
                    if Self::label_matches_error(&e, &label) {
                        break;
                    }
                    return Err(e);
                }
                Err(e) if Self::is_continue_error(&e) => {
                    if Self::label_matches_error(&e, &label) {
                        continue;
                    }
                    return Err(e);
                }
                Err(e) if Self::is_break_error(&e) => break,
                Err(e) => return Err(e),
            }

            let cond_result = self.evaluate_sql_condition(until_condition)?;
            if cond_result {
                break;
            }
        }

        Self::empty_result()
    }

    fn execute_while_custom(
        &mut self,
        label: Option<String>,
        condition: &str,
        body: &str,
    ) -> Result<Table> {
        const MAX_ITERATIONS: usize = 100000;
        let mut iterations = 0;

        loop {
            let cond_result = self.evaluate_sql_condition(condition)?;
            if !cond_result {
                break;
            }

            iterations += 1;
            if iterations > MAX_ITERATIONS {
                return Err(Error::invalid_query(format!(
                    "WHILE exceeded maximum iterations ({})",
                    MAX_ITERATIONS
                )));
            }

            match self.execute_body_statements(body) {
                Ok(_) => continue,
                Err(e) if Self::is_leave_error(&e) => {
                    if Self::label_matches_error(&e, &label) {
                        break;
                    }
                    return Err(e);
                }
                Err(e) if Self::is_continue_error(&e) => {
                    if Self::label_matches_error(&e, &label) {
                        continue;
                    }
                    return Err(e);
                }
                Err(e) if Self::is_break_error(&e) => break,
                Err(e) => return Err(e),
            }
        }

        Self::empty_result()
    }

    fn execute_for_custom(
        &mut self,
        _label: Option<String>,
        variable: &str,
        query: &str,
        body: &str,
    ) -> Result<Table> {
        let result_table = self.execute_sql(query)?;
        let schema = result_table.schema();
        let column_names: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
        let data_types: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| f.data_type.clone())
            .collect();

        for row_idx in 0..result_table.num_rows() {
            let row = result_table.row(row_idx)?;
            let values = row.values();
            for (col_idx, col_name) in column_names.iter().enumerate() {
                let full_name = format!("{}.{}", variable, col_name);
                let value = values[col_idx].clone();
                let data_type = data_types[col_idx].clone();
                self.session
                    .declare_variable(full_name.clone(), data_type, Some(value));
            }

            match self.execute_body_statements(body) {
                Ok(_) => continue,
                Err(e) if Self::is_leave_error(&e) || Self::is_break_error(&e) => break,
                Err(e) if Self::is_continue_error(&e) => continue,
                Err(e) => return Err(e),
            }
        }

        Self::empty_result()
    }

    fn execute_body_statements(&mut self, body: &str) -> Result<Table> {
        let preprocessed = Self::preprocess_control_flow(body);
        let statements = Self::split_body_statements(&preprocessed);
        for stmt_sql in statements {
            let stmt_sql = stmt_sql.trim();
            if stmt_sql.is_empty() {
                continue;
            }
            let table = self.execute_sql(stmt_sql)?;
            Self::check_control_flow_marker(&table)?;
        }
        Self::empty_result()
    }

    fn preprocess_control_flow(sql: &str) -> String {
        use regex::Regex;

        let leave_re = Regex::new(r"(?i)\bLEAVE\s+(\w+)\s*;").unwrap();
        let sql = leave_re
            .replace_all(sql, "SELECT '__LEAVE__$1';")
            .to_string();

        let leave_no_label_re = Regex::new(r"(?i)\bLEAVE\s*;").unwrap();
        let sql = leave_no_label_re
            .replace_all(&sql, "SELECT '__LEAVE__';")
            .to_string();

        let continue_re = Regex::new(r"(?i)\bCONTINUE\s+(\w+)\s*;").unwrap();
        let sql = continue_re
            .replace_all(&sql, "SELECT '__CONTINUE__$1';")
            .to_string();

        let continue_no_label_re = Regex::new(r"(?i)\bCONTINUE\s*;").unwrap();
        let sql = continue_no_label_re
            .replace_all(&sql, "SELECT '__CONTINUE__';")
            .to_string();

        let break_re = Regex::new(r"(?i)\bBREAK\s+(\w+)\s*;").unwrap();
        let sql = break_re
            .replace_all(&sql, "SELECT '__BREAK__$1';")
            .to_string();

        let break_no_label_re = Regex::new(r"(?i)\bBREAK\s*;").unwrap();
        let sql = break_no_label_re
            .replace_all(&sql, "SELECT '__BREAK__';")
            .to_string();

        sql
    }

    fn split_body_statements(sql: &str) -> Vec<String> {
        let mut statements = Vec::new();
        let mut start = 0;
        let mut depth = 0;
        let upper = sql.to_uppercase();

        let chars: Vec<char> = sql.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            if upper[i..].starts_with("BEGIN")
                || upper[i..].starts_with("IF ")
                || upper[i..].starts_with("LOOP")
                || upper[i..].starts_with("WHILE")
                || upper[i..].starts_with("REPEAT")
                || upper[i..].starts_with("FOR ")
                || upper[i..].starts_with("CASE ")
            {
                depth += 1;
            }

            if upper[i..].starts_with("END IF")
                || upper[i..].starts_with("END LOOP")
                || upper[i..].starts_with("END WHILE")
                || upper[i..].starts_with("END REPEAT")
                || upper[i..].starts_with("END FOR")
                || upper[i..].starts_with("END CASE")
            {
                depth -= 1;
            }
            if depth == 0 && upper[i..].starts_with("END;") {
                depth -= 1;
            }
            if depth == 0 && upper[i..].starts_with("END ") {
                let rest = upper[i + 4..].trim_start();
                if rest.starts_with(';') || rest.is_empty() {
                    depth -= 1;
                }
            }

            if chars[i] == ';' && depth == 0 {
                let stmt = sql[start..i].to_string();
                if !stmt.trim().is_empty() {
                    statements.push(stmt);
                }
                start = i + 1;
            }

            i += 1;
        }

        if start < sql.len() {
            let stmt = sql[start..].to_string();
            if !stmt.trim().is_empty() {
                statements.push(stmt);
            }
        }

        statements
    }

    fn evaluate_sql_condition(&mut self, condition: &str) -> Result<bool> {
        let select_sql = format!("SELECT {}", condition);
        let result = self.execute_sql(&select_sql)?;
        if result.num_rows() != 1 {
            return Err(Error::invalid_query(
                "Condition must evaluate to a single value".to_string(),
            ));
        }
        let row = result.row(0)?;
        let value = &row.values()[0];
        value
            .as_bool()
            .ok_or_else(|| Error::invalid_query("Condition must evaluate to boolean".to_string()))
    }

    fn is_break_error(e: &Error) -> bool {
        matches!(e, Error::InvalidQuery(msg) if msg.contains("BREAK"))
    }

    fn label_matches_error(e: &Error, label: &Option<String>) -> bool {
        match e {
            Error::InvalidQuery(msg) => {
                if let Some(lbl) = label {
                    let leave_pattern = format!("LEAVE {}", lbl);
                    let continue_pattern = format!("CONTINUE {}", lbl);
                    msg.contains(&leave_pattern) || msg.contains(&continue_pattern)
                } else {
                    !msg.contains(' ')
                        || msg == "LEAVE"
                        || msg == "CONTINUE"
                        || msg.trim() == "LEAVE"
                        || msg.trim() == "CONTINUE"
                }
            }
            _ => false,
        }
    }

    fn execute_begin_end_statement(&mut self, stmt: &SqlStatement) -> Result<Table> {
        use sqlparser::ast::Statement as Stmt;

        let Stmt::StartTransaction {
            statements,
            exception,
            ..
        } = stmt
        else {
            return Err(Error::internal(
                "Expected StartTransaction statement".to_string(),
            ));
        };

        self.session.push_variable_scope();
        let result = self.execute_begin_end_block(statements, exception.as_ref());
        self.session.pop_variable_scope();
        result
    }

    fn execute_begin_end_block(
        &mut self,
        statements: &[SqlStatement],
        exception: Option<&Vec<sqlparser::ast::ExceptionWhen>>,
    ) -> Result<Table> {
        let block_result = (|| {
            for inner_stmt in statements {
                match self.execute_inner_statement(inner_stmt) {
                    Ok(_) => continue,
                    Err(e) if Self::is_return_error(&e) => {
                        return Self::empty_result();
                    }
                    Err(e) => return Err(e),
                }
            }
            Self::empty_result()
        })();

        match block_result {
            Ok(result) => Ok(result),
            Err(e) => {
                if let Some(exception_handlers) = exception {
                    for handler in exception_handlers {
                        let matches_error = handler.idents.iter().any(|ident| {
                            let name = ident.value.to_uppercase();
                            name == "ERROR" || name == "OTHER"
                        });
                        if matches_error {
                            for handler_stmt in &handler.statements {
                                self.execute_inner_statement(handler_stmt)?;
                            }
                            return Self::empty_result();
                        }
                    }
                }
                Err(e)
            }
        }
    }

    fn execute_case_statement(&mut self, stmt: &SqlStatement) -> Result<Table> {
        use sqlparser::ast::Statement as Stmt;

        let Stmt::Case(case_stmt) = stmt else {
            return Err(Error::internal("Expected CASE statement".to_string()));
        };

        let match_operand = case_stmt
            .match_expr
            .as_ref()
            .map(|e| self.evaluate_constant_expr(e))
            .transpose()?;

        for when_block in &case_stmt.when_blocks {
            if let Some(condition) = &when_block.condition {
                let cond_result = if let Some(match_val) = &match_operand {
                    let when_val = self.evaluate_constant_expr(condition)?;
                    self.evaluate_binary_op(
                        match_val,
                        &sqlparser::ast::BinaryOperator::Eq,
                        &when_val,
                    )?
                } else {
                    self.evaluate_constant_expr(condition)?
                };

                let cond_bool = cond_result.as_bool().ok_or_else(|| {
                    Error::invalid_query("CASE WHEN condition must evaluate to boolean".to_string())
                })?;

                if cond_bool {
                    for inner_stmt in when_block.statements() {
                        self.execute_inner_statement(inner_stmt)?;
                    }
                    return Self::empty_result();
                }
            }
        }

        if let Some(else_block) = &case_stmt.else_block {
            for inner_stmt in else_block.statements() {
                self.execute_inner_statement(inner_stmt)?;
            }
        }

        Self::empty_result()
    }

    fn execute_execute_immediate(&mut self, stmt: &SqlStatement) -> Result<Table> {
        use sqlparser::ast::{ObjectNamePart, Statement as Stmt};

        let Stmt::Execute {
            name, parameters, ..
        } = stmt
        else {
            return Err(Error::internal(
                "Expected EXECUTE IMMEDIATE statement".to_string(),
            ));
        };

        let sql_string = if let Some(name) = name {
            if name.0.is_empty() {
                return Err(Error::invalid_query(
                    "EXECUTE IMMEDIATE requires a string expression".to_string(),
                ));
            }
            let part = &name.0[0];
            match part {
                ObjectNamePart::Identifier(ident) => {
                    if ident.quote_style == Some('\'') {
                        ident.value.clone()
                    } else {
                        let sql_expr = sqlparser::ast::Expr::Identifier(ident.clone());
                        let sql_value = self.evaluate_constant_expr(&sql_expr)?;
                        sql_value
                            .as_str()
                            .ok_or_else(|| {
                                Error::invalid_query(
                                    "EXECUTE IMMEDIATE argument must be a string".to_string(),
                                )
                            })?
                            .to_string()
                    }
                }
                ObjectNamePart::Function(_) => {
                    return Err(Error::invalid_query(
                        "EXECUTE IMMEDIATE does not support function expressions".to_string(),
                    ));
                }
            }
        } else if !parameters.is_empty() {
            let sql_value = self.evaluate_constant_expr(&parameters[0])?;
            sql_value
                .as_str()
                .ok_or_else(|| {
                    Error::invalid_query("EXECUTE IMMEDIATE argument must be a string".to_string())
                })?
                .to_string()
        } else {
            return Err(Error::invalid_query(
                "EXECUTE IMMEDIATE requires a string expression".to_string(),
            ));
        };

        self.execute_sql(&sql_string)
    }

    fn execute_inner_statement(&mut self, stmt: &SqlStatement) -> Result<Table> {
        let sql = stmt.to_string();
        let preprocessed = Self::preprocess_control_flow(&sql);
        let table = self.execute_sql(&preprocessed)?;
        Self::check_control_flow_marker(&table)
    }

    fn check_control_flow_marker(table: &Table) -> Result<Table> {
        if table.num_rows() == 1 {
            if let Ok(row) = table.row(0) {
                let vals = row.values();
                if !vals.is_empty() {
                    if let Some(s) = vals[0].as_str() {
                        if s.starts_with("__LEAVE__") {
                            let label = s.strip_prefix("__LEAVE__").unwrap_or("");
                            let msg = if label.is_empty() {
                                "LEAVE".to_string()
                            } else {
                                format!("LEAVE {}", label)
                            };
                            return Err(Error::invalid_query(msg));
                        }
                        if s.starts_with("__CONTINUE__") {
                            let label = s.strip_prefix("__CONTINUE__").unwrap_or("");
                            let msg = if label.is_empty() {
                                "CONTINUE".to_string()
                            } else {
                                format!("CONTINUE {}", label)
                            };
                            return Err(Error::invalid_query(msg));
                        }
                        if s.starts_with("__BREAK__") {
                            let label = s.strip_prefix("__BREAK__").unwrap_or("");
                            let msg = if label.is_empty() {
                                "BREAK".to_string()
                            } else {
                                format!("BREAK {}", label)
                            };
                            return Err(Error::invalid_query(msg));
                        }
                    }
                }
            }
        }
        Ok(table.clone())
    }

    fn execute_statement_block(&mut self, stmts: &[SqlStatement]) -> Result<Table> {
        let mut last_result = Self::empty_result()?;
        for stmt in stmts {
            last_result = self.execute_inner_statement(stmt)?;
        }
        Ok(last_result)
    }

    fn is_leave_error(e: &Error) -> bool {
        matches!(e, Error::InvalidQuery(msg) if msg.contains("LEAVE"))
    }

    fn is_continue_error(e: &Error) -> bool {
        matches!(e, Error::InvalidQuery(msg) if msg.contains("CONTINUE"))
    }

    fn is_return_error(e: &Error) -> bool {
        matches!(e, Error::InvalidQuery(msg) if msg.contains("RETURN"))
    }

    fn convert_sql_data_type(
        &self,
        sql_type: Option<sqlparser::ast::DataType>,
    ) -> Result<DataType> {
        use sqlparser::ast::DataType as SqlType;

        match sql_type {
            None => Ok(DataType::String),
            Some(t) => match t {
                SqlType::Int64 | SqlType::BigInt(_) => Ok(DataType::Int64),
                SqlType::Int32 | SqlType::Int(_) | SqlType::Integer(_) => Ok(DataType::Int64),
                SqlType::Float64 | SqlType::Double(_) => Ok(DataType::Float64),
                SqlType::Float32 => Ok(DataType::Float32),
                SqlType::Float(_) | SqlType::Real => Ok(DataType::Float64),
                SqlType::Bool | SqlType::Boolean => Ok(DataType::Bool),
                SqlType::String(_) | SqlType::Text | SqlType::Varchar(_) | SqlType::Char(_) => {
                    Ok(DataType::String)
                }
                SqlType::Date => Ok(DataType::Date),
                SqlType::Timestamp(_, _) => Ok(DataType::Timestamp),
                SqlType::Numeric(_) | SqlType::Decimal(_) => Ok(DataType::Numeric(None)),
                SqlType::Bytes(_) => Ok(DataType::Bytes),
                _ => Err(Error::unsupported_feature(format!(
                    "Unsupported data type for variable declaration: {:?}",
                    t
                ))),
            },
        }
    }

    fn evaluate_constant_expr(&self, expr: &sqlparser::ast::Expr) -> Result<Value> {
        use sqlparser::ast::{
            BinaryOperator, Expr as SqlExpr, UnaryOperator, Value as SqlValue, ValueWithSpan,
        };

        match expr {
            SqlExpr::Value(ValueWithSpan { value, .. }) => match value {
                SqlValue::Number(n, _) => {
                    if n.contains('.') {
                        n.parse::<f64>()
                            .map(Value::float64)
                            .map_err(|_| Error::invalid_query(format!("Invalid number: {}", n)))
                    } else {
                        n.parse::<i64>()
                            .map(Value::int64)
                            .map_err(|_| Error::invalid_query(format!("Invalid integer: {}", n)))
                    }
                }
                SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                    Ok(Value::string(s.clone()))
                }
                SqlValue::Boolean(b) => Ok(Value::bool_val(*b)),
                SqlValue::Null => Ok(Value::null()),
                _ => Err(Error::unsupported_feature(format!(
                    "Unsupported literal value type: {:?}",
                    value
                ))),
            },
            SqlExpr::Identifier(ident) => {
                let var_name = &ident.value;
                if let Some(var) = self.session.get_variable(var_name) {
                    var.value.clone().ok_or_else(|| {
                        Error::invalid_query(format!("Variable '{}' has no value", var_name))
                    })
                } else {
                    Err(Error::invalid_query(format!(
                        "Undeclared variable: {}",
                        var_name
                    )))
                }
            }
            SqlExpr::CompoundIdentifier(parts) => {
                let var_name = parts
                    .iter()
                    .map(|p| p.value.as_str())
                    .collect::<Vec<_>>()
                    .join(".");
                if let Some(var) = self.session.get_variable(&var_name) {
                    var.value.clone().ok_or_else(|| {
                        Error::invalid_query(format!("Variable '{}' has no value", var_name))
                    })
                } else {
                    Err(Error::invalid_query(format!(
                        "Undeclared variable: {}",
                        var_name
                    )))
                }
            }
            SqlExpr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_constant_expr(left)?;
                let right_val = self.evaluate_constant_expr(right)?;
                self.evaluate_binary_op(&left_val, op, &right_val)
            }
            SqlExpr::UnaryOp { op, expr } => {
                let inner = self.evaluate_constant_expr(expr)?;
                match op {
                    UnaryOperator::Minus => {
                        if let Some(i) = inner.as_i64() {
                            Ok(Value::int64(-i))
                        } else if let Some(f) = inner.as_f64() {
                            Ok(Value::float64(-f))
                        } else {
                            Err(Error::invalid_query(
                                "Cannot negate non-numeric value".to_string(),
                            ))
                        }
                    }
                    UnaryOperator::Plus => Ok(inner),
                    UnaryOperator::Not => {
                        if let Some(b) = inner.as_bool() {
                            Ok(Value::bool_val(!b))
                        } else {
                            Err(Error::invalid_query(
                                "Cannot apply NOT to non-boolean value".to_string(),
                            ))
                        }
                    }
                    _ => Err(Error::unsupported_feature(format!(
                        "Unsupported unary operator: {:?}",
                        op
                    ))),
                }
            }
            SqlExpr::Nested(inner) => self.evaluate_constant_expr(inner),
            SqlExpr::Subquery(query) => self.evaluate_scalar_subquery(query),
            _ => Err(Error::unsupported_feature(format!(
                "Unsupported expression type for constant evaluation: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &Value,
        op: &sqlparser::ast::BinaryOperator,
        right: &Value,
    ) -> Result<Value> {
        use sqlparser::ast::BinaryOperator;

        match op {
            BinaryOperator::Plus => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::int64(l + r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::float64(l + r))
                } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    Ok(Value::string(format!("{}{}", l, r)))
                } else {
                    Err(Error::invalid_query(
                        "Cannot add incompatible types".to_string(),
                    ))
                }
            }
            BinaryOperator::Minus => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::int64(l - r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::float64(l - r))
                } else {
                    Err(Error::invalid_query(
                        "Cannot subtract incompatible types".to_string(),
                    ))
                }
            }
            BinaryOperator::Multiply => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::int64(l * r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::float64(l * r))
                } else {
                    Err(Error::invalid_query(
                        "Cannot multiply incompatible types".to_string(),
                    ))
                }
            }
            BinaryOperator::Divide => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    if r == 0 {
                        Err(Error::invalid_query("Division by zero".to_string()))
                    } else {
                        Ok(Value::int64(l / r))
                    }
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    if r == 0.0 {
                        Err(Error::invalid_query("Division by zero".to_string()))
                    } else {
                        Ok(Value::float64(l / r))
                    }
                } else {
                    Err(Error::invalid_query(
                        "Cannot divide incompatible types".to_string(),
                    ))
                }
            }
            BinaryOperator::Modulo => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    if r == 0 {
                        Err(Error::invalid_query("Modulo by zero".to_string()))
                    } else {
                        Ok(Value::int64(l % r))
                    }
                } else {
                    Err(Error::invalid_query(
                        "Modulo only supported for integers".to_string(),
                    ))
                }
            }
            BinaryOperator::Gt => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::bool_val(l > r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::bool_val(l > r))
                } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    Ok(Value::bool_val(l > r))
                } else {
                    Err(Error::invalid_query(
                        "Cannot compare incompatible types".to_string(),
                    ))
                }
            }
            BinaryOperator::GtEq => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::bool_val(l >= r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::bool_val(l >= r))
                } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    Ok(Value::bool_val(l >= r))
                } else {
                    Err(Error::invalid_query(
                        "Cannot compare incompatible types".to_string(),
                    ))
                }
            }
            BinaryOperator::Lt => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::bool_val(l < r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::bool_val(l < r))
                } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    Ok(Value::bool_val(l < r))
                } else {
                    Err(Error::invalid_query(
                        "Cannot compare incompatible types".to_string(),
                    ))
                }
            }
            BinaryOperator::LtEq => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::bool_val(l <= r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::bool_val(l <= r))
                } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    Ok(Value::bool_val(l <= r))
                } else {
                    Err(Error::invalid_query(
                        "Cannot compare incompatible types".to_string(),
                    ))
                }
            }
            BinaryOperator::Eq => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::bool_val(l == r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::bool_val(l == r))
                } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    Ok(Value::bool_val(l == r))
                } else if let (Some(l), Some(r)) = (left.as_bool(), right.as_bool()) {
                    Ok(Value::bool_val(l == r))
                } else {
                    Err(Error::invalid_query(
                        "Cannot compare incompatible types".to_string(),
                    ))
                }
            }
            BinaryOperator::NotEq => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    Ok(Value::bool_val(l != r))
                } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    Ok(Value::bool_val(l != r))
                } else if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
                    Ok(Value::bool_val(l != r))
                } else if let (Some(l), Some(r)) = (left.as_bool(), right.as_bool()) {
                    Ok(Value::bool_val(l != r))
                } else {
                    Err(Error::invalid_query(
                        "Cannot compare incompatible types".to_string(),
                    ))
                }
            }
            BinaryOperator::And => {
                if let (Some(l), Some(r)) = (left.as_bool(), right.as_bool()) {
                    Ok(Value::bool_val(l && r))
                } else {
                    Err(Error::invalid_query(
                        "AND requires boolean operands".to_string(),
                    ))
                }
            }
            BinaryOperator::Or => {
                if let (Some(l), Some(r)) = (left.as_bool(), right.as_bool()) {
                    Ok(Value::bool_val(l || r))
                } else {
                    Err(Error::invalid_query(
                        "OR requires boolean operands".to_string(),
                    ))
                }
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unsupported binary operator: {:?}",
                op
            ))),
        }
    }

    fn evaluate_scalar_subquery(&self, query: &sqlparser::ast::Query) -> Result<Value> {
        let subquery_executor = self.create_subquery_executor();
        let sql = query.to_string();
        let result = subquery_executor.execute_subquery(&sql)?;

        if result.num_rows() != 1 {
            return Err(Error::invalid_query(format!(
                "Scalar subquery must return exactly one row, but returned {}",
                result.num_rows()
            )));
        }

        if result.num_columns() != 1 {
            return Err(Error::invalid_query(format!(
                "Scalar subquery must return exactly one column, but returned {}",
                result.num_columns()
            )));
        }

        result
            .column(0)
            .ok_or_else(|| Error::internal("Scalar subquery returned no column"))?
            .get(0)
    }

    fn execute_export_data(
        &mut self,
        uri: &str,
        format: yachtsql_parser::validator::ExportFormat,
        _overwrite: bool,
        _header: bool,
        _field_delimiter: Option<char>,
        _compression: Option<String>,
        query: &str,
    ) -> Result<Table> {
        use yachtsql_parser::validator::ExportFormat;

        debug_eprintln!(
            "[executor::export_data] uri={}, format={:?}, query={}",
            uri,
            format,
            query
        );

        let _query_result = self.execute_sql(query)?;

        let format_str = match format {
            ExportFormat::Csv => "CSV",
            ExportFormat::Json => "JSON",
            ExportFormat::Parquet => "PARQUET",
            ExportFormat::Avro => "AVRO",
        };

        debug_eprintln!(
            "[executor::export_data] Exported data to {} in {} format",
            uri,
            format_str
        );

        Self::empty_result()
    }

    fn execute_load_data(
        &mut self,
        table_name: &sqlparser::ast::ObjectName,
        overwrite: bool,
        is_temp: bool,
        temp_table_schema: Option<Vec<(String, String)>>,
        format: yachtsql_parser::validator::ExportFormat,
        uris: &[String],
        _allow_schema_update: bool,
    ) -> Result<Table> {
        use yachtsql_parser::validator::ExportFormat;

        debug_eprintln!(
            "[executor::load_data] table={}, overwrite={}, is_temp={}, format={:?}, uris={:?}",
            table_name,
            overwrite,
            is_temp,
            format,
            uris
        );

        let table_str = table_name.to_string();

        if is_temp && temp_table_schema.is_some() {
            let schema = temp_table_schema.unwrap();
            let columns: Vec<String> = schema
                .iter()
                .map(|(name, dtype)| format!("{} {}", name, dtype))
                .collect();
            let create_sql = format!("CREATE TEMP TABLE {} ({})", table_str, columns.join(", "));
            debug_eprintln!(
                "[executor::load_data] Creating temp table with: {}",
                create_sql
            );
            self.execute_sql(&create_sql)?;
        }

        if overwrite {
            let truncate_sql = format!("TRUNCATE TABLE {}", table_str);
            debug_eprintln!(
                "[executor::load_data] Truncating table with: {}",
                truncate_sql
            );
            let _ = self.execute_sql(&truncate_sql);
        }

        let format_str = match format {
            ExportFormat::Csv => "CSV",
            ExportFormat::Json => "JSON",
            ExportFormat::Parquet => "PARQUET",
            ExportFormat::Avro => "AVRO",
        };

        debug_eprintln!(
            "[executor::load_data] Would load {} file(s) in {} format into {}",
            uris.len(),
            format_str,
            table_str
        );

        Self::empty_result()
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct StorageSequenceExecutor {
    storage: Rc<RefCell<yachtsql_storage::Storage>>,
}

impl StorageSequenceExecutor {
    pub fn new(storage: Rc<RefCell<yachtsql_storage::Storage>>) -> Self {
        Self { storage }
    }

    fn parse_table_name(name: &str) -> (String, String) {
        if let Some((schema, table)) = name.split_once('.') {
            (schema.to_string(), table.to_string())
        } else {
            ("default".to_string(), name.to_string())
        }
    }
}

impl crate::query_executor::evaluator::physical_plan::SequenceValueExecutor
    for StorageSequenceExecutor
{
    fn nextval(&mut self, sequence_name: &str) -> Result<i64> {
        let (dataset_id, seq_id) = Self::parse_table_name(sequence_name);

        let mut storage = self.storage.borrow_mut();
        let dataset = storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
            Error::invalid_query(format!("Sequence '{}' does not exist", sequence_name))
        })?;

        dataset.sequences_mut().nextval(&seq_id)
    }

    fn currval(&self, sequence_name: &str) -> Result<i64> {
        let (dataset_id, seq_id) = Self::parse_table_name(sequence_name);

        let storage = self.storage.borrow();
        let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
            Error::invalid_query(format!("Sequence '{}' does not exist", sequence_name))
        })?;

        dataset.sequences().currval(&seq_id)
    }

    fn setval(&mut self, sequence_name: &str, value: i64, is_called: bool) -> Result<i64> {
        let (dataset_id, seq_id) = Self::parse_table_name(sequence_name);

        let mut storage = self.storage.borrow_mut();
        let dataset = storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
            Error::invalid_query(format!("Sequence '{}' does not exist", sequence_name))
        })?;

        dataset.sequences_mut().setval(&seq_id, value, is_called)
    }

    fn lastval(&self) -> Result<i64> {
        let storage = self.storage.borrow();
        let dataset = storage.get_dataset("default").ok_or_else(|| {
            Error::InvalidOperation(
                "LASTVAL: no sequences have been accessed in this session".to_string(),
            )
        })?;

        dataset.sequences().lastval()
    }
}

fn derive_output_name_from_expr(expr: &sqlparser::ast::Expr) -> String {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
        sqlparser::ast::Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| "?column?".to_string()),
        sqlparser::ast::Expr::Function(func) => func.name.to_string().to_lowercase(),
        sqlparser::ast::Expr::BinaryOp { .. } => "?column?".to_string(),
        sqlparser::ast::Expr::UnaryOp { expr: inner, .. } => derive_output_name_from_expr(inner),
        sqlparser::ast::Expr::Nested(inner) => derive_output_name_from_expr(inner),
        sqlparser::ast::Expr::Cast { .. } => "?column?".to_string(),
        _ => "?column?".to_string(),
    }
}

fn format_default_value(default: &yachtsql_storage::DefaultValue) -> String {
    match default {
        yachtsql_storage::DefaultValue::Literal(v) => v.to_string(),
        yachtsql_storage::DefaultValue::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
        yachtsql_storage::DefaultValue::CurrentDate => "CURRENT_DATE".to_string(),
        yachtsql_storage::DefaultValue::GenRandomUuid => "gen_random_uuid()".to_string(),
    }
}
