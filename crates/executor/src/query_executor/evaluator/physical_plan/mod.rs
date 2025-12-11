use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

use yachtsql_capability::FeatureId;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::DataType;
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::{Schema, TableEngine};

use crate::Table;

mod aggregate;
mod aggregate_strategy;
mod array_join;
pub mod cte;
mod distinct;
mod expression;
mod index_scan;
mod join;
mod join_strategy;
mod limit;
mod merge;
mod pivot;
mod set_ops;
mod sort;
mod tablesample;
mod type_inference;
mod values;
mod window;

pub use aggregate::{AggregateExec, SortAggregateExec};
pub use aggregate_strategy::AggregateStrategy;
pub use array_join::ArrayJoinExec;
pub use cte::{CteExec, EmptyRelationExec, MaterializedViewScanExec, SubqueryScanExec};
pub use distinct::{DistinctExec, DistinctOnExec};
pub use index_scan::IndexScanExec;
pub use join::{
    AsOfJoinExec, HashJoinExec, IndexNestedLoopJoinExec, LateralJoinExec, MergeJoinExec,
    NestedLoopJoinExec, PasteJoinExec,
};
pub use join_strategy::JoinStrategy;
pub use limit::{LimitExec, LimitPercentExec};
pub use merge::MergeExec;
pub use pivot::{PivotAggregateFunction, PivotExec, UnpivotExec};
pub use set_ops::{ExceptExec, IntersectExec, UnionExec};
pub use sort::SortExec;
pub use tablesample::{SampleSize, SamplingMethod, TableSampleExec};
pub use values::{ValuesExec, infer_values_schema};
pub use window::WindowExec;

thread_local! {
    pub(crate) static FEATURE_REGISTRY_CONTEXT: std::cell::RefCell<Option<Rc<yachtsql_capability::FeatureRegistry>>> =
        const { std::cell::RefCell::new(None) };
}

thread_local! {
    pub(super) static SUBQUERY_EXECUTOR_CONTEXT: std::cell::RefCell<Option<Rc<dyn SubqueryExecutor>>> =
        const { std::cell::RefCell::new(None) };
}

thread_local! {
    pub(crate) static SEQUENCE_EXECUTOR_CONTEXT: std::cell::RefCell<Option<Rc<RefCell<dyn SequenceValueExecutor>>>> =
        const { std::cell::RefCell::new(None) };
}

thread_local! {
    pub(super) static CORRELATION_CONTEXT: std::cell::RefCell<Option<crate::CorrelationContext>> =
        const { std::cell::RefCell::new(None) };
}

thread_local! {
    pub(crate) static UNCORRELATED_SUBQUERY_CACHE: std::cell::RefCell<std::collections::HashMap<u64, CachedSubqueryResult>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

thread_local! {
    pub(super) static STORAGE_CONTEXT: std::cell::RefCell<Option<Rc<RefCell<yachtsql_storage::Storage>>>> =
        const { std::cell::RefCell::new(None) };
}

#[derive(Clone, Debug)]
pub enum CachedSubqueryResult {
    Scalar(yachtsql_core::types::Value),
    Exists(bool),
    InList(Vec<yachtsql_core::types::Value>),
}

pub trait SubqueryExecutor {
    fn execute_scalar_subquery(
        &self,
        plan: &yachtsql_optimizer::plan::PlanNode,
    ) -> Result<yachtsql_core::types::Value>;

    fn execute_exists_subquery(&self, plan: &yachtsql_optimizer::plan::PlanNode) -> Result<bool>;

    fn execute_in_subquery(
        &self,
        plan: &yachtsql_optimizer::plan::PlanNode,
    ) -> Result<Vec<yachtsql_core::types::Value>>;

    fn execute_tuple_in_subquery(
        &self,
        plan: &yachtsql_optimizer::plan::PlanNode,
    ) -> Result<Vec<Vec<yachtsql_core::types::Value>>>;
}

pub trait SequenceValueExecutor {
    fn nextval(&mut self, sequence_name: &str) -> Result<i64>;
    fn currval(&self, sequence_name: &str) -> Result<i64>;
    fn setval(&mut self, sequence_name: &str, value: i64, is_called: bool) -> Result<i64>;
    fn lastval(&self) -> Result<i64>;
}

pub(crate) struct FeatureRegistryContextGuard {
    previous: Option<Rc<yachtsql_capability::FeatureRegistry>>,
}

impl FeatureRegistryContextGuard {
    fn set(registry: Rc<yachtsql_capability::FeatureRegistry>) -> Self {
        let previous = FEATURE_REGISTRY_CONTEXT.with(|ctx| {
            let mut slot = ctx.borrow_mut();
            let prior = slot.clone();
            *slot = Some(registry);
            prior
        });
        Self { previous }
    }
}

impl Drop for FeatureRegistryContextGuard {
    fn drop(&mut self) {
        let previous = self.previous.clone();
        FEATURE_REGISTRY_CONTEXT.with(|ctx| {
            *ctx.borrow_mut() = previous;
        });
    }
}

pub(crate) struct SubqueryExecutorContextGuard {
    previous: Option<Rc<dyn SubqueryExecutor>>,
}

impl SubqueryExecutorContextGuard {
    pub(crate) fn set(executor: Rc<dyn SubqueryExecutor>) -> Self {
        let previous = SUBQUERY_EXECUTOR_CONTEXT.with(|ctx| {
            let mut slot = ctx.borrow_mut();
            let prior = slot.clone();
            *slot = Some(executor);
            prior
        });
        Self { previous }
    }
}

impl Drop for SubqueryExecutorContextGuard {
    fn drop(&mut self) {
        let previous = self.previous.clone();
        SUBQUERY_EXECUTOR_CONTEXT.with(|ctx| {
            *ctx.borrow_mut() = previous;
        });
    }
}

pub(crate) struct SequenceExecutorContextGuard {
    previous: Option<Rc<RefCell<dyn SequenceValueExecutor>>>,
}

impl SequenceExecutorContextGuard {
    pub(crate) fn set(executor: Rc<RefCell<dyn SequenceValueExecutor>>) -> Self {
        let previous = SEQUENCE_EXECUTOR_CONTEXT.with(|ctx| {
            let mut slot = ctx.borrow_mut();
            let prior = slot.clone();
            *slot = Some(executor);
            prior
        });
        Self { previous }
    }
}

impl Drop for SequenceExecutorContextGuard {
    fn drop(&mut self) {
        let previous = self.previous.clone();
        SEQUENCE_EXECUTOR_CONTEXT.with(|ctx| {
            *ctx.borrow_mut() = previous;
        });
    }
}

pub(crate) struct SubqueryCacheGuard;

impl SubqueryCacheGuard {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl Drop for SubqueryCacheGuard {
    fn drop(&mut self) {
        UNCORRELATED_SUBQUERY_CACHE.with(|cache| {
            cache.borrow_mut().clear();
        });
    }
}

pub(crate) struct StorageContextGuard {
    previous: Option<Rc<RefCell<yachtsql_storage::Storage>>>,
}

impl StorageContextGuard {
    pub(crate) fn set(storage: Rc<RefCell<yachtsql_storage::Storage>>) -> Self {
        let previous = STORAGE_CONTEXT.with(|ctx| {
            let mut slot = ctx.borrow_mut();
            let prior = slot.clone();
            *slot = Some(storage);
            prior
        });
        Self { previous }
    }
}

impl Drop for StorageContextGuard {
    fn drop(&mut self) {
        let previous = self.previous.clone();
        STORAGE_CONTEXT.with(|ctx| {
            *ctx.borrow_mut() = previous;
        });
    }
}

pub(crate) fn hash_plan(plan: &yachtsql_optimizer::plan::PlanNode) -> u64 {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let plan_str = format!("{:?}", plan);
    let mut hasher = DefaultHasher::new();
    plan_str.hash(&mut hasher);
    hasher.finish()
}

#[allow(dead_code)]
fn require_feature_in_context(feature_id: FeatureId, feature_name: &str) -> Result<()> {
    let registry = FEATURE_REGISTRY_CONTEXT
        .with(|ctx| ctx.borrow().clone())
        .ok_or_else(|| {
            Error::InternalError(
                "Feature registry context missing during capability check".to_string(),
            )
        })?;

    if registry.is_enabled(feature_id) {
        Ok(())
    } else {
        Err(Error::unsupported_feature(format!(
            "Feature {} ({}) is not enabled",
            feature_id, feature_name
        )))
    }
}

#[allow(dead_code)]
pub(crate) fn infer_expr_type_for_returning(expr: &Expr, schema: &Schema) -> Option<DataType> {
    ProjectionWithExprExec::infer_expr_type_with_schema(expr, schema)
        .or_else(|| ProjectionWithExprExec::infer_expr_type(expr))
}

pub trait ExecutionPlan: fmt::Debug {
    fn schema(&self) -> &Schema;
    fn execute(&self) -> Result<Vec<Table>>;
    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>>;
    fn statistics(&self) -> ExecutionStatistics {
        ExecutionStatistics::default()
    }

    fn describe(&self) -> String;
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionStatistics {
    pub num_rows: Option<usize>,

    pub memory_usage: Option<usize>,

    pub is_sorted: bool,

    pub sort_columns: Option<Vec<String>>,
}

impl ExecutionStatistics {
    pub fn is_sorted_on(&self, columns: &[&str]) -> bool {
        if !self.is_sorted {
            return false;
        }
        match &self.sort_columns {
            Some(sort_cols) if sort_cols.len() >= columns.len() => columns
                .iter()
                .zip(sort_cols.iter())
                .all(|(expected, actual)| *expected == actual),
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    root: Rc<dyn ExecutionPlan>,
    schema: Schema,
}

impl PhysicalPlan {
    pub fn new(root: Rc<dyn ExecutionPlan>) -> Self {
        let schema = root.schema().clone();
        Self { root, schema }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn execute(&self) -> Result<Vec<Table>> {
        self.root.execute()
    }

    pub fn root(&self) -> &Rc<dyn ExecutionPlan> {
        &self.root
    }

    pub fn display_tree(&self) -> String {
        self.format_tree(self.root.as_ref(), 0)
    }

    fn format_tree(&self, plan: &dyn ExecutionPlan, indent: usize) -> String {
        let prefix = "  ".repeat(indent);
        let mut result = format!("{}{}\n", prefix, plan.describe());

        for child in plan.children() {
            result.push_str(&Self::format_tree_impl(child.as_ref(), indent + 1));
        }

        result
    }

    fn format_tree_impl(plan: &dyn ExecutionPlan, indent: usize) -> String {
        let prefix = "  ".repeat(indent);
        let mut result = format!("{}{}\n", prefix, plan.describe());

        for child in plan.children() {
            result.push_str(&Self::format_tree_impl(child.as_ref(), indent + 1));
        }

        result
    }
}

#[derive(Debug)]
pub struct TableScanExec {
    schema: Schema,
    table_name: String,
    projection: Option<Vec<usize>>,
    statistics: ExecutionStatistics,
    storage: Rc<RefCell<yachtsql_storage::Storage>>,
    transaction_manager: Option<Rc<RefCell<yachtsql_storage::TransactionManager>>>,
    only: bool,
    final_modifier: bool,
}

impl TableScanExec {
    pub fn new(
        schema: Schema,
        table_name: String,
        storage: Rc<RefCell<yachtsql_storage::Storage>>,
    ) -> Self {
        Self {
            schema,
            table_name,
            projection: None,
            statistics: ExecutionStatistics::default(),
            storage,
            transaction_manager: None,
            only: false,
            final_modifier: false,
        }
    }

    pub fn new_with_only(
        schema: Schema,
        table_name: String,
        storage: Rc<RefCell<yachtsql_storage::Storage>>,
        only: bool,
    ) -> Self {
        Self {
            schema,
            table_name,
            projection: None,
            statistics: ExecutionStatistics::default(),
            storage,
            transaction_manager: None,
            only,
            final_modifier: false,
        }
    }

    pub fn new_with_final(
        schema: Schema,
        table_name: String,
        storage: Rc<RefCell<yachtsql_storage::Storage>>,
        only: bool,
        final_modifier: bool,
    ) -> Self {
        Self {
            schema,
            table_name,
            projection: None,
            statistics: ExecutionStatistics::default(),
            storage,
            transaction_manager: None,
            only,
            final_modifier,
        }
    }

    pub fn new_with_transaction(
        schema: Schema,
        table_name: String,
        storage: Rc<RefCell<yachtsql_storage::Storage>>,
        transaction_manager: Rc<RefCell<yachtsql_storage::TransactionManager>>,
        only: bool,
        final_modifier: bool,
    ) -> Self {
        Self {
            schema,
            table_name,
            projection: None,
            statistics: ExecutionStatistics::default(),
            storage,
            transaction_manager: Some(transaction_manager),
            only,
            final_modifier,
        }
    }

    #[cfg(test)]
    pub fn new_empty(schema: Schema, table_name: String) -> Self {
        use yachtsql_storage::Storage;
        let storage = Rc::new(RefCell::new(Storage::new()));
        Self {
            schema,
            table_name,
            projection: None,
            statistics: ExecutionStatistics::default(),
            storage,
            transaction_manager: None,
            only: false,
            final_modifier: false,
        }
    }

    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_statistics(mut self, statistics: ExecutionStatistics) -> Self {
        self.statistics = statistics;
        self
    }

    fn apply_final_merge(
        &self,
        rows: Vec<yachtsql_storage::Row>,
        engine: &TableEngine,
        table_schema: &Schema,
    ) -> Result<Vec<yachtsql_storage::Row>> {
        use std::collections::HashMap;

        use yachtsql_core::types::Value;

        match engine {
            TableEngine::SummingMergeTree {
                order_by,
                sum_columns,
            } => {
                let key_indices = self.get_key_column_indices(order_by, table_schema);
                let sum_indices = if sum_columns.is_empty() {
                    self.get_numeric_column_indices(table_schema, &key_indices)
                } else {
                    self.get_column_indices(sum_columns, table_schema)
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

                Ok(groups.into_values().collect())
            }

            TableEngine::ReplacingMergeTree {
                order_by,
                version_column,
            } => {
                let key_indices = self.get_key_column_indices(order_by, table_schema);
                let version_idx = version_column
                    .as_ref()
                    .and_then(|vc| self.get_column_index(vc, table_schema));

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

                Ok(groups.into_values().collect())
            }

            TableEngine::CollapsingMergeTree {
                order_by,
                sign_column,
            } => {
                let key_indices = self.get_key_column_indices(order_by, table_schema);
                let sign_idx = self
                    .get_column_index(sign_column, table_schema)
                    .unwrap_or(0);

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
                        if let Some(row) = last_positive {
                            result.push(row);
                        }
                    } else if sum_sign < 0 {
                        if let Some(row) = last_negative {
                            result.push(row);
                        }
                    }
                }

                Ok(result)
            }

            TableEngine::VersionedCollapsingMergeTree {
                order_by,
                sign_column,
                version_column,
            } => {
                let key_indices = self.get_key_column_indices(order_by, table_schema);
                let sign_idx = self
                    .get_column_index(sign_column, table_schema)
                    .unwrap_or(0);
                let version_idx = self
                    .get_column_index(version_column, table_schema)
                    .unwrap_or(0);

                let mut groups: HashMap<Vec<Value>, Vec<yachtsql_storage::Row>> = HashMap::new();

                for row in rows {
                    let key: Vec<Value> = key_indices
                        .iter()
                        .map(|&i| row.get(i).cloned().unwrap_or(Value::null()))
                        .collect();
                    groups.entry(key).or_default().push(row);
                }

                let mut result = Vec::new();
                for (_key, mut group_rows) in groups {
                    group_rows.sort_by(|a, b| {
                        let va = a.get(version_idx).and_then(|v| v.as_i64()).unwrap_or(0);
                        let vb = b.get(version_idx).and_then(|v| v.as_i64()).unwrap_or(0);
                        va.cmp(&vb)
                    });

                    let mut sum_sign: i64 = 0;
                    let mut last_positive: Option<yachtsql_storage::Row> = None;

                    for row in group_rows {
                        let sign = row.get(sign_idx).and_then(|v| v.as_i64()).unwrap_or(1);
                        sum_sign += sign;
                        if sign > 0 {
                            last_positive = Some(row);
                        }
                    }

                    if sum_sign > 0 {
                        if let Some(row) = last_positive {
                            result.push(row);
                        }
                    }
                }

                Ok(result)
            }

            _ => Ok(rows),
        }
    }

    fn get_key_column_indices(&self, order_by: &[String], schema: &Schema) -> Vec<usize> {
        order_by
            .iter()
            .filter_map(|col| self.get_column_index(col, schema))
            .collect()
    }

    fn get_column_index(&self, col_name: &str, schema: &Schema) -> Option<usize> {
        let col_lower = col_name.to_lowercase();
        schema
            .fields()
            .iter()
            .position(|f| f.name.to_lowercase() == col_lower)
    }

    fn get_column_indices(&self, col_names: &[String], schema: &Schema) -> Vec<usize> {
        col_names
            .iter()
            .filter_map(|col| self.get_column_index(col, schema))
            .collect()
    }

    fn get_numeric_column_indices(&self, schema: &Schema, exclude: &[usize]) -> Vec<usize> {
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
    }
}

impl ExecutionPlan for TableScanExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let storage = self.storage.borrow();

        let (dataset_name, table_id) = if let Some(dot_pos) = self.table_name.find('.') {
            let dataset = &self.table_name[..dot_pos];
            let table = &self.table_name[dot_pos + 1..];
            (dataset, table)
        } else {
            ("default", self.table_name.as_str())
        };

        let dataset = storage.get_dataset(dataset_name).ok_or_else(|| {
            Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_name))
        })?;

        let table = dataset
            .get_table(table_id)
            .ok_or_else(|| Error::TableNotFound(format!("Table '{}' not found", table_id)))?;

        let (actual_table, actual_engine) = match table.engine() {
            TableEngine::Distributed {
                database,
                table: target_table,
                ..
            } => {
                let target_db = if database.is_empty() {
                    dataset_name
                } else {
                    database.as_str()
                };
                let target_dataset = storage.get_dataset(target_db).ok_or_else(|| {
                    Error::DatasetNotFound(format!("Dataset '{}' not found", target_db))
                })?;
                let underlying = target_dataset.get_table(target_table).ok_or_else(|| {
                    Error::TableNotFound(format!("Table '{}' not found", target_table))
                })?;
                (underlying, underlying.engine().clone())
            }
            engine => (table, engine.clone()),
        };

        let mut all_rows = actual_table.get_all_rows();

        if let TableEngine::Buffer {
            database,
            table: target_table,
        } = table.engine()
        {
            let target_db = if database.is_empty() {
                dataset_name
            } else {
                database.as_str()
            };
            if let Some(target_dataset) = storage.get_dataset(target_db) {
                if let Some(dest_table) = target_dataset.get_table(target_table) {
                    let dest_rows = dest_table.get_all_rows();
                    all_rows.extend(dest_rows);
                }
            }
        }

        if let Some(ref tm) = self.transaction_manager {
            let manager = tm.borrow();
            if let Some(txn) = manager.get_active_transaction() {
                let table_full_name = format!("{}.{}", dataset_name, table_id);
                if let Some(pending_changes) = txn.pending_changes() {
                    if let Some(delta) = pending_changes.get_table_delta(&table_full_name) {
                        all_rows.extend(delta.inserted_rows.clone());
                    }
                }
            }
        }

        if !self.only {
            let parent_col_count = self.schema.fields().len();
            let mut descendants_to_process: Vec<String> = table.schema().child_tables().to_vec();
            let mut processed: std::collections::HashSet<String> = std::collections::HashSet::new();

            while let Some(child_full_name) = descendants_to_process.pop() {
                if processed.contains(&child_full_name) {
                    continue;
                }
                processed.insert(child_full_name.clone());

                let (child_dataset_name, child_table_id) =
                    if let Some(dot_pos) = child_full_name.find('.') {
                        (&child_full_name[..dot_pos], &child_full_name[dot_pos + 1..])
                    } else {
                        ("default", child_full_name.as_str())
                    };

                if let Some(child_dataset) = storage.get_dataset(child_dataset_name) {
                    if let Some(child_table) = child_dataset.get_table(child_table_id) {
                        let child_rows = child_table.get_all_rows();
                        for row in child_rows {
                            let values: Vec<_> = row
                                .values()
                                .iter()
                                .take(parent_col_count)
                                .cloned()
                                .collect();
                            all_rows.push(yachtsql_storage::Row::from_values(values));
                        }
                        for grandchild in child_table.schema().child_tables() {
                            if !processed.contains(grandchild) {
                                descendants_to_process.push(grandchild.clone());
                            }
                        }
                    }
                }
            }
        }

        if self.final_modifier {
            all_rows = self.apply_final_merge(all_rows, &actual_engine, actual_table.schema())?;
        }

        if all_rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        use yachtsql_storage::Column;
        let num_rows = all_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns: Vec<Column> = Vec::with_capacity(num_cols);

        let table_oid = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            self.table_name.hash(&mut hasher);
            (hasher.finish() & 0xFFFFFFFF) as i64
        };

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_rows);

            let is_system_column = matches!(
                field.data_type,
                yachtsql_core::types::DataType::Tid
                    | yachtsql_core::types::DataType::Xid
                    | yachtsql_core::types::DataType::Cid
                    | yachtsql_core::types::DataType::Oid
            );

            for (row_idx, row) in all_rows.iter().enumerate() {
                let value = if is_system_column {
                    match field.name.as_str() {
                        "ctid" => yachtsql_core::types::Value::int64((row_idx + 1) as i64),
                        "xmin" => yachtsql_core::types::Value::int64(1),
                        "xmax" => yachtsql_core::types::Value::int64(0),
                        "cmin" => yachtsql_core::types::Value::int64(0),
                        "cmax" => yachtsql_core::types::Value::int64(0),
                        "tableoid" => yachtsql_core::types::Value::int64(table_oid),
                        _ => yachtsql_core::types::Value::null(),
                    }
                } else {
                    match row.get(col_idx) {
                        Some(v) => v.clone(),
                        None => yachtsql_core::types::Value::null(),
                    }
                };
                column.push(value)?;
            }

            columns.push(column);
        }

        Ok(vec![Table::new(self.schema.clone(), columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![]
    }

    fn statistics(&self) -> ExecutionStatistics {
        self.statistics.clone()
    }

    fn describe(&self) -> String {
        match &self.projection {
            Some(proj) => format!("TableScan: {} projection={:?}", self.table_name, proj),
            None => format!("TableScan: {}", self.table_name),
        }
    }
}

#[derive(Debug)]
pub struct ProjectionExec {
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,
    projection: Vec<usize>,
}

impl ProjectionExec {
    pub fn new(input: Rc<dyn ExecutionPlan>, schema: Schema, projection: Vec<usize>) -> Self {
        Self {
            input,
            schema,
            projection,
        }
    }
}

impl ExecutionPlan for ProjectionExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let input_batches = self.input.execute()?;
        let mut output_batches = Vec::with_capacity(input_batches.len());

        for batch in input_batches {
            output_batches.push(batch.project(&self.projection)?);
        }

        Ok(output_batches)
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!("Projection: {:?}", self.projection)
    }
}

#[derive(Debug)]
pub struct ProjectionWithExprExec {
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,
    expressions: Vec<(crate::optimizer::expr::Expr, Option<String>)>,
    dialect: crate::DialectType,
    feature_registry: Rc<yachtsql_capability::FeatureRegistry>,
}

impl ProjectionWithExprExec {
    pub fn new(
        input: Rc<dyn ExecutionPlan>,
        schema: Schema,
        expressions: Vec<(Expr, Option<String>)>,
    ) -> Self {
        Self {
            input,
            schema,
            expressions,
            dialect: crate::DialectType::PostgreSQL,
            feature_registry: Rc::new(yachtsql_capability::FeatureRegistry::new(
                crate::DialectType::PostgreSQL,
            )),
        }
    }

    pub fn with_dialect(mut self, dialect: crate::DialectType) -> Self {
        self.dialect = dialect;
        self
    }

    pub fn with_feature_registry(
        mut self,
        registry: Rc<yachtsql_capability::FeatureRegistry>,
    ) -> Self {
        self.feature_registry = registry;
        self
    }
}

impl ExecutionPlan for ProjectionWithExprExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        use yachtsql_storage::Column;

        let _registry_guard = Self::enter_feature_registry_context(self.feature_registry.clone());

        let input_batches = self.input.execute()?;
        let mut output_batches = Vec::new();

        let outer_table_aliases = Self::extract_outer_table_aliases(&self.expressions);

        let srf_indices = Self::find_set_returning_function_indices(&self.expressions);

        for batch in input_batches {
            if batch.is_empty() {
                output_batches.push(Table::empty(self.schema.clone()));
                continue;
            }

            let num_rows = batch.num_rows();

            let occurrence_indices = Self::compute_column_occurrence_indices(&self.expressions);

            if srf_indices.is_empty() {
                let mut output_columns: Vec<Column> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| Column::new(&f.data_type, num_rows))
                    .collect();

                for row_idx in 0..num_rows {
                    let correlation_ctx = Self::build_correlation_context_with_aliases(
                        &batch,
                        row_idx,
                        &outer_table_aliases,
                    )?;

                    CORRELATION_CONTEXT.with(|ctx| {
                        *ctx.borrow_mut() = Some(correlation_ctx);
                    });

                    for (expr_idx, (expr, _alias)) in self.expressions.iter().enumerate() {
                        let occurrence_index = occurrence_indices[expr_idx];
                        let value = Self::evaluate_expr_with_occurrence(
                            expr,
                            &batch,
                            row_idx,
                            occurrence_index,
                            self.dialect,
                        );
                        match value {
                            Ok(v) => output_columns[expr_idx].push(v)?,
                            Err(e) => return Err(e),
                        }
                    }

                    CORRELATION_CONTEXT.with(|ctx| {
                        *ctx.borrow_mut() = None;
                    });
                }

                let output_batch = Table::new(self.schema.clone(), output_columns)?;
                output_batches.push(output_batch);
            } else {
                let mut all_rows: Vec<Vec<yachtsql_core::types::Value>> = Vec::new();

                for row_idx in 0..num_rows {
                    let correlation_ctx = Self::build_correlation_context_with_aliases(
                        &batch,
                        row_idx,
                        &outer_table_aliases,
                    )?;

                    CORRELATION_CONTEXT.with(|ctx| {
                        *ctx.borrow_mut() = Some(correlation_ctx);
                    });

                    let mut row_values: Vec<yachtsql_core::types::Value> =
                        Vec::with_capacity(self.expressions.len());
                    let mut srf_expansion_count = 1usize;

                    for (expr_idx, (expr, _alias)) in self.expressions.iter().enumerate() {
                        let occurrence_index = occurrence_indices[expr_idx];
                        let value = Self::evaluate_expr_with_occurrence(
                            expr,
                            &batch,
                            row_idx,
                            occurrence_index,
                            self.dialect,
                        )?;

                        if srf_indices.contains(&expr_idx) {
                            if let Some(arr) = value.as_array() {
                                srf_expansion_count = srf_expansion_count.max(arr.len());
                            }
                        }
                        row_values.push(value);
                    }

                    CORRELATION_CONTEXT.with(|ctx| {
                        *ctx.borrow_mut() = None;
                    });

                    for expansion_idx in 0..srf_expansion_count {
                        let mut expanded_row: Vec<yachtsql_core::types::Value> =
                            Vec::with_capacity(self.expressions.len());
                        for (expr_idx, value) in row_values.iter().enumerate() {
                            if srf_indices.contains(&expr_idx) {
                                if let Some(arr) = value.as_array() {
                                    if expansion_idx < arr.len() {
                                        expanded_row.push(arr[expansion_idx].clone());
                                    } else {
                                        expanded_row.push(yachtsql_core::types::Value::null());
                                    }
                                } else {
                                    expanded_row.push(value.clone());
                                }
                            } else {
                                expanded_row.push(value.clone());
                            }
                        }
                        all_rows.push(expanded_row);
                    }
                }

                let mut output_columns: Vec<Column> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| Column::new(&f.data_type, all_rows.len()))
                    .collect();

                for row in all_rows {
                    for (col_idx, value) in row.into_iter().enumerate() {
                        output_columns[col_idx].push(value)?;
                    }
                }

                let output_batch = Table::new(self.schema.clone(), output_columns)?;
                output_batches.push(output_batch);
            }
        }

        Ok(output_batches)
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!("ProjectionWithExpr: {:?}", self.expressions)
    }
}

impl ProjectionWithExprExec {
    fn build_correlation_context(
        batch: &Table,
        row_idx: usize,
    ) -> Result<crate::CorrelationContext> {
        Self::build_correlation_context_with_aliases(batch, row_idx, &[])
    }

    fn build_correlation_context_with_aliases(
        batch: &Table,
        row_idx: usize,
        outer_table_aliases: &[String],
    ) -> Result<crate::CorrelationContext> {
        let mut ctx = crate::CorrelationContext::new();

        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            if let Some(column) = batch.column(col_idx) {
                let value = column.get(row_idx)?;

                ctx.bind(field.name.clone(), value.clone());

                let col_name = if let Some(dot_pos) = field.name.rfind('.') {
                    let col_name_only = &field.name[dot_pos + 1..];
                    ctx.bind(col_name_only.to_string(), value.clone());
                    col_name_only
                } else {
                    field.name.as_str()
                };

                if let Some(source_table) = &field.source_table {
                    ctx.bind(format!("{}.{}", source_table, col_name), value.clone());
                }

                for alias in outer_table_aliases {
                    ctx.bind(format!("{}.{}", alias, col_name), value.clone());
                }
            }
        }

        Ok(ctx)
    }

    fn extract_outer_table_aliases(expressions: &[(Expr, Option<String>)]) -> Vec<String> {
        let mut aliases = std::collections::HashSet::new();
        for (expr, _) in expressions {
            Self::collect_table_aliases_from_expr(expr, &mut aliases);
        }
        aliases.into_iter().collect()
    }

    fn find_set_returning_function_indices(
        expressions: &[(Expr, Option<String>)],
    ) -> std::collections::HashSet<usize> {
        let mut indices = std::collections::HashSet::new();
        for (idx, (expr, _)) in expressions.iter().enumerate() {
            if Self::is_set_returning_function(expr) {
                indices.insert(idx);
            }
        }
        indices
    }

    fn is_set_returning_function(expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => {
                let fn_name = name.as_str();
                matches!(
                    fn_name,
                    "SKEYS" | "SVALS" | "JSON_OBJECT_KEYS" | "JSONB_OBJECT_KEYS"
                )
            }
            Expr::Cast { expr, .. } | Expr::TryCast { expr, .. } => {
                Self::is_set_returning_function(expr)
            }
            _ => false,
        }
    }

    fn collect_table_aliases_from_expr(
        expr: &Expr,
        aliases: &mut std::collections::HashSet<String>,
    ) {
        match expr {
            Expr::Column { table: Some(t), .. } => {
                aliases.insert(t.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_table_aliases_from_expr(left, aliases);
                Self::collect_table_aliases_from_expr(right, aliases);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_table_aliases_from_expr(expr, aliases);
            }
            Expr::Function { args, .. } | Expr::Aggregate { args, .. } => {
                for arg in args {
                    Self::collect_table_aliases_from_expr(arg, aliases);
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    Self::collect_table_aliases_from_expr(op, aliases);
                }
                for (w, t) in when_then {
                    Self::collect_table_aliases_from_expr(w, aliases);
                    Self::collect_table_aliases_from_expr(t, aliases);
                }
                if let Some(e) = else_expr {
                    Self::collect_table_aliases_from_expr(e, aliases);
                }
            }
            Expr::Cast { expr, .. } | Expr::TryCast { expr, .. } => {
                Self::collect_table_aliases_from_expr(expr, aliases);
            }
            _ => {}
        }
    }
}

#[derive(Debug)]
pub struct FilterExec {
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,
    predicate: Expr,
}

impl FilterExec {
    pub fn new(input: Rc<dyn ExecutionPlan>, predicate: Expr) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            schema,
            predicate,
        }
    }
}

impl ExecutionPlan for FilterExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        use yachtsql_storage::Column;

        let input_batches = self.input.execute()?;
        let mut output_batches = Vec::with_capacity(input_batches.len());

        for batch in input_batches {
            if batch.is_empty() {
                output_batches.push(batch);
                continue;
            }

            let num_rows = batch.num_rows();
            let mut passing_rows = Vec::new();

            for row_idx in 0..num_rows {
                let correlation_ctx =
                    ProjectionWithExprExec::build_correlation_context(&batch, row_idx)?;
                CORRELATION_CONTEXT.with(|ctx| {
                    *ctx.borrow_mut() = Some(correlation_ctx);
                });

                let result =
                    ProjectionWithExprExec::evaluate_expr(&self.predicate, &batch, row_idx);

                CORRELATION_CONTEXT.with(|ctx| {
                    *ctx.borrow_mut() = None;
                });

                let result = result?;

                if let Some(true) = result.as_bool() {
                    passing_rows.push(row_idx);
                }
            }

            if passing_rows.is_empty() {
                output_batches.push(Table::empty(self.schema.clone()));
                continue;
            }

            let mut output_columns = Vec::with_capacity(batch.schema().fields().len());
            for col_idx in 0..batch.schema().fields().len() {
                let input_column = batch.column(col_idx).ok_or_else(|| {
                    Error::InternalError(format!("Column {} not found in batch", col_idx))
                })?;
                let field = &batch.schema().fields()[col_idx];

                let mut output_column = Column::new(&field.data_type, passing_rows.len());
                for &row_idx in &passing_rows {
                    let value = input_column.get(row_idx)?;
                    output_column.push(value)?;
                }
                output_columns.push(output_column);
            }

            let filtered_batch = Table::new(self.schema.clone(), output_columns)?;
            output_batches.push(filtered_batch);
        }

        Ok(output_batches)
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!("Filter: {:?}", self.predicate)
    }
}

#[derive(Debug)]
pub struct UnnestExec {
    pub schema: Schema,

    pub array_expr: Expr,

    pub with_offset: bool,
}

impl UnnestExec {
    fn evaluate_element_list(&self, elements: &[Expr]) -> Result<Vec<crate::types::Value>> {
        elements
            .iter()
            .map(|elem| self.evaluate_constant_expr(elem))
            .collect()
    }

    const fn empty_array() -> Vec<crate::types::Value> {
        Vec::new()
    }

    fn evaluate_generate_array(&self, args: &[Expr]) -> Result<Vec<crate::types::Value>> {
        use yachtsql_core::error::Error;

        if args.len() < 2 || args.len() > 3 {
            return Err(Error::invalid_query(
                "GENERATE_ARRAY requires 2 or 3 arguments (start, end, [step])".to_string(),
            ));
        }

        let start_val = self.evaluate_constant_expr(&args[0])?;
        let end_val = self.evaluate_constant_expr(&args[1])?;
        let step_val = if args.len() == 3 {
            Some(self.evaluate_constant_expr(&args[2])?)
        } else {
            None
        };

        match crate::functions::array::generate_array(&start_val, &end_val, step_val.as_ref()) {
            Ok(result) if result.is_null() => Ok(Self::empty_array()),
            Ok(result) => {
                if let Some(elements) = result.as_array() {
                    Ok(elements.to_vec())
                } else {
                    Err(Error::InternalError(
                        "GENERATE_ARRAY returned non-array value".to_string(),
                    ))
                }
            }
            Err(e) => Err(e),
        }
    }

    fn evaluate_array_expr(&self) -> Result<Vec<crate::types::Value>> {
        use yachtsql_core::error::Error;
        use yachtsql_optimizer::expr::LiteralValue;

        match &self.array_expr {

            Expr::Literal(lit) if matches!(lit, LiteralValue::Array(_)) => {
                if let LiteralValue::Array(elements) = lit {
                    self.evaluate_element_list(elements)
                } else {
                    unreachable!()
                }
            }


            Expr::Literal(LiteralValue::Null) => Ok(Self::empty_array()),


            Expr::Function { name, args } if matches!(name, yachtsql_ir::FunctionName::Array) => {
                self.evaluate_element_list(args)
            }


            Expr::Function { name, args } if matches!(name, yachtsql_ir::FunctionName::GenerateArray) => {
                self.evaluate_generate_array(args)
            }


            Expr::Cast { expr, .. } => match expr.as_ref() {
                Expr::Literal(lit) if matches!(lit, LiteralValue::Array(_)) => {
                    if let LiteralValue::Array(elements) = lit {
                        self.evaluate_element_list(elements)
                    } else {
                        unreachable!()
                    }
                }
                Expr::Literal(LiteralValue::Null) => Ok(Self::empty_array()),
                _ => Err(Error::unsupported_feature(
                    "UNNEST CAST only supports ARRAY[...] literals or NULL".to_string(),
                )),
            },

            _ => Err(Error::unsupported_feature(
                "UNNEST only supports ARRAY[...] literals, ARRAY(...) functions, GENERATE_ARRAY, or CAST expressions"
                    .to_string(),
            )),
        }
    }

    fn parse_timestamp(timestamp_str: &str) -> Result<chrono::DateTime<chrono::Utc>> {
        use yachtsql_core::error::Error;

        crate::types::parse_timestamp_to_utc(timestamp_str)
            .ok_or_else(|| Error::invalid_query(format!("Invalid timestamp '{}'", timestamp_str)))
    }

    fn infer_element_type(&self, elements: &[crate::types::Value]) -> crate::types::DataType {
        use yachtsql_core::types::DataType;

        if let Some(field) = self.schema.fields().first() {
            return field.data_type.clone();
        }

        if let Some(first_non_null) = elements.iter().find(|v| !v.is_null()) {
            return first_non_null.data_type();
        }

        DataType::String
    }

    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_constant_expr(&self, expr: &Expr) -> Result<crate::types::Value> {
        use yachtsql_core::error::Error;
        use yachtsql_core::types::Value;
        use yachtsql_optimizer::expr::LiteralValue;

        match expr {
            Expr::Literal(lit) => Ok(match lit {
                LiteralValue::Null => Value::null(),
                LiteralValue::Boolean(b) => Value::bool_val(*b),
                LiteralValue::Int64(i) => Value::int64(*i),
                LiteralValue::Float64(f) => Value::float64(*f),
                LiteralValue::Numeric(d) => Value::numeric(*d),
                LiteralValue::String(s) => Value::string(s.clone()),
                LiteralValue::Bytes(b) => Value::bytes(b.clone()),
                LiteralValue::Date(d) => {
                    use chrono::NaiveDate;
                    let date = NaiveDate::parse_from_str(d, "%Y-%m-%d").map_err(|e| {
                        Error::invalid_query(format!("Invalid date '{}': {}", d, e))
                    })?;
                    Value::date(date)
                }
                LiteralValue::Time(t) => {
                    use chrono::NaiveTime;
                    let time = NaiveTime::parse_from_str(t, "%H:%M:%S")
                        .or_else(|_| NaiveTime::parse_from_str(t, "%H:%M:%S%.f"))
                        .or_else(|_| NaiveTime::parse_from_str(t, "%H:%M"))
                        .map_err(|e| {
                            Error::invalid_query(format!("Invalid time '{}': {}", t, e))
                        })?;
                    Value::time(time)
                }
                LiteralValue::DateTime(dt) => Value::datetime(Self::parse_timestamp(dt)?),
                LiteralValue::Timestamp(t) => Value::timestamp(Self::parse_timestamp(t)?),
                LiteralValue::Json(s) => match serde_json::from_str(s) {
                    Ok(json_val) => Value::json(json_val),
                    Err(_) => Value::null(),
                },
                LiteralValue::Array(elements) => {
                    let array_values: Result<Vec<_>> = elements
                        .iter()
                        .map(|elem| self.evaluate_constant_expr(elem))
                        .collect();
                    Value::array(array_values?)
                }
                LiteralValue::Uuid(s) => crate::types::parse_uuid_strict(s)?,
                LiteralValue::Vector(vec) => Value::vector(vec.clone()),
                LiteralValue::Interval(_s) => Value::null(),
                LiteralValue::Range(_s) => Value::null(),
                LiteralValue::Point(s) => yachtsql_core::types::parse_point_literal(s),
                LiteralValue::PgBox(s) => yachtsql_core::types::parse_pgbox_literal(s),
                LiteralValue::Circle(s) => yachtsql_core::types::parse_circle_literal(s),
                LiteralValue::Line(_s) => Value::null(),
                LiteralValue::Lseg(_s) => Value::null(),
                LiteralValue::Path(_s) => Value::null(),
                LiteralValue::Polygon(_s) => Value::null(),
                LiteralValue::MacAddr(s) => {
                    use yachtsql_core::types::MacAddress;
                    match MacAddress::parse(s, false) {
                        Some(mac) => Value::macaddr(mac),
                        None => Value::null(),
                    }
                }
                LiteralValue::MacAddr8(s) => {
                    use yachtsql_core::types::MacAddress;
                    match MacAddress::parse(s, true) {
                        Some(mac) => Value::macaddr8(mac),
                        None => match MacAddress::parse(s, false) {
                            Some(mac) => Value::macaddr8(mac.to_eui64()),
                            None => Value::null(),
                        },
                    }
                }
            }),
            Expr::StructLiteral { fields } => {
                use indexmap::IndexMap;
                let mut map = IndexMap::new();
                for field in fields {
                    let value = self.evaluate_constant_expr(&field.expr)?;
                    map.insert(field.name.clone(), value);
                }
                Ok(Value::struct_val(map))
            }
            _ => Err(Error::unsupported_feature(
                "UNNEST currently only supports literal values in ARRAY()".to_string(),
            )),
        }
    }

    fn build_element_column(
        &self,
        elements: &[crate::types::Value],
        element_type: &crate::types::DataType,
    ) -> Result<crate::storage::Column> {
        use yachtsql_storage::Column;

        let mut column = Column::new(element_type, elements.len());
        for value in elements {
            column.push(value.clone())?;
        }
        Ok(column)
    }

    fn build_ordinality_column(num_rows: usize) -> Result<crate::storage::Column> {
        use yachtsql_core::types::{DataType, Value};
        use yachtsql_storage::Column;

        let mut column = Column::new(&DataType::Int64, num_rows);
        for position in 0..num_rows {
            column.push(Value::int64(position as i64))?;
        }
        Ok(column)
    }
}

impl ExecutionPlan for UnnestExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        use crate::Table;

        let elements = self.evaluate_array_expr()?;

        if elements.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let mut columns = Vec::new();

        let element_type = self.infer_element_type(&elements);
        let element_column = self.build_element_column(&elements, &element_type)?;
        columns.push(element_column);

        if self.with_offset {
            let ordinality_column = Self::build_ordinality_column(elements.len())?;
            columns.push(ordinality_column);
        }

        Ok(vec![Table::new(self.schema.clone(), columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![]
    }

    fn describe(&self) -> String {
        if self.with_offset {
            "Unnest (with offset)".to_string()
        } else {
            "Unnest".to_string()
        }
    }
}

#[derive(Debug)]
pub struct TableValuedFunctionExec {
    pub schema: Schema,

    pub function_name: String,

    pub args: Vec<yachtsql_ir::expr::Expr>,

    pub storage: std::rc::Rc<std::cell::RefCell<yachtsql_storage::Storage>>,
}

impl TableValuedFunctionExec {
    pub fn new(
        schema: Schema,
        function_name: String,
        args: Vec<yachtsql_ir::expr::Expr>,
        storage: std::rc::Rc<std::cell::RefCell<yachtsql_storage::Storage>>,
    ) -> Self {
        Self {
            schema,
            function_name,
            args,
            storage,
        }
    }

    fn evaluate_args(&self) -> Result<Vec<crate::types::Value>> {
        use yachtsql_ir::expr::{Expr, LiteralValue};
        use yachtsql_storage::row::Row;

        use crate::query_executor::expression_evaluator::ExpressionEvaluator;
        use crate::types::Value;

        let empty_schema = Schema::from_fields(vec![]);
        let evaluator = ExpressionEvaluator::new(&empty_schema);
        let empty_row = Row::from_values(vec![]);

        self.args
            .iter()
            .map(|expr| {
                if let Expr::Literal(LiteralValue::Json(s)) = expr {
                    return match serde_json::from_str(s) {
                        Ok(json) => Ok(Value::json(json)),
                        Err(_) => Ok(Value::null()),
                    };
                }
                if let Expr::Column { name, table } = expr {
                    let full_name = if let Some(t) = table {
                        format!("{}.{}", t, name)
                    } else {
                        name.clone()
                    };
                    return Ok(Value::string(full_name));
                }
                let ast_expr = self.ir_expr_to_sql_expr(expr);
                evaluator.evaluate_expr(&ast_expr, &empty_row)
            })
            .collect()
    }

    fn ir_expr_to_sql_expr(&self, expr: &yachtsql_ir::expr::Expr) -> sqlparser::ast::Expr {
        use sqlparser::ast::{self, Value as AstValue, ValueWithSpan};
        use sqlparser::tokenizer::Span;
        use yachtsql_ir::expr::{Expr, LiteralValue};

        let value_expr = |v: AstValue| -> ast::Expr {
            ast::Expr::Value(ValueWithSpan {
                value: v,
                span: Span::empty(),
            })
        };

        match expr {
            Expr::Literal(lit) => match lit {
                LiteralValue::Null => value_expr(AstValue::Null),
                LiteralValue::Boolean(b) => value_expr(AstValue::Boolean(*b)),
                LiteralValue::Int64(i) => value_expr(AstValue::Number(i.to_string(), false)),
                LiteralValue::Float64(f) => value_expr(AstValue::Number(f.to_string(), false)),
                LiteralValue::String(s) => value_expr(AstValue::SingleQuotedString(s.clone())),
                LiteralValue::Json(s) => ast::Expr::Cast {
                    kind: ast::CastKind::Cast,
                    expr: Box::new(value_expr(AstValue::SingleQuotedString(s.clone()))),
                    data_type: ast::DataType::JSON,
                    format: None,
                },
                _ => value_expr(AstValue::Null),
            },
            Expr::Column { name, table } => {
                let idents = if let Some(t) = table {
                    vec![ast::Ident::new(t.clone()), ast::Ident::new(name.clone())]
                } else {
                    vec![ast::Ident::new(name.clone())]
                };
                ast::Expr::CompoundIdentifier(idents)
            }
            Expr::Cast { expr, data_type } => {
                use yachtsql_ir::expr::CastDataType;
                let inner = self.ir_expr_to_sql_expr(expr);

                let sql_type = match data_type {
                    CastDataType::String => ast::DataType::Text,
                    CastDataType::Int64 => ast::DataType::BigInt(None),
                    CastDataType::Float64 => ast::DataType::DoublePrecision,
                    CastDataType::Bool => ast::DataType::Boolean,
                    CastDataType::Date => ast::DataType::Date,
                    CastDataType::Time => ast::DataType::Time(None, ast::TimezoneInfo::None),
                    CastDataType::Timestamp => {
                        ast::DataType::Timestamp(None, ast::TimezoneInfo::None)
                    }
                    CastDataType::TimestampTz => {
                        ast::DataType::Timestamp(None, ast::TimezoneInfo::WithTimeZone)
                    }
                    CastDataType::Interval => ast::DataType::Interval {
                        fields: None,
                        precision: None,
                    },
                    CastDataType::Json => ast::DataType::JSON,
                    CastDataType::Uuid => ast::DataType::Uuid,
                    CastDataType::Bytes => ast::DataType::Bytea,
                    CastDataType::Hstore => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "hstore",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Numeric(precision_scale) => {
                        if let Some((p, s)) = precision_scale {
                            ast::DataType::Numeric(ast::ExactNumberInfo::PrecisionAndScale(
                                (*p).into(),
                                (*s).into(),
                            ))
                        } else {
                            ast::DataType::Numeric(ast::ExactNumberInfo::None)
                        }
                    }
                    CastDataType::Custom(name, _) => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            name.clone(),
                        ))]),
                        vec![],
                    ),
                    CastDataType::DateTime => {
                        ast::DataType::Timestamp(None, ast::TimezoneInfo::None)
                    }
                    CastDataType::Geography => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "geography",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Array(elem_type) => {
                        let _ = elem_type;
                        ast::DataType::Array(ast::ArrayElemTypeDef::AngleBracket(Box::new(
                            ast::DataType::Text,
                        )))
                    }
                    CastDataType::Vector(dim) => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            format!("vector({})", dim),
                        ))]),
                        vec![],
                    ),
                    CastDataType::MacAddr => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "macaddr",
                        ))]),
                        vec![],
                    ),
                    CastDataType::MacAddr8 => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "macaddr8",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Inet => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "inet",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Cidr => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "cidr",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Int4Range => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "int4range",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Int8Range => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "int8range",
                        ))]),
                        vec![],
                    ),
                    CastDataType::NumRange => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "numrange",
                        ))]),
                        vec![],
                    ),
                    CastDataType::TsRange => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "tsrange",
                        ))]),
                        vec![],
                    ),
                    CastDataType::TsTzRange => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "tstzrange",
                        ))]),
                        vec![],
                    ),
                    CastDataType::DateRange => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "daterange",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Point => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "point",
                        ))]),
                        vec![],
                    ),
                    CastDataType::PgBox => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "box",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Circle => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "circle",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Xid => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "xid",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Xid8 => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "xid8",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Tid => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "tid",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Cid => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "cid",
                        ))]),
                        vec![],
                    ),
                    CastDataType::Oid => ast::DataType::Custom(
                        ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                            "oid",
                        ))]),
                        vec![],
                    ),
                };
                ast::Expr::Cast {
                    expr: Box::new(inner),
                    data_type: sql_type,
                    format: None,
                    kind: ast::CastKind::Cast,
                }
            }
            Expr::Tuple(elements) => {
                let ast_elements: Vec<ast::Expr> = elements
                    .iter()
                    .map(|e| self.ir_expr_to_sql_expr(e))
                    .collect();
                ast::Expr::Tuple(ast_elements)
            }
            _ => value_expr(AstValue::Null),
        }
    }

    fn execute_each(&self, args: &[crate::types::Value]) -> Result<Table> {
        use yachtsql_core::error::Error;
        use yachtsql_storage::Column;

        use crate::types::Value;

        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "each() requires exactly 1 argument".to_string(),
            ));
        }

        let hstore_map = args[0].as_hstore().ok_or_else(|| Error::TypeMismatch {
            expected: "HSTORE".to_string(),
            actual: args[0].data_type().to_string(),
        })?;

        let num_rows = hstore_map.len();
        let mut key_col = Column::new(&DataType::String, num_rows);
        let mut val_col = Column::new(&DataType::String, num_rows);

        for (k, v) in hstore_map.iter() {
            key_col.push(Value::string(k.clone()))?;
            val_col.push(
                v.as_ref()
                    .map(|s| Value::string(s.clone()))
                    .unwrap_or(Value::null()),
            )?;
        }

        Table::new(self.schema.clone(), vec![key_col, val_col])
    }

    fn execute_skeys(&self, args: &[crate::types::Value]) -> Result<Table> {
        use yachtsql_core::error::Error;
        use yachtsql_storage::Column;

        use crate::types::Value;

        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "skeys() requires exactly 1 argument".to_string(),
            ));
        }

        let hstore_map = args[0].as_hstore().ok_or_else(|| Error::TypeMismatch {
            expected: "HSTORE".to_string(),
            actual: args[0].data_type().to_string(),
        })?;

        let num_rows = hstore_map.len();
        let mut key_col = Column::new(&DataType::String, num_rows);

        for k in hstore_map.keys() {
            key_col.push(Value::string(k.clone()))?;
        }

        Table::new(self.schema.clone(), vec![key_col])
    }

    fn execute_svals(&self, args: &[crate::types::Value]) -> Result<Table> {
        use yachtsql_core::error::Error;
        use yachtsql_storage::Column;

        use crate::types::Value;

        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "svals() requires exactly 1 argument".to_string(),
            ));
        }

        let hstore_map = args[0].as_hstore().ok_or_else(|| Error::TypeMismatch {
            expected: "HSTORE".to_string(),
            actual: args[0].data_type().to_string(),
        })?;

        let num_rows = hstore_map.len();
        let mut val_col = Column::new(&DataType::String, num_rows);

        for v in hstore_map.values() {
            val_col.push(
                v.as_ref()
                    .map(|s| Value::string(s.clone()))
                    .unwrap_or(crate::types::Value::null()),
            )?;
        }

        Table::new(self.schema.clone(), vec![val_col])
    }

    fn execute_populate_record(&self, args: &[crate::types::Value]) -> Result<Table> {
        use yachtsql_core::error::Error;
        use yachtsql_storage::Column;

        use crate::types::Value;

        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "populate_record() requires at least 2 arguments".to_string(),
            ));
        }

        let hstore_map = args[1].as_hstore().ok_or_else(|| Error::TypeMismatch {
            expected: "HSTORE".to_string(),
            actual: args[1].data_type().to_string(),
        })?;

        let mut columns: Vec<Column> = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let mut col = Column::new(&field.data_type, 1);
            let col_name = &field.name;
            let value = hstore_map
                .get(col_name)
                .and_then(|v| v.as_ref().map(|s| Value::string(s.clone())))
                .unwrap_or(Value::null());
            col.push(value)?;
            columns.push(col);
        }

        Table::new(self.schema.clone(), columns)
    }

    fn execute_json_each(&self, args: &[crate::types::Value], as_text: bool) -> Result<Table> {
        use yachtsql_core::error::Error;
        use yachtsql_storage::Column;

        use crate::types::Value;

        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "json_each() requires exactly 1 argument".to_string(),
            ));
        }

        let json_val = args[0]
            .as_json()
            .ok_or_else(|| Error::TypeMismatch {
                expected: "JSON".to_string(),
                actual: args[0].data_type().to_string(),
            })?
            .clone();

        let obj = match json_val {
            serde_json::Value::Object(map) => map,
            _ => {
                return Err(Error::InvalidQuery(
                    "json_each() requires a JSON object".to_string(),
                ));
            }
        };

        let num_rows = obj.len();
        let mut key_col = Column::new(&DataType::String, num_rows);
        let value_type = if as_text {
            DataType::String
        } else {
            DataType::Json
        };
        let mut val_col = Column::new(&value_type, num_rows);

        for (k, v) in obj.iter() {
            key_col.push(Value::string(k.clone()))?;
            if as_text {
                let text_val = match v {
                    serde_json::Value::String(s) => Value::string(s.clone()),
                    serde_json::Value::Null => Value::null(),
                    serde_json::Value::Number(n) => Value::string(n.to_string()),
                    serde_json::Value::Bool(b) => Value::string(b.to_string()),
                    _ => Value::string(v.to_string()),
                };
                val_col.push(text_val)?;
            } else {
                val_col.push(Value::json(v.clone()))?;
            }
        }

        Table::new(self.schema.clone(), vec![key_col, val_col])
    }

    fn execute_numbers(&self, args: &[crate::types::Value]) -> Result<Table> {
        use yachtsql_storage::Column;

        use crate::types::Value;

        let (start, count) = match args.len() {
            1 => {
                let count = args[0].as_i64().unwrap_or(0);
                (0i64, count)
            }
            2 => {
                let offset = args[0].as_i64().unwrap_or(0);
                let count = args[1].as_i64().unwrap_or(0);
                (offset, count)
            }
            _ => (0i64, 0i64),
        };

        let mut col = Column::new(&DataType::Int64, count as usize);
        for i in start..(start + count) {
            col.push(Value::int64(i))?;
        }

        Table::new(self.schema.clone(), vec![col])
    }

    fn execute_zeros(&self, args: &[crate::types::Value]) -> Result<Table> {
        use yachtsql_storage::Column;

        use crate::types::Value;

        let count = args.first().and_then(|v| v.as_i64()).unwrap_or(0) as usize;

        let mut col = Column::new(&DataType::Int64, count);
        for _ in 0..count {
            col.push(Value::int64(0))?;
        }

        Table::new(self.schema.clone(), vec![col])
    }

    fn execute_one(&self) -> Result<Table> {
        use yachtsql_storage::Column;

        use crate::types::Value;

        let mut col = Column::new(&DataType::Int64, 1);
        col.push(Value::int64(0))?;

        Table::new(self.schema.clone(), vec![col])
    }

    fn execute_generate_series(&self, args: &[crate::types::Value]) -> Result<Table> {
        use yachtsql_core::error::Error;
        use yachtsql_storage::Column;

        use crate::types::Value;

        let (start, stop, step) = match args.len() {
            2 => {
                let start = args[0].as_i64().unwrap_or(0);
                let stop = args[1].as_i64().unwrap_or(0);
                (start, stop, 1i64)
            }
            3 => {
                let start = args[0].as_i64().unwrap_or(0);
                let stop = args[1].as_i64().unwrap_or(0);
                let step = args[2].as_i64().unwrap_or(1);
                (start, stop, step)
            }
            _ => {
                return Err(Error::InvalidQuery(
                    "generateSeries requires 2 or 3 arguments".to_string(),
                ));
            }
        };

        if step == 0 {
            return Err(Error::InvalidQuery(
                "generateSeries step cannot be zero".to_string(),
            ));
        }

        let mut values = Vec::new();
        let mut current = start;
        if step > 0 {
            while current <= stop {
                values.push(current);
                current += step;
            }
        } else {
            while current >= stop {
                values.push(current);
                current += step;
            }
        }

        let mut col = Column::new(&DataType::Int64, values.len());
        for v in values {
            col.push(Value::int64(v))?;
        }

        Table::new(self.schema.clone(), vec![col])
    }

    fn execute_generate_random(&self, args: &[crate::types::Value]) -> Result<Table> {
        use rand::{RngCore, SeedableRng};
        use yachtsql_storage::Column;

        use crate::types::Value;

        let seed = if args.len() >= 2 {
            args[1].as_i64().map(|s| s as u64)
        } else {
            None
        };

        let mut rng: Box<dyn rand::RngCore> = if let Some(s) = seed {
            Box::new(rand::rngs::StdRng::seed_from_u64(s))
        } else {
            Box::new(rand::rngs::StdRng::from_entropy())
        };

        let num_rows = 1000;
        let mut columns: Vec<Column> = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let mut col = Column::new(&field.data_type, num_rows);
            for _ in 0..num_rows {
                let value = match &field.data_type {
                    DataType::Int64 => Value::int64(rng.next_u64() as i64),
                    DataType::Float64 => Value::float64(rng.next_u64() as f64 / u64::MAX as f64),
                    DataType::String => {
                        let len = (rng.next_u32() % 15 + 5) as usize;
                        let s: String = (0..len)
                            .map(|_| ((rng.next_u32() % 26) as u8 + b'a') as char)
                            .collect();
                        Value::string(s)
                    }
                    DataType::Bool => Value::bool_val(rng.next_u32() % 2 == 0),
                    _ => Value::null(),
                };
                col.push(value)?;
            }
            columns.push(col);
        }

        Table::new(self.schema.clone(), columns)
    }

    fn execute_values(&self, args: &[crate::types::Value]) -> Result<Table> {
        use yachtsql_storage::Column;

        use crate::types::Value;

        let num_cols = self.schema.fields().len();
        let value_args = &args[1..];

        let mut rows: Vec<Vec<Value>> = Vec::new();

        for arg in value_args {
            if let Some(struct_val) = arg.as_struct() {
                let row_values: Vec<Value> = struct_val.values().cloned().collect();
                rows.push(row_values);
            } else {
                rows.push(vec![arg.clone()]);
            }
        }

        if rows.is_empty() {
            let num_flat_values = value_args.len();
            let num_rows = num_flat_values / num_cols;
            for row_idx in 0..num_rows {
                let mut row_values: Vec<Value> = Vec::with_capacity(num_cols);
                for col_idx in 0..num_cols {
                    let arg_idx = row_idx * num_cols + col_idx;
                    let value = value_args.get(arg_idx).cloned().unwrap_or(Value::null());
                    row_values.push(value);
                }
                rows.push(row_values);
            }
        }

        let mut columns: Vec<Column> = self
            .schema
            .fields()
            .iter()
            .map(|f| Column::new(&f.data_type, rows.len()))
            .collect();

        for row in &rows {
            for col_idx in 0..num_cols {
                let value = row.get(col_idx).cloned().unwrap_or(Value::null());
                columns[col_idx].push(value)?;
            }
        }

        Table::new(self.schema.clone(), columns)
    }

    fn execute_null_table(&self) -> Result<Table> {
        use yachtsql_storage::Column;

        let columns: Vec<Column> = self
            .schema
            .fields()
            .iter()
            .map(|f| Column::new(&f.data_type, 0))
            .collect();

        Table::new(self.schema.clone(), columns)
    }

    fn execute_input(&self, _args: &[crate::types::Value]) -> Result<Table> {
        use yachtsql_storage::Column;

        let columns: Vec<Column> = self
            .schema
            .fields()
            .iter()
            .map(|f| Column::new(&f.data_type, 0))
            .collect();

        Table::new(self.schema.clone(), columns)
    }

    fn execute_merge(&self, args: &[crate::types::Value]) -> Result<Table> {
        use yachtsql_storage::Column;

        use crate::types::Value;

        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "merge() requires database and table pattern".to_string(),
            ));
        }

        let table_pattern = args[1].as_str().map(|s| s.to_string()).unwrap_or_default();

        let re = regex::Regex::new(&table_pattern)
            .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;

        let storage = self.storage.borrow();
        let mut all_rows: Vec<Vec<Value>> = Vec::new();

        if let Some(dataset) = storage.get_dataset("default") {
            for table_name in dataset.list_tables() {
                if re.is_match(table_name) {
                    if let Some(table) = dataset.get_table(table_name) {
                        for row in table.get_all_rows() {
                            let row_values: Vec<Value> = row.values().to_vec();
                            all_rows.push(row_values);
                        }
                    }
                }
            }
        }

        let mut columns: Vec<Column> = self
            .schema
            .fields()
            .iter()
            .map(|f| Column::new(&f.data_type, all_rows.len()))
            .collect();

        for row in &all_rows {
            for (col_idx, value) in row.iter().enumerate() {
                if col_idx < columns.len() {
                    columns[col_idx].push(value.clone())?;
                }
            }
        }

        Table::new(self.schema.clone(), columns)
    }
}

impl ExecutionPlan for TableValuedFunctionExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        use yachtsql_core::error::Error;

        let args = self.evaluate_args()?;

        let batch = match self.function_name.to_uppercase().as_str() {
            "EACH" => self.execute_each(&args)?,
            "JSON_EACH" | "JSONB_EACH" => self.execute_json_each(&args, false)?,
            "JSON_EACH_TEXT" | "JSONB_EACH_TEXT" => self.execute_json_each(&args, true)?,
            "SKEYS" => self.execute_skeys(&args)?,
            "SVALS" => self.execute_svals(&args)?,
            "POPULATE_RECORD" => self.execute_populate_record(&args)?,
            "NUMBERS" | "NUMBERS_MT" => self.execute_numbers(&args)?,
            "ZEROS" | "ZEROS_MT" => self.execute_zeros(&args)?,
            "ONE" => self.execute_one()?,
            "GENERATESERIES" | "GENERATE_SERIES" => self.execute_generate_series(&args)?,
            "GENERATERANDOM" | "GENERATE_RANDOM" => self.execute_generate_random(&args)?,
            "VALUES" => self.execute_values(&args)?,
            "NULL" => self.execute_null_table()?,
            "CLUSTER" | "CLUSTERALLREPLICAS" => self.execute_one()?,
            "INPUT" => self.execute_input(&args)?,
            "MERGE" => self.execute_merge(&args)?,

            _ => {
                return Err(Error::UnsupportedFeature(format!(
                    "Table-valued function '{}' not yet supported in optimizer path",
                    self.function_name
                )));
            }
        };

        Ok(vec![batch])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![]
    }

    fn describe(&self) -> String {
        format!("TableValuedFunction({})", self.function_name)
    }
}
