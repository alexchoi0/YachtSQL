use std::rc::Rc;

use yachtsql_capability::{FeatureId, FeatureRegistry, Result as CapabilityResult};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::Schema;

use crate::query_executor::evaluator::physical_plan::{
    ExecutionPlan, ExecutionStatistics, ProjectionWithExprExec,
};
use crate::{DialectType, Table};

type ErrorFactory = Rc<dyn Fn() -> Error + Send + Sync>;

pub struct MockPlan {
    schema: Schema,
    batches: Vec<Table>,
    children: Vec<Rc<dyn ExecutionPlan>>,
    statistics: ExecutionStatistics,
    description: String,
    error_factory: Option<ErrorFactory>,
}

impl std::fmt::Debug for MockPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockPlan")
            .field("schema", &self.schema)
            .field("batches", &self.batches)
            .field("children", &self.children)
            .field("statistics", &self.statistics)
            .field("description", &self.description)
            .field(
                "error_factory",
                &self.error_factory.as_ref().map(|_| "<function>"),
            )
            .finish()
    }
}

impl MockPlan {
    pub fn new(schema: Schema, batches: Vec<Table>) -> Self {
        Self {
            schema,
            batches,
            children: Vec::new(),
            statistics: ExecutionStatistics::default(),
            description: "MockPlan".to_string(),
            error_factory: None,
        }
    }

    pub fn empty(schema: Schema) -> Self {
        Self::new(schema, Vec::new())
    }

    pub fn with_error(mut self, factory: impl Fn() -> Error + Send + Sync + 'static) -> Self {
        self.error_factory = Some(Rc::new(factory));
        self
    }

    pub fn with_statistics(mut self, statistics: ExecutionStatistics) -> Self {
        self.statistics = statistics;
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn with_children(mut self, children: Vec<Rc<dyn ExecutionPlan>>) -> Self {
        self.children = children;
        self
    }

    pub fn into_arc(self) -> Rc<dyn ExecutionPlan> {
        Rc::new(self)
    }
}

impl ExecutionPlan for MockPlan {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        if let Some(factory) = &self.error_factory {
            return Err(factory());
        }
        Ok(self.batches.clone())
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        self.children.clone()
    }

    fn statistics(&self) -> ExecutionStatistics {
        self.statistics.clone()
    }

    fn describe(&self) -> String {
        self.description.clone()
    }
}

pub fn record_batch_from_rows(schema: Schema, rows: Vec<Vec<Value>>) -> Table {
    Table::from_values(schema, rows).expect("record batch construction failed")
}

pub fn assert_values_eq(actual: &[Value], expected: &[Value]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "value length mismatch: actual={:?} expected={:?}",
        actual,
        expected
    );

    for (idx, (lhs, rhs)) in actual.iter().zip(expected.iter()).enumerate() {
        assert!(
            lhs == rhs,
            "value mismatch at index {}: left={:?} right={:?}",
            idx,
            lhs,
            rhs
        );
    }
}

pub fn assert_error_contains(err: &Error, needle: &str) {
    let message = err.to_string();
    assert!(
        message.contains(needle),
        "expected error '{}' to contain '{}'",
        message,
        needle
    );
}

pub fn feature_registry_with(
    dialect: DialectType,
    feature_ids: &[FeatureId],
) -> CapabilityResult<Rc<FeatureRegistry>> {
    let mut registry = FeatureRegistry::with_default_features(dialect);
    if !feature_ids.is_empty() {
        registry.enable_features(feature_ids.iter().copied())?;
    }
    Ok(Rc::new(registry))
}

pub fn default_feature_registry() -> Rc<FeatureRegistry> {
    Rc::new(FeatureRegistry::with_default_features(
        DialectType::PostgreSQL,
    ))
}
