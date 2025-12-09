use std::sync::Arc;

use debug_print::debug_eprintln;
use yachtsql_core::error::Result;
use yachtsql_ir::expr::{BinaryOp, Expr};
use yachtsql_ir::plan::{LogicalPlan, PlanNode};

use crate::catalog::{CatalogRef, EmptyCatalog, IndexInfo, IndexType};
use crate::rule::OptimizationRule;
use crate::statistics::StatisticsRegistry;
use crate::visitor::PlanRewriter;

pub struct IndexSelectionRule {
    enabled: bool,
    catalog: CatalogRef,
    statistics: Option<Arc<StatisticsRegistry>>,
}

impl IndexSelectionRule {
    pub fn new() -> Self {
        Self {
            enabled: true,
            catalog: Arc::new(EmptyCatalog),
            statistics: None,
        }
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            catalog: Arc::new(EmptyCatalog),
            statistics: None,
        }
    }

    pub fn with_catalog(catalog: CatalogRef) -> Self {
        Self {
            enabled: true,
            catalog,
            statistics: None,
        }
    }

    pub fn with_catalog_and_statistics(
        catalog: CatalogRef,
        statistics: Arc<StatisticsRegistry>,
    ) -> Self {
        Self {
            enabled: true,
            catalog,
            statistics: Some(statistics),
        }
    }

    fn extract_equality_predicate(&self, predicate: &Expr) -> Option<(String, Expr)> {
        let Expr::BinaryOp { left, op, right } = predicate else {
            return None;
        };

        if !matches!(op, BinaryOp::Equal) {
            return None;
        }

        match (left.as_ref(), right.as_ref()) {
            (Expr::Column { name, .. }, value @ Expr::Literal(_)) => {
                Some((name.clone(), value.clone()))
            }
            (value @ Expr::Literal(_), Expr::Column { name, .. }) => {
                Some((name.clone(), value.clone()))
            }
            _ => None,
        }
    }

    fn find_matching_index(&self, table_name: &str, column_name: &str) -> Option<IndexInfo> {
        let indexes = self.catalog.get_indexes_for_table(table_name);

        indexes.into_iter().find(|idx| {
            idx.is_single_column()
                && idx.columns.first().is_some_and(|c| c == column_name)
                && idx.supports_equality()
        })
    }

    fn estimate_selectivity(&self, table_name: &str, predicate: &Expr) -> f64 {
        if let Some(stats) = &self.statistics
            && stats.get_table(table_name).is_some()
        {
            return stats.estimate_predicate_selectivity(predicate, table_name);
        }

        match predicate {
            Expr::BinaryOp {
                op: BinaryOp::Equal,
                ..
            } => 0.01,
            Expr::BinaryOp {
                op: BinaryOp::NotEqual,
                ..
            } => 0.99,
            Expr::BinaryOp {
                op: BinaryOp::LessThan | BinaryOp::GreaterThan,
                ..
            } => 0.33,
            Expr::BinaryOp {
                op: BinaryOp::LessThanOrEqual | BinaryOp::GreaterThanOrEqual,
                ..
            } => 0.33,
            _ => 0.5,
        }
    }

    fn should_use_index(&self, table_name: &str, selectivity: f64, index: &IndexInfo) -> bool {
        let row_count = self
            .statistics
            .as_ref()
            .and_then(|s| s.get_table(table_name))
            .map(|t| t.row_count as f64)
            .unwrap_or(10_000.0);

        if row_count < 100.0 {
            return false;
        }

        let estimated_rows = row_count * selectivity;
        let index_cost = match index.index_type {
            IndexType::Hash => 1.0 + estimated_rows,
            IndexType::BTree => row_count.log2() + estimated_rows,
            IndexType::GiST | IndexType::GIN | IndexType::BRIN => row_count.log2() + estimated_rows,
        };
        let scan_cost = row_count;

        index_cost < scan_cost
    }
}

impl Default for IndexSelectionRule {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanRewriter for IndexSelectionRule {
    fn rewrite_node(&mut self, node: &PlanNode) -> Result<Option<PlanNode>> {
        if !self.enabled {
            return Ok(None);
        }

        let PlanNode::Filter { predicate, input } = node else {
            return Ok(None);
        };

        let PlanNode::Scan {
            table_name,
            alias,
            projection,
            ..
        } = input.as_ref()
        else {
            return Ok(None);
        };

        let Some((column_name, _value)) = self.extract_equality_predicate(predicate) else {
            return Ok(None);
        };

        let Some(index) = self.find_matching_index(table_name, &column_name) else {
            debug_eprintln!(
                "[optimizer::index_selection] No index found for {}.{}",
                table_name,
                column_name
            );
            return Ok(None);
        };

        let selectivity = self.estimate_selectivity(table_name, predicate);

        if !self.should_use_index(table_name, selectivity, &index) {
            debug_eprintln!(
                "[optimizer::index_selection] Index {} not beneficial for selectivity {:.1}%",
                index.index_name,
                selectivity * 100.0
            );
            return Ok(None);
        }

        let index_scan = PlanNode::IndexScan {
            table_name: table_name.clone(),
            alias: alias.clone(),
            index_name: index.index_name.clone(),
            predicate: predicate.clone(),
            projection: projection.clone(),
        };

        debug_eprintln!(
            "[optimizer::index_selection] Using index {} for {}.{} (selectivity: {:.1}%)",
            index.index_name,
            table_name,
            column_name,
            selectivity * 100.0
        );

        Ok(Some(index_scan))
    }
}

impl OptimizationRule for IndexSelectionRule {
    fn name(&self) -> &'static str {
        "IndexSelection"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let mut rewriter = IndexSelectionRule {
            enabled: self.enabled,
            catalog: Arc::clone(&self.catalog),
            statistics: self.statistics.clone(),
        };
        rewriter.rewrite(plan)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_ir::expr::LiteralValue;

    use super::*;
    use crate::catalog::mock::MockCatalog;

    fn create_filter_scan(table: &str, column: &str, value: i64) -> LogicalPlan {
        LogicalPlan::new(PlanNode::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: column.to_string(),
                    table: None,
                }),
                op: BinaryOp::Equal,
                right: Box::new(Expr::Literal(LiteralValue::Int64(value))),
            },
            input: Box::new(PlanNode::Scan {
                table_name: table.to_string(),
                alias: None,
                projection: None,
                only: false,
            }),
        })
    }

    #[test]
    fn test_index_selection_with_matching_index() {
        let mut catalog = MockCatalog::new();
        catalog.add_index(IndexInfo::new(
            "idx_users_id".to_string(),
            "users".to_string(),
            vec!["id".to_string()],
        ));

        let mut stats = StatisticsRegistry::new();
        stats.add_table(crate::statistics::TableStatistics::new(
            "users".to_string(),
            100_000,
        ));

        let rule =
            IndexSelectionRule::with_catalog_and_statistics(Arc::new(catalog), Arc::new(stats));

        let plan = create_filter_scan("users", "id", 42);
        let result = rule.optimize(&plan).unwrap();

        assert!(result.is_some());
        let optimized = result.unwrap();
        match optimized.root() {
            PlanNode::IndexScan {
                table_name,
                index_name,
                ..
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(index_name, "idx_users_id");
            }
            _ => panic!("Expected IndexScan, got {:?}", optimized.root()),
        }
    }

    #[test]
    fn test_no_index_when_none_exists() {
        let catalog = MockCatalog::new();
        let rule = IndexSelectionRule::with_catalog(Arc::new(catalog));

        let plan = create_filter_scan("users", "id", 42);
        let result = rule.optimize(&plan).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_no_index_for_small_table() {
        let mut catalog = MockCatalog::new();
        catalog.add_index(IndexInfo::new(
            "idx_users_id".to_string(),
            "users".to_string(),
            vec!["id".to_string()],
        ));

        let mut stats = StatisticsRegistry::new();
        stats.add_table(crate::statistics::TableStatistics::new(
            "users".to_string(),
            50,
        ));

        let rule =
            IndexSelectionRule::with_catalog_and_statistics(Arc::new(catalog), Arc::new(stats));

        let plan = create_filter_scan("users", "id", 42);
        let result = rule.optimize(&plan).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_no_index_for_wrong_column() {
        let mut catalog = MockCatalog::new();
        catalog.add_index(IndexInfo::new(
            "idx_users_email".to_string(),
            "users".to_string(),
            vec!["email".to_string()],
        ));

        let rule = IndexSelectionRule::with_catalog(Arc::new(catalog));
        let plan = create_filter_scan("users", "id", 42);
        let result = rule.optimize(&plan).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_disabled_rule() {
        let mut catalog = MockCatalog::new();
        catalog.add_index(IndexInfo::new(
            "idx_users_id".to_string(),
            "users".to_string(),
            vec!["id".to_string()],
        ));

        let mut rule = IndexSelectionRule::with_catalog(Arc::new(catalog));
        rule.enabled = false;

        let plan = create_filter_scan("users", "id", 42);
        let result = rule.optimize(&plan).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_hash_index_for_equality() {
        let mut catalog = MockCatalog::new();
        catalog.add_index(
            IndexInfo::new(
                "idx_users_id".to_string(),
                "users".to_string(),
                vec!["id".to_string()],
            )
            .with_type(IndexType::Hash),
        );

        let mut stats = StatisticsRegistry::new();
        stats.add_table(crate::statistics::TableStatistics::new(
            "users".to_string(),
            100_000,
        ));

        let rule =
            IndexSelectionRule::with_catalog_and_statistics(Arc::new(catalog), Arc::new(stats));

        let plan = create_filter_scan("users", "id", 42);
        let result = rule.optimize(&plan).unwrap();

        assert!(result.is_some());
    }

    #[test]
    fn test_extract_equality_predicate() {
        let rule = IndexSelectionRule::new();

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "id".to_string(),
                table: None,
            }),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Int64(42))),
        };

        let result = rule.extract_equality_predicate(&predicate);
        assert!(result.is_some());
        let (column, _) = result.unwrap();
        assert_eq!(column, "id");
    }

    #[test]
    fn test_extract_equality_predicate_reversed() {
        let rule = IndexSelectionRule::new();

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(42))),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Column {
                name: "id".to_string(),
                table: None,
            }),
        };

        let result = rule.extract_equality_predicate(&predicate);
        assert!(result.is_some());
        let (column, _) = result.unwrap();
        assert_eq!(column, "id");
    }

    #[test]
    fn test_non_equality_returns_none() {
        let rule = IndexSelectionRule::new();

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "id".to_string(),
                table: None,
            }),
            op: BinaryOp::GreaterThan,
            right: Box::new(Expr::Literal(LiteralValue::Int64(42))),
        };

        let result = rule.extract_equality_predicate(&predicate);
        assert!(result.is_none());
    }

    #[test]
    fn test_selectivity_estimates() {
        let rule = IndexSelectionRule::new();

        let eq_pred = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "x".to_string(),
                table: None,
            }),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Int64(1))),
        };
        assert!((rule.estimate_selectivity("t", &eq_pred) - 0.01).abs() < 0.001);

        let gt_pred = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "x".to_string(),
                table: None,
            }),
            op: BinaryOp::GreaterThan,
            right: Box::new(Expr::Literal(LiteralValue::Int64(1))),
        };
        assert!((rule.estimate_selectivity("t", &gt_pred) - 0.33).abs() < 0.001);
    }
}
