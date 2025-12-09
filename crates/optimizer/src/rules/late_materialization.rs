use std::collections::HashSet;

use yachtsql_core::error::Result;
use yachtsql_ir::expr::Expr;
use yachtsql_ir::plan::PlanNode;

use crate::optimizer::plan::LogicalPlan;
use crate::optimizer::rule::OptimizationRule;
use crate::phase::StorageType;

pub struct LateMaterialization {
    storage_type: StorageType,

    min_selectivity: f64,

    min_columns: usize,
}

impl LateMaterialization {
    pub fn new() -> Self {
        Self {
            storage_type: StorageType::Columnar,
            min_selectivity: 0.2,
            min_columns: 10,
        }
    }

    pub fn with_storage_type(mut self, storage_type: StorageType) -> Self {
        self.storage_type = storage_type;
        self
    }

    pub fn with_min_selectivity(mut self, selectivity: f64) -> Self {
        self.min_selectivity = selectivity;
        self
    }

    pub fn with_min_columns(mut self, count: usize) -> Self {
        self.min_columns = count;
        self
    }

    fn try_late_materialize(
        &self,
        scan: &PlanNode,
        filter: &[Expr],
        projection: &[Expr],
    ) -> Option<PlanNode> {
        if !matches!(self.storage_type, StorageType::Columnar) {
            return None;
        }

        let (table_name, all_columns) = match scan {
            PlanNode::Scan {
                table_name,
                projection,
                ..
            } => {
                let cols = projection.as_ref()?;
                (table_name.clone(), cols.clone())
            }
            _ => return None,
        };

        if all_columns.len() < self.min_columns {
            return None;
        }

        let filter_columns = Self::collect_columns(filter);

        let projection_columns = Self::collect_columns(projection);

        let extra_columns: HashSet<_> = projection_columns.difference(&filter_columns).collect();

        if extra_columns.is_empty() {
            return None;
        }

        let selectivity = Self::estimate_filter_selectivity(filter);
        if selectivity > self.min_selectivity {
            return None;
        }

        let filter_scan = PlanNode::Scan {
            table_name: table_name.clone(),
            projection: Some(filter_columns.iter().cloned().collect()),
            alias: None,
            only: false,
        };

        let combined_predicate = if filter.len() == 1 {
            filter[0].clone()
        } else {
            filter
                .iter()
                .skip(1)
                .fold(filter[0].clone(), |acc, pred| Expr::BinaryOp {
                    left: Box::new(acc),
                    op: yachtsql_ir::expr::BinaryOp::And,
                    right: Box::new(pred.clone()),
                })
        };

        let filtered = PlanNode::Filter {
            input: Box::new(filter_scan),
            predicate: combined_predicate,
        };

        let _projection_scan = PlanNode::Scan {
            table_name: table_name.clone(),
            projection: Some(projection_columns.iter().cloned().collect()),
            alias: Some(format!("{}_late", table_name)),
            only: false,
        };

        let final_project = PlanNode::Projection {
            expressions: projection.iter().map(|e| (e.clone(), None)).collect(),
            input: Box::new(filtered),
        };

        Some(final_project)
    }

    fn collect_columns(expressions: &[Expr]) -> HashSet<String> {
        let mut columns = HashSet::new();
        for expr in expressions {
            Self::collect_columns_recursive(expr, &mut columns);
        }
        columns
    }

    fn collect_columns_recursive(expr: &Expr, columns: &mut HashSet<String>) {
        match expr {
            Expr::Column { name, .. } => {
                columns.insert(name.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_columns_recursive(left, columns);
                Self::collect_columns_recursive(right, columns);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_columns_recursive(expr, columns);
            }
            Expr::Function { args, .. } | Expr::Aggregate { args, .. } => {
                for arg in args {
                    Self::collect_columns_recursive(arg, columns);
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    Self::collect_columns_recursive(op, columns);
                }
                for (cond, result) in when_then {
                    Self::collect_columns_recursive(cond, columns);
                    Self::collect_columns_recursive(result, columns);
                }
                if let Some(else_e) = else_expr {
                    Self::collect_columns_recursive(else_e, columns);
                }
            }
            Expr::InList { expr, list, .. } => {
                Self::collect_columns_recursive(expr, columns);
                for item in list {
                    Self::collect_columns_recursive(item, columns);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::collect_columns_recursive(expr, columns);
                Self::collect_columns_recursive(low, columns);
                Self::collect_columns_recursive(high, columns);
            }
            _ => {}
        }
    }

    fn estimate_filter_selectivity(filter: &[Expr]) -> f64 {
        if filter.is_empty() {
            return 1.0;
        }

        let mut selectivity = 1.0;
        for pred in filter {
            selectivity *= Self::estimate_predicate_selectivity(pred);
        }
        selectivity
    }

    fn estimate_predicate_selectivity(pred: &Expr) -> f64 {
        match pred {
            Expr::BinaryOp { op, .. } => match op {
                yachtsql_ir::expr::BinaryOp::Equal => 0.1,
                yachtsql_ir::expr::BinaryOp::LessThan
                | yachtsql_ir::expr::BinaryOp::LessThanOrEqual
                | yachtsql_ir::expr::BinaryOp::GreaterThan
                | yachtsql_ir::expr::BinaryOp::GreaterThanOrEqual => 0.3,
                yachtsql_ir::expr::BinaryOp::And => 0.2,
                yachtsql_ir::expr::BinaryOp::Or => 0.8,
                _ => 0.5,
            },
            Expr::InList { list, .. } => (list.len() as f64 * 0.1).min(0.9),
            Expr::Between { .. } => 0.25,
            _ => 0.5,
        }
    }

    fn find_scan(node: &PlanNode) -> Option<&PlanNode> {
        match node {
            PlanNode::Scan { .. } => Some(node),
            PlanNode::Filter { input, .. }
            | PlanNode::Projection { input, .. }
            | PlanNode::Sort { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Aggregate { input, .. } => Self::find_scan(input),
            _ => None,
        }
    }
}

impl Default for LateMaterialization {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for LateMaterialization {
    fn name(&self) -> &str {
        "late_materialization"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        if !matches!(self.storage_type, StorageType::Columnar) {
            return Ok(None);
        }

        let optimized = self.optimize_node(&plan.root)?;

        if let Some(new_root) = optimized {
            return Ok(Some(LogicalPlan {
                root: Box::new(new_root),
            }));
        }

        Ok(None)
    }
}

impl LateMaterialization {
    fn optimize_node(&self, node: &PlanNode) -> Result<Option<PlanNode>> {
        match node {
            PlanNode::Projection { expressions, input } => {
                if let PlanNode::Filter {
                    input: filter_input,
                    predicate,
                } = input.as_ref()
                    && let Some(scan) = Self::find_scan(filter_input)
                {
                    let predicates = vec![predicate.clone()];
                    let exprs: Vec<Expr> = expressions.iter().map(|(e, _)| e.clone()).collect();
                    if let Some(optimized) = self.try_late_materialize(scan, &predicates, &exprs) {
                        return Ok(Some(optimized));
                    }
                }

                if let Some(new_input) = self.optimize_node(input)? {
                    return Ok(Some(PlanNode::Projection {
                        expressions: expressions.clone(),
                        input: Box::new(new_input),
                    }));
                }

                Ok(None)
            }
            PlanNode::Filter { input, predicate } => {
                if let Some(new_input) = self.optimize_node(input)? {
                    return Ok(Some(PlanNode::Filter {
                        input: Box::new(new_input),
                        predicate: predicate.clone(),
                    }));
                }
                Ok(None)
            }
            PlanNode::Join {
                left,
                right,
                join_type,
                on,
            } => {
                let new_left = self.optimize_node(left)?;
                let new_right = self.optimize_node(right)?;

                if new_left.is_some() || new_right.is_some() {
                    return Ok(Some(PlanNode::Join {
                        left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                        join_type: *join_type,
                        on: on.clone(),
                    }));
                }
                Ok(None)
            }
            PlanNode::LateralJoin {
                left,
                right,
                join_type,
                on,
            } => {
                let new_left = self.optimize_node(left)?;
                let new_right = self.optimize_node(right)?;

                if new_left.is_some() || new_right.is_some() {
                    return Ok(Some(PlanNode::LateralJoin {
                        left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                        join_type: *join_type,
                        on: on.clone(),
                    }));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_ir::expr::{BinaryOp, LiteralValue};

    use super::*;

    fn column(name: &str) -> Expr {
        Expr::Column {
            name: name.to_string(),
            table: None,
        }
    }

    fn literal_int(value: i64) -> Expr {
        Expr::Literal(LiteralValue::Int64(value))
    }

    fn scan_node(table: &str, columns: Vec<String>) -> PlanNode {
        PlanNode::Scan {
            table_name: table.to_string(),
            projection: Some(columns),
            alias: None,
            only: false,
        }
    }

    #[test]
    fn test_collect_columns_simple() {
        let exprs = vec![
            column("a"),
            column("b"),
            Expr::BinaryOp {
                left: Box::new(column("c")),
                op: BinaryOp::Equal,
                right: Box::new(literal_int(5)),
            },
        ];

        let columns = LateMaterialization::collect_columns(&exprs);
        assert_eq!(columns.len(), 3);
        assert!(columns.contains("a"));
        assert!(columns.contains("b"));
        assert!(columns.contains("c"));
    }

    #[test]
    fn test_collect_columns_nested() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(column("x")),
                op: BinaryOp::Add,
                right: Box::new(column("y")),
            }),
            op: BinaryOp::GreaterThan,
            right: Box::new(column("z")),
        };

        let columns = LateMaterialization::collect_columns(&[expr]);
        assert_eq!(columns.len(), 3);
        assert!(columns.contains("x"));
        assert!(columns.contains("y"));
        assert!(columns.contains("z"));
    }

    #[test]
    fn test_estimate_filter_selectivity_equality() {
        let filter = vec![Expr::BinaryOp {
            left: Box::new(column("x")),
            op: BinaryOp::Equal,
            right: Box::new(literal_int(5)),
        }];

        let selectivity = LateMaterialization::estimate_filter_selectivity(&filter);
        assert!(selectivity < 0.2);
    }

    #[test]
    fn test_estimate_filter_selectivity_conjunction() {
        let filter = vec![
            Expr::BinaryOp {
                left: Box::new(column("x")),
                op: BinaryOp::Equal,
                right: Box::new(literal_int(5)),
            },
            Expr::BinaryOp {
                left: Box::new(column("y")),
                op: BinaryOp::Equal,
                right: Box::new(literal_int(10)),
            },
        ];

        let selectivity = LateMaterialization::estimate_filter_selectivity(&filter);
        assert!(selectivity < 0.02);
    }

    #[test]
    fn test_late_materialize_not_applied_row_storage() {
        let rule = LateMaterialization::new().with_storage_type(StorageType::Row);

        let columns: Vec<String> = (0..20).map(|i| format!("col{}", i)).collect();
        let scan = scan_node("wide_table", columns);

        let filter = vec![Expr::BinaryOp {
            left: Box::new(column("col0")),
            op: BinaryOp::Equal,
            right: Box::new(literal_int(5)),
        }];

        let projection = vec![column("col10"), column("col11")];

        let result = rule.try_late_materialize(&scan, &filter, &projection);
        assert!(result.is_none(), "Should not apply to row storage");
    }

    #[test]
    fn test_late_materialize_not_applied_narrow_table() {
        let rule = LateMaterialization::new()
            .with_storage_type(StorageType::Columnar)
            .with_min_columns(10);

        let columns: Vec<String> = (0..5).map(|i| format!("col{}", i)).collect();
        let scan = scan_node("narrow_table", columns);

        let filter = vec![Expr::BinaryOp {
            left: Box::new(column("col0")),
            op: BinaryOp::Equal,
            right: Box::new(literal_int(5)),
        }];

        let projection = vec![column("col1"), column("col2")];

        let result = rule.try_late_materialize(&scan, &filter, &projection);
        assert!(result.is_none(), "Should not apply to narrow tables");
    }

    #[test]
    fn test_late_materialize_not_applied_low_selectivity() {
        let rule = LateMaterialization::new()
            .with_storage_type(StorageType::Columnar)
            .with_min_columns(10)
            .with_min_selectivity(0.1);

        let columns: Vec<String> = (0..20).map(|i| format!("col{}", i)).collect();
        let scan = scan_node("wide_table", columns);

        let filter = vec![Expr::BinaryOp {
            left: Box::new(column("col0")),
            op: BinaryOp::Or,
            right: Box::new(column("col1")),
        }];

        let projection = vec![column("col10"), column("col11")];

        let result = rule.try_late_materialize(&scan, &filter, &projection);
        assert!(result.is_none(), "Should not apply with low selectivity");
    }

    #[test]
    fn test_late_materialize_applied_good_conditions() {
        let rule = LateMaterialization::new()
            .with_storage_type(StorageType::Columnar)
            .with_min_columns(10)
            .with_min_selectivity(0.2);

        let columns: Vec<String> = (0..20).map(|i| format!("col{}", i)).collect();
        let scan = scan_node("wide_table", columns);

        let filter = vec![Expr::BinaryOp {
            left: Box::new(column("col0")),
            op: BinaryOp::Equal,
            right: Box::new(literal_int(5)),
        }];

        let projection = vec![column("col10"), column("col11")];

        let result = rule.try_late_materialize(&scan, &filter, &projection);
        assert!(result.is_some(), "Should apply late materialization");
    }

    #[test]
    fn test_late_materialize_no_benefit_same_columns() {
        let rule = LateMaterialization::new()
            .with_storage_type(StorageType::Columnar)
            .with_min_columns(5);

        let columns: Vec<String> = (0..20).map(|i| format!("col{}", i)).collect();
        let scan = scan_node("wide_table", columns);

        let filter = vec![Expr::BinaryOp {
            left: Box::new(column("col0")),
            op: BinaryOp::Equal,
            right: Box::new(literal_int(5)),
        }];

        let projection = vec![column("col0")];

        let result = rule.try_late_materialize(&scan, &filter, &projection);
        assert!(
            result.is_none(),
            "No benefit when filter and projection use same columns"
        );
    }

    #[test]
    fn test_optimize_project_filter_scan_pattern() -> Result<()> {
        let rule = LateMaterialization::new()
            .with_storage_type(StorageType::Columnar)
            .with_min_columns(5)
            .with_min_selectivity(0.2);

        let columns: Vec<String> = (0..20).map(|i| format!("col{}", i)).collect();
        let scan = scan_node("wide_table", columns);

        let filter = PlanNode::Filter {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(column("col0")),
                op: BinaryOp::Equal,
                right: Box::new(literal_int(5)),
            },
        };

        let plan = LogicalPlan {
            root: Box::new(PlanNode::Projection {
                expressions: vec![(column("col10"), None), (column("col11"), None)],
                input: Box::new(filter),
            }),
        };

        let result = rule.optimize(&plan)?;
        assert!(result.is_some());

        Ok(())
    }

    #[test]
    fn test_find_scan_through_nodes() {
        let scan = scan_node("test", vec!["a".to_string()]);
        let filter = PlanNode::Filter {
            input: Box::new(scan.clone()),
            predicate: Expr::Literal(LiteralValue::Boolean(true)),
        };
        let project = PlanNode::Projection {
            expressions: vec![],
            input: Box::new(filter),
        };

        let found = LateMaterialization::find_scan(&project);
        assert!(found.is_some());
    }

    #[test]
    fn test_estimate_in_list_selectivity() {
        let pred = Expr::InList {
            expr: Box::new(column("x")),
            list: vec![literal_int(1), literal_int(2), literal_int(3)],
            negated: false,
        };

        let selectivity = LateMaterialization::estimate_predicate_selectivity(&pred);
        assert!(selectivity > 0.0 && selectivity < 1.0);
    }

    #[test]
    fn test_collect_columns_case_expression() {
        let expr = Expr::Case {
            operand: Some(Box::new(column("status"))),
            when_then: vec![
                (column("active"), column("count_active")),
                (column("inactive"), column("count_inactive")),
            ],
            else_expr: Some(Box::new(column("count_other"))),
        };

        let columns = LateMaterialization::collect_columns(&[expr]);
        assert_eq!(columns.len(), 6);
        assert!(columns.contains("status"));
        assert!(columns.contains("active"));
        assert!(columns.contains("inactive"));
        assert!(columns.contains("count_active"));
        assert!(columns.contains("count_inactive"));
        assert!(columns.contains("count_other"));
    }

    #[test]
    fn test_storage_type_configuration() {
        let rule = LateMaterialization::new().with_storage_type(StorageType::Row);
        assert!(matches!(rule.storage_type, StorageType::Row));

        let rule = LateMaterialization::new().with_storage_type(StorageType::Columnar);
        assert!(matches!(rule.storage_type, StorageType::Columnar));
    }
}
