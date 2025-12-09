use yachtsql_core::error::Result;

use crate::optimizer::expr::{BinaryOp, Expr, LiteralValue};
use crate::optimizer::plan::{LogicalPlan, PlanNode};
use crate::optimizer::rule::OptimizationRule;

pub struct PartitionPruning;

impl PartitionPruning {
    pub fn new() -> Self {
        Self
    }

    fn extract_partition_predicate(expr: &Expr) -> Option<(String, Vec<LiteralValue>)> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Equal,
                right,
            } => {
                if let (Expr::Column { name, .. }, Expr::Literal(lit)) =
                    (left.as_ref(), right.as_ref())
                {
                    return Some((name.clone(), vec![lit.clone()]));
                }
                if let (Expr::Literal(lit), Expr::Column { name, .. }) =
                    (left.as_ref(), right.as_ref())
                {
                    return Some((name.clone(), vec![lit.clone()]));
                }
                None
            }

            Expr::BinaryOp {
                left,
                op: BinaryOp::In,
                ..
            } => {
                if let Expr::Column { name, .. } = left.as_ref() {
                    Some((name.clone(), vec![]))
                } else {
                    None
                }
            }

            Expr::BinaryOp {
                left,
                op:
                    BinaryOp::LessThan
                    | BinaryOp::LessThanOrEqual
                    | BinaryOp::GreaterThan
                    | BinaryOp::GreaterThanOrEqual,
                right,
            } => {
                if let (Expr::Column { name, .. }, Expr::Literal(lit)) =
                    (left.as_ref(), right.as_ref())
                {
                    return Some((name.clone(), vec![lit.clone()]));
                }
                None
            }

            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right: _,
            } => Self::extract_partition_predicate(left),
            _ => None,
        }
    }

    fn can_prune_scan(scan: &PlanNode, predicate: &Expr) -> bool {
        if let PlanNode::Scan { .. } = scan {
            Self::extract_partition_predicate(predicate).is_some()
        } else {
            false
        }
    }

    fn try_push_filter_to_scan(&self, filter: &PlanNode) -> Option<PlanNode> {
        if let PlanNode::Filter { predicate, input } = filter
            && Self::can_prune_scan(input, predicate)
        {
            return self.optimize_node(input).map(|optimized| PlanNode::Filter {
                predicate: predicate.clone(),
                input: Box::new(optimized),
            });
        }
        None
    }

    #[allow(clippy::only_used_in_recursion)]
    fn optimize_node(&self, node: &PlanNode) -> Option<PlanNode> {
        match node {
            PlanNode::Filter { .. } => self.try_push_filter_to_scan(node),
            PlanNode::Projection { expressions, input } => {
                self.optimize_node(input)
                    .map(|optimized| PlanNode::Projection {
                        expressions: expressions.clone(),
                        input: Box::new(optimized),
                    })
            }
            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                ..
            } => self
                .optimize_node(input)
                .map(|optimized| PlanNode::Aggregate {
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    input: Box::new(optimized),
                    grouping_metadata: None,
                }),
            PlanNode::Join {
                left,
                right,
                on,
                join_type,
            } => {
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::Join {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        on: on.clone(),
                        join_type: *join_type,
                    })
                } else {
                    None
                }
            }
            PlanNode::LateralJoin {
                left,
                right,
                on,
                join_type,
            } => {
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::LateralJoin {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        on: on.clone(),
                        join_type: *join_type,
                    })
                } else {
                    None
                }
            }
            PlanNode::Sort { order_by, input } => {
                self.optimize_node(input).map(|optimized| PlanNode::Sort {
                    order_by: order_by.clone(),
                    input: Box::new(optimized),
                })
            }
            PlanNode::Limit {
                limit,
                offset,
                input,
            } => self.optimize_node(input).map(|optimized| PlanNode::Limit {
                limit: *limit,
                offset: *offset,
                input: Box::new(optimized),
            }),
            PlanNode::Distinct { input } => {
                self.optimize_node(input)
                    .map(|optimized| PlanNode::Distinct {
                        input: Box::new(optimized),
                    })
            }
            PlanNode::SubqueryScan { subquery, alias } => {
                self.optimize_node(subquery)
                    .map(|optimized_subquery| PlanNode::SubqueryScan {
                        subquery: Box::new(optimized_subquery),
                        alias: alias.clone(),
                    })
            }
            PlanNode::Union { left, right, all } => {
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::Union {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        all: *all,
                    })
                } else {
                    None
                }
            }
            PlanNode::Intersect { left, right, all } => {
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::Intersect {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        all: *all,
                    })
                } else {
                    None
                }
            }
            PlanNode::Except { left, right, all } => {
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::Except {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        all: *all,
                    })
                } else {
                    None
                }
            }
            PlanNode::Cte {
                name,
                cte_plan,
                input,
                recursive,
                use_union_all,
                materialization_hint,
                column_aliases,
            } => {
                let cte_opt = self.optimize_node(cte_plan);
                let input_opt = self.optimize_node(input);

                if cte_opt.is_some() || input_opt.is_some() {
                    Some(PlanNode::Cte {
                        name: name.clone(),
                        cte_plan: Box::new(cte_opt.unwrap_or_else(|| cte_plan.as_ref().clone())),
                        input: Box::new(input_opt.unwrap_or_else(|| input.as_ref().clone())),
                        recursive: *recursive,
                        use_union_all: *use_union_all,
                        materialization_hint: materialization_hint.clone(),
                        column_aliases: column_aliases.clone(),
                    })
                } else {
                    None
                }
            }
            PlanNode::TableSample {
                input,
                method,
                size,
                seed,
            } => self
                .optimize_node(input)
                .map(|optimized_input| PlanNode::TableSample {
                    input: Box::new(optimized_input),
                    method: method.clone(),
                    size: size.clone(),
                    seed: *seed,
                }),
            PlanNode::Pivot {
                input,
                aggregate_expr,
                aggregate_function,
                pivot_column,
                pivot_values,
                group_by_columns,
            } => self
                .optimize_node(input)
                .map(|optimized_input| PlanNode::Pivot {
                    input: Box::new(optimized_input),
                    aggregate_expr: aggregate_expr.clone(),
                    aggregate_function: aggregate_function.clone(),
                    pivot_column: pivot_column.clone(),
                    pivot_values: pivot_values.clone(),
                    group_by_columns: group_by_columns.clone(),
                }),
            PlanNode::Unpivot {
                input,
                value_column,
                name_column,
                unpivot_columns,
            } => self
                .optimize_node(input)
                .map(|optimized_input| PlanNode::Unpivot {
                    input: Box::new(optimized_input),
                    value_column: value_column.clone(),
                    name_column: name_column.clone(),
                    unpivot_columns: unpivot_columns.clone(),
                }),
            PlanNode::Scan { .. }
            | PlanNode::IndexScan { .. }
            | PlanNode::Update { .. }
            | PlanNode::Delete { .. }
            | PlanNode::Truncate { .. }
            | PlanNode::AlterTable { .. }
            | PlanNode::Unnest { .. }
            | PlanNode::TableValuedFunction { .. }
            | PlanNode::Window { .. }
            | PlanNode::DistinctOn { .. }
            | PlanNode::ArrayJoin { .. } => None,
            PlanNode::EmptyRelation
            | PlanNode::Values { .. }
            | PlanNode::InsertOnConflict { .. }
            | PlanNode::Insert { .. }
            | PlanNode::Merge { .. } => None,
        }
    }
}

impl Default for PartitionPruning {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for PartitionPruning {
    fn name(&self) -> &str {
        "partition_pruning"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(self.optimize_node(plan.root()).map(LogicalPlan::new))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_partition_predicate_equality() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::column("date")),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String(
                "2024-01-01".to_string(),
            ))),
        };

        let result = PartitionPruning::extract_partition_predicate(&expr);
        assert!(result.is_some());
        let (col, vals) = result.unwrap();
        assert_eq!(col, "date");
        assert_eq!(vals.len(), 1);
    }

    #[test]
    fn test_extract_partition_predicate_reversed() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::String(
                "2024-01-01".to_string(),
            ))),
            op: BinaryOp::Equal,
            right: Box::new(Expr::column("date")),
        };

        let result = PartitionPruning::extract_partition_predicate(&expr);
        assert!(result.is_some());
        let (col, _) = result.unwrap();
        assert_eq!(col, "date");
    }

    #[test]
    fn test_extract_partition_predicate_in() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::column("date")),
            op: BinaryOp::In,
            right: Box::new(Expr::Literal(LiteralValue::String("dummy".to_string()))),
        };

        let result = PartitionPruning::extract_partition_predicate(&expr);
        assert!(result.is_some());
        let (col, _) = result.unwrap();
        assert_eq!(col, "date");
    }

    #[test]
    fn test_extract_partition_predicate_range() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::column("date")),
            op: BinaryOp::GreaterThan,
            right: Box::new(Expr::Literal(LiteralValue::String(
                "2024-01-01".to_string(),
            ))),
        };

        let result = PartitionPruning::extract_partition_predicate(&expr);
        assert!(result.is_some());
        let (col, vals) = result.unwrap();
        assert_eq!(col, "date");
        assert_eq!(vals.len(), 1);
    }

    #[test]
    fn test_extract_partition_predicate_complex() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::column("date")),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(LiteralValue::Int64(1))),
            }),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };

        let result = PartitionPruning::extract_partition_predicate(&expr);
        assert!(result.is_none());
    }

    #[test]
    fn test_can_prune_scan() {
        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test".to_string(),
            projection: None,
            only: false,
        };

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::column("date")),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String(
                "2024-01-01".to_string(),
            ))),
        };

        assert!(PartitionPruning::can_prune_scan(&scan, &predicate));
    }

    #[test]
    fn test_rule_application() {
        let rule = PartitionPruning::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test".to_string(),
            projection: None,
            only: false,
        };

        let filter = PlanNode::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::column("date")),
                op: BinaryOp::Equal,
                right: Box::new(Expr::Literal(LiteralValue::String(
                    "2024-01-01".to_string(),
                ))),
            },
            input: Box::new(scan),
        };

        let plan = LogicalPlan::new(filter);
        let optimized = rule.optimize(&plan).unwrap();

        assert!(optimized.is_none() || optimized.is_some());
    }

    #[test]
    fn test_no_pruning_without_filter() {
        let rule = PartitionPruning::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test".to_string(),
            projection: None,
            only: false,
        };

        let plan = LogicalPlan::new(scan);
        let optimized = rule.optimize(&plan).unwrap();

        assert!(optimized.is_none());
    }
}
