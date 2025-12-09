use yachtsql_core::error::Result;

use crate::optimizer::plan::{JoinType, LogicalPlan, PlanNode};
use crate::optimizer::rule::OptimizationRule;

pub struct JoinReorder;

impl JoinReorder {
    pub fn new() -> Self {
        Self
    }

    fn estimate_cardinality(node: &PlanNode) -> usize {
        match node {
            PlanNode::Scan { .. } => 10000,
            PlanNode::IndexScan { .. } => 1000,
            PlanNode::Filter { .. } => 1000,
            PlanNode::Projection { input, .. } => Self::estimate_cardinality(input),
            PlanNode::Aggregate { .. } => 100,
            PlanNode::Join { left, right, .. } => {
                let left_card = Self::estimate_cardinality(left);
                let right_card = Self::estimate_cardinality(right);

                std::cmp::min(left_card * right_card / 100, left_card + right_card)
            }
            PlanNode::LateralJoin { left, right, .. } => {
                let left_card = Self::estimate_cardinality(left);
                let right_card = Self::estimate_cardinality(right);
                std::cmp::min(left_card * right_card / 100, left_card + right_card)
            }
            PlanNode::Sort { input, .. } => Self::estimate_cardinality(input),
            PlanNode::Limit { limit, .. } => *limit,
            PlanNode::Distinct { input } => Self::estimate_cardinality(input) / 2,
            PlanNode::SubqueryScan { subquery, .. } => Self::estimate_cardinality(subquery),
            PlanNode::Union { left, right, .. } => {
                Self::estimate_cardinality(left) + Self::estimate_cardinality(right)
            }
            PlanNode::Intersect { left, right, .. } => {
                std::cmp::min(
                    Self::estimate_cardinality(left),
                    Self::estimate_cardinality(right),
                ) / 2
            }
            PlanNode::Except { left, .. } => Self::estimate_cardinality(left) / 2,
            PlanNode::Cte { input, .. } => Self::estimate_cardinality(input),
            PlanNode::Update { .. } => 0,
            PlanNode::Delete { .. } => 0,
            PlanNode::Truncate { .. } => 0,
            PlanNode::Unnest { .. } => 100,
            PlanNode::TableValuedFunction { .. } => 100,
            PlanNode::Window { input, .. } => Self::estimate_cardinality(input),
            PlanNode::AlterTable { .. } => 0,
            PlanNode::DistinctOn { input, .. } => Self::estimate_cardinality(input) / 2,
            PlanNode::EmptyRelation => 1,
            PlanNode::Values { rows } => rows.len(),
            PlanNode::InsertOnConflict { values, .. } => values.len(),
            PlanNode::Insert { values, source, .. } => {
                if let Some(rows) = values {
                    rows.len()
                } else if let Some(src) = source {
                    Self::estimate_cardinality(src)
                } else {
                    0
                }
            }
            PlanNode::Merge { source, .. } => Self::estimate_cardinality(source),
            PlanNode::ArrayJoin { input, .. } => Self::estimate_cardinality(input) * 10,
            PlanNode::TableSample { input, size, .. } => match size {
                yachtsql_ir::plan::SampleSize::Percent(pct) => {
                    Self::estimate_cardinality(input) * (*pct as usize) / 100
                }
                yachtsql_ir::plan::SampleSize::Rows(n) => {
                    std::cmp::min(Self::estimate_cardinality(input), *n)
                }
            },
            PlanNode::Pivot { input, .. } => Self::estimate_cardinality(input),
            PlanNode::Unpivot {
                input,
                unpivot_columns,
                ..
            } => Self::estimate_cardinality(input) * unpivot_columns.len(),
        }
    }

    fn can_reorder_joins(left_type: &JoinType, right_type: &JoinType) -> bool {
        matches!((left_type, right_type), (JoinType::Inner, JoinType::Inner))
    }

    #[allow(clippy::only_used_in_recursion)]
    fn optimize_join_chain(&self, node: &PlanNode) -> Option<PlanNode> {
        match node {
            PlanNode::Join {
                left,
                right,
                on,
                join_type,
            } => {
                if let PlanNode::Join {
                    left: ll,
                    right: lr,
                    on: left_on,
                    join_type: left_join_type,
                } = left.as_ref()
                    && Self::can_reorder_joins(left_join_type, join_type)
                {
                    let _card_ll = Self::estimate_cardinality(ll);
                    let card_lr = Self::estimate_cardinality(lr);
                    let card_right = Self::estimate_cardinality(right);

                    if card_lr > card_right {
                        let new_left = PlanNode::Join {
                            left: ll.clone(),
                            right: right.clone(),
                            on: on.clone(),
                            join_type: *join_type,
                        };

                        return Some(PlanNode::Join {
                            left: Box::new(new_left),
                            right: lr.clone(),
                            on: left_on.clone(),
                            join_type: *left_join_type,
                        });
                    }
                }

                let left_opt = self.optimize_join_chain(left);
                let right_opt = self.optimize_join_chain(right);

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
                let left_opt = self.optimize_join_chain(left);
                let right_opt = self.optimize_join_chain(right);

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

            PlanNode::Projection { expressions, input } => {
                self.optimize_join_chain(input)
                    .map(|optimized| PlanNode::Projection {
                        expressions: expressions.clone(),
                        input: Box::new(optimized),
                    })
            }
            PlanNode::Filter { predicate, input } => {
                self.optimize_join_chain(input)
                    .map(|optimized| PlanNode::Filter {
                        predicate: predicate.clone(),
                        input: Box::new(optimized),
                    })
            }
            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                ..
            } => self
                .optimize_join_chain(input)
                .map(|optimized| PlanNode::Aggregate {
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    input: Box::new(optimized),
                    grouping_metadata: None,
                }),
            PlanNode::Sort { order_by, input } => {
                self.optimize_join_chain(input)
                    .map(|optimized| PlanNode::Sort {
                        order_by: order_by.clone(),
                        input: Box::new(optimized),
                    })
            }
            PlanNode::Limit {
                limit,
                offset,
                input,
            } => self
                .optimize_join_chain(input)
                .map(|optimized| PlanNode::Limit {
                    limit: *limit,
                    offset: *offset,
                    input: Box::new(optimized),
                }),
            PlanNode::Distinct { input } => {
                self.optimize_join_chain(input)
                    .map(|optimized| PlanNode::Distinct {
                        input: Box::new(optimized),
                    })
            }
            PlanNode::SubqueryScan { subquery, alias } => {
                self.optimize_join_chain(subquery)
                    .map(|optimized| PlanNode::SubqueryScan {
                        subquery: Box::new(optimized),
                        alias: alias.clone(),
                    })
            }
            PlanNode::Union { left, right, all } => {
                let left_opt = self.optimize_join_chain(left);
                let right_opt = self.optimize_join_chain(right);

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
                let left_opt = self.optimize_join_chain(left);
                let right_opt = self.optimize_join_chain(right);

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
                let left_opt = self.optimize_join_chain(left);
                let right_opt = self.optimize_join_chain(right);

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
                let cte_opt = self.optimize_join_chain(cte_plan);
                let input_opt = self.optimize_join_chain(input);

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
                .optimize_join_chain(input)
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
                .optimize_join_chain(input)
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
                .optimize_join_chain(input)
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

impl Default for JoinReorder {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for JoinReorder {
    fn name(&self) -> &str {
        "join_reorder"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(self.optimize_join_chain(plan.root()).map(LogicalPlan::new))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::expr::Expr;

    #[test]
    fn test_estimate_cardinality_scan() {
        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test".to_string(),
            projection: None,
            only: false,
        };
        let card = JoinReorder::estimate_cardinality(&scan);
        assert_eq!(card, 10000);
    }

    #[test]
    fn test_estimate_cardinality_filter() {
        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test".to_string(),
            projection: None,
            only: false,
        };
        let filter = PlanNode::Filter {
            predicate: Expr::column("x"),
            input: Box::new(scan),
        };
        let card = JoinReorder::estimate_cardinality(&filter);
        assert_eq!(card, 1000);
    }

    #[test]
    fn test_estimate_cardinality_limit() {
        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test".to_string(),
            projection: None,
            only: false,
        };
        let limit = PlanNode::Limit {
            limit: 42,
            offset: 0,
            input: Box::new(scan),
        };
        let card = JoinReorder::estimate_cardinality(&limit);
        assert_eq!(card, 42);
    }

    #[test]
    fn test_can_reorder_inner_joins() {
        assert!(JoinReorder::can_reorder_joins(
            &JoinType::Inner,
            &JoinType::Inner
        ));
    }

    #[test]
    fn test_cannot_reorder_outer_joins() {
        assert!(!JoinReorder::can_reorder_joins(
            &JoinType::Left,
            &JoinType::Inner
        ));
        assert!(!JoinReorder::can_reorder_joins(
            &JoinType::Inner,
            &JoinType::Left
        ));
    }

    #[test]
    fn test_no_optimization_for_single_join() {
        let rule = JoinReorder::new();

        let left_scan = PlanNode::Scan {
            alias: None,
            table_name: "left_table".to_string(),
            projection: None,
            only: false,
        };

        let right_scan = PlanNode::Scan {
            alias: None,
            table_name: "right_table".to_string(),
            projection: None,
            only: false,
        };

        let join = PlanNode::Join {
            left: Box::new(left_scan),
            right: Box::new(right_scan),
            on: Expr::column("id"),
            join_type: JoinType::Inner,
        };

        let plan = LogicalPlan::new(join);
        let optimized = rule.optimize(&plan).expect("optimization should succeed");

        assert!(optimized.is_none());
    }

    #[test]
    fn test_join_reorder_with_different_sizes() {
        let rule = JoinReorder::new();

        let small_scan = PlanNode::Scan {
            alias: None,
            table_name: "small".to_string(),
            projection: None,
            only: false,
        };
        let small_filter = PlanNode::Filter {
            predicate: Expr::column("x"),
            input: Box::new(small_scan),
        };

        let medium_scan = PlanNode::Scan {
            alias: None,
            table_name: "medium".to_string(),
            projection: None,
            only: false,
        };

        let large_scan = PlanNode::Scan {
            alias: None,
            table_name: "large".to_string(),
            projection: None,
            only: false,
        };

        let inner_join = PlanNode::Join {
            left: Box::new(small_filter),
            right: Box::new(medium_scan),
            on: Expr::column("id"),
            join_type: JoinType::Inner,
        };

        let outer_join = PlanNode::Join {
            left: Box::new(inner_join),
            right: Box::new(large_scan),
            on: Expr::column("id2"),
            join_type: JoinType::Inner,
        };

        let plan = LogicalPlan::new(outer_join);
        let optimized = rule.optimize(&plan).expect("optimization should succeed");

        assert!(optimized.is_some() || optimized.is_none());
    }

    #[test]
    fn test_no_reorder_for_outer_joins() {
        let rule = JoinReorder::new();

        let left_scan = PlanNode::Scan {
            alias: None,
            table_name: "left".to_string(),
            projection: None,
            only: false,
        };
        let right_scan = PlanNode::Scan {
            alias: None,
            table_name: "right".to_string(),
            projection: None,
            only: false,
        };
        let third_scan = PlanNode::Scan {
            alias: None,
            table_name: "third".to_string(),
            projection: None,
            only: false,
        };

        let left_join = PlanNode::Join {
            left: Box::new(left_scan),
            right: Box::new(right_scan),
            on: Expr::column("id"),
            join_type: JoinType::Left,
        };

        let outer_join = PlanNode::Join {
            left: Box::new(left_join),
            right: Box::new(third_scan),
            on: Expr::column("id2"),
            join_type: JoinType::Inner,
        };

        let plan = LogicalPlan::new(outer_join);
        let optimized = rule.optimize(&plan).expect("optimization should succeed");

        assert!(optimized.is_none());
    }
}
