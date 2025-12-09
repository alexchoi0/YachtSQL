use yachtsql_core::error::Result;

use crate::optimizer::expr::Expr;
use crate::optimizer::plan::{JoinType, LogicalPlan, PlanNode};
use crate::optimizer::rule::OptimizationRule;

pub struct AggregatePushdown;

impl AggregatePushdown {
    pub fn new() -> Self {
        Self
    }

    fn can_push_below_join(join_type: &JoinType) -> bool {
        matches!(join_type, JoinType::Inner)
    }

    fn try_push_aggregate_below_join(
        &self,
        group_by: &[Expr],
        aggregates: &[Expr],
        join: &PlanNode,
    ) -> Option<PlanNode> {
        if let PlanNode::Join {
            left,
            right,
            on,
            join_type,
        } = join
            && Self::can_push_below_join(join_type)
            && !group_by.is_empty()
        {
            let new_left = PlanNode::Aggregate {
                group_by: group_by.to_vec(),
                aggregates: aggregates.to_vec(),
                input: left.clone(),
                grouping_metadata: None,
            };

            return Some(PlanNode::Join {
                left: Box::new(new_left),
                right: right.clone(),
                on: on.clone(),
                join_type: *join_type,
            });
        }
        None
    }

    #[allow(clippy::only_used_in_recursion)]
    fn optimize_node(&self, node: &PlanNode) -> Option<PlanNode> {
        match node {
            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                grouping_metadata: _,
            } => {
                if let Some(pushed) =
                    self.try_push_aggregate_below_join(group_by, aggregates, input)
                {
                    return Some(pushed);
                }

                self.optimize_node(input)
                    .map(|optimized| PlanNode::Aggregate {
                        group_by: group_by.clone(),
                        aggregates: aggregates.clone(),
                        input: Box::new(optimized),
                        grouping_metadata: None,
                    })
            }
            PlanNode::Projection { expressions, input } => {
                self.optimize_node(input)
                    .map(|optimized| PlanNode::Projection {
                        expressions: expressions.clone(),
                        input: Box::new(optimized),
                    })
            }
            PlanNode::Filter { predicate, input } => {
                self.optimize_node(input).map(|optimized| PlanNode::Filter {
                    predicate: predicate.clone(),
                    input: Box::new(optimized),
                })
            }
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
            | PlanNode::ArrayJoin { .. }
            | PlanNode::EmptyRelation
            | PlanNode::Values { .. }
            | PlanNode::InsertOnConflict { .. }
            | PlanNode::Insert { .. }
            | PlanNode::Merge { .. } => None,
        }
    }
}

impl Default for AggregatePushdown {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for AggregatePushdown {
    fn name(&self) -> &str {
        "aggregate_pushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(self.optimize_node(plan.root()).map(LogicalPlan::new))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_push_below_inner_join() {
        assert!(AggregatePushdown::can_push_below_join(&JoinType::Inner));
    }

    #[test]
    fn test_cannot_push_below_outer_join() {
        assert!(!AggregatePushdown::can_push_below_join(&JoinType::Left));
        assert!(!AggregatePushdown::can_push_below_join(&JoinType::Right));
        assert!(!AggregatePushdown::can_push_below_join(&JoinType::Full));
    }

    #[test]
    fn test_push_aggregate_below_join() {
        let rule = AggregatePushdown::new();

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

        let aggregate = PlanNode::Aggregate {
            group_by: vec![Expr::column("x")],
            aggregates: vec![Expr::Aggregate {
                name: yachtsql_ir::FunctionName::Sum,
                args: vec![Expr::column("y")],
                distinct: false,
                order_by: None,
                filter: None,
            }],
            grouping_metadata: None,
            input: Box::new(join),
        };

        let plan = LogicalPlan::new(aggregate);
        let optimized = rule.optimize(&plan).expect("optimization should succeed");

        assert!(optimized.is_some());

        if let Some(opt_plan) = optimized {
            match opt_plan.root() {
                PlanNode::Join { left, .. } => {
                    assert!(matches!(left.as_ref(), PlanNode::Aggregate { .. }));
                }
                _ => panic!("Expected Join at root"),
            }
        }
    }

    #[test]
    fn test_no_pushdown_without_group_by() {
        let rule = AggregatePushdown::new();

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

        let aggregate = PlanNode::Aggregate {
            group_by: vec![],
            aggregates: vec![Expr::Aggregate {
                name: yachtsql_ir::FunctionName::Sum,
                args: vec![Expr::column("x")],
                distinct: false,
                order_by: None,
                filter: None,
            }],
            grouping_metadata: None,
            input: Box::new(join),
        };

        let plan = LogicalPlan::new(aggregate);
        let optimized = rule.optimize(&plan).expect("optimization should succeed");

        assert!(optimized.is_none());
    }

    #[test]
    fn test_no_pushdown_for_outer_join() {
        let rule = AggregatePushdown::new();

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
            join_type: JoinType::Left,
        };

        let aggregate = PlanNode::aggregate(
            vec![Expr::column("x")],
            vec![Expr::Aggregate {
                name: yachtsql_ir::FunctionName::Count,
                args: vec![Expr::Wildcard],
                distinct: false,
                order_by: None,
                filter: None,
            }],
            Box::new(join),
        );

        let plan = LogicalPlan::new(aggregate);
        let optimized = rule.optimize(&plan).expect("optimization should succeed");

        assert!(optimized.is_none());
    }

    #[test]
    fn test_no_optimization_for_scan() {
        let rule = AggregatePushdown::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
            only: false,
        };

        let plan = LogicalPlan::new(scan);
        let optimized = rule.optimize(&plan).expect("optimization should succeed");

        assert!(optimized.is_none());
    }

    #[test]
    fn test_aggregate_on_aggregate() {
        let rule = AggregatePushdown::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test".to_string(),
            projection: None,
            only: false,
        };

        let inner_agg = PlanNode::aggregate(
            vec![Expr::column("x")],
            vec![Expr::Aggregate {
                name: yachtsql_ir::FunctionName::Sum,
                args: vec![Expr::column("y")],
                distinct: false,
                order_by: None,
                filter: None,
            }],
            Box::new(scan),
        );

        let outer_agg = PlanNode::aggregate(
            vec![Expr::column("x")],
            vec![Expr::Aggregate {
                name: yachtsql_ir::FunctionName::Count,
                args: vec![Expr::Wildcard],
                distinct: false,
                order_by: None,
                filter: None,
            }],
            Box::new(inner_agg),
        );

        let plan = LogicalPlan::new(outer_agg);
        let optimized = rule.optimize(&plan).expect("optimization should succeed");

        assert!(optimized.is_none());
    }
}
