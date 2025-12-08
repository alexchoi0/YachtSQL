use yachtsql_core::error::Result;

use crate::optimizer::plan::{LogicalPlan, PlanNode};
use crate::optimizer::rule::OptimizationRule;

pub struct LimitPushdown;

impl LimitPushdown {
    pub fn new() -> Self {
        Self
    }

    #[allow(clippy::only_used_in_recursion)]
    fn optimize_node(&self, node: &PlanNode) -> Option<PlanNode> {
        match node {
            PlanNode::Limit {
                limit,
                offset,
                input,
            } => {
                if let PlanNode::Projection {
                    expressions,
                    input: proj_input,
                } = input.as_ref()
                {
                    let new_limit = PlanNode::Limit {
                        limit: *limit,
                        offset: *offset,
                        input: proj_input.clone(),
                    };
                    Some(PlanNode::Projection {
                        expressions: expressions.clone(),
                        input: Box::new(new_limit),
                    })
                } else {
                    self.optimize_node(input)
                        .map(|optimized_input| PlanNode::Limit {
                            limit: *limit,
                            offset: *offset,
                            input: Box::new(optimized_input),
                        })
                }
            }
            PlanNode::Filter { predicate, input } => {
                self.optimize_node(input)
                    .map(|optimized_input| PlanNode::Filter {
                        predicate: predicate.clone(),
                        input: Box::new(optimized_input),
                    })
            }
            PlanNode::Projection { expressions, input } => {
                self.optimize_node(input)
                    .map(|optimized_input| PlanNode::Projection {
                        expressions: expressions.clone(),
                        input: Box::new(optimized_input),
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
            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                ..
            } => self
                .optimize_node(input)
                .map(|optimized_input| PlanNode::Aggregate {
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    input: Box::new(optimized_input),
                    grouping_metadata: None,
                }),
            PlanNode::Sort { order_by, input } => {
                self.optimize_node(input)
                    .map(|optimized_input| PlanNode::Sort {
                        order_by: order_by.clone(),
                        input: Box::new(optimized_input),
                    })
            }
            PlanNode::Distinct { input } => {
                self.optimize_node(input)
                    .map(|optimized_input| PlanNode::Distinct {
                        input: Box::new(optimized_input),
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

impl Default for LimitPushdown {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for LimitPushdown {
    fn name(&self) -> &str {
        "limit_pushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(self.optimize_node(plan.root()).map(LogicalPlan::new))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::error::Result;

    use super::*;
    use crate::optimizer::expr::{BinaryOp, Expr, LiteralValue};

    #[test]
    fn test_pushdown_limit_through_projection() -> Result<()> {
        let rule = LimitPushdown::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let projection = PlanNode::Projection {
            expressions: vec![(Expr::column("x"), None)],
            input: Box::new(scan),
        };

        let limit = PlanNode::Limit {
            limit: 10,
            offset: 0,
            input: Box::new(projection.clone()),
        };

        let plan = LogicalPlan::new(limit);
        let optimized = rule.optimize(&plan)?;

        assert!(optimized.is_some());
        let optimized_plan = optimized.expect("optimization should produce a plan");

        match optimized_plan.root() {
            PlanNode::Projection {
                expressions: _,
                input,
            } => {
                assert!(matches!(input.as_ref(), PlanNode::Limit { .. }));
            }
            _ => panic!("Expected Projection at root"),
        }
        Ok(())
    }

    #[test]
    fn test_no_pushdown_through_filter() -> Result<()> {
        let rule = LimitPushdown::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let filter = PlanNode::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::column("x")),
                op: BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
            },
            input: Box::new(scan),
        };

        let limit = PlanNode::Limit {
            limit: 10,
            offset: 0,
            input: Box::new(filter),
        };

        let plan = LogicalPlan::new(limit);
        let optimized = rule.optimize(&plan)?;

        assert!(optimized.is_none());
        Ok(())
    }

    #[test]
    fn test_pushdown_with_offset() -> Result<()> {
        let rule = LimitPushdown::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let projection = PlanNode::Projection {
            expressions: vec![(Expr::column("x"), None)],
            input: Box::new(scan),
        };

        let limit = PlanNode::Limit {
            limit: 10,
            offset: 5,
            input: Box::new(projection),
        };

        let plan = LogicalPlan::new(limit);
        let optimized = rule.optimize(&plan)?;

        assert!(optimized.is_some());
        let optimized_plan = optimized.expect("optimization should produce a plan");

        match optimized_plan.root() {
            PlanNode::Projection { input, .. } => match input.as_ref() {
                PlanNode::Limit { limit, offset, .. } => {
                    assert_eq!(*limit, 10);
                    assert_eq!(*offset, 5);
                }
                _ => panic!("Expected Limit node"),
            },
            _ => panic!("Expected Projection at root"),
        }
        Ok(())
    }

    #[test]
    fn test_no_pushdown_on_scan() -> Result<()> {
        let rule = LimitPushdown::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let limit = PlanNode::Limit {
            limit: 10,
            offset: 0,
            input: Box::new(scan),
        };

        let plan = LogicalPlan::new(limit);
        let optimized = rule.optimize(&plan)?;

        assert!(optimized.is_none());
        Ok(())
    }

    #[test]
    fn test_pushdown_multiple_projections() -> Result<()> {
        let rule = LimitPushdown::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let projection1 = PlanNode::Projection {
            expressions: vec![(Expr::column("y"), None)],
            input: Box::new(scan),
        };

        let projection2 = PlanNode::Projection {
            expressions: vec![(Expr::column("x"), None)],
            input: Box::new(projection1),
        };

        let limit = PlanNode::Limit {
            limit: 10,
            offset: 0,
            input: Box::new(projection2),
        };

        let plan = LogicalPlan::new(limit);

        let optimized = rule.optimize(&plan)?;
        assert!(optimized.is_some());

        let optimized_plan = optimized.expect("optimization should produce a plan");
        match optimized_plan.root() {
            PlanNode::Projection { input, .. } => {
                assert!(matches!(input.as_ref(), PlanNode::Limit { .. }));
            }
            _ => panic!("Expected Projection at root"),
        }
        Ok(())
    }
}
