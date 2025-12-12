use yachtsql_common::error::Result;
use yachtsql_ir::expr::Expr;
use yachtsql_ir::plan::PlanNode;

use crate::optimizer::plan::LogicalPlan;
use crate::optimizer::rule::OptimizationRule;

pub struct UnionOptimization;

impl UnionOptimization {
    pub fn new() -> Self {
        Self
    }

    fn contains_unions(&self, plan: &LogicalPlan) -> bool {
        Self::has_union_node(&plan.root)
    }

    fn has_union_node(node: &PlanNode) -> bool {
        match node {
            PlanNode::Union { .. } => true,
            _ => node.children().iter().any(|c| Self::has_union_node(c)),
        }
    }

    fn transform_node(&self, node: &PlanNode) -> Result<PlanNode> {
        match node {
            PlanNode::Filter { predicate, input } => match input.as_ref() {
                PlanNode::Union { left, right, all } => {
                    let left_with_filter = PlanNode::Filter {
                        predicate: predicate.clone(),
                        input: left.clone(),
                    };
                    let right_with_filter = PlanNode::Filter {
                        predicate: predicate.clone(),
                        input: right.clone(),
                    };

                    Ok(PlanNode::Union {
                        left: Box::new(self.transform_node(&left_with_filter)?),
                        right: Box::new(self.transform_node(&right_with_filter)?),
                        all: *all,
                    })
                }
                _ => {
                    let transformed_input = self.transform_node(input)?;
                    Ok(PlanNode::Filter {
                        predicate: predicate.clone(),
                        input: Box::new(transformed_input),
                    })
                }
            },

            PlanNode::Union { left, right, all } => {
                let transformed_left = self.transform_node(left)?;
                let transformed_right = self.transform_node(right)?;

                let can_convert_to_all = if !*all {
                    self.branches_are_disjoint(&transformed_left, &transformed_right)
                } else {
                    false
                };

                Ok(PlanNode::Union {
                    left: Box::new(transformed_left),
                    right: Box::new(transformed_right),
                    all: *all || can_convert_to_all,
                })
            }

            PlanNode::Projection { expressions, input } => Ok(PlanNode::Projection {
                expressions: expressions.clone(),
                input: Box::new(self.transform_node(input)?),
            }),
            PlanNode::Sort { order_by, input } => Ok(PlanNode::Sort {
                order_by: order_by.clone(),
                input: Box::new(self.transform_node(input)?),
            }),
            PlanNode::Limit {
                limit,
                offset,
                input,
            } => Ok(PlanNode::Limit {
                limit: *limit,
                offset: *offset,
                input: Box::new(self.transform_node(input)?),
            }),
            PlanNode::Join {
                left,
                right,
                on,
                join_type,
                using_columns,
            } => Ok(PlanNode::Join {
                left: Box::new(self.transform_node(left)?),
                right: Box::new(self.transform_node(right)?),
                on: on.clone(),
                join_type: *join_type,
                using_columns: using_columns.clone(),
            }),

            PlanNode::Intersect { left, right, all } => Ok(PlanNode::Intersect {
                left: Box::new(self.transform_node(left)?),
                right: Box::new(self.transform_node(right)?),
                all: *all,
            }),
            PlanNode::Except { left, right, all } => Ok(PlanNode::Except {
                left: Box::new(self.transform_node(left)?),
                right: Box::new(self.transform_node(right)?),
                all: *all,
            }),

            _ => Ok(node.clone()),
        }
    }

    fn branches_are_disjoint(&self, left: &PlanNode, right: &PlanNode) -> bool {
        let left_pred = Self::extract_top_filter(left);
        let right_pred = Self::extract_top_filter(right);

        match (left_pred, right_pred) {
            (Some(l), Some(r)) => self.predicates_are_disjoint(&l, &r),
            _ => false,
        }
    }

    fn extract_top_filter(node: &PlanNode) -> Option<Expr> {
        match node {
            PlanNode::Filter { predicate, .. } => Some(predicate.clone()),
            PlanNode::Projection { input, .. } => Self::extract_top_filter(input),
            _ => None,
        }
    }

    fn predicates_are_disjoint(&self, left: &Expr, right: &Expr) -> bool {
        use yachtsql_ir::expr::{BinaryOp, LiteralValue};

        match (left, right) {
            (
                Expr::BinaryOp {
                    left: l_left,
                    op: l_op,
                    right: l_right,
                },
                Expr::BinaryOp {
                    left: r_left,
                    op: r_op,
                    right: r_right,
                },
            ) => {
                if let (Expr::Column { name: l_name, .. }, Expr::Column { name: r_name, .. }) =
                    (l_left.as_ref(), r_left.as_ref())
                    && l_name == r_name
                    && let (
                        Expr::Literal(LiteralValue::Int64(l_val)),
                        Expr::Literal(LiteralValue::Int64(r_val)),
                    ) = (l_right.as_ref(), r_right.as_ref())
                    && l_val == r_val
                {
                    return matches!(
                        (l_op, r_op),
                        (BinaryOp::LessThan, BinaryOp::GreaterThanOrEqual)
                            | (BinaryOp::GreaterThanOrEqual, BinaryOp::LessThan)
                            | (BinaryOp::LessThanOrEqual, BinaryOp::GreaterThan)
                            | (BinaryOp::GreaterThan, BinaryOp::LessThanOrEqual)
                            | (BinaryOp::Equal, BinaryOp::NotEqual)
                            | (BinaryOp::NotEqual, BinaryOp::Equal)
                    );
                }
                false
            }
            _ => false,
        }
    }
}

impl Default for UnionOptimization {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for UnionOptimization {
    fn name(&self) -> &str {
        "union_optimization"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        if !self.contains_unions(plan) {
            return Ok(None);
        }

        let transformed = self.transform_node(&plan.root)?;

        if transformed != *plan.root {
            Ok(Some(LogicalPlan::new(transformed)))
        } else {
            Ok(None)
        }
    }
}
