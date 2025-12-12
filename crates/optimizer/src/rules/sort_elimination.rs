use yachtsql_common::error::Result;
use yachtsql_ir::expr::OrderByExpr;
use yachtsql_ir::plan::PlanNode;

use crate::optimizer::plan::LogicalPlan;
use crate::optimizer::rule::OptimizationRule;

pub struct SortElimination;

impl SortElimination {
    pub fn new() -> Self {
        Self
    }

    fn can_eliminate_sort(&self, outer: &[OrderByExpr], inner: &[OrderByExpr]) -> bool {
        if outer.is_empty() || inner.is_empty() {
            return false;
        }

        if outer.len() > inner.len() {
            return false;
        }

        for (outer_expr, inner_expr) in outer.iter().zip(inner.iter()) {
            if format!("{:?}", outer_expr.expr) != format!("{:?}", inner_expr.expr) {
                return false;
            }

            if outer_expr.asc != inner_expr.asc {
                return false;
            }

            if outer_expr.nulls_first != inner_expr.nulls_first {
                return false;
            }
        }

        true
    }

    fn extract_sort_order(&self, node: &PlanNode) -> Option<Vec<OrderByExpr>> {
        match node {
            PlanNode::Sort { order_by, .. } => Some(order_by.clone()),
            _ => None,
        }
    }

    fn optimize_sort(&self, order_by: &[OrderByExpr], input: &PlanNode) -> Option<PlanNode> {
        if let Some(inner_order) = self.extract_sort_order(input)
            && self.can_eliminate_sort(order_by, &inner_order)
        {
            return Some(input.clone());
        }

        match input {
            PlanNode::Projection { input: inner, .. } => {
                if let Some(inner_order) = self.extract_sort_order(inner)
                    && self.can_eliminate_sort(order_by, &inner_order)
                {
                    return Some(input.clone());
                }
            }
            PlanNode::Filter { input: inner, .. } => {
                if let Some(inner_order) = self.extract_sort_order(inner)
                    && self.can_eliminate_sort(order_by, &inner_order)
                {
                    return Some(input.clone());
                }
            }
            _ => {}
        }

        None
    }
}

impl Default for SortElimination {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for SortElimination {
    fn name(&self) -> &str {
        "sort_elimination"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let optimized = self.optimize_node(&plan.root)?;

        if let Some(new_root) = optimized {
            return Ok(Some(LogicalPlan::new(new_root)));
        }

        Ok(None)
    }
}

impl SortElimination {
    fn optimize_node(&self, node: &PlanNode) -> Result<Option<PlanNode>> {
        match node {
            PlanNode::Sort { order_by, input } => {
                if let Some(optimized) = self.optimize_sort(order_by, input) {
                    return Ok(Some(optimized));
                }

                if let Some(new_input) = self.optimize_node(input)? {
                    return Ok(Some(PlanNode::Sort {
                        order_by: order_by.clone(),
                        input: Box::new(new_input),
                    }));
                }

                Ok(None)
            }
            PlanNode::Projection { expressions, input } => {
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
                using_columns,
            } => {
                let new_left = self.optimize_node(left)?;
                let new_right = self.optimize_node(right)?;

                if new_left.is_some() || new_right.is_some() {
                    return Ok(Some(PlanNode::Join {
                        left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                        join_type: *join_type,
                        on: on.clone(),
                        using_columns: using_columns.clone(),
                    }));
                }
                Ok(None)
            }
            PlanNode::LateralJoin {
                left,
                right,
                on,
                join_type,
            } => {
                let left_opt = self.optimize_node(left)?;
                let right_opt = self.optimize_node(right)?;

                if left_opt.is_some() || right_opt.is_some() {
                    Ok(Some(PlanNode::LateralJoin {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        on: on.clone(),
                        join_type: *join_type,
                    }))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}
