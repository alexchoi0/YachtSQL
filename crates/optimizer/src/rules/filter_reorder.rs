use yachtsql_core::error::Result;
use yachtsql_ir::expr::Expr;
use yachtsql_ir::plan::PlanNode;

use crate::optimizer::plan::LogicalPlan;
use crate::optimizer::rule::OptimizationRule;
use crate::statistics::StatisticsRegistry;

pub struct FilterReorder;

impl FilterReorder {
    pub fn new() -> Self {
        Self
    }

    pub fn with_statistics(_stats: StatisticsRegistry) -> Self {
        Self
    }

    fn reorder_filter(
        &self,
        _input: &PlanNode,
        _predicate: &Expr,
        _table: &str,
    ) -> Option<PlanNode> {
        None
    }

    fn extract_table_name(node: &PlanNode) -> Option<String> {
        match node {
            PlanNode::Scan { table_name, .. } => Some(table_name.clone()),
            PlanNode::Filter { input, .. }
            | PlanNode::Projection { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Sort { input, .. }
            | PlanNode::Limit { input, .. } => Self::extract_table_name(input),
            _ => None,
        }
    }
}

impl Default for FilterReorder {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for FilterReorder {
    fn name(&self) -> &str {
        "filter_reorder"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let optimized = self.optimize_node(&plan.root)?;

        if let Some(new_root) = optimized {
            return Ok(Some(LogicalPlan::new(new_root)));
        }

        Ok(None)
    }
}

impl FilterReorder {
    fn optimize_node(&self, node: &PlanNode) -> Result<Option<PlanNode>> {
        match node {
            PlanNode::Filter { input, predicate } => {
                let table =
                    Self::extract_table_name(input).unwrap_or_else(|| "unknown".to_string());

                if let Some(reordered) = self.reorder_filter(input, predicate, &table) {
                    return Ok(Some(reordered));
                }

                if let Some(new_input) = self.optimize_node(input)? {
                    return Ok(Some(PlanNode::Filter {
                        input: Box::new(new_input),
                        predicate: predicate.clone(),
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
    #[allow(unused_imports)]
    use yachtsql_ir::expr::{BinaryOp, LiteralValue};

    #[allow(unused_imports)]
    use super::*;

    #[allow(dead_code)]
    fn column_eq(col: &str, val: i64) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: col.to_string(),
                table: None,
            }),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Int64(val))),
        }
    }

    #[allow(dead_code)]
    fn expensive_function() -> Expr {
        Expr::Function {
            name: yachtsql_ir::FunctionName::Custom("expensive_udf".to_string()),
            args: vec![Expr::Column {
                name: "x".to_string(),
                table: None,
            }],
        }
    }
}
