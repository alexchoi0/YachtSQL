use std::collections::HashMap;

use yachtsql_common::error::Result;
use yachtsql_ir::expr::Expr;
use yachtsql_ir::plan::PlanNode;

use crate::optimizer::plan::LogicalPlan;
use crate::optimizer::rule::OptimizationRule;

pub struct WindowOptimization;

impl WindowOptimization {
    pub fn new() -> Self {
        Self
    }

    fn contains_windows(&self, plan: &LogicalPlan) -> bool {
        Self::has_windows_node(&plan.root)
    }

    fn has_windows_node(node: &PlanNode) -> bool {
        match node {
            PlanNode::Window { .. } => true,
            _ => node.children().iter().any(|c| Self::has_windows_node(c)),
        }
    }

    fn transform_node(&self, node: &PlanNode) -> Result<PlanNode> {
        match node {
            PlanNode::Window {
                window_exprs,
                input,
            } => {
                let transformed_input = self.transform_node(input)?;

                let groups = self.group_compatible_windows(window_exprs);

                if groups.len() == 1 || groups.len() == window_exprs.len() {
                    return Ok(PlanNode::Window {
                        window_exprs: window_exprs.clone(),
                        input: Box::new(transformed_input),
                    });
                }

                let mut reordered_exprs = Vec::with_capacity(window_exprs.len());
                for group in groups.values() {
                    reordered_exprs.extend(group.clone());
                }

                Ok(PlanNode::Window {
                    window_exprs: reordered_exprs,
                    input: Box::new(transformed_input),
                })
            }

            PlanNode::Filter { predicate, input } => match input.as_ref() {
                PlanNode::Window {
                    window_exprs,
                    input: window_input,
                } => {
                    if !self.predicate_references_windows(predicate, window_exprs) {
                        let new_filter = PlanNode::Filter {
                            predicate: predicate.clone(),
                            input: window_input.clone(),
                        };
                        return Ok(PlanNode::Window {
                            window_exprs: window_exprs.clone(),
                            input: Box::new(new_filter),
                        });
                    }

                    let transformed_input = self.transform_node(input)?;
                    Ok(PlanNode::Filter {
                        predicate: predicate.clone(),
                        input: Box::new(transformed_input),
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

            _ => Ok(node.clone()),
        }
    }

    fn group_compatible_windows(
        &self,
        window_exprs: &[(Expr, Option<String>)],
    ) -> HashMap<String, Vec<(Expr, Option<String>)>> {
        let mut groups: HashMap<String, Vec<(Expr, Option<String>)>> = HashMap::new();

        for (expr, alias) in window_exprs {
            let signature = self.window_signature(expr);
            groups
                .entry(signature)
                .or_default()
                .push((expr.clone(), alias.clone()));
        }

        groups
    }

    fn window_signature(&self, expr: &Expr) -> String {
        match expr {
            Expr::WindowFunction {
                partition_by,
                order_by,
                ..
            } => {
                format!("P:{:?}|O:{:?}", partition_by, order_by)
            }
            _ => "non-window".to_string(),
        }
    }

    fn predicate_references_windows(
        &self,
        predicate: &Expr,
        window_exprs: &[(Expr, Option<String>)],
    ) -> bool {
        let window_cols: Vec<&str> = window_exprs
            .iter()
            .filter_map(|(_, alias)| alias.as_deref())
            .collect();

        Self::expr_references_columns(predicate, &window_cols)
    }

    fn expr_references_columns(expr: &Expr, columns: &[&str]) -> bool {
        match expr {
            Expr::Column { name, .. } => columns.contains(&name.as_str()),
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_references_columns(left, columns)
                    || Self::expr_references_columns(right, columns)
            }
            Expr::UnaryOp { expr: inner, .. } => Self::expr_references_columns(inner, columns),
            _ => false,
        }
    }
}

impl Default for WindowOptimization {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for WindowOptimization {
    fn name(&self) -> &str {
        "window_optimization"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        if !self.contains_windows(plan) {
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
