use yachtsql_core::error::Result;
use yachtsql_ir::expr::{BinaryOp, Expr};
use yachtsql_ir::plan::{LogicalPlan, PlanNode};

use crate::rule::OptimizationRule;
use crate::visitor::PlanRewriter;

pub struct FilterMergeV2;

impl FilterMergeV2 {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FilterMergeV2 {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for FilterMergeV2 {
    fn name(&self) -> &str {
        "FilterMergeV2"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        struct Rewriter;

        impl PlanRewriter for Rewriter {
            fn rewrite_node(&mut self, node: &PlanNode) -> Result<Option<PlanNode>> {
                match node {
                    PlanNode::Filter { predicate, input } => {
                        if let PlanNode::Filter {
                            predicate: inner_predicate,
                            input: inner_input,
                        } = input.as_ref()
                        {
                            let combined_predicate = Expr::BinaryOp {
                                left: Box::new(predicate.clone()),
                                op: BinaryOp::And,
                                right: Box::new(inner_predicate.clone()),
                            };

                            Ok(Some(PlanNode::Filter {
                                predicate: combined_predicate,
                                input: inner_input.clone(),
                            }))
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Ok(None),
                }
            }
        }

        let mut rewriter = Rewriter;
        rewriter.rewrite(plan)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_ir::expr::LiteralValue;

    use super::*;

    #[test]
    fn test_merges_consecutive_filters() {
        let plan = LogicalPlan::new(PlanNode::Filter {
            predicate: Expr::Literal(LiteralValue::Boolean(true)),
            input: Box::new(PlanNode::Filter {
                predicate: Expr::Literal(LiteralValue::Boolean(false)),
                input: Box::new(PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: None,
                    only: false,
                }),
            }),
        });

        let rule = FilterMergeV2::new();
        let result = rule.optimize(&plan).unwrap().unwrap();

        match result.root() {
            PlanNode::Filter { predicate, input } => {
                assert!(matches!(
                    predicate,
                    Expr::BinaryOp {
                        op: BinaryOp::And,
                        ..
                    }
                ));
                assert!(matches!(input.as_ref(), PlanNode::Scan { .. }));
            }
            _ => panic!("Expected single merged filter"),
        }
    }

    #[test]
    fn test_doesnt_merge_non_consecutive() {
        let plan = LogicalPlan::new(PlanNode::Filter {
            predicate: Expr::Literal(LiteralValue::Boolean(true)),
            input: Box::new(PlanNode::Projection {
                expressions: vec![(Expr::Wildcard, None)],
                input: Box::new(PlanNode::Filter {
                    predicate: Expr::Literal(LiteralValue::Boolean(false)),
                    input: Box::new(PlanNode::Scan {
                        table_name: "users".to_string(),
                        alias: None,
                        projection: None,
                        only: false,
                    }),
                }),
            }),
        });

        let rule = FilterMergeV2::new();
        let result = rule.optimize(&plan).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_merges_three_filters() {
        let plan = LogicalPlan::new(PlanNode::Filter {
            predicate: Expr::Column {
                name: "a".to_string(),
                table: None,
            },
            input: Box::new(PlanNode::Filter {
                predicate: Expr::Column {
                    name: "b".to_string(),
                    table: None,
                },
                input: Box::new(PlanNode::Filter {
                    predicate: Expr::Column {
                        name: "c".to_string(),
                        table: None,
                    },
                    input: Box::new(PlanNode::Scan {
                        table_name: "users".to_string(),
                        alias: None,
                        projection: None,
                        only: false,
                    }),
                }),
            }),
        });

        let rule = FilterMergeV2::new();
        let mut current_plan = plan;

        current_plan = rule.optimize(&current_plan).unwrap().unwrap();

        if let Some(final_plan) = rule.optimize(&current_plan).unwrap() {
            current_plan = final_plan;
        }

        match current_plan.root() {
            PlanNode::Filter { input, .. } => {
                assert!(matches!(input.as_ref(), PlanNode::Scan { .. }));
            }
            _ => panic!("Expected merged filter"),
        }
    }
}
