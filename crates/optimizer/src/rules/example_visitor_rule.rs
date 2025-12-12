use yachtsql_common::error::Result;
use yachtsql_ir::expr::{Expr, LiteralValue};
use yachtsql_ir::plan::{LogicalPlan, PlanNode};

use crate::rule::OptimizationRule;
use crate::visitor::PlanRewriter;

pub struct RemoveTrueFilters;

impl RemoveTrueFilters {
    pub fn new() -> Self {
        Self
    }

    fn is_true_literal(expr: &Expr) -> bool {
        matches!(expr, Expr::Literal(LiteralValue::Boolean(true)))
    }
}

impl Default for RemoveTrueFilters {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for RemoveTrueFilters {
    fn name(&self) -> &str {
        "RemoveTrueFilters"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        struct Rewriter;

        impl PlanRewriter for Rewriter {
            fn rewrite_node(&mut self, node: &PlanNode) -> Result<Option<PlanNode>> {
                match node {
                    PlanNode::Filter { predicate, input }
                        if RemoveTrueFilters::is_true_literal(predicate) =>
                    {
                        Ok(Some(input.as_ref().clone()))
                    }
                    _ => Ok(None),
                }
            }
        }

        let mut rewriter = Rewriter;
        rewriter.rewrite(plan)
    }
}
