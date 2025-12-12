use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::expr::Expr;

use super::super::LogicalPlanBuilder;

impl LogicalPlanBuilder {
    pub(super) fn convert_scalar_subquery(&self, query: &ast::Query) -> Result<Expr> {
        let subquery_plan = self.query_to_plan(query)?;
        Ok(Expr::ScalarSubquery {
            subquery: Box::new(subquery_plan.root().clone()),
        })
    }

    pub(super) fn convert_exists(&self, subquery: &ast::Query, negated: bool) -> Result<Expr> {
        let subquery_plan = self.query_to_plan(subquery)?;
        Ok(Expr::Exists {
            plan: Box::new(subquery_plan.root().clone()),
            negated,
        })
    }

    pub(super) fn convert_in_subquery(
        &self,
        expr: &ast::Expr,
        subquery: &ast::Query,
        negated: bool,
    ) -> Result<Expr> {
        let lhs_expr = self.sql_expr_to_expr(expr)?;
        let subquery_plan = self.query_to_plan(subquery)?;

        if let Expr::Tuple(tuple_exprs) = &lhs_expr {
            let tuple_arity = tuple_exprs.len();
            if tuple_arity < 2 {
                return Err(Error::invalid_query(format!(
                    "Tuple IN subquery requires at least 2 columns, got {}",
                    tuple_arity
                )));
            }

            Ok(Expr::TupleInSubquery {
                tuple: tuple_exprs.clone(),
                plan: Box::new(subquery_plan.root().clone()),
                negated,
            })
        } else {
            Ok(Expr::InSubquery {
                expr: Box::new(lhs_expr),
                plan: Box::new(subquery_plan.root().clone()),
                negated,
            })
        }
    }
}
