use sqlparser::ast;
use yachtsql_core::error::{Error, Result};
use yachtsql_ir::expr::{BinaryOp, Expr, LiteralValue};

use super::super::LogicalPlanBuilder;

impl LogicalPlanBuilder {
    pub(super) fn convert_in_list(
        &self,
        expr: &ast::Expr,
        list: &[ast::Expr],
        negated: bool,
    ) -> Result<Expr> {
        let inner_expr = self.sql_expr_to_expr(expr)?;
        let list_exprs = list
            .iter()
            .map(|e| self.sql_expr_to_expr(e))
            .collect::<Result<Vec<_>>>()?;

        if let Expr::Tuple(tuple_exprs) = &inner_expr {
            let tuple_arity = tuple_exprs.len();
            if tuple_arity < 2 {
                return Err(Error::invalid_query(format!(
                    "Tuple IN predicate requires at least 2 columns, got {}",
                    tuple_arity
                )));
            }

            let mut tuple_list = Vec::new();
            for list_item in &list_exprs {
                if let Expr::Tuple(item_tuple) = list_item {
                    if item_tuple.len() != tuple_arity {
                        return Err(Error::invalid_query(format!(
                            "Tuple IN arity mismatch: expected {}, got {}",
                            tuple_arity,
                            item_tuple.len()
                        )));
                    }
                    tuple_list.push(item_tuple.clone());
                } else {
                    return Err(Error::invalid_query(
                        "Tuple IN predicate requires all list items to be tuples".to_string(),
                    ));
                }
            }

            Ok(Expr::TupleInList {
                tuple: tuple_exprs.clone(),
                list: tuple_list,
                negated,
            })
        } else {
            Ok(Expr::InList {
                expr: Box::new(inner_expr),
                list: list_exprs,
                negated,
            })
        }
    }

    pub(super) fn convert_between(
        &self,
        expr: &ast::Expr,
        low: &ast::Expr,
        high: &ast::Expr,
        negated: bool,
    ) -> Result<Expr> {
        let inner_expr = self.sql_expr_to_expr(expr)?;
        let low_expr = self.sql_expr_to_expr(low)?;
        let high_expr = self.sql_expr_to_expr(high)?;
        Ok(Expr::Between {
            expr: Box::new(inner_expr),
            low: Box::new(low_expr),
            high: Box::new(high_expr),
            negated,
        })
    }

    pub(super) fn convert_like_expr(
        &self,
        expr: &ast::Expr,
        pattern: &ast::Expr,
        negated: bool,
        base_op: BinaryOp,
        negated_op: BinaryOp,
    ) -> Result<Expr> {
        let left_expr = self.sql_expr_to_expr(expr)?;
        let right_expr = self.sql_expr_to_expr(pattern)?;
        let op = if negated { negated_op } else { base_op };
        Ok(Expr::binary_op(left_expr, op, right_expr))
    }

    pub(super) fn convert_like_any_all_expr(
        &self,
        expr: &ast::Expr,
        pattern: &ast::Expr,
        negated: bool,
        base_op: BinaryOp,
        is_any: bool,
    ) -> Result<Expr> {
        let left_expr = self.sql_expr_to_expr(expr)?;

        let patterns_array = match pattern {
            ast::Expr::Tuple(elements) => {
                let array_elements = elements
                    .iter()
                    .map(|e| self.sql_expr_to_expr(e))
                    .collect::<Result<Vec<_>>>()?;
                Expr::Literal(LiteralValue::Array(array_elements))
            }
            ast::Expr::Subquery(query) => {
                let subquery_plan = self.query_to_plan(query)?;
                Expr::Subquery {
                    plan: Box::new(subquery_plan.root().clone()),
                }
            }
            _ => self.sql_expr_to_expr(pattern)?,
        };

        let op_expr = if is_any {
            Expr::AnyOp {
                left: Box::new(left_expr),
                compare_op: base_op,
                right: Box::new(patterns_array),
            }
        } else {
            Expr::AllOp {
                left: Box::new(left_expr),
                compare_op: base_op,
                right: Box::new(patterns_array),
            }
        };

        if negated {
            Ok(Expr::unary_op(yachtsql_ir::expr::UnaryOp::Not, op_expr))
        } else {
            Ok(op_expr)
        }
    }
}
