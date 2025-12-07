use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::{BinaryOp, Expr};
use yachtsql_optimizer::plan::PlanNode;

use super::super::{ProjectionWithExprExec, SUBQUERY_EXECUTOR_CONTEXT};
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_scalar_subquery_expr(plan: &PlanNode) -> Result<Value> {
        let executor = SUBQUERY_EXECUTOR_CONTEXT
            .with(|ctx| ctx.borrow().clone())
            .ok_or_else(|| {
                Error::InternalError(
                    "Subquery executor context not available - subqueries must be executed through QueryExecutor".to_string()
                )
            })?;

        executor.execute_scalar_subquery(plan)
    }

    pub(super) fn evaluate_exists_subquery_expr(plan: &PlanNode, negated: bool) -> Result<Value> {
        let executor = SUBQUERY_EXECUTOR_CONTEXT
            .with(|ctx| ctx.borrow().clone())
            .ok_or_else(|| {
                Error::InternalError(
                    "Subquery executor context not available - subqueries must be executed through QueryExecutor".to_string()
                )
            })?;

        let exists = executor.execute_exists_subquery(plan)?;

        Ok(Value::bool_val(if negated { !exists } else { exists }))
    }

    pub(super) fn evaluate_in_subquery_expr(
        expr: &Expr,
        plan: &PlanNode,
        negated: bool,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let executor = SUBQUERY_EXECUTOR_CONTEXT
            .with(|ctx| ctx.borrow().clone())
            .ok_or_else(|| {
                Error::InternalError(
                    "Subquery executor context not available - subqueries must be executed through QueryExecutor".to_string()
                )
            })?;

        let value = Self::evaluate_expr(expr, batch, row_idx)?;

        let list_values = executor.execute_in_subquery(plan)?;

        Self::evaluate_in_list_expression(&value, &list_values, negated)
    }

    pub(super) fn evaluate_tuple_in_subquery_expr(
        tuple: &[Expr],
        plan: &PlanNode,
        negated: bool,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let executor = SUBQUERY_EXECUTOR_CONTEXT
            .with(|ctx| ctx.borrow().clone())
            .ok_or_else(|| {
                Error::InternalError(
                    "Subquery executor context not available - subqueries must be executed through QueryExecutor".to_string()
                )
            })?;

        let left_values: Result<Vec<Value>> = tuple
            .iter()
            .map(|e| Self::evaluate_expr(e, batch, row_idx))
            .collect();
        let left_values = left_values?;

        let values = executor.execute_in_subquery(plan)?;
        let right_tuples: Vec<Vec<Value>> = values.into_iter().map(|v| vec![v]).collect();

        Self::evaluate_tuple_in_expression(&left_values, &right_tuples, negated)
    }

    pub(super) fn evaluate_any_op_expr(
        left: &Expr,
        compare_op: &BinaryOp,
        right: &Expr,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let left_value = Self::evaluate_expr(left, batch, row_idx)?;

        let right_values = Self::get_quantified_op_values(right, batch, row_idx)?;

        Self::evaluate_quantified_comparison_array(&left_value, compare_op, &right_values, true)
    }

    pub(super) fn evaluate_all_op_expr(
        left: &Expr,
        compare_op: &BinaryOp,
        right: &Expr,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let left_value = Self::evaluate_expr(left, batch, row_idx)?;

        let right_values = Self::get_quantified_op_values(right, batch, row_idx)?;

        Self::evaluate_quantified_comparison_array(&left_value, compare_op, &right_values, false)
    }

    fn get_quantified_op_values(right: &Expr, batch: &Table, row_idx: usize) -> Result<Vec<Value>> {
        match right {
            Expr::Subquery { plan } => {
                let executor = SUBQUERY_EXECUTOR_CONTEXT
                    .with(|ctx| ctx.borrow().clone())
                    .ok_or_else(|| {
                        Error::InternalError(
                            "Subquery executor context not available - subqueries must be executed through QueryExecutor".to_string()
                        )
                    })?;

                executor.execute_in_subquery(plan)
            }

            _ => Self::extract_array_for_quantified_op(right, batch, row_idx),
        }
    }
}
