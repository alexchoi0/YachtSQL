use std::collections::HashSet;

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{BinaryOp, Expr, LogicalPlan};
use yachtsql_optimizer::optimize;
use yachtsql_storage::{Record, Schema, Table};

use super::super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub(crate) fn eval_expr_with_subquery(
        &mut self,
        expr: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        match expr {
            Expr::Exists { subquery, negated } => {
                let has_rows = self.eval_exists(subquery, outer_schema, outer_record)?;
                Ok(Value::Bool(if *negated { !has_rows } else { has_rows }))
            }
            Expr::InSubquery {
                expr: value_expr,
                subquery,
                negated,
            } => {
                let value = self.eval_expr_with_subquery(value_expr, outer_schema, outer_record)?;
                let in_list = self.eval_in_subquery(subquery)?;
                let is_in = in_list.contains(&value);
                Ok(Value::Bool(if *negated { !is_in } else { is_in }))
            }
            Expr::BinaryOp { left, op, right } => {
                self.eval_binary_op_with_subquery(left, *op, right, outer_schema, outer_record)
            }
            Expr::UnaryOp {
                op: yachtsql_ir::UnaryOp::Not,
                expr: inner,
            } => {
                let val = self.eval_expr_with_subquery(inner, outer_schema, outer_record)?;
                Ok(Value::Bool(!val.as_bool().unwrap_or(false)))
            }
            Expr::Subquery(subquery) | Expr::ScalarSubquery(subquery) => {
                if Self::plan_contains_outer_refs(subquery, outer_schema) {
                    self.evaluate_scalar_subquery_with_outer(subquery, outer_schema, outer_record)
                } else {
                    self.evaluate_scalar_subquery(subquery)
                }
            }
            Expr::ArraySubquery(subquery) => {
                self.evaluate_array_subquery(subquery, outer_schema, outer_record)
            }
            Expr::ScalarFunction { name, args } => {
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| self.eval_expr_with_subquery(a, outer_schema, outer_record))
                    .collect::<Result<_>>()?;
                let evaluator = IrEvaluator::new(outer_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables())
                    .with_user_functions(&self.user_function_defs);
                evaluator.eval_scalar_function_with_values(name, &arg_vals)
            }
            _ => {
                let evaluator = IrEvaluator::new(outer_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables())
                    .with_user_functions(&self.user_function_defs);
                evaluator.evaluate(expr, outer_record)
            }
        }
    }

    fn eval_binary_op_with_subquery(
        &mut self,
        left: &Expr,
        op: BinaryOp,
        right: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        match op {
            BinaryOp::And => {
                let left_val = self.eval_expr_with_subquery(left, outer_schema, outer_record)?;
                if !left_val.as_bool().unwrap_or(false) {
                    return Ok(Value::Bool(false));
                }
                let right_val = self.eval_expr_with_subquery(right, outer_schema, outer_record)?;
                Ok(Value::Bool(right_val.as_bool().unwrap_or(false)))
            }
            BinaryOp::Or => {
                let left_val = self.eval_expr_with_subquery(left, outer_schema, outer_record)?;
                if left_val.as_bool().unwrap_or(false) {
                    return Ok(Value::Bool(true));
                }
                let right_val = self.eval_expr_with_subquery(right, outer_schema, outer_record)?;
                Ok(Value::Bool(right_val.as_bool().unwrap_or(false)))
            }
            _ => {
                let left_val = self.eval_expr_with_subquery(left, outer_schema, outer_record)?;
                let right_val = self.eval_expr_with_subquery(right, outer_schema, outer_record)?;
                self.eval_subquery_binary_op(&left_val, op, &right_val, outer_schema, outer_record)
            }
        }
    }

    fn eval_subquery_binary_op(
        &self,
        left_val: &Value,
        op: BinaryOp,
        right_val: &Value,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        match op {
            BinaryOp::Eq => Ok(Value::Bool(Self::values_equal(left_val, right_val))),
            BinaryOp::NotEq => Ok(Value::Bool(!Self::values_equal(left_val, right_val))),
            BinaryOp::Lt => Ok(Value::Bool(
                Self::compare_values(left_val, right_val) == std::cmp::Ordering::Less,
            )),
            BinaryOp::LtEq => Ok(Value::Bool(matches!(
                Self::compare_values(left_val, right_val),
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal
            ))),
            BinaryOp::Gt => Ok(Value::Bool(
                Self::compare_values(left_val, right_val) == std::cmp::Ordering::Greater,
            )),
            BinaryOp::GtEq => Ok(Value::Bool(matches!(
                Self::compare_values(left_val, right_val),
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
            ))),
            BinaryOp::Add => Self::arithmetic_op(left_val, right_val, |a, b| a + b),
            BinaryOp::Sub => Self::arithmetic_op(left_val, right_val, |a, b| a - b),
            BinaryOp::Mul => Self::arithmetic_op(left_val, right_val, |a, b| a * b),
            BinaryOp::Div => Self::arithmetic_op(left_val, right_val, |a, b| a / b),
            _ => {
                let new_left = Self::value_to_literal(left_val.clone());
                let new_right = Self::value_to_literal(right_val.clone());
                let simplified_expr = Expr::BinaryOp {
                    left: Box::new(Expr::Literal(new_left)),
                    op,
                    right: Box::new(Expr::Literal(new_right)),
                };
                let evaluator = IrEvaluator::new(outer_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables())
                    .with_user_functions(&self.user_function_defs);
                evaluator.evaluate(&simplified_expr, outer_record)
            }
        }
    }

    pub(super) fn eval_exists(
        &mut self,
        subquery: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<bool> {
        let mut inner_tables = HashSet::new();
        Self::collect_plan_tables(subquery, &mut inner_tables);
        let substituted = self.substitute_outer_refs_in_plan_with_inner_tables(
            subquery,
            outer_schema,
            outer_record,
            &inner_tables,
        )?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;
        Ok(!result_table.is_empty())
    }

    fn eval_in_subquery(&mut self, subquery: &LogicalPlan) -> Result<Vec<Value>> {
        let physical = optimize(subquery)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        let mut values = Vec::new();
        for record in result_table.rows()? {
            let row_values = record.values();
            if !row_values.is_empty() {
                values.push(row_values[0].clone());
            }
        }
        Ok(values)
    }

    fn evaluate_scalar_subquery(&mut self, subquery: &LogicalPlan) -> Result<Value> {
        let physical = optimize(subquery)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        for record in result_table.rows()? {
            let row_values = record.values();
            if !row_values.is_empty() {
                return Ok(row_values[0].clone());
            }
        }
        Ok(Value::Null)
    }

    fn evaluate_scalar_subquery_with_outer(
        &mut self,
        subquery: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let mut inner_tables = HashSet::new();
        Self::collect_plan_tables(subquery, &mut inner_tables);
        let substituted = self.substitute_outer_refs_in_plan_with_inner_tables(
            subquery,
            outer_schema,
            outer_record,
            &inner_tables,
        )?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        for record in result_table.rows()? {
            let row_values = record.values();
            if !row_values.is_empty() {
                return Ok(row_values[0].clone());
            }
        }
        Ok(Value::Null)
    }

    fn evaluate_array_subquery(
        &mut self,
        subquery: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Value> {
        let mut inner_tables = HashSet::new();
        Self::collect_plan_tables(subquery, &mut inner_tables);
        let substituted = self.substitute_outer_refs_in_plan_with_inner_tables(
            subquery,
            outer_schema,
            outer_record,
            &inner_tables,
        )?;
        let physical = optimize(&substituted)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        let result_schema = result_table.schema();
        let num_fields = result_schema.field_count();

        let mut array_values = Vec::new();
        for record in result_table.rows()? {
            let values = record.values();
            if num_fields == 1 {
                array_values.push(values[0].clone());
            } else {
                let fields: Vec<(String, Value)> = result_schema
                    .fields()
                    .iter()
                    .zip(values.iter())
                    .map(|(f, v)| (f.name.clone(), v.clone()))
                    .collect();
                array_values.push(Value::Struct(fields));
            }
        }

        Ok(Value::Array(array_values))
    }
}
