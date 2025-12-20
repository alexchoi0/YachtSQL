use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{BinaryOp, Expr, LogicalPlan, PlanSchema};
use yachtsql_optimizer::optimize;
use yachtsql_storage::{Record, Schema, Table};

use super::{PlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_project(
        &mut self,
        input: &ExecutorPlan,
        expressions: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let input_schema = input_table.schema().clone();

        if expressions
            .iter()
            .any(Self::expr_contains_subquery_or_scalar_subquery)
        {
            self.execute_project_with_subqueries(&input_table, expressions, schema)
        } else {
            let evaluator =
                IrEvaluator::new(&input_schema).with_user_functions(&self.user_function_defs);
            let result_schema = plan_schema_to_schema(schema);
            let mut result = Table::empty(result_schema);

            for record in input_table.rows()? {
                let mut row = Vec::with_capacity(expressions.len());
                for expr in expressions {
                    let val = evaluator.evaluate(expr, &record)?;
                    row.push(val);
                }
                result.push_row(row)?;
            }

            Ok(result)
        }
    }

    fn execute_project_with_subqueries(
        &mut self,
        input_table: &Table,
        expressions: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_schema = input_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        for record in input_table.rows()? {
            let mut row = Vec::with_capacity(expressions.len());
            for expr in expressions {
                let val = self.eval_expr_with_subqueries(expr, &input_schema, &record)?;
                row.push(val);
            }
            result.push_row(row)?;
        }

        Ok(result)
    }

    fn eval_expr_with_subqueries(
        &mut self,
        expr: &Expr,
        schema: &Schema,
        record: &Record,
    ) -> Result<Value> {
        match expr {
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) => self.eval_scalar_subquery(plan),
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_expr_with_subqueries(left, schema, record)?;
                let right_val = self.eval_expr_with_subqueries(right, schema, record)?;
                self.eval_binary_op_values(left_val, *op, right_val)
            }
            Expr::UnaryOp { op, expr: inner } => {
                let val = self.eval_expr_with_subqueries(inner, schema, record)?;
                self.eval_unary_op_value(*op, val)
            }
            Expr::ScalarFunction { name, args } => {
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| self.eval_expr_with_subqueries(a, schema, record))
                    .collect::<Result<_>>()?;
                let evaluator =
                    IrEvaluator::new(schema).with_user_functions(&self.user_function_defs);
                evaluator.eval_scalar_function_with_values(name, &arg_vals)
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let val = self.eval_expr_with_subqueries(inner, schema, record)?;
                IrEvaluator::cast_value(val, data_type, *safe)
            }
            _ => {
                let evaluator =
                    IrEvaluator::new(schema).with_user_functions(&self.user_function_defs);
                evaluator.evaluate(expr, record)
            }
        }
    }

    fn eval_scalar_subquery(&mut self, plan: &LogicalPlan) -> Result<Value> {
        let physical = optimize(plan)?;
        let executor_plan = ExecutorPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        if result_table.is_empty() {
            return Ok(Value::Null);
        }

        let rows: Vec<_> = result_table.rows()?.into_iter().collect();
        if rows.is_empty() {
            return Ok(Value::Null);
        }

        let first_row = &rows[0];
        let values = first_row.values();
        if values.is_empty() {
            return Ok(Value::Null);
        }

        Ok(values[0].clone())
    }

    fn eval_binary_op_values(&self, left: Value, op: BinaryOp, right: Value) -> Result<Value> {
        use yachtsql_ir::BinaryOp::*;
        match op {
            Add => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l + r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l + *r)),
                (Value::Int64(l), Value::Float64(r)) => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) + *r))
                }
                (Value::Float64(l), Value::Int64(r)) => {
                    Ok(Value::Float64(*l + ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            Sub => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l - r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l - *r)),
                (Value::Int64(l), Value::Float64(r)) => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) - *r))
                }
                (Value::Float64(l), Value::Int64(r)) => {
                    Ok(Value::Float64(*l - ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            Mul => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l * r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l * *r)),
                (Value::Int64(l), Value::Float64(r)) => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) * *r))
                }
                (Value::Float64(l), Value::Int64(r)) => {
                    Ok(Value::Float64(*l * ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            Div => match (&left, &right) {
                (Value::Int64(l), Value::Int64(r)) if *r != 0 => Ok(Value::Float64(
                    ordered_float::OrderedFloat(*l as f64 / *r as f64),
                )),
                (Value::Float64(l), Value::Float64(r)) if r.0 != 0.0 => Ok(Value::Float64(*l / *r)),
                (Value::Int64(l), Value::Float64(r)) if r.0 != 0.0 => {
                    Ok(Value::Float64(ordered_float::OrderedFloat(*l as f64) / *r))
                }
                (Value::Float64(l), Value::Int64(r)) if *r != 0 => {
                    Ok(Value::Float64(*l / ordered_float::OrderedFloat(*r as f64)))
                }
                _ => Ok(Value::Null),
            },
            And => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::Bool(l && r))
            }
            Or => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::Bool(l || r))
            }
            Eq => Ok(Value::Bool(left == right)),
            NotEq => Ok(Value::Bool(left != right)),
            Lt => Ok(Value::Bool(left < right)),
            LtEq => Ok(Value::Bool(left <= right)),
            Gt => Ok(Value::Bool(left > right)),
            GtEq => Ok(Value::Bool(left >= right)),
            _ => Ok(Value::Null),
        }
    }

    fn eval_unary_op_value(&self, op: yachtsql_ir::UnaryOp, val: Value) -> Result<Value> {
        use yachtsql_ir::UnaryOp::*;
        match op {
            Not => Ok(Value::Bool(!val.as_bool().unwrap_or(false))),
            Minus => match val {
                Value::Int64(n) => Ok(Value::Int64(-n)),
                Value::Float64(f) => Ok(Value::Float64(-f)),
                _ => Ok(Value::Null),
            },
            Plus => Ok(val),
            BitwiseNot => match val {
                Value::Int64(n) => Ok(Value::Int64(!n)),
                _ => Ok(Value::Null),
            },
        }
    }

    fn expr_contains_subquery_or_scalar_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Subquery(_) | Expr::ScalarSubquery(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery_or_scalar_subquery(left)
                    || Self::expr_contains_subquery_or_scalar_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery_or_scalar_subquery(expr),
            Expr::ScalarFunction { args, .. } => args
                .iter()
                .any(Self::expr_contains_subquery_or_scalar_subquery),
            Expr::Cast { expr, .. } => Self::expr_contains_subquery_or_scalar_subquery(expr),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|o| Self::expr_contains_subquery_or_scalar_subquery(o))
                    || when_clauses.iter().any(|w| {
                        Self::expr_contains_subquery_or_scalar_subquery(&w.condition)
                            || Self::expr_contains_subquery_or_scalar_subquery(&w.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::expr_contains_subquery_or_scalar_subquery(e))
            }
            Expr::Alias { expr, .. } => Self::expr_contains_subquery_or_scalar_subquery(expr),
            _ => false,
        }
    }
}
