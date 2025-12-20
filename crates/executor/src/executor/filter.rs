use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{BinaryOp, Expr, Literal, LogicalPlan};
use yachtsql_optimizer::optimize;
use yachtsql_storage::{Record, Schema, Table};

use super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_filter(&mut self, input: &ExecutorPlan, predicate: &Expr) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();

        if Self::expr_contains_subquery(predicate) {
            self.execute_filter_with_subquery(&input_table, predicate)
        } else {
            let evaluator = IrEvaluator::new(&schema);
            let mut result = Table::empty(schema.clone());

            for record in input_table.rows()? {
                let val = evaluator.evaluate(predicate, &record)?;
                if val.as_bool().unwrap_or(false) {
                    result.push_row(record.values().to_vec())?;
                }
            }

            Ok(result)
        }
    }

    fn execute_filter_with_subquery(&mut self, input: &Table, predicate: &Expr) -> Result<Table> {
        let schema = input.schema().clone();
        let mut result = Table::empty(schema.clone());

        for record in input.rows()? {
            let val = self.eval_expr_with_subquery(predicate, &schema, &record)?;
            if val.as_bool().unwrap_or(false) {
                result.push_row(record.values().to_vec())?;
            }
        }

        Ok(result)
    }

    fn eval_expr_with_subquery(
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
                let in_list = self.eval_in_subquery(subquery, outer_schema, outer_record)?;
                let is_in = in_list.contains(&value);
                Ok(Value::Bool(if *negated { !is_in } else { is_in }))
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_expr_with_subquery(left, outer_schema, outer_record)?;
                let right_val = self.eval_expr_with_subquery(right, outer_schema, outer_record)?;

                match op {
                    BinaryOp::And => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l && r))
                    }
                    BinaryOp::Or => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l || r))
                    }
                    BinaryOp::Eq => Ok(Value::Bool(Self::values_equal(&left_val, &right_val))),
                    BinaryOp::NotEq => Ok(Value::Bool(!Self::values_equal(&left_val, &right_val))),
                    BinaryOp::Lt => Ok(Value::Bool(
                        Self::compare_values(&left_val, &right_val) == std::cmp::Ordering::Less,
                    )),
                    BinaryOp::LtEq => Ok(Value::Bool(matches!(
                        Self::compare_values(&left_val, &right_val),
                        std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                    ))),
                    BinaryOp::Gt => Ok(Value::Bool(
                        Self::compare_values(&left_val, &right_val) == std::cmp::Ordering::Greater,
                    )),
                    BinaryOp::GtEq => Ok(Value::Bool(matches!(
                        Self::compare_values(&left_val, &right_val),
                        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                    ))),
                    _ => {
                        let evaluator = IrEvaluator::new(outer_schema);
                        evaluator.evaluate(expr, outer_record)
                    }
                }
            }
            Expr::UnaryOp {
                op: yachtsql_ir::UnaryOp::Not,
                expr: inner,
            } => {
                let val = self.eval_expr_with_subquery(inner, outer_schema, outer_record)?;
                Ok(Value::Bool(!val.as_bool().unwrap_or(false)))
            }
            Expr::Subquery(subquery) => self.evaluate_scalar_subquery(subquery),
            _ => {
                let evaluator = IrEvaluator::new(outer_schema);
                evaluator.evaluate(expr, outer_record)
            }
        }
    }

    fn eval_exists(
        &mut self,
        subquery: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<bool> {
        let substituted =
            self.substitute_outer_refs_in_plan(subquery, outer_schema, outer_record)?;
        let physical = optimize(&substituted)?;
        let executor_plan = ExecutorPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;
        Ok(!result_table.is_empty())
    }

    fn eval_in_subquery(
        &mut self,
        subquery: &LogicalPlan,
        _outer_schema: &Schema,
        _outer_record: &Record,
    ) -> Result<Vec<Value>> {
        let physical = optimize(subquery)?;
        let executor_plan = ExecutorPlan::from_physical(&physical);
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
        let executor_plan = ExecutorPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        for record in result_table.rows()? {
            let row_values = record.values();
            if !row_values.is_empty() {
                return Ok(row_values[0].clone());
            }
        }
        Ok(Value::Null)
    }

    fn substitute_outer_refs_in_plan(
        &self,
        plan: &LogicalPlan,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            } => Ok(LogicalPlan::Scan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: projection.clone(),
            }),
            LogicalPlan::Filter { input, predicate } => {
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_predicate =
                    self.substitute_outer_refs_in_expr(predicate, outer_schema, outer_record)?;
                Ok(LogicalPlan::Filter {
                    input: Box::new(new_input),
                    predicate: new_predicate,
                })
            }
            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let new_input =
                    self.substitute_outer_refs_in_plan(input, outer_schema, outer_record)?;
                let new_expressions = expressions
                    .iter()
                    .map(|e| self.substitute_outer_refs_in_expr(e, outer_schema, outer_record))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Project {
                    input: Box::new(new_input),
                    expressions: new_expressions,
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let new_left =
                    self.substitute_outer_refs_in_plan(left, outer_schema, outer_record)?;
                let new_right =
                    self.substitute_outer_refs_in_plan(right, outer_schema, outer_record)?;
                let new_condition = condition
                    .as_ref()
                    .map(|c| self.substitute_outer_refs_in_expr(c, outer_schema, outer_record))
                    .transpose()?;
                Ok(LogicalPlan::Join {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    join_type: join_type.clone(),
                    condition: new_condition,
                    schema: schema.clone(),
                })
            }
            other => Ok(other.clone()),
        }
    }

    fn substitute_outer_refs_in_expr(
        &self,
        expr: &Expr,
        outer_schema: &Schema,
        outer_record: &Record,
    ) -> Result<Expr> {
        match expr {
            Expr::Column { table, name, index } => {
                if let Some(idx) = outer_schema.field_index(name) {
                    let value = outer_record
                        .values()
                        .get(idx)
                        .cloned()
                        .unwrap_or(Value::Null);
                    Ok(Expr::Literal(Self::value_to_literal(value)))
                } else {
                    Ok(Expr::Column {
                        table: table.clone(),
                        name: name.clone(),
                        index: *index,
                    })
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let new_left =
                    self.substitute_outer_refs_in_expr(left, outer_schema, outer_record)?;
                let new_right =
                    self.substitute_outer_refs_in_expr(right, outer_schema, outer_record)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(new_left),
                    op: op.clone(),
                    right: Box::new(new_right),
                })
            }
            Expr::IsNull {
                expr: inner,
                negated,
            } => {
                let new_inner =
                    self.substitute_outer_refs_in_expr(inner, outer_schema, outer_record)?;
                Ok(Expr::IsNull {
                    expr: Box::new(new_inner),
                    negated: *negated,
                })
            }
            Expr::ScalarFunction { name, args } => {
                let new_args = args
                    .iter()
                    .map(|a| self.substitute_outer_refs_in_expr(a, outer_schema, outer_record))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::ScalarFunction {
                    name: name.clone(),
                    args: new_args,
                })
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let new_inner =
                    self.substitute_outer_refs_in_expr(inner, outer_schema, outer_record)?;
                Ok(Expr::Cast {
                    expr: Box::new(new_inner),
                    data_type: data_type.clone(),
                    safe: *safe,
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    fn expr_contains_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::Subquery(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_contains_subquery(left) || Self::expr_contains_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_contains_subquery(expr),
            _ => false,
        }
    }

    fn value_to_literal(value: Value) -> Literal {
        use chrono::{Datelike, NaiveDate, Timelike};
        const UNIX_EPOCH_DATE: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
            Some(d) => d,
            None => panic!("Invalid date"),
        };
        match value {
            Value::Null => Literal::Null,
            Value::Bool(b) => Literal::Bool(b),
            Value::Int64(n) => Literal::Int64(n),
            Value::Float64(f) => Literal::Float64(f),
            Value::Numeric(d) => Literal::Numeric(d),
            Value::String(s) => Literal::String(s),
            Value::Bytes(b) => Literal::Bytes(b),
            Value::Date(d) => {
                let days = d.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i32;
                Literal::Date(days)
            }
            Value::Time(t) => {
                let nanos =
                    t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64;
                Literal::Time(nanos)
            }
            Value::Timestamp(ts) => {
                let micros = ts.timestamp_micros();
                Literal::Timestamp(micros)
            }
            Value::DateTime(dt) => {
                let micros = dt.and_utc().timestamp_micros();
                Literal::Datetime(micros)
            }
            Value::Interval(iv) => Literal::Interval {
                months: iv.months,
                days: iv.days,
                nanos: iv.nanos,
            },
            Value::Array(arr) => {
                Literal::Array(arr.into_iter().map(Self::value_to_literal).collect())
            }
            Value::Struct(fields) => Literal::Struct(
                fields
                    .into_iter()
                    .map(|(k, v)| (k, Self::value_to_literal(v)))
                    .collect(),
            ),
            Value::Json(j) => Literal::Json(j),
            Value::Geography(_) => Literal::Null,
            Value::Range(_) => Literal::Null,
            Value::Default => Literal::Null,
        }
    }

    fn values_equal(left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Null, Value::Null) => true,
            (Value::Null, _) | (_, Value::Null) => false,
            (Value::Int64(a), Value::Float64(b)) => (*a as f64) == b.0,
            (Value::Float64(a), Value::Int64(b)) => a.0 == (*b as f64),
            _ => left == right,
        }
    }

    fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            (Value::Int64(a), Value::Float64(b)) => {
                let a_f64 = *a as f64;
                a_f64.partial_cmp(&b.0).unwrap_or(Ordering::Equal)
            }
            (Value::Float64(a), Value::Int64(b)) => {
                let b_f64 = *b as f64;
                a.0.partial_cmp(&b_f64).unwrap_or(Ordering::Equal)
            }
            _ => left.cmp(right),
        }
    }
}
