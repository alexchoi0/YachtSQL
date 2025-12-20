use std::collections::HashMap;

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, SortExpr, WindowFrame};
use yachtsql_storage::{Record, Schema, Table};

use super::PlanExecutor;
use super::window::WindowFuncType;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_qualify(&mut self, input: &ExecutorPlan, predicate: &Expr) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();

        if Self::expr_has_window_function(predicate) {
            self.execute_qualify_with_window(&input_table, predicate)
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

    fn execute_qualify_with_window(&mut self, input: &Table, predicate: &Expr) -> Result<Table> {
        let schema = input.schema().clone();
        let rows: Vec<Record> = input.rows()?;
        let evaluator = IrEvaluator::new(&schema);

        let window_exprs = Self::collect_window_exprs(predicate);
        let mut window_results: HashMap<String, Vec<Value>> = HashMap::new();

        for window_expr in &window_exprs {
            let key = format!("{:?}", window_expr);
            if window_results.contains_key(&key) {
                continue;
            }

            let (partition_by, order_by, frame, func_type) =
                Self::extract_qualify_window_spec(window_expr)?;

            let partitions = Self::partition_rows(&rows, &partition_by, &evaluator)?;
            let mut results = vec![Value::Null; rows.len()];

            for (_key, mut indices) in partitions {
                Self::sort_partition(&rows, &mut indices, &order_by, &evaluator)?;

                let partition_results = Self::compute_window_function(
                    &rows,
                    &indices,
                    window_expr,
                    &func_type,
                    &order_by,
                    &frame,
                    &evaluator,
                )?;

                for (local_idx, row_idx) in indices.iter().enumerate() {
                    results[*row_idx] = partition_results[local_idx].clone();
                }
            }

            window_results.insert(key, results);
        }

        let mut result = Table::empty(schema.clone());

        for (row_idx, record) in rows.iter().enumerate() {
            let val = Self::evaluate_qualify_predicate(
                predicate,
                &schema,
                record,
                row_idx,
                &window_results,
            )?;
            if val.as_bool().unwrap_or(false) {
                result.push_row(record.values().to_vec())?;
            }
        }

        Ok(result)
    }

    fn evaluate_qualify_predicate(
        expr: &Expr,
        schema: &Schema,
        record: &Record,
        row_idx: usize,
        window_results: &HashMap<String, Vec<Value>>,
    ) -> Result<Value> {
        match expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => {
                let key = format!("{:?}", expr);
                Ok(window_results
                    .get(&key)
                    .and_then(|r| r.get(row_idx))
                    .cloned()
                    .unwrap_or(Value::Null))
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_qualify_predicate(
                    left,
                    schema,
                    record,
                    row_idx,
                    window_results,
                )?;
                let right_val = Self::evaluate_qualify_predicate(
                    right,
                    schema,
                    record,
                    row_idx,
                    window_results,
                )?;

                if left_val.is_null() || right_val.is_null() {
                    match op {
                        yachtsql_ir::BinaryOp::And | yachtsql_ir::BinaryOp::Or => {}
                        _ => return Ok(Value::Bool(false)),
                    }
                }

                match op {
                    yachtsql_ir::BinaryOp::Eq => Ok(Value::Bool(left_val == right_val)),
                    yachtsql_ir::BinaryOp::NotEq => Ok(Value::Bool(left_val != right_val)),
                    yachtsql_ir::BinaryOp::Lt => Ok(Value::Bool(left_val < right_val)),
                    yachtsql_ir::BinaryOp::LtEq => Ok(Value::Bool(left_val <= right_val)),
                    yachtsql_ir::BinaryOp::Gt => Ok(Value::Bool(left_val > right_val)),
                    yachtsql_ir::BinaryOp::GtEq => Ok(Value::Bool(left_val >= right_val)),
                    yachtsql_ir::BinaryOp::And => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l && r))
                    }
                    yachtsql_ir::BinaryOp::Or => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l || r))
                    }
                    _ => {
                        let evaluator = IrEvaluator::new(schema);
                        evaluator.evaluate(expr, record)
                    }
                }
            }
            Expr::UnaryOp {
                op: yachtsql_ir::UnaryOp::Not,
                expr: inner,
            } => {
                let val = Self::evaluate_qualify_predicate(
                    inner,
                    schema,
                    record,
                    row_idx,
                    window_results,
                )?;
                Ok(Value::Bool(!val.as_bool().unwrap_or(false)))
            }
            _ => {
                let evaluator = IrEvaluator::new(schema);
                evaluator.evaluate(expr, record)
            }
        }
    }

    fn collect_window_exprs(expr: &Expr) -> Vec<Expr> {
        let mut exprs = Vec::new();
        Self::collect_window_exprs_inner(expr, &mut exprs);
        exprs
    }

    fn collect_window_exprs_inner(expr: &Expr, exprs: &mut Vec<Expr>) {
        match expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => {
                exprs.push(expr.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_window_exprs_inner(left, exprs);
                Self::collect_window_exprs_inner(right, exprs);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_window_exprs_inner(expr, exprs);
            }
            _ => {}
        }
    }

    fn expr_has_window_function(expr: &Expr) -> bool {
        match expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_window_function(left) || Self::expr_has_window_function(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_has_window_function(expr),
            _ => false,
        }
    }

    fn extract_qualify_window_spec(
        expr: &Expr,
    ) -> Result<(
        Vec<Expr>,
        Vec<SortExpr>,
        Option<WindowFrame>,
        WindowFuncType,
    )> {
        match expr {
            Expr::Window {
                func,
                partition_by,
                order_by,
                frame,
                ..
            } => Ok((
                partition_by.clone(),
                order_by.clone(),
                frame.clone(),
                WindowFuncType::Window(func.clone()),
            )),
            Expr::AggregateWindow {
                func,
                partition_by,
                order_by,
                frame,
                ..
            } => Ok((
                partition_by.clone(),
                order_by.clone(),
                frame.clone(),
                WindowFuncType::Aggregate(func.clone()),
            )),
            _ => panic!("Expected window expression in qualify"),
        }
    }
}
