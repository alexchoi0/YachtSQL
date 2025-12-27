use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{BinaryOp, Expr, PlanSchema, SortExpr, UnnestColumn, WindowFrame};
use yachtsql_storage::{Record, Schema, Table};

use super::ConcurrentPlanExecutor;
use crate::executor::plan_schema_to_schema;
use crate::executor::window::{
    WindowFuncType, compute_window_function, partition_rows, sort_partition,
};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl ConcurrentPlanExecutor<'_> {
    pub(crate) async fn execute_unnest(
        &self,
        input: &PhysicalPlan,
        columns: &[UnnestColumn],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input).await?;
        let input_schema = input_table.schema().clone();
        let vars = self.get_variables();
        let sys_vars = self.get_system_variables();
        let udf = self.get_user_functions();
        let evaluator = IrEvaluator::new(&input_schema)
            .with_variables(&vars)
            .with_system_variables(&sys_vars)
            .with_user_functions(&udf);

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        let input_rows = input_table.rows()?;

        if input_rows.is_empty() && !columns.is_empty() {
            let empty_record = Record::new();
            let first_col = &columns[0];
            let array_val = evaluator.evaluate(&first_col.expr, &empty_record)?;
            Self::unnest_array(&array_val, first_col, &[], &mut result)?;
        } else {
            for record in input_rows {
                let base_values = record.values().to_vec();

                if columns.is_empty() {
                    result.push_row(base_values)?;
                    continue;
                }

                let first_col = &columns[0];
                let array_val = evaluator.evaluate(&first_col.expr, &record)?;
                Self::unnest_array(&array_val, first_col, &base_values, &mut result)?;
            }
        }

        Ok(result)
    }

    fn unnest_array(
        array_val: &Value,
        unnest_col: &UnnestColumn,
        base_values: &[Value],
        result: &mut Table,
    ) -> Result<()> {
        match array_val {
            Value::Array(elements) => {
                for (idx, elem) in elements.iter().enumerate() {
                    let mut row = base_values.to_vec();
                    match elem {
                        Value::Struct(struct_fields) => {
                            for (_, value) in struct_fields {
                                row.push(value.clone());
                            }
                        }
                        _ => {
                            row.push(elem.clone());
                        }
                    }
                    if unnest_col.with_offset {
                        row.push(Value::Int64(idx as i64));
                    }
                    result.push_row(row)?;
                }
            }
            Value::Null => {}
            _ => {
                return Err(Error::InvalidQuery("UNNEST requires array argument".into()));
            }
        }
        Ok(())
    }

    pub(crate) async fn execute_qualify(
        &self,
        input: &PhysicalPlan,
        predicate: &Expr,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input).await?;
        let schema = input_table.schema().clone();

        if Self::expr_has_window_function(predicate) {
            self.execute_qualify_with_window(&input_table, predicate)
                .await
        } else {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);
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

    async fn execute_qualify_with_window(&self, input: &Table, predicate: &Expr) -> Result<Table> {
        let schema = input.schema().clone();
        let rows: Vec<Record> = input.rows()?;
        let vars = self.get_variables();
        let sys_vars = self.get_system_variables();
        let udf = self.get_user_functions();
        let evaluator = IrEvaluator::new(&schema)
            .with_variables(&vars)
            .with_system_variables(&sys_vars)
            .with_user_functions(&udf);

        let window_exprs = Self::collect_window_exprs(predicate);
        let mut window_results: HashMap<String, Vec<Value>> = HashMap::new();

        for window_expr in &window_exprs {
            let key = format!("{:?}", window_expr);
            if window_results.contains_key(&key) {
                continue;
            }

            let (partition_by, order_by, frame, func_type) =
                Self::extract_qualify_window_spec(window_expr)?;

            let partitions = partition_rows(&rows, &partition_by, &evaluator)?;
            let mut results = vec![Value::Null; rows.len()];

            for (_key, mut indices) in partitions {
                sort_partition(&rows, &mut indices, &order_by, &evaluator)?;

                let partition_results = compute_window_function(
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
                        BinaryOp::And | BinaryOp::Or => {}
                        _ => return Ok(Value::Bool(false)),
                    }
                }

                match op {
                    BinaryOp::Eq => Ok(Value::Bool(left_val == right_val)),
                    BinaryOp::NotEq => Ok(Value::Bool(left_val != right_val)),
                    BinaryOp::Lt => Ok(Value::Bool(left_val < right_val)),
                    BinaryOp::LtEq => Ok(Value::Bool(left_val <= right_val)),
                    BinaryOp::Gt => Ok(Value::Bool(left_val > right_val)),
                    BinaryOp::GtEq => Ok(Value::Bool(left_val >= right_val)),
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

    pub(crate) fn expr_has_window_function(expr: &Expr) -> bool {
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
                WindowFuncType::Window(*func),
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
                WindowFuncType::Aggregate(*func),
            )),
            _ => panic!("Expected window expression in qualify"),
        }
    }
}
