use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{
    AggregateFunction, Expr, PlanSchema, SortExpr, WindowFrame, WindowFrameBound, WindowFunction,
};
use yachtsql_storage::{Record, Table};

use super::{PlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_window(
        &mut self,
        input: &PhysicalPlan,
        window_exprs: &[Expr],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let input_schema = input_table.schema().clone();
        let result_schema = plan_schema_to_schema(schema);
        let evaluator = IrEvaluator::new(&input_schema);

        let rows: Vec<Record> = input_table.rows()?;
        let mut all_window_results: Vec<Vec<Value>> = vec![Vec::new(); window_exprs.len()];

        for (expr_idx, window_expr) in window_exprs.iter().enumerate() {
            let (partition_by, order_by, frame, func_type) =
                Self::extract_window_spec(window_expr)?;

            let partitions = Self::partition_rows(&rows, &partition_by, &evaluator)?;

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
                    while all_window_results[expr_idx].len() <= *row_idx {
                        all_window_results[expr_idx].push(Value::Null);
                    }
                    all_window_results[expr_idx][*row_idx] = partition_results[local_idx].clone();
                }
            }
        }

        let mut result = Table::empty(result_schema);
        for (row_idx, record) in rows.iter().enumerate() {
            let mut new_row = record.values().to_vec();
            for window_result in &all_window_results {
                if row_idx < window_result.len() {
                    new_row.push(window_result[row_idx].clone());
                } else {
                    new_row.push(Value::Null);
                }
            }
            result.push_row(new_row)?;
        }

        Ok(result)
    }

    fn extract_window_spec(
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
            Expr::Alias { expr: inner, .. } => Self::extract_window_spec(inner),
            Expr::BinaryOp { left, right, .. } => {
                if let Ok(spec) = Self::extract_window_spec(left) {
                    Ok(spec)
                } else {
                    Self::extract_window_spec(right)
                }
            }
            Expr::UnaryOp { expr: inner, .. } => Self::extract_window_spec(inner),
            Expr::Cast { expr: inner, .. } => Self::extract_window_spec(inner),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                if let Some(op) = operand
                    && let Ok(spec) = Self::extract_window_spec(op)
                {
                    return Ok(spec);
                }
                for clause in when_clauses {
                    if let Ok(spec) = Self::extract_window_spec(&clause.condition) {
                        return Ok(spec);
                    }
                    if let Ok(spec) = Self::extract_window_spec(&clause.result) {
                        return Ok(spec);
                    }
                }
                if let Some(e) = else_result
                    && let Ok(spec) = Self::extract_window_spec(e)
                {
                    return Ok(spec);
                }
                Err(Error::InvalidQuery(format!(
                    "Expected window expression, got {:?}",
                    expr
                )))
            }
            Expr::ScalarFunction { args, .. } => {
                for arg in args {
                    if let Ok(spec) = Self::extract_window_spec(arg) {
                        return Ok(spec);
                    }
                }
                Err(Error::InvalidQuery(format!(
                    "Expected window expression, got {:?}",
                    expr
                )))
            }
            _ => Err(Error::InvalidQuery(format!(
                "Expected window expression, got {:?}",
                expr
            ))),
        }
    }

    pub fn partition_rows(
        rows: &[Record],
        partition_by: &[Expr],
        evaluator: &IrEvaluator,
    ) -> Result<HashMap<Vec<Value>, Vec<usize>>> {
        let mut partitions: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();

        if partition_by.is_empty() {
            partitions.insert(vec![], (0..rows.len()).collect());
            return Ok(partitions);
        }

        for (idx, record) in rows.iter().enumerate() {
            let key: Vec<Value> = partition_by
                .iter()
                .map(|e| evaluator.evaluate(e, record).unwrap_or(Value::Null))
                .collect();
            partitions.entry(key).or_default().push(idx);
        }

        Ok(partitions)
    }

    pub fn sort_partition(
        rows: &[Record],
        indices: &mut [usize],
        order_by: &[SortExpr],
        evaluator: &IrEvaluator,
    ) -> Result<()> {
        if order_by.is_empty() {
            return Ok(());
        }

        indices.sort_by(|&a, &b| {
            for sort_expr in order_by {
                let val_a = evaluator
                    .evaluate(&sort_expr.expr, &rows[a])
                    .unwrap_or(Value::Null);
                let val_b = evaluator
                    .evaluate(&sort_expr.expr, &rows[b])
                    .unwrap_or(Value::Null);

                let ordering = val_a
                    .partial_cmp(&val_b)
                    .unwrap_or(std::cmp::Ordering::Equal);
                let ordering = if sort_expr.asc {
                    ordering
                } else {
                    ordering.reverse()
                };

                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(())
    }

    pub fn compute_window_function(
        rows: &[Record],
        sorted_indices: &[usize],
        expr: &Expr,
        func_type: &WindowFuncType,
        order_by: &[SortExpr],
        frame: &Option<WindowFrame>,
        evaluator: &IrEvaluator,
    ) -> Result<Vec<Value>> {
        let partition_size = sorted_indices.len();
        let mut results = Vec::with_capacity(partition_size);

        match func_type {
            WindowFuncType::Window(func) => match func {
                WindowFunction::RowNumber => {
                    for i in 0..partition_size {
                        results.push(Value::Int64((i + 1) as i64));
                    }
                }
                WindowFunction::Rank => {
                    if order_by.is_empty() {
                        results = vec![Value::Int64(1); partition_size];
                    } else {
                        let mut rank = 1i64;
                        let mut prev_values: Option<Vec<Value>> = None;
                        for (i, &idx) in sorted_indices.iter().enumerate() {
                            let curr_values: Vec<Value> = order_by
                                .iter()
                                .map(|ob| {
                                    evaluator
                                        .evaluate(&ob.expr, &rows[idx])
                                        .unwrap_or(Value::Null)
                                })
                                .collect();

                            if let Some(prev) = &prev_values
                                && curr_values != *prev
                            {
                                rank = (i + 1) as i64;
                            }
                            results.push(Value::Int64(rank));
                            prev_values = Some(curr_values);
                        }
                    }
                }
                WindowFunction::DenseRank => {
                    if order_by.is_empty() {
                        results = vec![Value::Int64(1); partition_size];
                    } else {
                        let mut rank = 1i64;
                        let mut prev_values: Option<Vec<Value>> = None;
                        for &idx in sorted_indices {
                            let curr_values: Vec<Value> = order_by
                                .iter()
                                .map(|ob| {
                                    evaluator
                                        .evaluate(&ob.expr, &rows[idx])
                                        .unwrap_or(Value::Null)
                                })
                                .collect();

                            if let Some(prev) = &prev_values
                                && curr_values != *prev
                            {
                                rank += 1;
                            }
                            results.push(Value::Int64(rank));
                            prev_values = Some(curr_values);
                        }
                    }
                }
                WindowFunction::Ntile => {
                    let n = Self::extract_window_arg(expr, 0, evaluator, &rows[sorted_indices[0]])?
                        .as_i64()
                        .unwrap_or(1) as usize;
                    let n = n.max(1);
                    let base_size = partition_size / n;
                    let extra = partition_size % n;
                    let mut bucket = 1usize;
                    let mut in_bucket = 0usize;
                    for _ in 0..partition_size {
                        let bucket_size = if bucket <= extra {
                            base_size + 1
                        } else {
                            base_size
                        };
                        results.push(Value::Int64(bucket as i64));
                        in_bucket += 1;
                        if in_bucket >= bucket_size && bucket < n {
                            bucket += 1;
                            in_bucket = 0;
                        }
                    }
                }
                WindowFunction::Lag => {
                    let offset =
                        Self::extract_window_arg(expr, 1, evaluator, &rows[sorted_indices[0]])?
                            .as_i64()
                            .unwrap_or(1) as usize;
                    let default =
                        Self::extract_window_arg(expr, 2, evaluator, &rows[sorted_indices[0]])
                            .unwrap_or(Value::Null);

                    for i in 0..partition_size {
                        if i >= offset {
                            let lag_idx = sorted_indices[i - offset];
                            let val = Self::extract_window_arg(expr, 0, evaluator, &rows[lag_idx])?;
                            results.push(val);
                        } else {
                            results.push(default.clone());
                        }
                    }
                }
                WindowFunction::Lead => {
                    let offset =
                        Self::extract_window_arg(expr, 1, evaluator, &rows[sorted_indices[0]])?
                            .as_i64()
                            .unwrap_or(1) as usize;
                    let default =
                        Self::extract_window_arg(expr, 2, evaluator, &rows[sorted_indices[0]])
                            .unwrap_or(Value::Null);

                    for i in 0..partition_size {
                        if i + offset < partition_size {
                            let lead_idx = sorted_indices[i + offset];
                            let val =
                                Self::extract_window_arg(expr, 0, evaluator, &rows[lead_idx])?;
                            results.push(val);
                        } else {
                            results.push(default.clone());
                        }
                    }
                }
                WindowFunction::FirstValue => {
                    let first_idx = sorted_indices[0];
                    let first_val = Self::extract_window_arg(expr, 0, evaluator, &rows[first_idx])?;
                    results = vec![first_val; partition_size];
                }
                WindowFunction::LastValue => {
                    if let Some(frame) = frame {
                        for curr_pos in 0..partition_size {
                            let end_bound =
                                frame.end.as_ref().unwrap_or(&WindowFrameBound::CurrentRow);
                            let end_idx =
                                Self::compute_frame_end(end_bound, curr_pos, partition_size);
                            let actual_idx = sorted_indices[end_idx.min(partition_size - 1)];
                            let val =
                                Self::extract_window_arg(expr, 0, evaluator, &rows[actual_idx])?;
                            results.push(val);
                        }
                    } else {
                        let last_idx = sorted_indices[partition_size - 1];
                        let last_val =
                            Self::extract_window_arg(expr, 0, evaluator, &rows[last_idx])?;
                        results = vec![last_val; partition_size];
                    }
                }
                WindowFunction::NthValue => {
                    let n = Self::extract_window_arg(expr, 1, evaluator, &rows[sorted_indices[0]])?
                        .as_i64()
                        .unwrap_or(1) as usize;
                    let nth_val = if n > 0 && n <= partition_size {
                        let nth_idx = sorted_indices[n - 1];
                        Self::extract_window_arg(expr, 0, evaluator, &rows[nth_idx])?
                    } else {
                        Value::Null
                    };
                    results = vec![nth_val; partition_size];
                }
                WindowFunction::PercentRank => {
                    if partition_size <= 1 {
                        results =
                            vec![Value::Float64(ordered_float::OrderedFloat(0.0)); partition_size];
                    } else {
                        let mut prev_values: Option<Vec<Value>> = None;
                        let mut rank = 0i64;
                        for (i, &idx) in sorted_indices.iter().enumerate() {
                            let curr_values: Vec<Value> = order_by
                                .iter()
                                .map(|ob| {
                                    evaluator
                                        .evaluate(&ob.expr, &rows[idx])
                                        .unwrap_or(Value::Null)
                                })
                                .collect();

                            if i == 0 || prev_values.as_ref().is_some_and(|p| *p != curr_values) {
                                rank = i as i64;
                            }
                            let pct = rank as f64 / (partition_size - 1) as f64;
                            results.push(Value::Float64(ordered_float::OrderedFloat(pct)));
                            prev_values = Some(curr_values);
                        }
                    }
                }
                WindowFunction::CumeDist => {
                    let mut prev_values: Option<Vec<Value>> = None;
                    let mut count_less_or_equal = 0usize;
                    for (i, &idx) in sorted_indices.iter().enumerate() {
                        let curr_values: Vec<Value> = order_by
                            .iter()
                            .map(|ob| {
                                evaluator
                                    .evaluate(&ob.expr, &rows[idx])
                                    .unwrap_or(Value::Null)
                            })
                            .collect();

                        if i == 0 || prev_values.as_ref().is_some_and(|p| *p != curr_values) {
                            count_less_or_equal = sorted_indices
                                .iter()
                                .enumerate()
                                .filter(|(_j, jdx)| {
                                    let j_values: Vec<Value> = order_by
                                        .iter()
                                        .map(|ob| {
                                            evaluator
                                                .evaluate(&ob.expr, &rows[**jdx])
                                                .unwrap_or(Value::Null)
                                        })
                                        .collect();
                                    j_values <= curr_values
                                })
                                .count();
                        }
                        let cume = count_less_or_equal as f64 / partition_size as f64;
                        results.push(Value::Float64(ordered_float::OrderedFloat(cume)));
                        prev_values = Some(curr_values);
                    }
                }
            },
            WindowFuncType::Aggregate(func) => {
                let has_order_by = !order_by.is_empty();

                if let Some(frame) = frame {
                    for curr_pos in 0..partition_size {
                        let start_idx =
                            Self::compute_frame_start(&frame.start, curr_pos, partition_size);
                        let end_bound = frame.end.as_ref().unwrap_or(&WindowFrameBound::CurrentRow);
                        let end_idx = Self::compute_frame_end(end_bound, curr_pos, partition_size);
                        let frame_indices: Vec<usize> = if start_idx <= end_idx {
                            sorted_indices[start_idx..=end_idx].to_vec()
                        } else {
                            vec![]
                        };
                        let agg_result =
                            Self::compute_aggregate(func, expr, rows, &frame_indices, evaluator)?;
                        results.push(agg_result);
                    }
                } else if has_order_by {
                    let mut peer_groups: Vec<(usize, usize)> = Vec::new();
                    let mut group_start = 0;
                    let mut prev_values: Option<Vec<Value>> = None;

                    for (i, &idx) in sorted_indices.iter().enumerate() {
                        let curr_values: Vec<Value> = order_by
                            .iter()
                            .map(|ob| {
                                evaluator
                                    .evaluate(&ob.expr, &rows[idx])
                                    .unwrap_or(Value::Null)
                            })
                            .collect();

                        if let Some(prev) = &prev_values
                            && curr_values != *prev
                        {
                            peer_groups.push((group_start, i - 1));
                            group_start = i;
                        }
                        prev_values = Some(curr_values);
                    }
                    peer_groups.push((group_start, partition_size - 1));

                    for (group_start, group_end) in &peer_groups {
                        let running_indices: Vec<usize> = sorted_indices[..=*group_end].to_vec();
                        let agg_result =
                            Self::compute_aggregate(func, expr, rows, &running_indices, evaluator)?;
                        for _ in *group_start..=*group_end {
                            results.push(agg_result.clone());
                        }
                    }
                } else {
                    let agg_result =
                        Self::compute_aggregate(func, expr, rows, sorted_indices, evaluator)?;
                    results = vec![agg_result; partition_size];
                }
            }
        }

        Ok(results)
    }

    fn extract_window_arg(
        expr: &Expr,
        n: usize,
        evaluator: &IrEvaluator,
        record: &Record,
    ) -> Result<Value> {
        let args = match expr {
            Expr::Window { args, .. } | Expr::AggregateWindow { args, .. } => args,
            _ => return Err(Error::InvalidQuery("Expected window expression".into())),
        };

        if n < args.len() {
            evaluator.evaluate(&args[n], record)
        } else {
            Ok(Value::Null)
        }
    }

    fn compute_frame_start(
        bound: &WindowFrameBound,
        curr_pos: usize,
        _partition_size: usize,
    ) -> usize {
        match bound {
            WindowFrameBound::Preceding(None) => 0,
            WindowFrameBound::Preceding(Some(n)) => curr_pos.saturating_sub(*n as usize),
            WindowFrameBound::CurrentRow => curr_pos,
            WindowFrameBound::Following(Some(n)) => curr_pos + *n as usize,
            WindowFrameBound::Following(None) => curr_pos,
        }
    }

    fn compute_frame_end(
        bound: &WindowFrameBound,
        curr_pos: usize,
        partition_size: usize,
    ) -> usize {
        match bound {
            WindowFrameBound::Preceding(None) => 0,
            WindowFrameBound::Preceding(Some(n)) => curr_pos.saturating_sub(*n as usize),
            WindowFrameBound::CurrentRow => curr_pos,
            WindowFrameBound::Following(Some(n)) => {
                (curr_pos + *n as usize).min(partition_size - 1)
            }
            WindowFrameBound::Following(None) => partition_size - 1,
        }
    }

    fn compute_aggregate(
        func: &AggregateFunction,
        expr: &Expr,
        rows: &[Record],
        indices: &[usize],
        evaluator: &IrEvaluator,
    ) -> Result<Value> {
        if indices.is_empty() {
            return match func {
                AggregateFunction::Count => Ok(Value::Int64(0)),
                _ => Ok(Value::Null),
            };
        }

        let has_args = match expr {
            Expr::Window { args, .. } | Expr::AggregateWindow { args, .. } => {
                !args.is_empty() && !matches!(args.first(), Some(Expr::Wildcard { .. }))
            }
            _ => false,
        };

        let values: Vec<Value> = indices
            .iter()
            .map(|&idx| {
                Self::extract_window_arg(expr, 0, evaluator, &rows[idx]).unwrap_or(Value::Null)
            })
            .collect();

        match func {
            AggregateFunction::Count => {
                let count = if has_args {
                    values.iter().filter(|v| !v.is_null()).count()
                } else {
                    indices.len()
                };
                Ok(Value::Int64(count as i64))
            }
            AggregateFunction::Sum => {
                let mut sum = 0f64;
                for v in &values {
                    if let Some(n) = v.as_f64() {
                        sum += n;
                    } else if let Some(n) = v.as_i64() {
                        sum += n as f64;
                    }
                }
                Ok(Value::Float64(ordered_float::OrderedFloat(sum)))
            }
            AggregateFunction::Avg => {
                let mut sum = 0f64;
                let mut count = 0;
                for v in &values {
                    if let Some(n) = v.as_f64() {
                        sum += n;
                        count += 1;
                    } else if let Some(n) = v.as_i64() {
                        sum += n as f64;
                        count += 1;
                    }
                }
                if count > 0 {
                    Ok(Value::Float64(ordered_float::OrderedFloat(
                        sum / count as f64,
                    )))
                } else {
                    Ok(Value::Null)
                }
            }
            AggregateFunction::Min => {
                let min = values.iter().filter(|v| !v.is_null()).min().cloned();
                Ok(min.unwrap_or(Value::Null))
            }
            AggregateFunction::Max => {
                let max = values.iter().filter(|v| !v.is_null()).max().cloned();
                Ok(max.unwrap_or(Value::Null))
            }
            _ => Ok(Value::Null),
        }
    }
}

pub enum WindowFuncType {
    Window(WindowFunction),
    Aggregate(AggregateFunction),
}
