use std::cmp::Ordering;

use rust_decimal::prelude::ToPrimitive;
use sqlparser::ast::Expr as SqlExpr;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_storage::{Row, Schema};

#[derive(Debug, Clone)]
pub struct WindowFunction {
    pub function_type: WindowFunctionType,

    pub spec: WindowSpecification,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum WindowFunctionType {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile {
        buckets: i64,
    },
    Lag {
        expr: SqlExpr,
        offset: i64,
        default: Option<Value>,
    },
    Lead {
        expr: SqlExpr,
        offset: i64,
        default: Option<Value>,
    },
    FirstValue {
        expr: SqlExpr,
    },
    LastValue {
        expr: SqlExpr,
    },
    NthValue {
        expr: SqlExpr,
        n: i64,
    },
    Aggregate {
        agg_type: AggregateType,
        expr: Option<SqlExpr>,
    },
    PercentileCont {
        expr: SqlExpr,
        percentile: f64,
    },
    PercentileDisc {
        expr: SqlExpr,
        percentile: f64,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateType {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    Stddev,
    StddevPop,
    StddevSamp,
    Variance,
    VarPop,
    VarSamp,
}

#[derive(Debug, Clone)]
pub struct WindowSpecification {
    pub partition_by: Vec<usize>,

    pub order_by: Vec<(usize, bool, Option<bool>)>,

    pub frame: Option<FrameSpecification>,
}

#[derive(Debug, Clone)]
pub struct FrameSpecification {
    pub frame_type: FrameType,

    pub start_bound: FrameBound,

    pub end_bound: FrameBound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Rows,
    Range,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
    UnboundedFollowing,
}

#[allow(dead_code)]
impl WindowSpecification {
    pub fn new(
        partition_by: Vec<usize>,
        order_by: Vec<(usize, bool, Option<bool>)>,
        frame: Option<FrameSpecification>,
    ) -> Self {
        Self {
            partition_by,
            order_by,
            frame,
        }
    }

    #[allow(dead_code)]
    pub fn default() -> Self {
        Self {
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        }
    }
}

#[allow(dead_code)]
impl FrameSpecification {
    #[allow(dead_code)]
    pub fn default_range() -> Self {
        Self {
            frame_type: FrameType::Range,
            start_bound: FrameBound::UnboundedPreceding,
            end_bound: FrameBound::CurrentRow,
        }
    }

    #[allow(dead_code)]
    pub fn full_partition() -> Self {
        Self {
            frame_type: FrameType::Rows,
            start_bound: FrameBound::UnboundedPreceding,
            end_bound: FrameBound::UnboundedFollowing,
        }
    }
}

pub struct WindowExecutor<'a> {
    #[allow(dead_code)]
    schema: &'a Schema,
    rows: Vec<Row>,
}

impl<'a> WindowExecutor<'a> {
    pub fn new(schema: &'a Schema, rows: Vec<Row>) -> Self {
        Self { schema, rows }
    }

    pub fn execute(&self, window_fn: &WindowFunction) -> Result<Vec<Value>> {
        let partitions = self.partition_rows(&window_fn.spec.partition_by)?;

        let mut results = vec![Value::null(); self.rows.len()];

        for partition in partitions {
            let sorted_partition = self.sort_partition(partition, &window_fn.spec.order_by)?;

            let partition_results = self.compute_window_function(window_fn, &sorted_partition)?;

            for (sorted_idx, &original_idx) in sorted_partition.iter().enumerate() {
                results[original_idx] = partition_results[sorted_idx].clone();
            }
        }

        Ok(results)
    }

    fn partition_rows(&self, partition_cols: &[usize]) -> Result<Vec<Vec<usize>>> {
        if partition_cols.is_empty() {
            return Ok(vec![(0..self.rows.len()).collect()]);
        }

        use std::collections::HashMap;
        let mut partitions: HashMap<Vec<String>, Vec<usize>> = HashMap::new();

        for (row_idx, row) in self.rows.iter().enumerate() {
            let key: Vec<String> = partition_cols
                .iter()
                .map(|&col_idx| format!("{:?}", row.values()[col_idx]))
                .collect();

            partitions.entry(key).or_default().push(row_idx);
        }

        Ok(partitions.into_values().collect())
    }

    fn sort_partition(
        &self,
        mut partition: Vec<usize>,
        order_by: &[(usize, bool, Option<bool>)],
    ) -> Result<Vec<usize>> {
        if order_by.is_empty() {
            return Ok(partition);
        }

        partition.sort_by(|&a_idx, &b_idx| {
            let row_a = &self.rows[a_idx];
            let row_b = &self.rows[b_idx];

            for &(col_idx, is_asc, nulls_first) in order_by {
                let val_a = &row_a.values()[col_idx];
                let val_b = &row_b.values()[col_idx];

                let cmp = self.compare_values_with_nulls(val_a, val_b, is_asc, nulls_first);

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        Ok(partition)
    }

    fn compare_values_with_nulls(
        &self,
        a: &Value,
        b: &Value,
        is_asc: bool,
        nulls_first: Option<bool>,
    ) -> Ordering {
        let nulls_first = nulls_first.unwrap_or(!is_asc);

        match (a.is_null(), b.is_null()) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (false, true) => {
                return if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (false, false) => {
                let cmp = self.compare_non_null_values(a, b);
                return if is_asc { cmp } else { cmp.reverse() };
            }
        }
    }

    fn compare_non_null_values(&self, a: &Value, b: &Value) -> Ordering {
        if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
            return x.cmp(&y);
        }

        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
        }

        if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
            return x.cmp(y);
        }

        if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
            return x.cmp(&y);
        }

        Ordering::Equal
    }

    fn compute_window_function(
        &self,
        window_fn: &WindowFunction,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        match &window_fn.function_type {
            WindowFunctionType::RowNumber => self.compute_row_number(partition),
            WindowFunctionType::Rank => self.compute_rank(window_fn, partition),
            WindowFunctionType::DenseRank => self.compute_dense_rank(window_fn, partition),
            WindowFunctionType::PercentRank => self.compute_percent_rank(window_fn, partition),
            WindowFunctionType::CumeDist => self.compute_cume_dist(window_fn, partition),
            WindowFunctionType::Ntile { buckets } => self.compute_ntile(*buckets, partition),
            WindowFunctionType::Lag {
                expr,
                offset,
                default,
            } => self.compute_lag(expr, *offset, default, partition),
            WindowFunctionType::Lead {
                expr,
                offset,
                default,
            } => self.compute_lead(expr, *offset, default, partition),
            WindowFunctionType::FirstValue { expr } => {
                self.compute_first_value(expr, window_fn, partition)
            }
            WindowFunctionType::LastValue { expr } => {
                self.compute_last_value(expr, window_fn, partition)
            }
            WindowFunctionType::NthValue { expr, n } => {
                self.compute_nth_value(expr, *n, window_fn, partition)
            }
            WindowFunctionType::Aggregate { agg_type, expr } => {
                self.compute_aggregate(*agg_type, expr, window_fn, partition)
            }
            WindowFunctionType::PercentileCont { expr, percentile } => {
                self.compute_percentile_cont(expr, *percentile, partition)
            }
            WindowFunctionType::PercentileDisc { expr, percentile } => {
                self.compute_percentile_disc(expr, *percentile, partition)
            }
        }
    }

    fn compute_row_number(&self, partition: &[usize]) -> Result<Vec<Value>> {
        Ok((1..=partition.len() as i64).map(Value::int64).collect())
    }

    fn compute_rank(&self, window_fn: &WindowFunction, partition: &[usize]) -> Result<Vec<Value>> {
        let mut results = Vec::new();
        let mut current_rank = 1i64;
        let mut prev_row_idx: Option<usize> = None;

        for (idx, &row_idx) in partition.iter().enumerate() {
            if let Some(prev_idx) = prev_row_idx {
                if self.rows_differ_in_order_by(
                    &self.rows[prev_idx],
                    &self.rows[row_idx],
                    &window_fn.spec.order_by,
                ) {
                    current_rank = (idx + 1) as i64;
                }
            }
            results.push(Value::int64(current_rank));
            prev_row_idx = Some(row_idx);
        }

        Ok(results)
    }

    fn compute_dense_rank(
        &self,
        window_fn: &WindowFunction,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();
        let mut current_rank = 1i64;
        let mut prev_row_idx: Option<usize> = None;

        for &row_idx in partition.iter() {
            if let Some(prev_idx) = prev_row_idx
                && self.rows_differ_in_order_by(
                    &self.rows[prev_idx],
                    &self.rows[row_idx],
                    &window_fn.spec.order_by,
                )
            {
                current_rank += 1;
            }
            results.push(Value::int64(current_rank));
            prev_row_idx = Some(row_idx);
        }

        Ok(results)
    }

    fn rows_differ_in_order_by(
        &self,
        row_a: &Row,
        row_b: &Row,
        order_by: &[(usize, bool, Option<bool>)],
    ) -> bool {
        for &(col_idx, _, _) in order_by {
            if row_a.values()[col_idx] != row_b.values()[col_idx] {
                return true;
            }
        }
        false
    }

    fn compute_ntile(&self, buckets: i64, partition: &[usize]) -> Result<Vec<Value>> {
        if buckets <= 0 {
            return Err(Error::InvalidQuery(
                "NTILE buckets must be positive".to_string(),
            ));
        }

        let partition_size = partition.len() as i64;
        let bucket_size = partition_size / buckets;
        let remainder = partition_size % buckets;

        let mut results = Vec::new();
        for (idx, _) in partition.iter().enumerate() {
            let bucket = if (idx as i64) < remainder * (bucket_size + 1) {
                (idx as i64) / (bucket_size + 1) + 1
            } else {
                (idx as i64 - remainder * (bucket_size + 1)) / bucket_size + remainder + 1
            };
            results.push(Value::int64(bucket));
        }

        Ok(results)
    }

    fn compute_percent_rank(
        &self,
        window_fn: &WindowFunction,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        let n = partition.len() as f64;

        if n <= 1.0 {
            return Ok(partition.iter().map(|_| Value::float64(0.0)).collect());
        }

        let ranks = self.compute_rank(window_fn, partition)?;

        let results: Vec<Value> = ranks
            .iter()
            .map(|rank_val| {
                let rank = rank_val.as_i64().unwrap_or(1) as f64;
                let percent_rank = (rank - 1.0) / (n - 1.0);
                Value::float64(percent_rank)
            })
            .collect();

        Ok(results)
    }

    fn compute_cume_dist(
        &self,
        window_fn: &WindowFunction,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        let n = partition.len() as f64;

        if partition.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = vec![0.0; partition.len()];

        for (idx, &row_idx) in partition.iter().enumerate() {
            let mut last_peer_idx = idx;
            for (check_idx, &check_row_idx) in partition.iter().enumerate().skip(idx + 1) {
                if !self.rows_differ_in_order_by(
                    &self.rows[row_idx],
                    &self.rows[check_row_idx],
                    &window_fn.spec.order_by,
                ) {
                    last_peer_idx = check_idx;
                } else {
                    break;
                }
            }

            results[idx] = (last_peer_idx + 1) as f64 / n;
        }

        Ok(results.into_iter().map(Value::float64).collect())
    }

    fn calculate_frame_bounds(
        &self,
        row_position: usize,
        partition: &[usize],
        frame: Option<&FrameSpecification>,
        order_by: &[(usize, bool, Option<bool>)],
    ) -> (usize, usize) {
        let partition_len = partition.len();

        let frame = match frame {
            Some(f) => f,
            None => {
                let end_idx = self.find_last_peer(row_position, partition, order_by);
                return (0, end_idx);
            }
        };

        let is_range = frame.frame_type == FrameType::Range;

        let start_idx = match frame.start_bound {
            FrameBound::UnboundedPreceding => 0,
            FrameBound::Preceding(n) => {
                if is_range {
                    row_position.saturating_sub(n)
                } else {
                    row_position.saturating_sub(n)
                }
            }
            FrameBound::CurrentRow => {
                if is_range {
                    self.find_first_peer(row_position, partition, order_by)
                } else {
                    row_position
                }
            }
            FrameBound::Following(n) => (row_position + n).min(partition_len.saturating_sub(1)),
            FrameBound::UnboundedFollowing => partition_len.saturating_sub(1),
        };

        let end_idx = match frame.end_bound {
            FrameBound::UnboundedPreceding => 0,
            FrameBound::Preceding(n) => row_position.saturating_sub(n),
            FrameBound::CurrentRow => {
                if is_range {
                    self.find_last_peer(row_position, partition, order_by)
                } else {
                    row_position
                }
            }
            FrameBound::Following(n) => {
                if is_range {
                    (row_position + n).min(partition_len.saturating_sub(1))
                } else {
                    (row_position + n).min(partition_len.saturating_sub(1))
                }
            }
            FrameBound::UnboundedFollowing => partition_len.saturating_sub(1),
        };

        (start_idx, end_idx)
    }

    fn find_first_peer(
        &self,
        row_position: usize,
        partition: &[usize],
        order_by: &[(usize, bool, Option<bool>)],
    ) -> usize {
        if order_by.is_empty() || row_position == 0 {
            return row_position;
        }

        let row_idx = partition[row_position];
        let current_row = &self.rows[row_idx];

        let mut first_peer = row_position;
        for check_position in (0..row_position).rev() {
            let check_row_idx = partition[check_position];
            let check_row = &self.rows[check_row_idx];

            if !self.rows_differ_in_order_by(current_row, check_row, order_by) {
                first_peer = check_position;
            } else {
                break;
            }
        }

        first_peer
    }

    fn find_last_peer(
        &self,
        row_position: usize,
        partition: &[usize],
        order_by: &[(usize, bool, Option<bool>)],
    ) -> usize {
        if order_by.is_empty() || row_position >= partition.len().saturating_sub(1) {
            return row_position;
        }

        let row_idx = partition[row_position];
        let current_row = &self.rows[row_idx];

        let mut last_peer = row_position;
        for (check_position, &check_row_idx) in partition.iter().enumerate().skip(row_position + 1)
        {
            let check_row = &self.rows[check_row_idx];

            if !self.rows_differ_in_order_by(current_row, check_row, order_by) {
                last_peer = check_position;
            } else {
                break;
            }
        }

        last_peer
    }

    fn evaluate_expr(&self, expr: &SqlExpr, row: &Row) -> Result<Value> {
        use crate::query_executor::expression_evaluator::ExpressionEvaluator;
        let evaluator = ExpressionEvaluator::new(self.schema);
        evaluator.evaluate_expr(expr, row)
    }

    fn compute_lag(
        &self,
        expr: &SqlExpr,
        offset: i64,
        default: &Option<Value>,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();

        for (idx, &_row_idx) in partition.iter().enumerate() {
            let lag_idx = idx as i64 - offset;

            let value = if lag_idx >= 0 && (lag_idx as usize) < partition.len() {
                let lag_row_idx = partition[lag_idx as usize];
                let lag_row = &self.rows[lag_row_idx];
                self.evaluate_expr(expr, lag_row)?
            } else {
                default.clone().unwrap_or(Value::null())
            };

            results.push(value);
        }

        Ok(results)
    }

    fn compute_lead(
        &self,
        expr: &SqlExpr,
        offset: i64,
        default: &Option<Value>,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();

        for (idx, &_row_idx) in partition.iter().enumerate() {
            let lead_idx = idx as i64 + offset;

            let value = if lead_idx >= 0 && (lead_idx as usize) < partition.len() {
                let lead_row_idx = partition[lead_idx as usize];
                let lead_row = &self.rows[lead_row_idx];
                self.evaluate_expr(expr, lead_row)?
            } else {
                default.clone().unwrap_or(Value::null())
            };

            results.push(value);
        }

        Ok(results)
    }

    fn compute_first_value(
        &self,
        expr: &SqlExpr,
        window_fn: &WindowFunction,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();

        for (idx, _) in partition.iter().enumerate() {
            let (start_idx, end_idx) = self.calculate_frame_bounds(
                idx,
                partition,
                window_fn.spec.frame.as_ref(),
                &window_fn.spec.order_by,
            );

            if start_idx <= end_idx {
                let first_row_idx = partition[start_idx];
                let first_row = &self.rows[first_row_idx];
                let value = self.evaluate_expr(expr, first_row)?;
                results.push(value);
            } else {
                results.push(Value::null());
            }
        }

        Ok(results)
    }

    fn compute_last_value(
        &self,
        expr: &SqlExpr,
        window_fn: &WindowFunction,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();

        for (idx, _) in partition.iter().enumerate() {
            let (start_idx, end_idx) = self.calculate_frame_bounds(
                idx,
                partition,
                window_fn.spec.frame.as_ref(),
                &window_fn.spec.order_by,
            );

            if start_idx <= end_idx {
                let last_row_idx = partition[end_idx];
                let last_row = &self.rows[last_row_idx];
                let value = self.evaluate_expr(expr, last_row)?;
                results.push(value);
            } else {
                results.push(Value::null());
            }
        }

        Ok(results)
    }

    fn compute_nth_value(
        &self,
        expr: &SqlExpr,
        n: i64,
        window_fn: &WindowFunction,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();

        for (idx, _) in partition.iter().enumerate() {
            let (start_idx, end_idx) = self.calculate_frame_bounds(
                idx,
                partition,
                window_fn.spec.frame.as_ref(),
                &window_fn.spec.order_by,
            );

            let nth_offset = n - 1;
            let target_idx = start_idx as i64 + nth_offset;

            if target_idx >= 0
                && target_idx <= end_idx as i64
                && (target_idx as usize) < partition.len()
            {
                let target_row_idx = partition[target_idx as usize];
                let target_row = &self.rows[target_row_idx];
                let value = self.evaluate_expr(expr, target_row)?;
                results.push(value);
            } else {
                results.push(Value::null());
            }
        }

        Ok(results)
    }

    fn compute_aggregate(
        &self,
        agg_type: AggregateType,
        expr: &Option<SqlExpr>,
        window_fn: &WindowFunction,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();

        for (idx, _) in partition.iter().enumerate() {
            let (start_idx, end_idx) = self.calculate_frame_bounds(
                idx,
                partition,
                window_fn.spec.frame.as_ref(),
                &window_fn.spec.order_by,
            );

            let frame_values: Result<Vec<Value>> = if start_idx <= end_idx {
                (start_idx..=end_idx)
                    .map(|frame_idx| {
                        let row_idx = partition[frame_idx];
                        let row = &self.rows[row_idx];

                        if let Some(expression) = expr {
                            self.evaluate_expr(expression, row)
                        } else {
                            Ok(Value::int64(1))
                        }
                    })
                    .collect()
            } else {
                Ok(Vec::new())
            };

            let frame_values = frame_values?;

            let value = match agg_type {
                AggregateType::Count => {
                    let count = frame_values.iter().filter(|v| !v.is_null()).count();
                    Value::int64(count as i64)
                }
                AggregateType::Sum => {
                    let sum =
                        frame_values
                            .iter()
                            .filter(|v| !v.is_null())
                            .try_fold(0.0, |acc, v| {
                                if let Some(n) = v.as_i64() {
                                    Ok(acc + n as f64)
                                } else if let Some(f) = v.as_f64() {
                                    Ok(acc + f)
                                } else if let Some(d) = v.as_numeric() {
                                    d.to_f64().map(|n| acc + n).ok_or(())
                                } else {
                                    Err(())
                                }
                            });

                    if let Ok(sum_val) = sum {
                        Value::float64(sum_val)
                    } else {
                        Value::null()
                    }
                }
                AggregateType::Avg => {
                    let values: Vec<f64> = frame_values
                        .iter()
                        .filter(|v| !v.is_null())
                        .filter_map(|v| {
                            if let Some(n) = v.as_i64() {
                                Some(n as f64)
                            } else if let Some(f) = v.as_f64() {
                                Some(f)
                            } else if let Some(d) = v.as_numeric() {
                                d.to_f64()
                            } else {
                                None
                            }
                        })
                        .collect();

                    if values.is_empty() {
                        Value::null()
                    } else {
                        let sum: f64 = values.iter().sum();
                        Value::float64(sum / values.len() as f64)
                    }
                }
                AggregateType::Min => {
                    let min = frame_values
                        .iter()
                        .filter(|v| !v.is_null())
                        .min_by(|a, b| self.compare_non_null_values(a, b));

                    min.cloned().unwrap_or(Value::null())
                }
                AggregateType::Max => {
                    let max = frame_values
                        .iter()
                        .filter(|v| !v.is_null())
                        .max_by(|a, b| self.compare_non_null_values(a, b));

                    max.cloned().unwrap_or(Value::null())
                }
                AggregateType::Stddev | AggregateType::StddevSamp => {
                    self.compute_stddev_value(&frame_values, false)
                }
                AggregateType::StddevPop => self.compute_stddev_value(&frame_values, true),
                AggregateType::Variance | AggregateType::VarSamp => {
                    self.compute_variance_value(&frame_values, false)
                }
                AggregateType::VarPop => self.compute_variance_value(&frame_values, true),
            };

            results.push(value);
        }

        Ok(results)
    }

    fn extract_numeric_values(&self, values: &[Value]) -> Vec<f64> {
        values
            .iter()
            .filter(|v| !v.is_null())
            .filter_map(|v| {
                if let Some(n) = v.as_i64() {
                    Some(n as f64)
                } else if let Some(f) = v.as_f64() {
                    Some(f)
                } else if let Some(d) = v.as_numeric() {
                    d.to_f64()
                } else {
                    None
                }
            })
            .collect()
    }

    fn compute_stddev_value(&self, frame_values: &[Value], population: bool) -> Value {
        let values = self.extract_numeric_values(frame_values);
        let n = values.len();

        if n == 0 || (n == 1 && !population) {
            return Value::null();
        }

        let mean: f64 = values.iter().sum::<f64>() / n as f64;
        let variance: f64 = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>();

        let divisor = if population { n as f64 } else { (n - 1) as f64 };
        let stddev = (variance / divisor).sqrt();

        Value::float64(stddev)
    }

    fn compute_variance_value(&self, frame_values: &[Value], population: bool) -> Value {
        let values = self.extract_numeric_values(frame_values);
        let n = values.len();

        if n == 0 || (n == 1 && !population) {
            return Value::null();
        }

        let mean: f64 = values.iter().sum::<f64>() / n as f64;
        let variance: f64 = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>();

        let divisor = if population { n as f64 } else { (n - 1) as f64 };

        Value::float64(variance / divisor)
    }

    fn compute_percentile_cont(
        &self,
        expr: &SqlExpr,
        percentile: f64,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        let mut values: Vec<f64> = Vec::new();
        for &row_idx in partition {
            let row = &self.rows[row_idx];
            let value = self.evaluate_expr(expr, row)?;
            if !value.is_null() {
                if let Some(n) = value.as_i64() {
                    values.push(n as f64);
                } else if let Some(f) = value.as_f64() {
                    values.push(f);
                } else if let Some(d) = value.as_numeric() {
                    if let Some(f) = d.to_f64() {
                        values.push(f);
                    }
                }
            }
        }

        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        let result = if values.is_empty() {
            Value::null()
        } else if values.len() == 1 {
            Value::float64(values[0])
        } else {
            let n = values.len();
            let position = percentile * (n - 1) as f64;
            let lower_idx = position.floor() as usize;
            let upper_idx = position.ceil() as usize;

            if lower_idx == upper_idx || upper_idx >= n {
                Value::float64(values[lower_idx.min(n - 1)])
            } else {
                let fraction = position - lower_idx as f64;
                let lower = values[lower_idx];
                let upper = values[upper_idx];
                Value::float64(lower + (upper - lower) * fraction)
            }
        };

        Ok(vec![result; partition.len()])
    }

    fn compute_percentile_disc(
        &self,
        expr: &SqlExpr,
        percentile: f64,
        partition: &[usize],
    ) -> Result<Vec<Value>> {
        let mut values: Vec<(f64, Value)> = Vec::new();
        for &row_idx in partition {
            let row = &self.rows[row_idx];
            let value = self.evaluate_expr(expr, row)?;
            if !value.is_null() {
                if let Some(n) = value.as_i64() {
                    values.push((n as f64, value));
                } else if let Some(f) = value.as_f64() {
                    values.push((f, value));
                } else if let Some(d) = value.as_numeric() {
                    if let Some(f) = d.to_f64() {
                        values.push((f, value));
                    }
                }
            }
        }

        values.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

        let result = if values.is_empty() {
            Value::null()
        } else {
            let n = values.len();
            let target_idx = (percentile * n as f64).ceil() as usize;
            let idx = target_idx.saturating_sub(1).min(n - 1);
            values[idx].1.clone()
        };

        Ok(vec![result; partition.len()])
    }
}
