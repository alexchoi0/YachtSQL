use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

use crate::aggregate::{Accumulator, AggregateFunction};

fn numeric_value_to_f64(value: &Value) -> Result<Option<f64>> {
    use rust_decimal::prelude::ToPrimitive;
    if value.is_null() {
        return Ok(None);
    }

    if let Some(i) = value.as_i64() {
        return Ok(Some(i as f64));
    }

    if let Some(f) = value.as_f64() {
        return Ok(Some(f));
    }

    if let Some(n) = value.as_numeric() {
        return Ok(n.to_f64());
    }

    Err(Error::InvalidOperation(format!(
        "Cannot convert {:?} to f64",
        value
    )))
}

#[derive(Debug, Clone)]
pub struct NtileAccumulator {
    values: Vec<Value>,
    buckets: usize,
}

impl Default for NtileAccumulator {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            buckets: 4,
        }
    }
}

impl NtileAccumulator {
    pub fn new(buckets: usize) -> Self {
        Self {
            values: Vec::new(),
            buckets: buckets.max(1),
        }
    }
}

impl Accumulator for NtileAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        self.values.push(value.clone());
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<NtileAccumulator>() {
            self.values.extend(other_acc.values.clone());
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let total_rows = self.values.len();
        let bucket_size = (total_rows as f64 / self.buckets as f64).ceil() as usize;

        let mut bucket_assignments = Vec::new();
        for i in 0..total_rows {
            let bucket = (i / bucket_size) + 1;
            bucket_assignments.push(Value::int64(bucket.min(self.buckets) as i64));
        }

        Ok(Value::array(bucket_assignments))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct NtileFunction {
    buckets: usize,
}

impl Default for NtileFunction {
    fn default() -> Self {
        Self { buckets: 4 }
    }
}

impl NtileFunction {
    pub fn new(buckets: usize) -> Self {
        Self { buckets }
    }
}

impl AggregateFunction for NtileFunction {
    fn name(&self) -> &str {
        "NTILE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Int64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(NtileAccumulator::new(self.buckets))
    }
}

#[derive(Debug, Clone, Default)]
pub struct CumeDistAccumulator {
    values: Vec<f64>,
}

impl Accumulator for CumeDistAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.values.push(val);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<CumeDistAccumulator>() {
            self.values.extend(&other_acc.values);
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::array(Vec::new()));
        }

        let mut sorted_with_indices: Vec<(usize, f64)> = self
            .values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i, v))
            .collect();

        sorted_with_indices
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let total = self.values.len() as f64;
        let mut result = vec![0.0; self.values.len()];

        for (rank, (original_idx, _)) in sorted_with_indices.iter().enumerate() {
            result[*original_idx] = (rank + 1) as f64 / total;
        }

        Ok(Value::array(
            result.iter().map(|&v| Value::float64(v)).collect(),
        ))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct CumeDistFunction;

impl AggregateFunction for CumeDistFunction {
    fn name(&self) -> &str {
        "CUME_DIST"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CumeDistAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct PercentRankAccumulator {
    values: Vec<f64>,
}

impl Accumulator for PercentRankAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.values.push(val);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<PercentRankAccumulator>() {
            self.values.extend(&other_acc.values);
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::array(Vec::new()));
        }

        if self.values.len() == 1 {
            return Ok(Value::array(vec![Value::float64(0.0)]));
        }

        let mut sorted_with_indices: Vec<(usize, f64)> = self
            .values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i, v))
            .collect();

        sorted_with_indices
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let total = (self.values.len() - 1) as f64;
        let mut result = vec![0.0; self.values.len()];

        for (rank, (original_idx, _)) in sorted_with_indices.iter().enumerate() {
            result[*original_idx] = rank as f64 / total;
        }

        Ok(Value::array(
            result.iter().map(|&v| Value::float64(v)).collect(),
        ))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct PercentRankFunction;

impl AggregateFunction for PercentRankFunction {
    fn name(&self) -> &str {
        "PERCENT_RANK"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(PercentRankAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RowNumberAccumulator {
    count: usize,
}

impl Accumulator for RowNumberAccumulator {
    fn accumulate(&mut self, _value: &Value) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<RowNumberAccumulator>() {
            self.count += other_acc.count;
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        let mut result = Vec::new();
        for i in 1..=self.count {
            result.push(Value::int64(i as i64));
        }
        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.count = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct RowNumberFunction;

impl AggregateFunction for RowNumberFunction {
    fn name(&self) -> &str {
        "ROW_NUMBER"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Int64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RowNumberAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RankAccumulator {
    values: Vec<f64>,
}

impl Accumulator for RankAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.values.push(val);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<RankAccumulator>() {
            self.values.extend(&other_acc.values);
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::array(Vec::new()));
        }

        let mut sorted_with_indices: Vec<(usize, f64)> = self
            .values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i, v))
            .collect();

        sorted_with_indices
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut result = vec![0i64; self.values.len()];
        let mut current_rank = 1i64;

        for i in 0..sorted_with_indices.len() {
            if i > 0
                && (sorted_with_indices[i].1 - sorted_with_indices[i - 1].1).abs() > f64::EPSILON
            {
                current_rank = (i + 1) as i64;
            }
            result[sorted_with_indices[i].0] = current_rank;
        }

        Ok(Value::array(
            result.iter().map(|&v| Value::int64(v)).collect(),
        ))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct RankFunction;

impl AggregateFunction for RankFunction {
    fn name(&self) -> &str {
        "RANK"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Int64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RankAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct DenseRankAccumulator {
    values: Vec<f64>,
}

impl Accumulator for DenseRankAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.values.push(val);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<DenseRankAccumulator>() {
            self.values.extend(&other_acc.values);
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::array(Vec::new()));
        }

        let mut sorted_with_indices: Vec<(usize, f64)> = self
            .values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i, v))
            .collect();

        sorted_with_indices
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut result = vec![0i64; self.values.len()];
        let mut dense_rank = 1i64;

        for i in 0..sorted_with_indices.len() {
            if i > 0
                && (sorted_with_indices[i].1 - sorted_with_indices[i - 1].1).abs() > f64::EPSILON
            {
                dense_rank += 1;
            }
            result[sorted_with_indices[i].0] = dense_rank;
        }

        Ok(Value::array(
            result.iter().map(|&v| Value::int64(v)).collect(),
        ))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct DenseRankFunction;

impl AggregateFunction for DenseRankFunction {
    fn name(&self) -> &str {
        "DENSE_RANK"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Int64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(DenseRankAccumulator::default())
    }
}
