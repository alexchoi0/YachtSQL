use std::cmp::Ordering;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};

use super::super::{Accumulator, AggregateFunction};
use super::common::numeric_value_to_f64;
use crate::approximate::TDigest;

#[derive(Debug, Clone)]
pub struct QuantileAccumulator {
    digest: TDigest,
    quantile: f64,
}

impl QuantileAccumulator {
    pub fn new(quantile: f64) -> Self {
        Self {
            digest: TDigest::new(),
            quantile: quantile.clamp(0.0, 1.0),
        }
    }
}

impl Accumulator for QuantileAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(num) = numeric_value_to_f64(value)? {
            self.digest.add(num);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.digest.merge(&other.digest);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        let mut digest_clone = self.digest.clone();
        let result = digest_clone.quantile(self.quantile);
        Ok(Value::float64(result))
    }

    fn reset(&mut self) {
        self.digest = TDigest::new();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QuantileFunction {
    quantile: f64,
}

impl QuantileFunction {
    pub fn new(quantile: f64) -> Self {
        Self { quantile }
    }
}

impl Default for QuantileFunction {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl AggregateFunction for QuantileFunction {
    fn name(&self) -> &str {
        "QUANTILE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileAccumulator::new(self.quantile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileExactAccumulator {
    values: Vec<f64>,
    quantile: f64,
}

impl QuantileExactAccumulator {
    pub fn new(quantile: f64) -> Self {
        Self {
            values: Vec::new(),
            quantile: quantile.clamp(0.0, 1.0),
        }
    }
}

impl Accumulator for QuantileExactAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(num) = numeric_value_to_f64(value)? {
            self.values.push(num);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.values.extend_from_slice(&other.values);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        let idx = ((sorted.len() as f64 - 1.0) * self.quantile) as usize;
        let result = sorted[idx.min(sorted.len() - 1)];

        Ok(Value::float64(result))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QuantileExactFunction {
    quantile: f64,
}

impl QuantileExactFunction {
    pub fn new(quantile: f64) -> Self {
        Self { quantile }
    }
}

impl Default for QuantileExactFunction {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl AggregateFunction for QuantileExactFunction {
    fn name(&self) -> &str {
        "QUANTILE_EXACT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileExactAccumulator::new(self.quantile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantilesAccumulator {
    digest: TDigest,
    quantiles: Vec<f64>,
}

impl QuantilesAccumulator {
    pub fn new(quantiles: Vec<f64>) -> Self {
        Self {
            digest: TDigest::new(),
            quantiles: quantiles.into_iter().map(|q| q.clamp(0.0, 1.0)).collect(),
        }
    }
}

impl Accumulator for QuantilesAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(num) = numeric_value_to_f64(value)? {
            self.digest.add(num);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.digest.merge(&other.digest);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        let mut digest_clone = self.digest.clone();
        let results: Vec<Value> = self
            .quantiles
            .iter()
            .map(|&q| Value::float64(digest_clone.quantile(q)))
            .collect();
        Ok(Value::array(results))
    }

    fn reset(&mut self) {
        self.digest = TDigest::new();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantilesFunction {
    quantiles: Vec<f64>,
}

impl QuantilesFunction {
    pub fn new(quantiles: Vec<f64>) -> Self {
        Self { quantiles }
    }
}

impl Default for QuantilesFunction {
    fn default() -> Self {
        Self::new(vec![0.25, 0.5, 0.75])
    }
}

impl AggregateFunction for QuantilesFunction {
    fn name(&self) -> &str {
        "QUANTILES"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantilesAccumulator::new(self.quantiles.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct QuantilesExactAccumulator {
    values: Vec<f64>,
    quantiles: Vec<f64>,
}

impl QuantilesExactAccumulator {
    pub fn new(quantiles: Vec<f64>) -> Self {
        Self {
            values: Vec::new(),
            quantiles: quantiles.into_iter().map(|q| q.clamp(0.0, 1.0)).collect(),
        }
    }
}

impl Accumulator for QuantilesExactAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(num) = numeric_value_to_f64(value)? {
            self.values.push(num);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.values.extend_from_slice(&other.values);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::array(vec![Value::null(); self.quantiles.len()]));
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        let results: Vec<Value> = self
            .quantiles
            .iter()
            .map(|&q| {
                let idx = ((sorted.len() as f64 - 1.0) * q) as usize;
                Value::float64(sorted[idx.min(sorted.len() - 1)])
            })
            .collect();

        Ok(Value::array(results))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantilesExactFunction {
    quantiles: Vec<f64>,
}

impl QuantilesExactFunction {
    pub fn new(quantiles: Vec<f64>) -> Self {
        Self { quantiles }
    }
}

impl Default for QuantilesExactFunction {
    fn default() -> Self {
        Self::new(vec![0.25, 0.5, 0.75])
    }
}

impl AggregateFunction for QuantilesExactFunction {
    fn name(&self) -> &str {
        "QUANTILES_EXACT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantilesExactAccumulator::new(self.quantiles.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileExactWeightedAccumulator {
    values: Vec<(f64, f64)>,
    quantile: f64,
}

impl Default for QuantileExactWeightedAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileExactWeightedAccumulator {
    pub fn new(quantile: f64) -> Self {
        Self {
            values: Vec::new(),
            quantile,
        }
    }
}

impl Accumulator for QuantileExactWeightedAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (val, weight) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if arr[0].is_null() || arr[1].is_null() {
                    return Ok(());
                }
                (&arr[0], &arr[1])
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY[value, weight]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY[value, weight]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        let val_f64 = numeric_value_to_f64(val)?.ok_or_else(|| Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: val.data_type().to_string(),
        })?;

        let weight_f64 = numeric_value_to_f64(weight)?.ok_or_else(|| Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: weight.data_type().to_string(),
        })?;

        self.values.push((val_f64, weight_f64));
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.values.extend_from_slice(&other.values);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let mut weighted_values = self.values.clone();
        weighted_values.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

        let total_weight: f64 = weighted_values.iter().map(|(_, w)| w).sum();
        let target_weight = total_weight * self.quantile;

        let mut cumulative_weight = 0.0;
        for (val, weight) in &weighted_values {
            cumulative_weight += weight;
            if cumulative_weight >= target_weight {
                return Ok(Value::float64(*val));
            }
        }

        weighted_values
            .last()
            .map(|(val, _)| Value::float64(*val))
            .ok_or_else(|| Error::internal("Weighted values unexpectedly empty".to_string()))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileExactWeightedFunction {
    quantile: f64,
}

impl Default for QuantileExactWeightedFunction {
    fn default() -> Self {
        Self { quantile: 0.5 }
    }
}

impl AggregateFunction for QuantileExactWeightedFunction {
    fn name(&self) -> &str {
        "QUANTILE_EXACT_WEIGHTED"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileExactWeightedAccumulator::new(self.quantile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileTimingAccumulator {
    values: Vec<f64>,
    quantile: f64,
}

impl Default for QuantileTimingAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileTimingAccumulator {
    pub fn new(quantile: f64) -> Self {
        Self {
            values: Vec::new(),
            quantile,
        }
    }
}

impl Accumulator for QuantileTimingAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.values.push(val.clamp(0.0, 30000.0));
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.values.extend_from_slice(&other.values);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        let index = (sorted.len() as f64 * self.quantile).floor() as usize;
        let index = index.min(sorted.len() - 1);

        Ok(Value::float64(sorted[index]))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileTimingFunction {
    quantile: f64,
}

impl Default for QuantileTimingFunction {
    fn default() -> Self {
        Self { quantile: 0.5 }
    }
}

impl AggregateFunction for QuantileTimingFunction {
    fn name(&self) -> &str {
        "QUANTILE_TIMING"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileTimingAccumulator::new(self.quantile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileTimingWeightedAccumulator {
    values: Vec<(f64, f64)>,
    quantile: f64,
}

impl Default for QuantileTimingWeightedAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileTimingWeightedAccumulator {
    pub fn new(quantile: f64) -> Self {
        Self {
            values: Vec::new(),
            quantile,
        }
    }
}

impl Accumulator for QuantileTimingWeightedAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (val, weight) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if arr[0].is_null() || arr[1].is_null() {
                    return Ok(());
                }
                (&arr[0], &arr[1])
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY[value, weight]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY[value, weight]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        let val_f64 = numeric_value_to_f64(val)?
            .unwrap_or(0.0)
            .clamp(0.0, 30000.0);
        let weight_f64 = numeric_value_to_f64(weight)?.unwrap_or(0.0);

        self.values.push((val_f64, weight_f64));
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.values.extend_from_slice(&other.values);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let mut weighted_values = self.values.clone();
        weighted_values.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

        let total_weight: f64 = weighted_values.iter().map(|(_, w)| w).sum();
        let target_weight = total_weight * self.quantile;

        let mut cumulative_weight = 0.0;
        for (val, weight) in &weighted_values {
            cumulative_weight += weight;
            if cumulative_weight >= target_weight {
                return Ok(Value::float64(*val));
            }
        }

        weighted_values
            .last()
            .map(|(val, _)| Value::float64(*val))
            .ok_or_else(|| Error::internal("Weighted values unexpectedly empty".to_string()))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileTimingWeightedFunction {
    quantile: f64,
}

impl Default for QuantileTimingWeightedFunction {
    fn default() -> Self {
        Self { quantile: 0.5 }
    }
}

impl AggregateFunction for QuantileTimingWeightedFunction {
    fn name(&self) -> &str {
        "QUANTILE_TIMING_WEIGHTED"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileTimingWeightedAccumulator::new(self.quantile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileTDigestAccumulator {
    digest: TDigest,
    quantile: f64,
}

impl Default for QuantileTDigestAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileTDigestAccumulator {
    pub fn new(quantile: f64) -> Self {
        Self {
            digest: TDigest::new(),
            quantile,
        }
    }
}

impl Accumulator for QuantileTDigestAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.digest.add(val);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.digest.merge(&other.digest);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        let mut digest_clone = self.digest.clone();
        Ok(Value::float64(digest_clone.quantile(self.quantile)))
    }

    fn reset(&mut self) {
        self.digest = TDigest::new();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileTDigestFunction {
    quantile: f64,
}

impl Default for QuantileTDigestFunction {
    fn default() -> Self {
        Self { quantile: 0.5 }
    }
}

impl AggregateFunction for QuantileTDigestFunction {
    fn name(&self) -> &str {
        "QUANTILE_TDIGEST"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileTDigestAccumulator::new(self.quantile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileTDigestWeightedAccumulator {
    digest: TDigest,
    quantile: f64,
}

impl Default for QuantileTDigestWeightedAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileTDigestWeightedAccumulator {
    pub fn new(quantile: f64) -> Self {
        Self {
            digest: TDigest::new(),
            quantile,
        }
    }
}

impl Accumulator for QuantileTDigestWeightedAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (val, weight) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if arr[0].is_null() || arr[1].is_null() {
                    return Ok(());
                }
                (&arr[0], &arr[1])
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY[value, weight]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY[value, weight]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        let val_f64 = numeric_value_to_f64(val)?.unwrap_or(0.0);
        let weight_f64 = numeric_value_to_f64(weight)?.unwrap_or(1.0);

        for _ in 0..(weight_f64 as i64).max(1) {
            self.digest.add(val_f64);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.digest.merge(&other.digest);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        let mut digest_clone = self.digest.clone();
        Ok(Value::float64(digest_clone.quantile(self.quantile)))
    }

    fn reset(&mut self) {
        self.digest = TDigest::new();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileTDigestWeightedFunction {
    quantile: f64,
}

impl Default for QuantileTDigestWeightedFunction {
    fn default() -> Self {
        Self { quantile: 0.5 }
    }
}

impl AggregateFunction for QuantileTDigestWeightedFunction {
    fn name(&self) -> &str {
        "QUANTILE_TDIGEST_WEIGHTED"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileTDigestWeightedAccumulator::new(self.quantile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantilesTimingAccumulator {
    values: Vec<f64>,
    quantiles: Vec<f64>,
}

impl Default for QuantilesTimingAccumulator {
    fn default() -> Self {
        Self::new(vec![0.5])
    }
}

impl QuantilesTimingAccumulator {
    pub fn new(quantiles: Vec<f64>) -> Self {
        Self {
            values: Vec::new(),
            quantiles,
        }
    }
}

impl Accumulator for QuantilesTimingAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.values.push(val.clamp(0.0, 30000.0));
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.values.extend_from_slice(&other.values);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            let nulls = vec![Value::null(); self.quantiles.len()];
            return Ok(Value::array(nulls));
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        let results: Vec<Value> = self
            .quantiles
            .iter()
            .map(|&q| {
                let index = (sorted.len() as f64 * q).floor() as usize;
                let index = index.min(sorted.len() - 1);
                Value::float64(sorted[index])
            })
            .collect();

        Ok(Value::array(results))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantilesTimingFunction {
    quantiles: Vec<f64>,
}

impl Default for QuantilesTimingFunction {
    fn default() -> Self {
        Self {
            quantiles: vec![0.5],
        }
    }
}

impl AggregateFunction for QuantilesTimingFunction {
    fn name(&self) -> &str {
        "QUANTILES_TIMING"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantilesTimingAccumulator::new(self.quantiles.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct QuantilesTDigestAccumulator {
    digest: TDigest,
    quantiles: Vec<f64>,
}

impl Default for QuantilesTDigestAccumulator {
    fn default() -> Self {
        Self::new(vec![0.5])
    }
}

impl QuantilesTDigestAccumulator {
    pub fn new(quantiles: Vec<f64>) -> Self {
        Self {
            digest: TDigest::new(),
            quantiles,
        }
    }
}

impl Accumulator for QuantilesTDigestAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.digest.add(val);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.digest.merge(&other.digest);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        let mut digest_clone = self.digest.clone();
        let results: Vec<Value> = self
            .quantiles
            .iter()
            .map(|&q| Value::float64(digest_clone.quantile(q)))
            .collect();
        Ok(Value::array(results))
    }

    fn reset(&mut self) {
        self.digest = TDigest::new();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantilesTDigestFunction {
    quantiles: Vec<f64>,
}

impl Default for QuantilesTDigestFunction {
    fn default() -> Self {
        Self {
            quantiles: vec![0.5],
        }
    }
}

impl AggregateFunction for QuantilesTDigestFunction {
    fn name(&self) -> &str {
        "QUANTILES_TDIGEST"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantilesTDigestAccumulator::new(self.quantiles.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileDeterministicAccumulator {
    values: Vec<(f64, u64)>,
    quantile: f64,
}

impl Default for QuantileDeterministicAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileDeterministicAccumulator {
    pub fn new(quantile: f64) -> Self {
        Self {
            values: Vec::new(),
            quantile: quantile.clamp(0.0, 1.0),
        }
    }

    fn hash_value(val: f64) -> u64 {
        val.to_bits()
    }
}

impl Accumulator for QuantileDeterministicAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let (Some(val), Some(det)) = (numeric_value_to_f64(&arr[0])?, arr[1].as_i64()) {
                    self.values.push((val, det as u64));
                }
            }
        } else if let Some(val) = numeric_value_to_f64(value)? {
            self.values.push((val, Self::hash_value(val)));
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.values.extend_from_slice(&other.values);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|(a, hash_a), (b, hash_b)| {
            hash_a
                .cmp(hash_b)
                .then_with(|| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        });

        let index = ((sorted.len() - 1) as f64 * self.quantile).round() as usize;
        Ok(Value::float64(sorted[index].0))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileDeterministicFunction {
    quantile: f64,
}

impl Default for QuantileDeterministicFunction {
    fn default() -> Self {
        Self { quantile: 0.5 }
    }
}

impl QuantileDeterministicFunction {
    pub fn new(quantile: f64) -> Self {
        Self { quantile }
    }
}

impl AggregateFunction for QuantileDeterministicFunction {
    fn name(&self) -> &str {
        "QUANTILE_DETERMINISTIC"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileDeterministicAccumulator::new(self.quantile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileBFloat16Accumulator {
    values: Vec<u16>,
    quantile: f64,
}

impl Default for QuantileBFloat16Accumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileBFloat16Accumulator {
    pub fn new(quantile: f64) -> Self {
        Self {
            values: Vec::new(),
            quantile: quantile.clamp(0.0, 1.0),
        }
    }

    fn f32_to_bf16(f: f32) -> u16 {
        (f.to_bits() >> 16) as u16
    }

    fn bf16_to_f32(bf: u16) -> f32 {
        f32::from_bits((bf as u32) << 16)
    }
}

impl Accumulator for QuantileBFloat16Accumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(num) = numeric_value_to_f64(value)? {
            let bf16 = Self::f32_to_bf16(num as f32);
            self.values.push(bf16);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for merge"))?;
        self.values.extend_from_slice(&other.values);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let mut sorted = self.values.clone();
        sorted.sort_unstable();

        let index = ((sorted.len() - 1) as f64 * self.quantile).round() as usize;
        let result = Self::bf16_to_f32(sorted[index]);
        Ok(Value::float64(result as f64))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileBFloat16Function {
    quantile: f64,
}

impl Default for QuantileBFloat16Function {
    fn default() -> Self {
        Self { quantile: 0.5 }
    }
}

impl QuantileBFloat16Function {
    pub fn new(quantile: f64) -> Self {
        Self { quantile }
    }
}

impl AggregateFunction for QuantileBFloat16Function {
    fn name(&self) -> &str {
        "QUANTILE_BFLOAT16"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileBFloat16Accumulator::new(self.quantile))
    }
}
