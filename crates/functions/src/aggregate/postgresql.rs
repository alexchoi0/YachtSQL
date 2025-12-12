use std::collections::HashMap;
use std::sync::LazyLock;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, RangeType, Value};

use crate::aggregate::{Accumulator, AggregateFunction};

static ARRAY_OF_UNKNOWN: LazyLock<Vec<DataType>> =
    LazyLock::new(|| vec![DataType::Array(Box::new(DataType::Unknown))]);

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
pub struct ArrayRemoveAccumulator {
    arrays: Vec<Vec<Value>>,
    remove_value: Value,
}

impl Default for ArrayRemoveAccumulator {
    fn default() -> Self {
        Self {
            arrays: Vec::new(),
            remove_value: Value::null(),
        }
    }
}

impl ArrayRemoveAccumulator {
    pub fn new(remove_value: Value) -> Self {
        Self {
            arrays: Vec::new(),
            remove_value,
        }
    }
}

impl Accumulator for ArrayRemoveAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            let filtered: Vec<Value> = arr
                .iter()
                .filter(|v| *v != &self.remove_value)
                .cloned()
                .collect();
            self.arrays.push(filtered);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::InternalError(
            "Merge not supported for ARRAY_REMOVE".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.arrays.is_empty() {
            return Ok(Value::array(Vec::new()));
        }

        let mut result = Vec::new();
        for arr in &self.arrays {
            result.extend(arr.clone());
        }
        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.arrays.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct ArrayRemoveFunction {
    remove_value: Value,
}

impl Default for ArrayRemoveFunction {
    fn default() -> Self {
        Self {
            remove_value: Value::null(),
        }
    }
}

impl ArrayRemoveFunction {
    pub fn new(remove_value: Value) -> Self {
        Self { remove_value }
    }
}

impl AggregateFunction for ArrayRemoveFunction {
    fn name(&self) -> &str {
        "ARRAY_REMOVE"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArrayRemoveAccumulator::new(self.remove_value.clone()))
    }
}

#[derive(Debug, Clone, Default)]
pub struct ArrayPositionsAccumulator {
    search_value: Value,
    result: Vec<i64>,
}

impl ArrayPositionsAccumulator {
    pub fn new(search_value: Value) -> Self {
        Self {
            search_value,
            result: Vec::new(),
        }
    }
}

impl Accumulator for ArrayPositionsAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            for (idx, val) in arr.iter().enumerate() {
                if val == &self.search_value {
                    self.result.push((idx + 1) as i64);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::InternalError(
            "Merge not supported for ARRAY_POSITIONS".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::array(
            self.result.iter().map(|&i| Value::int64(i)).collect(),
        ))
    }

    fn reset(&mut self) {
        self.result.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct ArrayPositionsFunction {
    search_value: Value,
}

impl Default for ArrayPositionsFunction {
    fn default() -> Self {
        Self {
            search_value: Value::null(),
        }
    }
}

impl ArrayPositionsFunction {
    pub fn new(search_value: Value) -> Self {
        Self { search_value }
    }
}

impl AggregateFunction for ArrayPositionsFunction {
    fn name(&self) -> &str {
        "ARRAY_POSITIONS"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Int64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArrayPositionsAccumulator::new(self.search_value.clone()))
    }
}

#[derive(Debug, Clone, Default)]
pub struct RegrAvgXAccumulator {
    x_values: Vec<f64>,
}

impl Accumulator for RegrAvgXAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let Some(x) = numeric_value_to_f64(&arr[1])? {
                    self.x_values.push(x);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<RegrAvgXAccumulator>() {
            self.x_values.extend(&other_acc.x_values);
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.x_values.is_empty() {
            return Ok(Value::null());
        }

        let sum: f64 = self.x_values.iter().sum();
        let avg = sum / self.x_values.len() as f64;
        Ok(Value::float64(avg))
    }

    fn reset(&mut self) {
        self.x_values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct RegrAvgXFunction;

impl AggregateFunction for RegrAvgXFunction {
    fn name(&self) -> &str {
        "REGR_AVGX"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64, DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RegrAvgXAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RegrAvgYAccumulator {
    y_values: Vec<f64>,
}

impl Accumulator for RegrAvgYAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let Some(y) = numeric_value_to_f64(&arr[0])? {
                    self.y_values.push(y);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<RegrAvgYAccumulator>() {
            self.y_values.extend(&other_acc.y_values);
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.y_values.is_empty() {
            return Ok(Value::null());
        }

        let sum: f64 = self.y_values.iter().sum();
        let avg = sum / self.y_values.len() as f64;
        Ok(Value::float64(avg))
    }

    fn reset(&mut self) {
        self.y_values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct RegrAvgYFunction;

impl AggregateFunction for RegrAvgYFunction {
    fn name(&self) -> &str {
        "REGR_AVGY"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64, DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RegrAvgYAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RegrCountAccumulator {
    count: i64,
}

impl Accumulator for RegrCountAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 && !arr[0].is_null() && !arr[1].is_null() {
                self.count += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<RegrCountAccumulator>() {
            self.count += other_acc.count;
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::int64(self.count))
    }

    fn reset(&mut self) {
        self.count = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct RegrCountFunction;

impl AggregateFunction for RegrCountFunction {
    fn name(&self) -> &str {
        "REGR_COUNT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64, DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RegrCountAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RegrSxxAccumulator {
    x_values: Vec<f64>,
}

impl Accumulator for RegrSxxAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let Some(x) = numeric_value_to_f64(&arr[1])? {
                    self.x_values.push(x);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<RegrSxxAccumulator>() {
            self.x_values.extend(&other_acc.x_values);
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.x_values.is_empty() {
            return Ok(Value::null());
        }

        let mean = self.x_values.iter().sum::<f64>() / self.x_values.len() as f64;
        let sxx: f64 = self.x_values.iter().map(|x| (x - mean).powi(2)).sum();
        Ok(Value::float64(sxx))
    }

    fn reset(&mut self) {
        self.x_values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct RegrSxxFunction;

impl AggregateFunction for RegrSxxFunction {
    fn name(&self) -> &str {
        "REGR_SXX"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64, DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RegrSxxAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RegrSyyAccumulator {
    y_values: Vec<f64>,
}

impl Accumulator for RegrSyyAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let Some(y) = numeric_value_to_f64(&arr[0])? {
                    self.y_values.push(y);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<RegrSyyAccumulator>() {
            self.y_values.extend(&other_acc.y_values);
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.y_values.is_empty() {
            return Ok(Value::null());
        }

        let mean = self.y_values.iter().sum::<f64>() / self.y_values.len() as f64;
        let syy: f64 = self.y_values.iter().map(|y| (y - mean).powi(2)).sum();
        Ok(Value::float64(syy))
    }

    fn reset(&mut self) {
        self.y_values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct RegrSyyFunction;

impl AggregateFunction for RegrSyyFunction {
    fn name(&self) -> &str {
        "REGR_SYY"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64, DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RegrSyyAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RegrSxyAccumulator {
    pairs: Vec<(f64, f64)>,
}

impl Accumulator for RegrSxyAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let (Some(y), Some(x)) = (
                    numeric_value_to_f64(&arr[0])?,
                    numeric_value_to_f64(&arr[1])?,
                ) {
                    self.pairs.push((x, y));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<RegrSxyAccumulator>() {
            self.pairs.extend(&other_acc.pairs);
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.pairs.is_empty() {
            return Ok(Value::null());
        }

        let n = self.pairs.len() as f64;
        let mean_x = self.pairs.iter().map(|(x, _)| x).sum::<f64>() / n;
        let mean_y = self.pairs.iter().map(|(_, y)| y).sum::<f64>() / n;

        let sxy: f64 = self
            .pairs
            .iter()
            .map(|(x, y)| (x - mean_x) * (y - mean_y))
            .sum();

        Ok(Value::float64(sxy))
    }

    fn reset(&mut self) {
        self.pairs.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct RegrSxyFunction;

impl AggregateFunction for RegrSxyFunction {
    fn name(&self) -> &str {
        "REGR_SXY"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64, DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RegrSxyAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ModeAccumulator {
    value_counts: HashMap<String, (i64, Value)>,
}

impl Accumulator for ModeAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if !value.is_null() {
            let key = format!("{:?}", value);
            self.value_counts
                .entry(key)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, value.clone()));
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<ModeAccumulator>() {
            for (key, (count, value)) in &other_acc.value_counts {
                self.value_counts
                    .entry(key.clone())
                    .and_modify(|(c, _)| *c += count)
                    .or_insert((*count, value.clone()));
            }
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.value_counts.is_empty() {
            return Ok(Value::null());
        }

        let max_count = self
            .value_counts
            .values()
            .map(|(count, _)| count)
            .max()
            .copied()
            .unwrap_or(0);
        let mode_value = self
            .value_counts
            .values()
            .find(|(count, _)| *count == max_count)
            .map(|(_, value)| value.clone())
            .unwrap_or_else(Value::null);

        Ok(mode_value)
    }

    fn reset(&mut self) {
        self.value_counts.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct ModeFunction;

impl AggregateFunction for ModeFunction {
    fn name(&self) -> &str {
        "MODE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            Ok(DataType::Unknown)
        } else {
            Ok(arg_types[0].clone())
        }
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ModeAccumulator::default())
    }
}

#[derive(Debug, Clone)]
pub struct PercentileContAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl Default for PercentileContAccumulator {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            percentile: 0.5,
        }
    }
}

impl PercentileContAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }
}

impl Accumulator for PercentileContAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let Some(val) = numeric_value_to_f64(&arr[0])? {
                    self.values.push(val);
                }

                if let Some(pct) = numeric_value_to_f64(&arr[1])? {
                    self.percentile = pct;
                }
                return Ok(());
            }
        }

        if let Some(val) = numeric_value_to_f64(value)? {
            self.values.push(val);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<PercentileContAccumulator>() {
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
            return Ok(Value::null());
        }

        if self.percentile < 0.0 || self.percentile > 1.0 {
            return Err(Error::InvalidOperation(format!(
                "PERCENTILE_CONT percentile must be between 0 and 1, got {}",
                self.percentile
            )));
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let position = self.percentile * (sorted.len() - 1) as f64;
        let lower_idx = position.floor() as usize;
        let upper_idx = position.ceil() as usize;
        let fraction = position - position.floor();

        let result = if lower_idx == upper_idx {
            sorted[lower_idx]
        } else {
            sorted[lower_idx] * (1.0 - fraction) + sorted[upper_idx] * fraction
        };

        Ok(Value::float64(result))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct PercentileContFunction {
    percentile: f64,
}

impl Default for PercentileContFunction {
    fn default() -> Self {
        Self { percentile: 0.5 }
    }
}

impl PercentileContFunction {
    pub fn new(percentile: f64) -> Self {
        Self { percentile }
    }
}

impl AggregateFunction for PercentileContFunction {
    fn name(&self) -> &str {
        "PERCENTILE_CONT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64, DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(PercentileContAccumulator::new(self.percentile))
    }
}

#[derive(Debug, Clone)]
pub struct PercentileDiscAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl Default for PercentileDiscAccumulator {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            percentile: 0.5,
        }
    }
}

impl PercentileDiscAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }
}

impl Accumulator for PercentileDiscAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let Some(val) = numeric_value_to_f64(&arr[0])? {
                    self.values.push(val);
                }

                if let Some(pct) = numeric_value_to_f64(&arr[1])? {
                    self.percentile = pct;
                }
                return Ok(());
            }
        }

        if let Some(val) = numeric_value_to_f64(value)? {
            self.values.push(val);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<PercentileDiscAccumulator>() {
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
            return Ok(Value::null());
        }

        if self.percentile < 0.0 || self.percentile > 1.0 {
            return Err(Error::InvalidOperation(format!(
                "PERCENTILE_DISC percentile must be between 0 and 1, got {}",
                self.percentile
            )));
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let position = (self.percentile * (sorted.len() - 1) as f64).round() as usize;
        Ok(Value::float64(sorted[position]))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct PercentileDiscFunction {
    percentile: f64,
}

impl Default for PercentileDiscFunction {
    fn default() -> Self {
        Self { percentile: 0.5 }
    }
}

impl PercentileDiscFunction {
    pub fn new(percentile: f64) -> Self {
        Self { percentile }
    }
}

impl AggregateFunction for PercentileDiscFunction {
    fn name(&self) -> &str {
        "PERCENTILE_DISC"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64, DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(PercentileDiscAccumulator::new(self.percentile))
    }
}

#[derive(Debug, Clone, Default)]
pub struct FirstValueAccumulator {
    first_value: Option<Value>,
}

impl Accumulator for FirstValueAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if self.first_value.is_none() && !value.is_null() {
            self.first_value = Some(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<FirstValueAccumulator>() {
            if self.first_value.is_none() {
                self.first_value = other_acc.first_value.clone();
            }
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.first_value.clone().unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.first_value = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct FirstValueFunction;

impl AggregateFunction for FirstValueFunction {
    fn name(&self) -> &str {
        "FIRST_VALUE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Unknown)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(FirstValueAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct LastValueAccumulator {
    last_value: Option<Value>,
}

impl Accumulator for LastValueAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if !value.is_null() {
            self.last_value = Some(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<LastValueAccumulator>() {
            if other_acc.last_value.is_some() {
                self.last_value = other_acc.last_value.clone();
            }
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.last_value.clone().unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.last_value = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct LastValueFunction;

impl AggregateFunction for LastValueFunction {
    fn name(&self) -> &str {
        "LAST_VALUE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Unknown)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(LastValueAccumulator::default())
    }
}

#[derive(Debug, Clone)]
pub struct NthValueAccumulator {
    values: Vec<Value>,
    n: usize,
}

impl Default for NthValueAccumulator {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            n: 1,
        }
    }
}

impl NthValueAccumulator {
    pub fn new(n: usize) -> Self {
        Self {
            values: Vec::new(),
            n,
        }
    }
}

impl Accumulator for NthValueAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if !value.is_null() {
            self.values.push(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<NthValueAccumulator>() {
            self.values.extend(other_acc.values.clone());
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.n == 0 || self.n > self.values.len() {
            return Ok(Value::null());
        }
        Ok(self.values[self.n - 1].clone())
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct NthValueFunction {
    n: usize,
}

impl Default for NthValueFunction {
    fn default() -> Self {
        Self { n: 1 }
    }
}

impl NthValueFunction {
    pub fn new(n: usize) -> Self {
        Self { n }
    }
}

impl AggregateFunction for NthValueFunction {
    fn name(&self) -> &str {
        "NTH_VALUE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Unknown)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(NthValueAccumulator::new(self.n))
    }
}

#[derive(Debug, Clone)]
pub struct LagAccumulator {
    values: Vec<Value>,
    offset: usize,
    default_value: Value,
}

impl Default for LagAccumulator {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            offset: 1,
            default_value: Value::null(),
        }
    }
}

impl LagAccumulator {
    pub fn new(offset: usize, default_value: Value) -> Self {
        Self {
            values: Vec::new(),
            offset,
            default_value,
        }
    }
}

impl Accumulator for LagAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        self.values.push(value.clone());
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<LagAccumulator>() {
            self.values.extend(other_acc.values.clone());
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.len() <= self.offset {
            return Ok(self.default_value.clone());
        }
        let idx = self.values.len() - self.offset - 1;
        Ok(self.values[idx].clone())
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct LagFunction {
    offset: usize,
    default_value: Value,
}

impl Default for LagFunction {
    fn default() -> Self {
        Self {
            offset: 1,
            default_value: Value::null(),
        }
    }
}

impl LagFunction {
    pub fn new(offset: usize, default_value: Value) -> Self {
        Self {
            offset,
            default_value,
        }
    }
}

impl AggregateFunction for LagFunction {
    fn name(&self) -> &str {
        "LAG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Unknown)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(LagAccumulator::new(self.offset, self.default_value.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct LeadAccumulator {
    values: Vec<Value>,
    offset: usize,
    default_value: Value,
    current_index: usize,
}

impl Default for LeadAccumulator {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            offset: 1,
            default_value: Value::null(),
            current_index: 0,
        }
    }
}

impl LeadAccumulator {
    pub fn new(offset: usize, default_value: Value) -> Self {
        Self {
            values: Vec::new(),
            offset,
            default_value,
            current_index: 0,
        }
    }
}

impl Accumulator for LeadAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        self.values.push(value.clone());
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<LeadAccumulator>() {
            self.values.extend(other_acc.values.clone());
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        let target_idx = self.current_index + self.offset;
        if target_idx >= self.values.len() {
            return Ok(self.default_value.clone());
        }
        Ok(self.values[target_idx].clone())
    }

    fn reset(&mut self) {
        self.values.clear();
        self.current_index = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct LeadFunction {
    offset: usize,
    default_value: Value,
}

impl Default for LeadFunction {
    fn default() -> Self {
        Self {
            offset: 1,
            default_value: Value::null(),
        }
    }
}

impl LeadFunction {
    pub fn new(offset: usize, default_value: Value) -> Self {
        Self {
            offset,
            default_value,
        }
    }
}

impl AggregateFunction for LeadFunction {
    fn name(&self) -> &str {
        "LEAD"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Unknown)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(LeadAccumulator::new(
            self.offset,
            self.default_value.clone(),
        ))
    }
}

#[derive(Debug, Clone, Default)]
pub struct RangeAggAccumulator {
    ranges: Vec<Value>,
}

impl Accumulator for RangeAggAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if !value.is_null() && value.as_range().is_some() {
            self.ranges.push(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<RangeAggAccumulator>() {
            self.ranges.extend(other_acc.ranges.clone());
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.ranges.is_empty() {
            return Ok(Value::null());
        }

        let mut result = self.ranges[0].clone();
        for range in self.ranges.iter().skip(1) {
            match crate::range::range_union(&result, range) {
                Ok(merged) => result = merged,
                Err(_) => continue,
            }
        }
        Ok(result)
    }

    fn reset(&mut self) {
        self.ranges.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RangeAggFunction;

impl AggregateFunction for RangeAggFunction {
    fn name(&self) -> &str {
        "RANGE_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Range(RangeType::Int4Range)]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            Ok(DataType::Range(RangeType::Int4Range))
        } else {
            Ok(arg_types[0].clone())
        }
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RangeAggAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RangeIntersectAggAccumulator {
    ranges: Vec<Value>,
}

impl Accumulator for RangeIntersectAggAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if !value.is_null() && value.as_range().is_some() {
            self.ranges.push(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other
            .as_any()
            .downcast_ref::<RangeIntersectAggAccumulator>()
        {
            self.ranges.extend(other_acc.ranges.clone());
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.ranges.is_empty() {
            return Ok(Value::null());
        }

        let mut result = self.ranges[0].clone();
        for range in self.ranges.iter().skip(1) {
            match crate::range::range_intersection(&result, range) {
                Ok(intersected) => result = intersected,
                Err(_) => return Ok(Value::null()),
            }
        }
        Ok(result)
    }

    fn reset(&mut self) {
        self.ranges.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RangeIntersectAggFunction;

impl AggregateFunction for RangeIntersectAggFunction {
    fn name(&self) -> &str {
        "RANGE_INTERSECT_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Range(RangeType::Int4Range)]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            Ok(DataType::Range(RangeType::Int4Range))
        } else {
            Ok(arg_types[0].clone())
        }
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RangeIntersectAggAccumulator::default())
    }
}
