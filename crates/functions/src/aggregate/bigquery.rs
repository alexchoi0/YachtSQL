use std::collections::HashSet;
use std::sync::LazyLock;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

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

#[derive(Debug, Clone, Default)]
pub struct HllCountInitAccumulator {
    values: HashSet<String>,
    precision: u8,
}

impl HllCountInitAccumulator {
    pub fn new(precision: u8) -> Self {
        Self {
            values: HashSet::new(),
            precision: precision.clamp(10, 24),
        }
    }
}

impl Accumulator for HllCountInitAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value != &Value::null() {
            let key = format!("{:?}", value);
            self.values.insert(key);
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<HllCountInitAccumulator>() {
            self.values.extend(other_acc.values.clone());
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        let sketch_data = format!("HLL_SKETCH:p{}:n{}", self.precision, self.values.len());
        Ok(Value::string(sketch_data))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct HllCountInitFunction {
    precision: u8,
}

impl Default for HllCountInitFunction {
    fn default() -> Self {
        Self { precision: 15 }
    }
}

impl HllCountInitFunction {
    pub fn new(precision: u8) -> Self {
        Self { precision }
    }
}

impl AggregateFunction for HllCountInitFunction {
    fn name(&self) -> &str {
        "HLL_COUNT_INIT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(HllCountInitAccumulator::new(self.precision))
    }
}

#[derive(Debug, Clone, Default)]
pub struct HllCountMergeAccumulator {
    sketches: Vec<String>,
}

impl Accumulator for HllCountMergeAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(sketch) = value.as_str()
            && sketch.starts_with("HLL_SKETCH:")
        {
            self.sketches.push(sketch.to_string());
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<HllCountMergeAccumulator>() {
            self.sketches.extend(other_acc.sketches.clone());
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.sketches.is_empty() {
            return Ok(Value::string("HLL_SKETCH:p15:n0".to_string()));
        }

        let mut total_count = 0;
        let mut max_precision = 10;

        for sketch in &self.sketches {
            if let Some(parts) = sketch.strip_prefix("HLL_SKETCH:p") {
                let segments: Vec<&str> = parts.split(':').collect();
                if segments.len() == 2 {
                    if let Ok(p) = segments[0].parse::<u8>() {
                        max_precision = max_precision.max(p);
                    }
                    if let Some(count_str) = segments[1].strip_prefix('n')
                        && let Ok(count) = count_str.parse::<usize>()
                    {
                        total_count += count;
                    }
                }
            }
        }

        Ok(Value::string(format!(
            "HLL_SKETCH:p{}:n{}",
            max_precision, total_count
        )))
    }

    fn reset(&mut self) {
        self.sketches.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct HllCountMergeFunction;

impl AggregateFunction for HllCountMergeFunction {
    fn name(&self) -> &str {
        "HLL_COUNT_MERGE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::String]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(HllCountMergeAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct HllCountExtractAccumulator {
    sketch: Option<String>,
}

impl Accumulator for HllCountExtractAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(sketch) = value.as_str()
            && sketch.starts_with("HLL_SKETCH:")
        {
            self.sketch = Some(sketch.to_string());
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<HllCountExtractAccumulator>() {
            if other_acc.sketch.is_some() {
                self.sketch = other_acc.sketch.clone();
            }
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if let Some(sketch) = &self.sketch
            && let Some(parts) = sketch.strip_prefix("HLL_SKETCH:p")
        {
            let segments: Vec<&str> = parts.split(':').collect();
            if segments.len() == 2
                && let Some(count_str) = segments[1].strip_prefix('n')
                && let Ok(count) = count_str.parse::<i64>()
            {
                return Ok(Value::int64(count));
            }
        }
        Ok(Value::int64(0))
    }

    fn reset(&mut self) {
        self.sketch = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct HllCountExtractFunction;

impl AggregateFunction for HllCountExtractFunction {
    fn name(&self) -> &str {
        "HLL_COUNT_EXTRACT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::String]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(HllCountExtractAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct CorrAccumulator {
    count: usize,
    sum_x: f64,
    sum_y: f64,
    sum_xx: f64,
    sum_yy: f64,
    sum_xy: f64,
}

impl Accumulator for CorrAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array()
            && arr.len() == 2
            && let (Some(x), Some(y)) = (
                numeric_value_to_f64(&arr[0])?,
                numeric_value_to_f64(&arr[1])?,
            )
        {
            self.count += 1;
            self.sum_x += x;
            self.sum_y += y;
            self.sum_xx += x * x;
            self.sum_yy += y * y;
            self.sum_xy += x * y;
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<CorrAccumulator>() {
            self.count += other_acc.count;
            self.sum_x += other_acc.sum_x;
            self.sum_y += other_acc.sum_y;
            self.sum_xx += other_acc.sum_xx;
            self.sum_yy += other_acc.sum_yy;
            self.sum_xy += other_acc.sum_xy;
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.count < 2 {
            return Ok(Value::null());
        }

        let n = self.count as f64;
        let numerator = n * self.sum_xy - self.sum_x * self.sum_y;
        let denom_x = n * self.sum_xx - self.sum_x * self.sum_x;
        let denom_y = n * self.sum_yy - self.sum_y * self.sum_y;

        if denom_x.abs() < f64::EPSILON || denom_y.abs() < f64::EPSILON {
            return Ok(Value::null());
        }

        let correlation = numerator / (denom_x * denom_y).sqrt();
        Ok(Value::float64(correlation))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.sum_x = 0.0;
        self.sum_y = 0.0;
        self.sum_xx = 0.0;
        self.sum_yy = 0.0;
        self.sum_xy = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct CorrFunction;

impl AggregateFunction for CorrFunction {
    fn name(&self) -> &str {
        "CORR"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64, DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CorrAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct StddevAccumulator {
    count: usize,
    mean: f64,
    m2: f64,
}

impl Accumulator for StddevAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.count += 1;
            let delta = val - self.mean;
            self.mean += delta / self.count as f64;
            let delta2 = val - self.mean;
            self.m2 += delta * delta2;
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<StddevAccumulator>() {
            if other_acc.count == 0 {
                return Ok(());
            }

            let combined_count = self.count + other_acc.count;
            let delta = other_acc.mean - self.mean;
            let combined_mean = (self.count as f64 * self.mean
                + other_acc.count as f64 * other_acc.mean)
                / combined_count as f64;
            let combined_m2 = self.m2
                + other_acc.m2
                + delta * delta * (self.count * other_acc.count) as f64 / combined_count as f64;

            self.count = combined_count;
            self.mean = combined_mean;
            self.m2 = combined_m2;
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.count < 2 {
            return Ok(Value::null());
        }
        let variance = self.m2 / (self.count - 1) as f64;
        Ok(Value::float64(variance.sqrt()))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct StddevFunction;

impl AggregateFunction for StddevFunction {
    fn name(&self) -> &str {
        "STDDEV"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(StddevAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct VarianceAccumulator {
    count: usize,
    mean: f64,
    m2: f64,
}

impl Accumulator for VarianceAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.count += 1;
            let delta = val - self.mean;
            self.mean += delta / self.count as f64;
            let delta2 = val - self.mean;
            self.m2 += delta * delta2;
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<VarianceAccumulator>() {
            if other_acc.count == 0 {
                return Ok(());
            }

            let combined_count = self.count + other_acc.count;
            let delta = other_acc.mean - self.mean;
            let combined_mean = (self.count as f64 * self.mean
                + other_acc.count as f64 * other_acc.mean)
                / combined_count as f64;
            let combined_m2 = self.m2
                + other_acc.m2
                + delta * delta * (self.count * other_acc.count) as f64 / combined_count as f64;

            self.count = combined_count;
            self.mean = combined_mean;
            self.m2 = combined_m2;
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        if self.count < 2 {
            return Ok(Value::null());
        }
        let variance = self.m2 / (self.count - 1) as f64;
        Ok(Value::float64(variance))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct VarianceFunction;

impl AggregateFunction for VarianceFunction {
    fn name(&self) -> &str {
        "VARIANCE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Float64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(VarianceAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct AnyValueAccumulator {
    value: Option<Value>,
}

impl Accumulator for AnyValueAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if self.value.is_none() && value != &Value::null() {
            self.value = Some(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<AnyValueAccumulator>() {
            if self.value.is_none() {
                self.value = other_acc.value.clone();
            }
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.value.clone().unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.value = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct AnyValueFunction;

impl AggregateFunction for AnyValueFunction {
    fn name(&self) -> &str {
        "ANY_VALUE"
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
        Box::new(AnyValueAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ArrayAggDistinctAccumulator {
    values: Vec<Value>,
    distinct: bool,
}

impl ArrayAggDistinctAccumulator {
    pub fn new(distinct: bool) -> Self {
        Self {
            values: Vec::new(),
            distinct,
        }
    }
}

impl Accumulator for ArrayAggDistinctAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value != &Value::null() {
            if self.distinct {
                if !self.values.contains(value) {
                    self.values.push(value.clone());
                }
            } else {
                self.values.push(value.clone());
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<ArrayAggDistinctAccumulator>() {
            for value in &other_acc.values {
                if self.distinct {
                    if !self.values.contains(value) {
                        self.values.push(value.clone());
                    }
                } else {
                    self.values.push(value.clone());
                }
            }
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::array(self.values.clone()))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default)]
pub struct ArrayAggDistinctFunction {
    distinct: bool,
}

impl ArrayAggDistinctFunction {
    pub fn new(distinct: bool) -> Self {
        Self { distinct }
    }
}

impl AggregateFunction for ArrayAggDistinctFunction {
    fn name(&self) -> &str {
        "ARRAY_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            Ok(DataType::Array(Box::new(DataType::Unknown)))
        } else {
            Ok(DataType::Array(Box::new(arg_types[0].clone())))
        }
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArrayAggDistinctAccumulator::new(self.distinct))
    }
}

#[derive(Debug, Clone, Default)]
pub struct ArrayConcatAggAccumulator {
    result: Vec<Value>,
}

impl Accumulator for ArrayConcatAggAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            self.result.extend(arr.iter().cloned());
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other.as_any().downcast_ref::<ArrayConcatAggAccumulator>() {
            self.result.extend(other_acc.result.clone());
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::array(self.result.clone()))
    }

    fn reset(&mut self) {
        self.result.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct ArrayConcatAggFunction;

impl AggregateFunction for ArrayConcatAggFunction {
    fn name(&self) -> &str {
        "ARRAY_CONCAT_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArrayConcatAggAccumulator::default())
    }
}

#[derive(Debug, Clone)]
pub struct StringAggDistinctAccumulator {
    strings: Vec<String>,
    delimiter: String,
    distinct: bool,
}

impl Default for StringAggDistinctAccumulator {
    fn default() -> Self {
        Self {
            strings: Vec::new(),
            delimiter: ",".to_string(),
            distinct: false,
        }
    }
}

impl StringAggDistinctAccumulator {
    pub fn new(delimiter: String, distinct: bool) -> Self {
        Self {
            strings: Vec::new(),
            delimiter,
            distinct,
        }
    }
}

impl Accumulator for StringAggDistinctAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(s) = value.as_str() {
            if self.distinct {
                if !self.strings.contains(&s.to_string()) {
                    self.strings.push(s.to_string());
                }
            } else {
                self.strings.push(s.to_string());
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_acc) = other
            .as_any()
            .downcast_ref::<StringAggDistinctAccumulator>()
        {
            for s in &other_acc.strings {
                if self.distinct {
                    if !self.strings.contains(s) {
                        self.strings.push(s.clone());
                    }
                } else {
                    self.strings.push(s.clone());
                }
            }
            Ok(())
        } else {
            Err(Error::InternalError(
                "Cannot merge different accumulator types".to_string(),
            ))
        }
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::string(self.strings.join(&self.delimiter)))
    }

    fn reset(&mut self) {
        self.strings.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct StringAggDistinctFunction {
    delimiter: String,
    distinct: bool,
}

impl Default for StringAggDistinctFunction {
    fn default() -> Self {
        Self {
            delimiter: ",".to_string(),
            distinct: false,
        }
    }
}

impl StringAggDistinctFunction {
    pub fn new(delimiter: String, distinct: bool) -> Self {
        Self {
            delimiter,
            distinct,
        }
    }
}

impl AggregateFunction for StringAggDistinctFunction {
    fn name(&self) -> &str {
        "STRING_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::String]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(StringAggDistinctAccumulator::new(
            self.delimiter.clone(),
            self.distinct,
        ))
    }
}
