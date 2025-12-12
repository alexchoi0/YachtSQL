use std::cmp::Ordering;
use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

use super::{Accumulator, AggregateFunction};

pub mod common;
pub mod quantile;
pub mod top_k;
pub mod uniq;

pub use quantile::*;
pub use top_k::*;
pub use uniq::*;

use self::common::*;

#[derive(Debug, Clone)]
pub struct ArgMinAccumulator {
    min_arg: Option<f64>,
    value_at_min: Option<Value>,
}

impl Default for ArgMinAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl ArgMinAccumulator {
    pub fn new() -> Self {
        Self {
            min_arg: None,
            value_at_min: None,
        }
    }
}

impl Accumulator for ArgMinAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (arg_to_return, val_to_compare) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if arr[1].is_null() {
                    return Ok(());
                }
                (&arr[0], &arr[1])
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY with 2 values [arg, value]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY with 2 values [arg, value]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        let val_f64 = numeric_value_to_f64(val_to_compare)?.ok_or_else(|| Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: "NULL".to_string(),
        })?;

        match self.min_arg {
            None => {
                self.min_arg = Some(val_f64);
                self.value_at_min = Some(arg_to_return.clone());
            }
            Some(current_min) if val_f64 < current_min => {
                self.min_arg = Some(val_f64);
                self.value_at_min = Some(arg_to_return.clone());
            }
            _ => {}
        }

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ARG_MIN merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.value_at_min.clone().unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.min_arg = None;
        self.value_at_min = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ArgMinFunction;

impl AggregateFunction for ArgMinFunction {
    fn name(&self) -> &str {
        "ARG_MIN"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::String))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArgMinAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct ArgMaxAccumulator {
    max_arg: Option<f64>,
    value_at_max: Option<Value>,
}

impl Default for ArgMaxAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl ArgMaxAccumulator {
    pub fn new() -> Self {
        Self {
            max_arg: None,
            value_at_max: None,
        }
    }
}

impl Accumulator for ArgMaxAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (arg_to_return, val_to_compare) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if arr[1].is_null() {
                    return Ok(());
                }
                (&arr[0], &arr[1])
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY with 2 values [arg, value]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY with 2 values [arg, value]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        let val_f64 = numeric_value_to_f64(val_to_compare)?.ok_or_else(|| Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: "NULL".to_string(),
        })?;

        match self.max_arg {
            None => {
                self.max_arg = Some(val_f64);
                self.value_at_max = Some(arg_to_return.clone());
            }
            Some(current_max) if val_f64 > current_max => {
                self.max_arg = Some(val_f64);
                self.value_at_max = Some(arg_to_return.clone());
            }
            _ => {}
        }

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ARG_MAX merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.value_at_max.clone().unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.max_arg = None;
        self.value_at_max = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ArgMaxFunction;

impl AggregateFunction for ArgMaxFunction {
    fn name(&self) -> &str {
        "ARG_MAX"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::String))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArgMaxAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupArrayAccumulator {
    values: Vec<Value>,
}

impl Default for GroupArrayAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupArrayAccumulator {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }
}

impl Accumulator for GroupArrayAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        self.values.push(value.clone());
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_ARRAY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::array(vec![]));
        }
        Ok(Value::array(self.values.clone()))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupArrayFunction;

impl AggregateFunction for GroupArrayFunction {
    fn name(&self) -> &str {
        "GROUP_ARRAY"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupArrayAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupUniqArrayAccumulator {
    seen: std::collections::HashSet<String>,
    values: Vec<Value>,
}

impl Default for GroupUniqArrayAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupUniqArrayAccumulator {
    pub fn new() -> Self {
        Self {
            seen: std::collections::HashSet::new(),
            values: Vec::new(),
        }
    }
}

impl Accumulator for GroupUniqArrayAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let key = value_to_string(value);
        if self.seen.insert(key) {
            self.values.push(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_UNIQ_ARRAY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::array(self.values.clone()))
    }

    fn reset(&mut self) {
        self.seen.clear();
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupUniqArrayFunction;

impl AggregateFunction for GroupUniqArrayFunction {
    fn name(&self) -> &str {
        "GROUP_UNIQ_ARRAY"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupUniqArrayAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct AnyAccumulator {
    value: Option<Value>,
}

impl Default for AnyAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyAccumulator {
    pub fn new() -> Self {
        Self { value: None }
    }
}

impl Accumulator for AnyAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if self.value.is_none() && !value.is_null() {
            self.value = Some(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ANY merge not implemented".to_string(),
        ))
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

#[derive(Debug, Default, Clone, Copy)]
pub struct AnyFunction;

impl AggregateFunction for AnyFunction {
    fn name(&self) -> &str {
        "ANY"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::String))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AnyAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupBitAndAccumulator {
    result: Option<i64>,
}

impl Default for GroupBitAndAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupBitAndAccumulator {
    pub fn new() -> Self {
        Self { result: None }
    }
}

impl Accumulator for GroupBitAndAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(n) = value.as_i64() {
            self.result = Some(match self.result {
                None => n,
                Some(prev) => prev & n,
            });
            Ok(())
        } else {
            Err(Error::TypeMismatch {
                expected: "INT64".to_string(),
                actual: value.data_type().to_string(),
            })
        }
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_BIT_AND merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.result.map(Value::int64).unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.result = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupBitAndFunction;

impl AggregateFunction for GroupBitAndFunction {
    fn name(&self) -> &str {
        "GROUP_BIT_AND"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Int64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupBitAndAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupBitOrAccumulator {
    result: Option<i64>,
}

impl Default for GroupBitOrAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupBitOrAccumulator {
    pub fn new() -> Self {
        Self { result: None }
    }
}

impl Accumulator for GroupBitOrAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(n) = value.as_i64() {
            self.result = Some(match self.result {
                None => n,
                Some(prev) => prev | n,
            });
            Ok(())
        } else {
            Err(Error::TypeMismatch {
                expected: "INT64".to_string(),
                actual: value.data_type().to_string(),
            })
        }
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_BIT_OR merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.result.map(Value::int64).unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.result = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupBitOrFunction;

impl AggregateFunction for GroupBitOrFunction {
    fn name(&self) -> &str {
        "GROUP_BIT_OR"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Int64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupBitOrAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupBitXorAccumulator {
    result: Option<i64>,
}

impl Default for GroupBitXorAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupBitXorAccumulator {
    pub fn new() -> Self {
        Self { result: None }
    }
}

impl Accumulator for GroupBitXorAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(n) = value.as_i64() {
            self.result = Some(match self.result {
                None => n,
                Some(prev) => prev ^ n,
            });
            Ok(())
        } else {
            Err(Error::TypeMismatch {
                expected: "INT64".to_string(),
                actual: value.data_type().to_string(),
            })
        }
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_BIT_XOR merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.result.map(Value::int64).unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.result = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupBitXorFunction;

impl AggregateFunction for GroupBitXorFunction {
    fn name(&self) -> &str {
        "GROUP_BIT_XOR"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Int64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupBitXorAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct AnyLastAccumulator {
    value: Option<Value>,
}

impl Default for AnyLastAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyLastAccumulator {
    pub fn new() -> Self {
        Self { value: None }
    }
}

impl Accumulator for AnyLastAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if !value.is_null() {
            self.value = Some(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ANY_LAST merge not implemented".to_string(),
        ))
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

#[derive(Debug, Default, Clone, Copy)]
pub struct AnyLastFunction;

impl AggregateFunction for AnyLastFunction {
    fn name(&self) -> &str {
        "ANY_LAST"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::String))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AnyLastAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct AnyHeavyAccumulator {
    counts: HashMap<String, i64>,
    threshold: i64,
}

impl Default for AnyHeavyAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyHeavyAccumulator {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
            threshold: 10,
        }
    }
}

impl Accumulator for AnyHeavyAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let key = value_to_string(value);
        *self.counts.entry(key).or_insert(0) += 1;
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ANY_HEAVY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.counts.is_empty() {
            return Ok(Value::null());
        }

        let max_count = self.counts.values().max().copied().unwrap_or(0);
        if max_count < self.threshold {
            return Ok(Value::null());
        }

        for (key, count) in &self.counts {
            if *count == max_count {
                return Ok(Value::string(key.clone()));
            }
        }

        Ok(Value::null())
    }

    fn reset(&mut self) {
        self.counts.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AnyHeavyFunction;

impl AggregateFunction for AnyHeavyFunction {
    fn name(&self) -> &str {
        "ANY_HEAVY"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::String))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AnyHeavyAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct SumWithOverflowAccumulator {
    sum: i64,
}

impl Default for SumWithOverflowAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SumWithOverflowAccumulator {
    pub fn new() -> Self {
        Self { sum: 0 }
    }
}

impl Accumulator for SumWithOverflowAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.sum = self.sum.wrapping_add(val as i64);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "SUM_WITH_OVERFLOW merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::int64(self.sum))
    }

    fn reset(&mut self) {
        self.sum = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SumWithOverflowFunction;

impl AggregateFunction for SumWithOverflowFunction {
    fn name(&self) -> &str {
        "SUM_WITH_OVERFLOW"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumWithOverflowAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupArrayInsertAtAccumulator {
    elements: HashMap<i64, Value>,
}

impl Default for GroupArrayInsertAtAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupArrayInsertAtAccumulator {
    pub fn new() -> Self {
        Self {
            elements: HashMap::new(),
        }
    }
}

impl Accumulator for GroupArrayInsertAtAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (position, val) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                (&arr[0], &arr[1])
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY[position, value]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY[position, value]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        if let Some(pos) = position.as_i64() {
            self.elements.insert(pos, val.clone());
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_ARRAY_INSERTAT merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.elements.is_empty() {
            return Ok(Value::array(Vec::new()));
        }

        let max_idx = *self.elements.keys().max().unwrap_or(&0);
        let mut result = vec![Value::null(); (max_idx + 1) as usize];

        for (idx, val) in &self.elements {
            if *idx >= 0 && (*idx as usize) < result.len() {
                result[*idx as usize] = val.clone();
            }
        }

        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.elements.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupArrayInsertAtFunction;

impl AggregateFunction for GroupArrayInsertAtFunction {
    fn name(&self) -> &str {
        "GROUP_ARRAY_INSERTAT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupArrayInsertAtAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupArrayMovingAvgAccumulator {
    values: Vec<f64>,
    window_size: usize,
}

impl Default for GroupArrayMovingAvgAccumulator {
    fn default() -> Self {
        Self::new(3)
    }
}

impl GroupArrayMovingAvgAccumulator {
    pub fn new(window_size: usize) -> Self {
        Self {
            values: Vec::new(),
            window_size,
        }
    }
}

impl Accumulator for GroupArrayMovingAvgAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.values.push(val);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_ARRAY_MOVING_AVG merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::array(Vec::new()));
        }

        let mut result = Vec::new();
        for i in 0..self.values.len() {
            let start = i.saturating_sub(self.window_size - 1);
            let window = &self.values[start..=i];
            let avg = window.iter().sum::<f64>() / window.len() as f64;
            result.push(Value::float64(avg));
        }

        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct GroupArrayMovingAvgFunction {
    window_size: usize,
}

impl Default for GroupArrayMovingAvgFunction {
    fn default() -> Self {
        Self { window_size: 3 }
    }
}

impl AggregateFunction for GroupArrayMovingAvgFunction {
    fn name(&self) -> &str {
        "GROUP_ARRAY_MOVING_AVG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupArrayMovingAvgAccumulator::new(self.window_size))
    }
}

#[derive(Debug, Clone)]
pub struct GroupArrayMovingSumAccumulator {
    values: Vec<f64>,
    window_size: usize,
}

impl Default for GroupArrayMovingSumAccumulator {
    fn default() -> Self {
        Self::new(3)
    }
}

impl GroupArrayMovingSumAccumulator {
    pub fn new(window_size: usize) -> Self {
        Self {
            values: Vec::new(),
            window_size,
        }
    }
}

impl Accumulator for GroupArrayMovingSumAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            self.values.push(val);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_ARRAY_MOVING_SUM merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::array(Vec::new()));
        }

        let mut result = Vec::new();
        for i in 0..self.values.len() {
            let start = i.saturating_sub(self.window_size - 1);
            let window = &self.values[start..=i];
            let sum = window.iter().sum::<f64>();
            result.push(Value::float64(sum));
        }

        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct GroupArrayMovingSumFunction {
    window_size: usize,
}

impl Default for GroupArrayMovingSumFunction {
    fn default() -> Self {
        Self { window_size: 3 }
    }
}

impl AggregateFunction for GroupArrayMovingSumFunction {
    fn name(&self) -> &str {
        "GROUP_ARRAY_MOVING_SUM"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupArrayMovingSumAccumulator::new(self.window_size))
    }
}

#[derive(Debug, Clone)]
pub struct SumMapAccumulator {
    sums: HashMap<String, f64>,
}

impl Default for SumMapAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SumMapAccumulator {
    pub fn new() -> Self {
        Self {
            sums: HashMap::new(),
        }
    }
}

impl Accumulator for SumMapAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (keys, values) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let (Some(k), Some(v)) = (arr[0].as_array(), arr[1].as_array()) {
                    (k, v)
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "ARRAY[ARRAY, ARRAY]".to_string(),
                        actual: value.data_type().to_string(),
                    });
                }
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY[keys, values]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY[keys, values]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        for (key, val) in keys.iter().zip(values.iter()) {
            let key_str = value_to_string(key);
            if let Some(val_f64) = numeric_value_to_f64(val)? {
                *self.sums.entry(key_str).or_insert(0.0) += val_f64;
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "SUM_MAP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let mut keys: Vec<_> = self.sums.keys().collect();
        keys.sort();

        let key_values: Vec<Value> = keys.iter().map(|k| Value::string((*k).clone())).collect();

        let sum_values: Vec<Value> = keys
            .iter()
            .filter_map(|k| self.sums.get(*k).map(|v| Value::float64(*v)))
            .collect();

        Ok(Value::array(vec![
            Value::array(key_values),
            Value::array(sum_values),
        ]))
    }

    fn reset(&mut self) {
        self.sums.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SumMapFunction;

impl AggregateFunction for SumMapFunction {
    fn name(&self) -> &str {
        "SUM_MAP"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Unknown,
        )))))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumMapAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct MinMapAccumulator {
    mins: HashMap<String, f64>,
}

impl Default for MinMapAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl MinMapAccumulator {
    pub fn new() -> Self {
        Self {
            mins: HashMap::new(),
        }
    }
}

impl Accumulator for MinMapAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (keys, values) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let (Some(k), Some(v)) = (arr[0].as_array(), arr[1].as_array()) {
                    (k, v)
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "ARRAY[ARRAY, ARRAY]".to_string(),
                        actual: value.data_type().to_string(),
                    });
                }
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY[keys, values]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY[keys, values]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        for (key, val) in keys.iter().zip(values.iter()) {
            let key_str = value_to_string(key);
            if let Some(val_f64) = numeric_value_to_f64(val)? {
                self.mins
                    .entry(key_str)
                    .and_modify(|v| *v = v.min(val_f64))
                    .or_insert(val_f64);
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MIN_MAP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let mut keys: Vec<_> = self.mins.keys().collect();
        keys.sort();

        let key_values: Vec<Value> = keys.iter().map(|k| Value::string((*k).clone())).collect();

        let min_values: Vec<Value> = keys
            .iter()
            .filter_map(|k| self.mins.get(*k).map(|v| Value::float64(*v)))
            .collect();

        Ok(Value::array(vec![
            Value::array(key_values),
            Value::array(min_values),
        ]))
    }

    fn reset(&mut self) {
        self.mins.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MinMapFunction;

impl AggregateFunction for MinMapFunction {
    fn name(&self) -> &str {
        "MIN_MAP"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Unknown,
        )))))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MinMapAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct MaxMapAccumulator {
    maxs: HashMap<String, f64>,
}

impl Default for MaxMapAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl MaxMapAccumulator {
    pub fn new() -> Self {
        Self {
            maxs: HashMap::new(),
        }
    }
}

impl Accumulator for MaxMapAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (keys, values) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let (Some(k), Some(v)) = (arr[0].as_array(), arr[1].as_array()) {
                    (k, v)
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "ARRAY[ARRAY, ARRAY]".to_string(),
                        actual: value.data_type().to_string(),
                    });
                }
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY[keys, values]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY[keys, values]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        for (key, val) in keys.iter().zip(values.iter()) {
            let key_str = value_to_string(key);
            if let Some(val_f64) = numeric_value_to_f64(val)? {
                self.maxs
                    .entry(key_str)
                    .and_modify(|v| *v = v.max(val_f64))
                    .or_insert(val_f64);
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MAX_MAP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let mut keys: Vec<_> = self.maxs.keys().collect();
        keys.sort();

        let key_values: Vec<Value> = keys.iter().map(|k| Value::string((*k).clone())).collect();

        let max_values: Vec<Value> = keys
            .iter()
            .filter_map(|k| self.maxs.get(*k).map(|v| Value::float64(*v)))
            .collect();

        Ok(Value::array(vec![
            Value::array(key_values),
            Value::array(max_values),
        ]))
    }

    fn reset(&mut self) {
        self.maxs.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MaxMapFunction;

impl AggregateFunction for MaxMapFunction {
    fn name(&self) -> &str {
        "MAX_MAP"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Unknown,
        )))))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MaxMapAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupBitmapAccumulator {
    bitmap: std::collections::HashSet<i64>,
}

impl Default for GroupBitmapAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupBitmapAccumulator {
    pub fn new() -> Self {
        Self {
            bitmap: std::collections::HashSet::new(),
        }
    }
}

impl Accumulator for GroupBitmapAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(i) = value.as_i64() {
            self.bitmap.insert(i);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_BITMAP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let mut values: Vec<i64> = self.bitmap.iter().copied().collect();
        values.sort_unstable();
        let value_array: Vec<Value> = values.into_iter().map(Value::int64).collect();
        Ok(Value::array(value_array))
    }

    fn reset(&mut self) {
        self.bitmap.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupBitmapFunction;

impl AggregateFunction for GroupBitmapFunction {
    fn name(&self) -> &str {
        "GROUP_BITMAP"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Int64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Int64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupBitmapAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupBitmapAndAccumulator {
    bitmap: Option<std::collections::HashSet<i64>>,
}

impl Default for GroupBitmapAndAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupBitmapAndAccumulator {
    pub fn new() -> Self {
        Self { bitmap: None }
    }
}

impl Accumulator for GroupBitmapAndAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            let current_set: std::collections::HashSet<i64> =
                arr.iter().filter_map(|v| v.as_i64()).collect();

            if let Some(ref mut bitmap) = self.bitmap {
                *bitmap = bitmap.intersection(&current_set).copied().collect();
            } else {
                self.bitmap = Some(current_set);
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_BITMAP_AND merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if let Some(ref bitmap) = self.bitmap {
            let mut values: Vec<i64> = bitmap.iter().copied().collect();
            values.sort_unstable();
            let value_array: Vec<Value> = values.into_iter().map(Value::int64).collect();
            Ok(Value::array(value_array))
        } else {
            Ok(Value::array(Vec::new()))
        }
    }

    fn reset(&mut self) {
        self.bitmap = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupBitmapAndFunction;

impl AggregateFunction for GroupBitmapAndFunction {
    fn name(&self) -> &str {
        "GROUP_BITMAP_AND"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Int64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupBitmapAndAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupBitmapOrAccumulator {
    bitmap: std::collections::HashSet<i64>,
}

impl Default for GroupBitmapOrAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupBitmapOrAccumulator {
    pub fn new() -> Self {
        Self {
            bitmap: std::collections::HashSet::new(),
        }
    }
}

impl Accumulator for GroupBitmapOrAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            for v in arr {
                if let Some(i) = v.as_i64() {
                    self.bitmap.insert(i);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_BITMAP_OR merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let mut values: Vec<i64> = self.bitmap.iter().copied().collect();
        values.sort_unstable();
        let value_array: Vec<Value> = values.into_iter().map(Value::int64).collect();
        Ok(Value::array(value_array))
    }

    fn reset(&mut self) {
        self.bitmap.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupBitmapOrFunction;

impl AggregateFunction for GroupBitmapOrFunction {
    fn name(&self) -> &str {
        "GROUP_BITMAP_OR"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Int64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupBitmapOrAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupBitmapXorAccumulator {
    bitmap: std::collections::HashMap<i64, bool>,
}

impl Default for GroupBitmapXorAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupBitmapXorAccumulator {
    pub fn new() -> Self {
        Self {
            bitmap: std::collections::HashMap::new(),
        }
    }
}

impl Accumulator for GroupBitmapXorAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            for v in arr {
                if let Some(i) = v.as_i64() {
                    self.bitmap
                        .entry(i)
                        .and_modify(|present| *present = !*present)
                        .or_insert(true);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_BITMAP_XOR merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let mut values: Vec<i64> = self
            .bitmap
            .iter()
            .filter_map(|(k, v)| if *v { Some(*k) } else { None })
            .collect();
        values.sort_unstable();
        let value_array: Vec<Value> = values.into_iter().map(Value::int64).collect();
        Ok(Value::array(value_array))
    }

    fn reset(&mut self) {
        self.bitmap.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupBitmapXorFunction;

impl AggregateFunction for GroupBitmapXorFunction {
    fn name(&self) -> &str {
        "GROUP_BITMAP_XOR"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Int64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupBitmapXorAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct RankCorrAccumulator {
    x_values: Vec<f64>,
    y_values: Vec<f64>,
}

impl Default for RankCorrAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl RankCorrAccumulator {
    pub fn new() -> Self {
        Self {
            x_values: Vec::new(),
            y_values: Vec::new(),
        }
    }

    fn rank_values(values: &[f64]) -> Vec<f64> {
        let mut indexed: Vec<(usize, f64)> = values.iter().copied().enumerate().collect();
        indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        let mut ranks = vec![0.0; values.len()];
        let mut i = 0;
        while i < indexed.len() {
            let mut j = i;
            while j < indexed.len() && indexed[j].1 == indexed[i].1 {
                j += 1;
            }
            let rank = (i + j - 1) as f64 / 2.0 + 1.0;
            for k in i..j {
                ranks[indexed[k].0] = rank;
            }
            i = j;
        }
        ranks
    }
}

impl Accumulator for RankCorrAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (x, y) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if arr[0].is_null() || arr[1].is_null() {
                    return Ok(());
                }
                (&arr[0], &arr[1])
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY[x, y]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY[x, y]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        let x_f64 = numeric_value_to_f64(x)?.unwrap_or(0.0);
        let y_f64 = numeric_value_to_f64(y)?.unwrap_or(0.0);

        self.x_values.push(x_f64);
        self.y_values.push(y_f64);
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "RANK_CORR merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.x_values.len() < 2 {
            return Ok(Value::null());
        }

        let x_ranks = Self::rank_values(&self.x_values);
        let y_ranks = Self::rank_values(&self.y_values);

        let n = x_ranks.len() as f64;
        let mean_x: f64 = x_ranks.iter().sum::<f64>() / n;
        let mean_y: f64 = y_ranks.iter().sum::<f64>() / n;

        let mut sum_xy = 0.0;
        let mut sum_x2 = 0.0;
        let mut sum_y2 = 0.0;

        for i in 0..x_ranks.len() {
            let dx = x_ranks[i] - mean_x;
            let dy = y_ranks[i] - mean_y;
            sum_xy += dx * dy;
            sum_x2 += dx * dx;
            sum_y2 += dy * dy;
        }

        if sum_x2 == 0.0 || sum_y2 == 0.0 {
            return Ok(Value::null());
        }

        let corr = sum_xy / (sum_x2 * sum_y2).sqrt();
        Ok(Value::float64(corr))
    }

    fn reset(&mut self) {
        self.x_values.clear();
        self.y_values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RankCorrFunction;

impl AggregateFunction for RankCorrFunction {
    fn name(&self) -> &str {
        "RANK_CORR"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RankCorrAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct ExponentialMovingAverageAccumulator {
    ema: Option<f64>,
    alpha: f64,
}

impl Default for ExponentialMovingAverageAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl ExponentialMovingAverageAccumulator {
    pub fn new(alpha: f64) -> Self {
        Self { ema: None, alpha }
    }
}

impl Accumulator for ExponentialMovingAverageAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(val) = numeric_value_to_f64(value)? {
            if let Some(current_ema) = self.ema {
                self.ema = Some(self.alpha * val + (1.0 - self.alpha) * current_ema);
            } else {
                self.ema = Some(val);
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "EXPONENTIAL_MOVING_AVERAGE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.ema.map(Value::float64).unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.ema = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct ExponentialMovingAverageFunction {
    alpha: f64,
}

impl Default for ExponentialMovingAverageFunction {
    fn default() -> Self {
        Self { alpha: 0.5 }
    }
}

impl AggregateFunction for ExponentialMovingAverageFunction {
    fn name(&self) -> &str {
        "EXPONENTIAL_MOVING_AVERAGE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ExponentialMovingAverageAccumulator::new(self.alpha))
    }
}

#[derive(Debug, Clone)]
pub struct IntervalLengthSumAccumulator {
    total_length: i64,
}

impl Default for IntervalLengthSumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl IntervalLengthSumAccumulator {
    pub fn new() -> Self {
        Self { total_length: 0 }
    }
}

impl Accumulator for IntervalLengthSumAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (start, end) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if arr[0].is_null() || arr[1].is_null() {
                    return Ok(());
                }
                (&arr[0], &arr[1])
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY[start, end]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY[start, end]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        let start_val = numeric_value_to_f64(start)?.unwrap_or(0.0) as i64;
        let end_val = numeric_value_to_f64(end)?.unwrap_or(0.0) as i64;

        if end_val > start_val {
            self.total_length += end_val - start_val;
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "INTERVAL_LENGTH_SUM merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::int64(self.total_length))
    }

    fn reset(&mut self) {
        self.total_length = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct IntervalLengthSumFunction;

impl AggregateFunction for IntervalLengthSumFunction {
    fn name(&self) -> &str {
        "INTERVAL_LENGTH_SUM"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(IntervalLengthSumAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct RetentionAccumulator {
    event_flags: Vec<bool>,
}

impl Default for RetentionAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl RetentionAccumulator {
    pub fn new() -> Self {
        Self {
            event_flags: Vec::new(),
        }
    }
}

impl Accumulator for RetentionAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if self.event_flags.is_empty() {
                self.event_flags = vec![false; arr.len()];
            }

            for (i, v) in arr.iter().enumerate() {
                if i < self.event_flags.len() {
                    if let Some(b) = v.as_bool() {
                        self.event_flags[i] = self.event_flags[i] || b;
                    } else if let Some(n) = v.as_i64() {
                        self.event_flags[i] = self.event_flags[i] || (n != 0);
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "RETENTION merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let result: Vec<Value> = self
            .event_flags
            .iter()
            .map(|&b| Value::bool_val(b))
            .collect();
        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.event_flags.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RetentionFunction;

impl AggregateFunction for RetentionFunction {
    fn name(&self) -> &str {
        "RETENTION"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Bool)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RetentionAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct WindowFunnelAccumulator {
    events: Vec<(i64, usize)>,
    window: i64,
    num_steps: usize,
}

impl Default for WindowFunnelAccumulator {
    fn default() -> Self {
        Self::new(3600, 5)
    }
}

impl WindowFunnelAccumulator {
    pub fn new(window: i64, num_steps: usize) -> Self {
        Self {
            events: Vec::new(),
            window,
            num_steps,
        }
    }
}

impl Accumulator for WindowFunnelAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (timestamp, step) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if arr[0].is_null() || arr[1].is_null() {
                    return Ok(());
                }
                (&arr[0], &arr[1])
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY[timestamp, step]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY[timestamp, step]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        let ts = numeric_value_to_f64(timestamp)?.unwrap_or(0.0) as i64;
        let step_num = numeric_value_to_f64(step)?.unwrap_or(0.0) as usize;

        if step_num > 0 && step_num <= self.num_steps {
            self.events.push((ts, step_num));
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "WINDOW_FUNNEL merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.events.is_empty() {
            return Ok(Value::int64(0));
        }

        let mut sorted_events = self.events.clone();
        sorted_events.sort_by_key(|e| e.0);

        let mut max_level = 0;

        for start_idx in 0..sorted_events.len() {
            if sorted_events[start_idx].1 != 1 {
                continue;
            }

            let start_time = sorted_events[start_idx].0;
            let mut current_level = 1;
            let mut last_step_time = start_time;

            for event_idx in (start_idx + 1)..sorted_events.len() {
                let (event_time, event_step) = sorted_events[event_idx];

                if event_time - start_time > self.window {
                    break;
                }

                if event_step == current_level + 1 && event_time >= last_step_time {
                    current_level += 1;
                    last_step_time = event_time;

                    if current_level == self.num_steps {
                        return Ok(Value::int64(self.num_steps as i64));
                    }
                }
            }

            max_level = max_level.max(current_level);
        }

        Ok(Value::int64(max_level as i64))
    }

    fn reset(&mut self) {
        self.events.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct WindowFunnelFunction {
    window: i64,
    num_steps: usize,
}

impl Default for WindowFunnelFunction {
    fn default() -> Self {
        Self {
            window: 3600,
            num_steps: 5,
        }
    }
}

impl AggregateFunction for WindowFunnelFunction {
    fn name(&self) -> &str {
        "WINDOW_FUNNEL"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(WindowFunnelAccumulator::new(self.window, self.num_steps))
    }
}

#[derive(Debug, Clone)]
pub struct GroupArraySampleAccumulator {
    values: Vec<Value>,
    max_size: usize,
    count: usize,
}

impl Default for GroupArraySampleAccumulator {
    fn default() -> Self {
        Self::new(10)
    }
}

impl GroupArraySampleAccumulator {
    pub fn new(max_size: usize) -> Self {
        Self {
            values: Vec::new(),
            max_size,
            count: 0,
        }
    }
}

impl Accumulator for GroupArraySampleAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        self.count += 1;
        if self.values.len() < self.max_size {
            self.values.push(value.clone());
        } else {
            let idx = rand::random::<usize>() % self.count;
            if idx < self.max_size {
                self.values[idx] = value.clone();
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_ARRAY_SAMPLE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::array(self.values.clone()))
    }

    fn reset(&mut self) {
        self.values.clear();
        self.count = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct GroupArraySampleFunction {
    max_size: usize,
}

impl Default for GroupArraySampleFunction {
    fn default() -> Self {
        Self { max_size: 10 }
    }
}

impl GroupArraySampleFunction {
    pub fn new(max_size: usize) -> Self {
        Self { max_size }
    }
}

impl AggregateFunction for GroupArraySampleFunction {
    fn name(&self) -> &str {
        "GROUP_ARRAY_SAMPLE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupArraySampleAccumulator::new(self.max_size))
    }
}

#[derive(Debug, Clone)]
pub struct SequenceMatchAccumulator {
    events: Vec<(i64, String)>,
    pattern: String,
}

impl Default for SequenceMatchAccumulator {
    fn default() -> Self {
        Self::new(String::new())
    }
}

impl SequenceMatchAccumulator {
    pub fn new(pattern: String) -> Self {
        Self {
            events: Vec::new(),
            pattern,
        }
    }

    fn matches_pattern(&self) -> bool {
        if self.events.is_empty() || self.pattern.is_empty() {
            return false;
        }

        let pattern_parts: Vec<&str> = self.pattern.split("->").map(|s| s.trim()).collect();
        if pattern_parts.is_empty() {
            return false;
        }

        let mut events_sorted = self.events.clone();
        events_sorted.sort_by_key(|(timestamp, _)| *timestamp);

        let mut pattern_idx = 0;
        for (_timestamp, event_name) in &events_sorted {
            if pattern_idx < pattern_parts.len() && event_name == pattern_parts[pattern_idx] {
                pattern_idx += 1;
                if pattern_idx == pattern_parts.len() {
                    return true;
                }
            }
        }

        false
    }
}

impl Accumulator for SequenceMatchAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() >= 2 {
                if let (Some(timestamp), Some(event_name)) = (arr[0].as_i64(), arr[1].as_str()) {
                    self.events.push((timestamp, event_name.to_string()));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "SEQUENCE_MATCH merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::bool_val(self.matches_pattern()))
    }

    fn reset(&mut self) {
        self.events.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct SequenceMatchFunction {
    pattern: String,
}

impl SequenceMatchFunction {
    pub fn new(pattern: String) -> Self {
        Self { pattern }
    }
}

impl AggregateFunction for SequenceMatchFunction {
    fn name(&self) -> &str {
        "SEQUENCE_MATCH"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Bool)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SequenceMatchAccumulator::new(self.pattern.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct SequenceCountAccumulator {
    events: Vec<(i64, String)>,
    pattern: String,
}

impl Default for SequenceCountAccumulator {
    fn default() -> Self {
        Self::new(String::new())
    }
}

impl SequenceCountAccumulator {
    pub fn new(pattern: String) -> Self {
        Self {
            events: Vec::new(),
            pattern,
        }
    }

    fn count_pattern(&self) -> i64 {
        if self.events.is_empty() || self.pattern.is_empty() {
            return 0;
        }

        let pattern_parts: Vec<&str> = self.pattern.split("->").map(|s| s.trim()).collect();
        if pattern_parts.is_empty() {
            return 0;
        }

        let mut events_sorted = self.events.clone();
        events_sorted.sort_by_key(|(timestamp, _)| *timestamp);

        let mut count = 0i64;
        for start_idx in 0..events_sorted.len() {
            let mut pattern_idx = 0;
            for i in start_idx..events_sorted.len() {
                let (_timestamp, event_name) = &events_sorted[i];
                if pattern_idx < pattern_parts.len() && event_name == pattern_parts[pattern_idx] {
                    pattern_idx += 1;
                    if pattern_idx == pattern_parts.len() {
                        count += 1;
                        break;
                    }
                }
            }
        }

        count
    }
}

impl Accumulator for SequenceCountAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() >= 2 {
                if let (Some(timestamp), Some(event_name)) = (arr[0].as_i64(), arr[1].as_str()) {
                    self.events.push((timestamp, event_name.to_string()));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "SEQUENCE_COUNT merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::int64(self.count_pattern()))
    }

    fn reset(&mut self) {
        self.events.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct SequenceCountFunction {
    pattern: String,
}

impl SequenceCountFunction {
    pub fn new(pattern: String) -> Self {
        Self { pattern }
    }
}

impl AggregateFunction for SequenceCountFunction {
    fn name(&self) -> &str {
        "SEQUENCE_COUNT"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SequenceCountAccumulator::new(self.pattern.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct SumIfAccumulator {
    sum: f64,
}

impl Default for SumIfAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SumIfAccumulator {
    pub fn new() -> Self {
        Self { sum: 0.0 }
    }
}

impl Accumulator for SumIfAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let Some(condition) = arr[1].as_bool() {
                    if condition {
                        if let Some(num) = numeric_value_to_f64(&arr[0])? {
                            self.sum += num;
                        }
                    }
                } else if let Some(cond) = arr[1].as_i64() {
                    if cond != 0 {
                        if let Some(num) = numeric_value_to_f64(&arr[0])? {
                            self.sum += num;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "SUM_IF merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::float64(self.sum))
    }

    fn reset(&mut self) {
        self.sum = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SumIfFunction;

impl AggregateFunction for SumIfFunction {
    fn name(&self) -> &str {
        "SUM_IF"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumIfAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct AvgIfAccumulator {
    sum: f64,
    count: usize,
}

impl Default for AvgIfAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AvgIfAccumulator {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Accumulator for AvgIfAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                let condition = if let Some(b) = arr[1].as_bool() {
                    b
                } else if let Some(i) = arr[1].as_i64() {
                    i != 0
                } else {
                    false
                };

                if condition {
                    if let Some(num) = numeric_value_to_f64(&arr[0])? {
                        self.sum += num;
                        self.count += 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "AVG_IF merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count == 0 {
            Ok(Value::null())
        } else {
            Ok(Value::float64(self.sum / self.count as f64))
        }
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AvgIfFunction;

impl AggregateFunction for AvgIfFunction {
    fn name(&self) -> &str {
        "AVG_IF"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgIfAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct MinIfAccumulator {
    min: Option<f64>,
}

impl Default for MinIfAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl MinIfAccumulator {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl Accumulator for MinIfAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                let condition = if let Some(b) = arr[1].as_bool() {
                    b
                } else if let Some(i) = arr[1].as_i64() {
                    i != 0
                } else {
                    false
                };

                if condition {
                    if let Some(num) = numeric_value_to_f64(&arr[0])? {
                        self.min = Some(match self.min {
                            Some(current) => current.min(num),
                            None => num,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MIN_IF merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        match self.min {
            Some(min) => Ok(Value::float64(min)),
            None => Ok(Value::null()),
        }
    }

    fn reset(&mut self) {
        self.min = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MinIfFunction;

impl AggregateFunction for MinIfFunction {
    fn name(&self) -> &str {
        "MIN_IF"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MinIfAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct MaxIfAccumulator {
    max: Option<f64>,
}

impl Default for MaxIfAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl MaxIfAccumulator {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl Accumulator for MaxIfAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                let condition = if let Some(b) = arr[1].as_bool() {
                    b
                } else if let Some(i) = arr[1].as_i64() {
                    i != 0
                } else {
                    false
                };

                if condition {
                    if let Some(num) = numeric_value_to_f64(&arr[0])? {
                        self.max = Some(match self.max {
                            Some(current) => current.max(num),
                            None => num,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MAX_IF merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        match self.max {
            Some(max) => Ok(Value::float64(max)),
            None => Ok(Value::null()),
        }
    }

    fn reset(&mut self) {
        self.max = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MaxIfFunction;

impl AggregateFunction for MaxIfFunction {
    fn name(&self) -> &str {
        "MAX_IF"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MaxIfAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct CountEqualAccumulator {
    target: Value,
    count: i64,
}

impl Default for CountEqualAccumulator {
    fn default() -> Self {
        Self::new(Value::null())
    }
}

impl CountEqualAccumulator {
    pub fn new(target: Value) -> Self {
        Self { target, count: 0 }
    }

    fn values_equal(a: &Value, b: &Value) -> bool {
        if a.is_null() && b.is_null() {
            return true;
        }
        if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
            return x == y;
        }
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return (x - y).abs() < f64::EPSILON;
        }
        if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
            return x == y;
        }
        if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
            return x == y;
        }
        false
    }
}

impl Accumulator for CountEqualAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if Self::values_equal(&arr[0], &self.target) {
                    self.count += 1;
                }
            }
        } else if Self::values_equal(value, &self.target) {
            self.count += 1;
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "COUNT_EQUAL merge not implemented".to_string(),
        ))
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

#[derive(Debug, Clone)]
pub struct CountEqualFunction {
    target: Value,
}

impl Default for CountEqualFunction {
    fn default() -> Self {
        Self {
            target: Value::null(),
        }
    }
}

impl CountEqualFunction {
    pub fn new(target: Value) -> Self {
        Self { target }
    }
}

impl AggregateFunction for CountEqualFunction {
    fn name(&self) -> &str {
        "COUNT_EQUAL"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountEqualAccumulator::new(self.target.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct BoundingRatioAccumulator {
    min: Option<f64>,
    max: Option<f64>,
}

impl Default for BoundingRatioAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl BoundingRatioAccumulator {
    pub fn new() -> Self {
        Self {
            min: None,
            max: None,
        }
    }
}

impl Accumulator for BoundingRatioAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(num) = numeric_value_to_f64(value)? {
            self.min = Some(match self.min {
                Some(current) => current.min(num),
                None => num,
            });
            self.max = Some(match self.max {
                Some(current) => current.max(num),
                None => num,
            });
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "BOUNDING_RATIO merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        match (self.min, self.max) {
            (Some(min), Some(max)) => {
                if min.abs() < f64::EPSILON {
                    Ok(Value::null())
                } else {
                    Ok(Value::float64(max / min))
                }
            }
            _ => Ok(Value::null()),
        }
    }

    fn reset(&mut self) {
        self.min = None;
        self.max = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BoundingRatioFunction;

impl AggregateFunction for BoundingRatioFunction {
    fn name(&self) -> &str {
        "BOUNDING_RATIO"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BoundingRatioAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct SimpleLinearRegressionAccumulator {
    sum_x: f64,
    sum_y: f64,
    sum_xx: f64,
    sum_xy: f64,
    count: usize,
}

impl Default for SimpleLinearRegressionAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleLinearRegressionAccumulator {
    pub fn new() -> Self {
        Self {
            sum_x: 0.0,
            sum_y: 0.0,
            sum_xx: 0.0,
            sum_xy: 0.0,
            count: 0,
        }
    }
}

impl Accumulator for SimpleLinearRegressionAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let (Some(x), Some(y)) = (
                    numeric_value_to_f64(&arr[0])?,
                    numeric_value_to_f64(&arr[1])?,
                ) {
                    self.sum_x += x;
                    self.sum_y += y;
                    self.sum_xx += x * x;
                    self.sum_xy += x * y;
                    self.count += 1;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "SIMPLE_LINEAR_REGRESSION merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count < 2 {
            return Ok(Value::null());
        }

        let n = self.count as f64;
        let denominator = n * self.sum_xx - self.sum_x * self.sum_x;

        if denominator.abs() < f64::EPSILON {
            return Ok(Value::null());
        }

        let slope = (n * self.sum_xy - self.sum_x * self.sum_y) / denominator;
        let intercept = (self.sum_y - slope * self.sum_x) / n;

        Ok(Value::array(vec![
            Value::float64(slope),
            Value::float64(intercept),
        ]))
    }

    fn reset(&mut self) {
        self.sum_x = 0.0;
        self.sum_y = 0.0;
        self.sum_xx = 0.0;
        self.sum_xy = 0.0;
        self.count = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SimpleLinearRegressionFunction;

impl AggregateFunction for SimpleLinearRegressionFunction {
    fn name(&self) -> &str {
        "SIMPLE_LINEAR_REGRESSION"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SimpleLinearRegressionAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct ContingencyAccumulator {
    counts: HashMap<(String, String), usize>,
    row_totals: HashMap<String, usize>,
    col_totals: HashMap<String, usize>,
    total: usize,
}

impl Default for ContingencyAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl ContingencyAccumulator {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
            row_totals: HashMap::new(),
            col_totals: HashMap::new(),
            total: 0,
        }
    }
}

impl Accumulator for ContingencyAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                let x = value_to_string(&arr[0]);
                let y = value_to_string(&arr[1]);

                *self.counts.entry((x.clone(), y.clone())).or_insert(0) += 1;
                *self.row_totals.entry(x).or_insert(0) += 1;
                *self.col_totals.entry(y).or_insert(0) += 1;
                self.total += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "CONTINGENCY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.total == 0 {
            return Ok(Value::null());
        }

        let mut chi_square = 0.0;
        for ((row, col), &observed) in &self.counts {
            let row_total = *self.row_totals.get(row).unwrap_or(&0) as f64;
            let col_total = *self.col_totals.get(col).unwrap_or(&0) as f64;
            let expected = (row_total * col_total) / self.total as f64;

            if expected > 0.0 {
                let diff = observed as f64 - expected;
                chi_square += (diff * diff) / expected;
            }
        }

        let contingency = (chi_square / (chi_square + self.total as f64)).sqrt();
        Ok(Value::float64(contingency))
    }

    fn reset(&mut self) {
        self.counts.clear();
        self.row_totals.clear();
        self.col_totals.clear();
        self.total = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ContingencyFunction;

impl AggregateFunction for ContingencyFunction {
    fn name(&self) -> &str {
        "CONTINGENCY"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ContingencyAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct CramersVAccumulator {
    counts: HashMap<(String, String), usize>,
    row_totals: HashMap<String, usize>,
    col_totals: HashMap<String, usize>,
    total: usize,
}

impl Default for CramersVAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl CramersVAccumulator {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
            row_totals: HashMap::new(),
            col_totals: HashMap::new(),
            total: 0,
        }
    }
}

impl Accumulator for CramersVAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                let x = value_to_string(&arr[0]);
                let y = value_to_string(&arr[1]);

                *self.counts.entry((x.clone(), y.clone())).or_insert(0) += 1;
                *self.row_totals.entry(x).or_insert(0) += 1;
                *self.col_totals.entry(y).or_insert(0) += 1;
                self.total += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "CRAMERS_V merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.total == 0 {
            return Ok(Value::null());
        }

        let mut chi_square = 0.0;
        for ((row, col), &observed) in &self.counts {
            let row_total = *self.row_totals.get(row).unwrap_or(&0) as f64;
            let col_total = *self.col_totals.get(col).unwrap_or(&0) as f64;
            let expected = (row_total * col_total) / self.total as f64;

            if expected > 0.0 {
                let diff = observed as f64 - expected;
                chi_square += (diff * diff) / expected;
            }
        }

        let rows = self.row_totals.len();
        let cols = self.col_totals.len();
        let min_dim = rows.min(cols) as f64;

        if min_dim <= 1.0 {
            return Ok(Value::null());
        }

        let cramers_v = (chi_square / (self.total as f64 * (min_dim - 1.0))).sqrt();
        Ok(Value::float64(cramers_v))
    }

    fn reset(&mut self) {
        self.counts.clear();
        self.row_totals.clear();
        self.col_totals.clear();
        self.total = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CramersVFunction;

impl AggregateFunction for CramersVFunction {
    fn name(&self) -> &str {
        "CRAMERS_V"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CramersVAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupConcatAccumulator {
    values: Vec<String>,
    separator: String,
}

impl Default for GroupConcatAccumulator {
    fn default() -> Self {
        Self::new(",".to_string())
    }
}

impl GroupConcatAccumulator {
    pub fn new(separator: String) -> Self {
        Self {
            values: Vec::new(),
            separator,
        }
    }
}

impl Accumulator for GroupConcatAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if !value.is_null() {
            self.values.push(value_to_string(value));
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_CONCAT merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::string(self.values.join(&self.separator)))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct GroupConcatFunction {
    separator: String,
}

impl Default for GroupConcatFunction {
    fn default() -> Self {
        Self {
            separator: ",".to_string(),
        }
    }
}

impl GroupConcatFunction {
    pub fn new(separator: String) -> Self {
        Self { separator }
    }
}

impl AggregateFunction for GroupConcatFunction {
    fn name(&self) -> &str {
        "GROUP_CONCAT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupConcatAccumulator::new(self.separator.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct EntropyAccumulator {
    counts: HashMap<String, usize>,
    total: usize,
}

impl Default for EntropyAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl EntropyAccumulator {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
            total: 0,
        }
    }
}

impl Accumulator for EntropyAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if !value.is_null() {
            let key = value_to_string(value);
            *self.counts.entry(key).or_insert(0) += 1;
            self.total += 1;
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ENTROPY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.total == 0 {
            return Ok(Value::null());
        }

        let mut entropy = 0.0;
        for &count in self.counts.values() {
            if count > 0 {
                let p = count as f64 / self.total as f64;
                entropy -= p * p.log2();
            }
        }

        Ok(Value::float64(entropy))
    }

    fn reset(&mut self) {
        self.counts.clear();
        self.total = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EntropyFunction;

impl AggregateFunction for EntropyFunction {
    fn name(&self) -> &str {
        "ENTROPY"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(EntropyAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct TheilUAccumulator {
    counts: HashMap<(String, String), usize>,
    x_counts: HashMap<String, usize>,
    y_counts: HashMap<String, usize>,
    total: usize,
}

impl Default for TheilUAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl TheilUAccumulator {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
            x_counts: HashMap::new(),
            y_counts: HashMap::new(),
            total: 0,
        }
    }
}

impl Accumulator for TheilUAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                let x = value_to_string(&arr[0]);
                let y = value_to_string(&arr[1]);

                *self.counts.entry((x.clone(), y.clone())).or_insert(0) += 1;
                *self.x_counts.entry(x).or_insert(0) += 1;
                *self.y_counts.entry(y).or_insert(0) += 1;
                self.total += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "THEIL_U merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.total == 0 {
            return Ok(Value::null());
        }

        let n = self.total as f64;
        let mut h_y = 0.0;
        for &count in self.y_counts.values() {
            if count > 0 {
                let p = count as f64 / n;
                h_y -= p * p.log2();
            }
        }

        let mut h_y_given_x = 0.0;
        for ((x, _y), &count) in &self.counts {
            if count > 0 {
                let p_xy = count as f64 / n;
                let p_x = *self.x_counts.get(x).unwrap_or(&0) as f64 / n;
                if p_x > 0.0 {
                    h_y_given_x -= p_xy * (p_xy / p_x).log2();
                }
            }
        }

        if h_y.abs() < f64::EPSILON {
            return Ok(Value::null());
        }

        let theil_u = (h_y - h_y_given_x) / h_y;
        Ok(Value::float64(theil_u))
    }

    fn reset(&mut self) {
        self.counts.clear();
        self.x_counts.clear();
        self.y_counts.clear();
        self.total = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TheilUFunction;

impl AggregateFunction for TheilUFunction {
    fn name(&self) -> &str {
        "THEIL_U"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(TheilUAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct CategoricalInformationValueAccumulator {
    good_counts: HashMap<String, usize>,
    bad_counts: HashMap<String, usize>,
    total_good: usize,
    total_bad: usize,
}

impl Default for CategoricalInformationValueAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl CategoricalInformationValueAccumulator {
    pub fn new() -> Self {
        Self {
            good_counts: HashMap::new(),
            bad_counts: HashMap::new(),
            total_good: 0,
            total_bad: 0,
        }
    }
}

impl Accumulator for CategoricalInformationValueAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                let category = value_to_string(&arr[0]);
                let is_good = if let Some(b) = arr[1].as_bool() {
                    b
                } else if let Some(i) = arr[1].as_i64() {
                    i != 0
                } else {
                    false
                };

                if is_good {
                    *self.good_counts.entry(category).or_insert(0) += 1;
                    self.total_good += 1;
                } else {
                    *self.bad_counts.entry(category).or_insert(0) += 1;
                    self.total_bad += 1;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "CATEGORICAL_INFORMATION_VALUE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.total_good == 0 || self.total_bad == 0 {
            return Ok(Value::null());
        }

        let mut iv = 0.0;
        let mut all_categories = std::collections::HashSet::new();
        for key in self.good_counts.keys() {
            all_categories.insert(key.clone());
        }
        for key in self.bad_counts.keys() {
            all_categories.insert(key.clone());
        }

        for category in all_categories {
            let good = *self.good_counts.get(&category).unwrap_or(&0) as f64;
            let bad = *self.bad_counts.get(&category).unwrap_or(&0) as f64;

            let dist_good = good / self.total_good as f64;
            let dist_bad = bad / self.total_bad as f64;

            if dist_good > 0.0 && dist_bad > 0.0 {
                let woe = (dist_good / dist_bad).ln();
                iv += (dist_good - dist_bad) * woe;
            }
        }

        Ok(Value::float64(iv))
    }

    fn reset(&mut self) {
        self.good_counts.clear();
        self.bad_counts.clear();
        self.total_good = 0;
        self.total_bad = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CategoricalInformationValueFunction;

impl AggregateFunction for CategoricalInformationValueFunction {
    fn name(&self) -> &str {
        "CATEGORICAL_INFORMATION_VALUE"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CategoricalInformationValueAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct DeltaSumAccumulator {
    last_value: Option<f64>,
    delta_sum: f64,
}

impl Default for DeltaSumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl DeltaSumAccumulator {
    pub fn new() -> Self {
        Self {
            last_value: None,
            delta_sum: 0.0,
        }
    }
}

impl Accumulator for DeltaSumAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(num) = numeric_value_to_f64(value)? {
            if let Some(last) = self.last_value {
                self.delta_sum += num - last;
            }
            self.last_value = Some(num);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "DELTA_SUM merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::float64(self.delta_sum))
    }

    fn reset(&mut self) {
        self.last_value = None;
        self.delta_sum = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DeltaSumFunction;

impl AggregateFunction for DeltaSumFunction {
    fn name(&self) -> &str {
        "DELTA_SUM"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(DeltaSumAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct DeltaSumTimestampAccumulator {
    points: Vec<(i64, f64)>,
}

impl Default for DeltaSumTimestampAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl DeltaSumTimestampAccumulator {
    pub fn new() -> Self {
        Self { points: Vec::new() }
    }
}

impl Accumulator for DeltaSumTimestampAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let (Some(timestamp), Some(val)) =
                    (arr[0].as_i64(), numeric_value_to_f64(&arr[1])?)
                {
                    self.points.push((timestamp, val));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "DELTA_SUM_TIMESTAMP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.points.len() < 2 {
            return Ok(Value::float64(0.0));
        }

        let mut sorted_points = self.points.clone();
        sorted_points.sort_by_key(|(timestamp, _)| *timestamp);

        let mut weighted_sum = 0.0;
        for i in 1..sorted_points.len() {
            let (t1, v1) = sorted_points[i - 1];
            let (t2, v2) = sorted_points[i];
            let time_diff = (t2 - t1) as f64;
            let value_diff = v2 - v1;
            weighted_sum += value_diff * time_diff;
        }

        Ok(Value::float64(weighted_sum))
    }

    fn reset(&mut self) {
        self.points.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DeltaSumTimestampFunction;

impl AggregateFunction for DeltaSumTimestampFunction {
    fn name(&self) -> &str {
        "DELTA_SUM_TIMESTAMP"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(DeltaSumTimestampAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct ArrayFlattenAccumulator {
    values: Vec<Value>,
}

impl Default for ArrayFlattenAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayFlattenAccumulator {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    fn flatten_value(&mut self, value: &Value) {
        if let Some(arr) = value.as_array() {
            for item in arr {
                self.flatten_value(item);
            }
        } else {
            self.values.push(value.clone());
        }
    }
}

impl Accumulator for ArrayFlattenAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        self.flatten_value(value);
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ARRAY_FLATTEN merge not implemented".to_string(),
        ))
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

#[derive(Debug, Default, Clone, Copy)]
pub struct ArrayFlattenFunction;

impl AggregateFunction for ArrayFlattenFunction {
    fn name(&self) -> &str {
        "ARRAY_FLATTEN"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArrayFlattenAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct ArrayReduceAccumulator {
    operation: String,
    result: Option<f64>,
}

impl Default for ArrayReduceAccumulator {
    fn default() -> Self {
        Self::new("sum".to_string())
    }
}

impl ArrayReduceAccumulator {
    pub fn new(operation: String) -> Self {
        Self {
            operation,
            result: None,
        }
    }
}

impl Accumulator for ArrayReduceAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            for item in arr {
                if let Some(num) = numeric_value_to_f64(item)? {
                    self.result = Some(match self.result {
                        Some(current) => match self.operation.as_str() {
                            "sum" => current + num,
                            "product" | "mul" => current * num,
                            "min" => current.min(num),
                            "max" => current.max(num),
                            _ => current + num,
                        },
                        None => num,
                    });
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ARRAY_REDUCE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        match self.result {
            Some(val) => Ok(Value::float64(val)),
            None => Ok(Value::null()),
        }
    }

    fn reset(&mut self) {
        self.result = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct ArrayReduceFunction {
    operation: String,
}

impl Default for ArrayReduceFunction {
    fn default() -> Self {
        Self {
            operation: "sum".to_string(),
        }
    }
}

impl ArrayReduceFunction {
    pub fn new(operation: String) -> Self {
        Self { operation }
    }
}

impl AggregateFunction for ArrayReduceFunction {
    fn name(&self) -> &str {
        "ARRAY_REDUCE"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArrayReduceAccumulator::new(self.operation.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct BitmapCardinalityAccumulator {
    bitmap: std::collections::HashSet<i64>,
}

impl Default for BitmapCardinalityAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapCardinalityAccumulator {
    pub fn new() -> Self {
        Self {
            bitmap: std::collections::HashSet::new(),
        }
    }
}

impl Accumulator for BitmapCardinalityAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(bit) = value.as_i64() {
            self.bitmap.insert(bit);
        } else if let Some(arr) = value.as_array() {
            for item in arr {
                if let Some(bit) = item.as_i64() {
                    self.bitmap.insert(bit);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "BITMAP_CARDINALITY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::int64(self.bitmap.len() as i64))
    }

    fn reset(&mut self) {
        self.bitmap.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BitmapCardinalityFunction;

impl AggregateFunction for BitmapCardinalityFunction {
    fn name(&self) -> &str {
        "BITMAP_CARDINALITY"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BitmapCardinalityAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct BitmapAndCardinalityAccumulator {
    bitmaps: Vec<std::collections::HashSet<i64>>,
}

impl Default for BitmapAndCardinalityAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapAndCardinalityAccumulator {
    pub fn new() -> Self {
        Self {
            bitmaps: Vec::new(),
        }
    }
}

impl Accumulator for BitmapAndCardinalityAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            let mut bitmap = std::collections::HashSet::new();
            for item in arr {
                if let Some(bit) = item.as_i64() {
                    bitmap.insert(bit);
                }
            }
            self.bitmaps.push(bitmap);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "BITMAP_AND_CARDINALITY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.bitmaps.is_empty() {
            return Ok(Value::int64(0));
        }

        let mut result = self.bitmaps[0].clone();
        for bitmap in &self.bitmaps[1..] {
            result = result.intersection(bitmap).copied().collect();
        }

        Ok(Value::int64(result.len() as i64))
    }

    fn reset(&mut self) {
        self.bitmaps.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BitmapAndCardinalityFunction;

impl AggregateFunction for BitmapAndCardinalityFunction {
    fn name(&self) -> &str {
        "BITMAP_AND_CARDINALITY"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_INT64
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BitmapAndCardinalityAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct BitmapOrCardinalityAccumulator {
    bitmap: std::collections::HashSet<i64>,
}

impl Default for BitmapOrCardinalityAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapOrCardinalityAccumulator {
    pub fn new() -> Self {
        Self {
            bitmap: std::collections::HashSet::new(),
        }
    }
}

impl Accumulator for BitmapOrCardinalityAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            for item in arr {
                if let Some(bit) = item.as_i64() {
                    self.bitmap.insert(bit);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "BITMAP_OR_CARDINALITY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::int64(self.bitmap.len() as i64))
    }

    fn reset(&mut self) {
        self.bitmap.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BitmapOrCardinalityFunction;

impl AggregateFunction for BitmapOrCardinalityFunction {
    fn name(&self) -> &str {
        "BITMAP_OR_CARDINALITY"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_INT64
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BitmapOrCardinalityAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct MannWhitneyUTestAccumulator {
    sample1: Vec<f64>,
    sample2: Vec<f64>,
}

impl Default for MannWhitneyUTestAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl MannWhitneyUTestAccumulator {
    pub fn new() -> Self {
        Self {
            sample1: Vec::new(),
            sample2: Vec::new(),
        }
    }
}

impl Accumulator for MannWhitneyUTestAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let (Some(val), Some(group)) = (numeric_value_to_f64(&arr[0])?, arr[1].as_i64())
                {
                    if group == 0 {
                        self.sample1.push(val);
                    } else {
                        self.sample2.push(val);
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MANN_WHITNEY_U_TEST merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.sample1.is_empty() || self.sample2.is_empty() {
            return Ok(Value::null());
        }

        let n1 = self.sample1.len() as f64;
        let n2 = self.sample2.len() as f64;

        let mut combined: Vec<(f64, usize)> = Vec::new();
        for &val in &self.sample1 {
            combined.push((val, 0));
        }
        for &val in &self.sample2 {
            combined.push((val, 1));
        }
        combined.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        let mut rank_sum1 = 0.0;
        for (i, (_val, group)) in combined.iter().enumerate() {
            if *group == 0 {
                rank_sum1 += (i + 1) as f64;
            }
        }

        let u1 = rank_sum1 - (n1 * (n1 + 1.0)) / 2.0;
        let u2 = n1 * n2 - u1;
        let u = u1.min(u2);

        let mean_u = (n1 * n2) / 2.0;
        let std_u = ((n1 * n2 * (n1 + n2 + 1.0)) / 12.0).sqrt();

        let z = (u - mean_u) / std_u;

        Ok(Value::array(vec![Value::float64(u), Value::float64(z)]))
    }

    fn reset(&mut self) {
        self.sample1.clear();
        self.sample2.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MannWhitneyUTestFunction;

impl AggregateFunction for MannWhitneyUTestFunction {
    fn name(&self) -> &str {
        "MANN_WHITNEY_U_TEST"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MannWhitneyUTestAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct StudentTTestAccumulator {
    sample1: Vec<f64>,
    sample2: Vec<f64>,
}

impl Default for StudentTTestAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl StudentTTestAccumulator {
    pub fn new() -> Self {
        Self {
            sample1: Vec::new(),
            sample2: Vec::new(),
        }
    }

    fn mean(data: &[f64]) -> f64 {
        if data.is_empty() {
            return 0.0;
        }
        data.iter().sum::<f64>() / data.len() as f64
    }

    fn variance(data: &[f64], mean: f64) -> f64 {
        if data.len() <= 1 {
            return 0.0;
        }
        data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (data.len() - 1) as f64
    }
}

impl Accumulator for StudentTTestAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let (Some(val), Some(group)) = (numeric_value_to_f64(&arr[0])?, arr[1].as_i64())
                {
                    if group == 0 {
                        self.sample1.push(val);
                    } else {
                        self.sample2.push(val);
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "STUDENT_T_TEST merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.sample1.len() < 2 || self.sample2.len() < 2 {
            return Ok(Value::null());
        }

        let n1 = self.sample1.len() as f64;
        let n2 = self.sample2.len() as f64;

        let mean1 = Self::mean(&self.sample1);
        let mean2 = Self::mean(&self.sample2);

        let var1 = Self::variance(&self.sample1, mean1);
        let var2 = Self::variance(&self.sample2, mean2);

        let pooled_var = ((n1 - 1.0) * var1 + (n2 - 1.0) * var2) / (n1 + n2 - 2.0);
        let se = (pooled_var * (1.0 / n1 + 1.0 / n2)).sqrt();

        let t = if se.abs() < f64::EPSILON {
            0.0
        } else {
            (mean1 - mean2) / se
        };

        let df = n1 + n2 - 2.0;

        Ok(Value::array(vec![Value::float64(t), Value::float64(df)]))
    }

    fn reset(&mut self) {
        self.sample1.clear();
        self.sample2.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StudentTTestFunction;

impl AggregateFunction for StudentTTestFunction {
    fn name(&self) -> &str {
        "STUDENT_T_TEST"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(StudentTTestAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct WelchTTestAccumulator {
    sample1: Vec<f64>,
    sample2: Vec<f64>,
}

impl Default for WelchTTestAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl WelchTTestAccumulator {
    pub fn new() -> Self {
        Self {
            sample1: Vec::new(),
            sample2: Vec::new(),
        }
    }

    fn mean(data: &[f64]) -> f64 {
        if data.is_empty() {
            return 0.0;
        }
        data.iter().sum::<f64>() / data.len() as f64
    }

    fn variance(data: &[f64], mean: f64) -> f64 {
        if data.len() <= 1 {
            return 0.0;
        }
        data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (data.len() - 1) as f64
    }
}

impl Accumulator for WelchTTestAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let (Some(val), Some(group)) = (numeric_value_to_f64(&arr[0])?, arr[1].as_i64())
                {
                    if group == 0 {
                        self.sample1.push(val);
                    } else {
                        self.sample2.push(val);
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "WELCH_T_TEST merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.sample1.len() < 2 || self.sample2.len() < 2 {
            return Ok(Value::null());
        }

        let n1 = self.sample1.len() as f64;
        let n2 = self.sample2.len() as f64;

        let mean1 = Self::mean(&self.sample1);
        let mean2 = Self::mean(&self.sample2);

        let var1 = Self::variance(&self.sample1, mean1);
        let var2 = Self::variance(&self.sample2, mean2);

        let se = (var1 / n1 + var2 / n2).sqrt();

        let t = if se.abs() < f64::EPSILON {
            0.0
        } else {
            (mean1 - mean2) / se
        };

        let df_num = (var1 / n1 + var2 / n2).powi(2);
        let df_denom = (var1 / n1).powi(2) / (n1 - 1.0) + (var2 / n2).powi(2) / (n2 - 1.0);
        let df = if df_denom.abs() < f64::EPSILON {
            n1 + n2 - 2.0
        } else {
            df_num / df_denom
        };

        Ok(Value::array(vec![Value::float64(t), Value::float64(df)]))
    }

    fn reset(&mut self) {
        self.sample1.clear();
        self.sample2.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct WelchTTestFunction;

impl AggregateFunction for WelchTTestFunction {
    fn name(&self) -> &str {
        "WELCH_T_TEST"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(WelchTTestAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct ArrayMapAccumulator {
    operation: String,
    result: Vec<Value>,
}

impl Default for ArrayMapAccumulator {
    fn default() -> Self {
        Self::new("abs".to_string())
    }
}

impl ArrayMapAccumulator {
    pub fn new(operation: String) -> Self {
        Self {
            operation,
            result: Vec::new(),
        }
    }

    fn apply_operation(&self, value: &Value) -> Value {
        match self.operation.as_str() {
            "abs" => {
                if let Some(num) = numeric_value_to_f64(value).ok().flatten() {
                    Value::float64(num.abs())
                } else {
                    value.clone()
                }
            }
            "negate" => {
                if let Some(num) = numeric_value_to_f64(value).ok().flatten() {
                    Value::float64(-num)
                } else {
                    value.clone()
                }
            }
            "square" => {
                if let Some(num) = numeric_value_to_f64(value).ok().flatten() {
                    Value::float64(num * num)
                } else {
                    value.clone()
                }
            }
            "sqrt" => {
                if let Some(num) = numeric_value_to_f64(value).ok().flatten() {
                    if num >= 0.0 {
                        Value::float64(num.sqrt())
                    } else {
                        Value::null()
                    }
                } else {
                    value.clone()
                }
            }
            "upper" => {
                if let Some(s) = value.as_str() {
                    Value::string(s.to_uppercase())
                } else {
                    value.clone()
                }
            }
            "lower" => {
                if let Some(s) = value.as_str() {
                    Value::string(s.to_lowercase())
                } else {
                    value.clone()
                }
            }
            _ => value.clone(),
        }
    }
}

impl Accumulator for ArrayMapAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            for item in arr {
                self.result.push(self.apply_operation(item));
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ARRAY_MAP merge not implemented".to_string(),
        ))
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

#[derive(Debug, Clone)]
pub struct ArrayMapFunction {
    operation: String,
}

impl Default for ArrayMapFunction {
    fn default() -> Self {
        Self {
            operation: "abs".to_string(),
        }
    }
}

impl ArrayMapFunction {
    pub fn new(operation: String) -> Self {
        Self { operation }
    }
}

impl AggregateFunction for ArrayMapFunction {
    fn name(&self) -> &str {
        "ARRAY_MAP"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArrayMapAccumulator::new(self.operation.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct ArrayFilterAccumulator {
    predicate: String,
    result: Vec<Value>,
}

impl Default for ArrayFilterAccumulator {
    fn default() -> Self {
        Self::new("positive".to_string())
    }
}

impl ArrayFilterAccumulator {
    pub fn new(predicate: String) -> Self {
        Self {
            predicate,
            result: Vec::new(),
        }
    }

    fn check_predicate(&self, value: &Value) -> bool {
        match self.predicate.as_str() {
            "positive" => {
                if let Some(num) = numeric_value_to_f64(value).ok().flatten() {
                    num > 0.0
                } else {
                    false
                }
            }
            "negative" => {
                if let Some(num) = numeric_value_to_f64(value).ok().flatten() {
                    num < 0.0
                } else {
                    false
                }
            }
            "nonzero" => {
                if let Some(num) = numeric_value_to_f64(value).ok().flatten() {
                    num.abs() > f64::EPSILON
                } else {
                    false
                }
            }
            "notnull" => !value.is_null(),
            "even" => {
                if let Some(i) = value.as_i64() {
                    i % 2 == 0
                } else {
                    false
                }
            }
            "odd" => {
                if let Some(i) = value.as_i64() {
                    i % 2 != 0
                } else {
                    false
                }
            }
            _ => true,
        }
    }
}

impl Accumulator for ArrayFilterAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            for item in arr {
                if self.check_predicate(item) {
                    self.result.push(item.clone());
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ARRAY_FILTER merge not implemented".to_string(),
        ))
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

#[derive(Debug, Clone)]
pub struct ArrayFilterFunction {
    predicate: String,
}

impl Default for ArrayFilterFunction {
    fn default() -> Self {
        Self {
            predicate: "positive".to_string(),
        }
    }
}

impl ArrayFilterFunction {
    pub fn new(predicate: String) -> Self {
        Self { predicate }
    }
}

impl AggregateFunction for ArrayFilterFunction {
    fn name(&self) -> &str {
        "ARRAY_FILTER"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArrayFilterAccumulator::new(self.predicate.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct SumArrayAccumulator {
    sums: Vec<f64>,
}

impl Default for SumArrayAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SumArrayAccumulator {
    pub fn new() -> Self {
        Self { sums: Vec::new() }
    }
}

impl Accumulator for SumArrayAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            for (i, item) in arr.iter().enumerate() {
                if let Some(num) = numeric_value_to_f64(item)? {
                    if i >= self.sums.len() {
                        self.sums.resize(i + 1, 0.0);
                    }
                    self.sums[i] += num;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "SUM_ARRAY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let result: Vec<Value> = self.sums.iter().map(|&v| Value::float64(v)).collect();
        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.sums.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SumArrayFunction;

impl AggregateFunction for SumArrayFunction {
    fn name(&self) -> &str {
        "SUM_ARRAY"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumArrayAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct AvgArrayAccumulator {
    sums: Vec<f64>,
    counts: Vec<usize>,
}

impl Default for AvgArrayAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AvgArrayAccumulator {
    pub fn new() -> Self {
        Self {
            sums: Vec::new(),
            counts: Vec::new(),
        }
    }
}

impl Accumulator for AvgArrayAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            for (i, item) in arr.iter().enumerate() {
                if let Some(num) = numeric_value_to_f64(item)? {
                    if i >= self.sums.len() {
                        self.sums.resize(i + 1, 0.0);
                        self.counts.resize(i + 1, 0);
                    }
                    self.sums[i] += num;
                    self.counts[i] += 1;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "AVG_ARRAY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let result: Vec<Value> = self
            .sums
            .iter()
            .enumerate()
            .map(|(i, &sum)| {
                let count = self.counts.get(i).copied().unwrap_or(0);
                if count > 0 {
                    Value::float64(sum / count as f64)
                } else {
                    Value::null()
                }
            })
            .collect();
        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.sums.clear();
        self.counts.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AvgArrayFunction;

impl AggregateFunction for AvgArrayFunction {
    fn name(&self) -> &str {
        "AVG_ARRAY"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgArrayAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct MinArrayAccumulator {
    mins: Vec<Option<f64>>,
}

impl Default for MinArrayAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl MinArrayAccumulator {
    pub fn new() -> Self {
        Self { mins: Vec::new() }
    }
}

impl Accumulator for MinArrayAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            for (i, item) in arr.iter().enumerate() {
                if let Some(num) = numeric_value_to_f64(item)? {
                    if i >= self.mins.len() {
                        self.mins.resize(i + 1, None);
                    }
                    self.mins[i] = Some(match self.mins[i] {
                        Some(current) => current.min(num),
                        None => num,
                    });
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MIN_ARRAY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let result: Vec<Value> = self
            .mins
            .iter()
            .map(|&v| match v {
                Some(val) => Value::float64(val),
                None => Value::null(),
            })
            .collect();
        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.mins.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MinArrayFunction;

impl AggregateFunction for MinArrayFunction {
    fn name(&self) -> &str {
        "MIN_ARRAY"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MinArrayAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct MaxArrayAccumulator {
    maxs: Vec<Option<f64>>,
}

impl Default for MaxArrayAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl MaxArrayAccumulator {
    pub fn new() -> Self {
        Self { maxs: Vec::new() }
    }
}

impl Accumulator for MaxArrayAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(arr) = value.as_array() {
            for (i, item) in arr.iter().enumerate() {
                if let Some(num) = numeric_value_to_f64(item)? {
                    if i >= self.maxs.len() {
                        self.maxs.resize(i + 1, None);
                    }
                    self.maxs[i] = Some(match self.maxs[i] {
                        Some(current) => current.max(num),
                        None => num,
                    });
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MAX_ARRAY merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let result: Vec<Value> = self
            .maxs
            .iter()
            .map(|&v| match v {
                Some(val) => Value::float64(val),
                None => Value::null(),
            })
            .collect();
        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.maxs.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MaxArrayFunction;

impl AggregateFunction for MaxArrayFunction {
    fn name(&self) -> &str {
        "MAX_ARRAY"
    }

    fn arg_types(&self) -> &[DataType] {
        &ARRAY_OF_UNKNOWN
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MaxArrayAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct AvgMapAccumulator {
    sums: HashMap<String, f64>,
    counts: HashMap<String, usize>,
}

impl Default for AvgMapAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AvgMapAccumulator {
    pub fn new() -> Self {
        Self {
            sums: HashMap::new(),
            counts: HashMap::new(),
        }
    }
}

impl Accumulator for AvgMapAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (keys, values) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                if let (Some(k), Some(v)) = (arr[0].as_array(), arr[1].as_array()) {
                    (k, v)
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "ARRAY[ARRAY, ARRAY]".to_string(),
                        actual: value.data_type().to_string(),
                    });
                }
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY[keys, values]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY[keys, values]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        for (key, val) in keys.iter().zip(values.iter()) {
            let key_str = value_to_string(key);
            if let Some(val_f64) = numeric_value_to_f64(val)? {
                *self.sums.entry(key_str.clone()).or_insert(0.0) += val_f64;
                *self.counts.entry(key_str).or_insert(0) += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "AVG_MAP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let mut keys: Vec<_> = self.sums.keys().collect();
        keys.sort();

        let key_values: Vec<Value> = keys.iter().map(|k| Value::string((*k).clone())).collect();

        let avg_values: Vec<Value> = keys
            .iter()
            .filter_map(|k| {
                let sum = self.sums.get(*k)?;
                let count = *self.counts.get(*k)? as f64;
                if count > 0.0 {
                    Some(Value::float64(sum / count))
                } else {
                    Some(Value::null())
                }
            })
            .collect();

        Ok(Value::array(vec![
            Value::array(key_values),
            Value::array(avg_values),
        ]))
    }

    fn reset(&mut self) {
        self.sums.clear();
        self.counts.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AvgMapFunction;

impl AggregateFunction for AvgMapFunction {
    fn name(&self) -> &str {
        "AVG_MAP"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Unknown,
        )))))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgMapAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupArrayIntersectAccumulator {
    intersection: Option<std::collections::HashSet<String>>,
}

impl Default for GroupArrayIntersectAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupArrayIntersectAccumulator {
    pub fn new() -> Self {
        Self { intersection: None }
    }
}

impl Accumulator for GroupArrayIntersectAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(arr) = value.as_array() {
            let current_set: std::collections::HashSet<String> =
                arr.iter().map(value_to_string).collect();

            if let Some(ref mut intersection) = self.intersection {
                *intersection = intersection.intersection(&current_set).cloned().collect();
            } else {
                self.intersection = Some(current_set);
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_ARRAY_INTERSECT merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if let Some(ref intersection) = self.intersection {
            let mut values: Vec<String> = intersection.iter().cloned().collect();
            values.sort();
            let value_array: Vec<Value> = values.into_iter().map(Value::string).collect();
            Ok(Value::array(value_array))
        } else {
            Ok(Value::array(Vec::new()))
        }
    }

    fn reset(&mut self) {
        self.intersection = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupArrayIntersectFunction;

impl AggregateFunction for GroupArrayIntersectFunction {
    fn name(&self) -> &str {
        "GROUP_ARRAY_INTERSECT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupArrayIntersectAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct GroupArraySortedAccumulator {
    values: Vec<Value>,
    max_size: usize,
}

impl Default for GroupArraySortedAccumulator {
    fn default() -> Self {
        Self::new(10)
    }
}

impl GroupArraySortedAccumulator {
    pub fn new(max_size: usize) -> Self {
        Self {
            values: Vec::new(),
            max_size,
        }
    }
}

impl Accumulator for GroupArraySortedAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        self.values.push(value.clone());
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_ARRAY_SORTED merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| {
            if let (Some(a_i), Some(b_i)) = (a.as_i64(), b.as_i64()) {
                a_i.cmp(&b_i)
            } else if let (Some(a_f), Some(b_f)) = (a.as_f64(), b.as_f64()) {
                a_f.partial_cmp(&b_f).unwrap_or(Ordering::Equal)
            } else if let (Some(a_s), Some(b_s)) = (a.as_str(), b.as_str()) {
                a_s.cmp(b_s)
            } else {
                Ordering::Equal
            }
        });
        let result: Vec<Value> = sorted.into_iter().take(self.max_size).collect();
        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct GroupArraySortedFunction {
    max_size: usize,
}

impl Default for GroupArraySortedFunction {
    fn default() -> Self {
        Self { max_size: 10 }
    }
}

impl GroupArraySortedFunction {
    pub fn new(max_size: usize) -> Self {
        Self { max_size }
    }
}

impl AggregateFunction for GroupArraySortedFunction {
    fn name(&self) -> &str {
        "GROUP_ARRAY_SORTED"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupArraySortedAccumulator::new(self.max_size))
    }
}

#[derive(Debug, Clone)]
pub struct GroupArrayLastAccumulator {
    values: std::collections::VecDeque<Value>,
    max_size: usize,
}

impl Default for GroupArrayLastAccumulator {
    fn default() -> Self {
        Self::new(10)
    }
}

impl GroupArrayLastAccumulator {
    pub fn new(max_size: usize) -> Self {
        Self {
            values: std::collections::VecDeque::new(),
            max_size,
        }
    }
}

impl Accumulator for GroupArrayLastAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        self.values.push_back(value.clone());
        if self.values.len() > self.max_size {
            self.values.pop_front();
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "GROUP_ARRAY_LAST merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let result: Vec<Value> = self.values.iter().cloned().collect();
        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct GroupArrayLastFunction {
    max_size: usize,
}

impl Default for GroupArrayLastFunction {
    fn default() -> Self {
        Self { max_size: 10 }
    }
}

impl GroupArrayLastFunction {
    pub fn new(max_size: usize) -> Self {
        Self { max_size }
    }
}

impl AggregateFunction for GroupArrayLastFunction {
    fn name(&self) -> &str {
        "GROUP_ARRAY_LAST"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(GroupArrayLastAccumulator::new(self.max_size))
    }
}

#[derive(Debug, Clone)]
pub struct SumDistinctAccumulator {
    values: std::collections::HashSet<String>,
    sum: f64,
}

impl Default for SumDistinctAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SumDistinctAccumulator {
    pub fn new() -> Self {
        Self {
            values: std::collections::HashSet::new(),
            sum: 0.0,
        }
    }
}

impl Accumulator for SumDistinctAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        let key = value_to_string(value);
        if self.values.insert(key) {
            if let Some(v) = numeric_value_to_f64(value)? {
                self.sum += v;
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "SUM_DISTINCT merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::float64(self.sum))
    }

    fn reset(&mut self) {
        self.values.clear();
        self.sum = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SumDistinctFunction;

impl AggregateFunction for SumDistinctFunction {
    fn name(&self) -> &str {
        "SUM_DISTINCT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumDistinctAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct AvgDistinctAccumulator {
    values: std::collections::HashSet<String>,
    sum: f64,
    count: usize,
}

impl Default for AvgDistinctAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AvgDistinctAccumulator {
    pub fn new() -> Self {
        Self {
            values: std::collections::HashSet::new(),
            sum: 0.0,
            count: 0,
        }
    }
}

impl Accumulator for AvgDistinctAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        let key = value_to_string(value);
        if self.values.insert(key) {
            if let Some(v) = numeric_value_to_f64(value)? {
                self.sum += v;
                self.count += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "AVG_DISTINCT merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count == 0 {
            return Ok(Value::null());
        }
        Ok(Value::float64(self.sum / self.count as f64))
    }

    fn reset(&mut self) {
        self.values.clear();
        self.sum = 0.0;
        self.count = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AvgDistinctFunction;

impl AggregateFunction for AvgDistinctFunction {
    fn name(&self) -> &str {
        "AVG_DISTINCT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgDistinctAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct HistogramAccumulator {
    values: Vec<f64>,
    num_bins: usize,
}

impl Default for HistogramAccumulator {
    fn default() -> Self {
        Self::new(10)
    }
}

impl HistogramAccumulator {
    pub fn new(num_bins: usize) -> Self {
        Self {
            values: Vec::new(),
            num_bins,
        }
    }
}

impl Accumulator for HistogramAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(v) = numeric_value_to_f64(value)? {
            self.values.push(v);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "HISTOGRAM merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::array(Vec::new()));
        }

        let min_val = self.values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_val = self
            .values
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);

        if min_val == max_val {
            let result = vec![Value::array(vec![
                Value::float64(min_val),
                Value::float64(max_val),
                Value::int64(self.values.len() as i64),
            ])];
            return Ok(Value::array(result));
        }

        let bin_width = (max_val - min_val) / self.num_bins as f64;
        let mut counts = vec![0usize; self.num_bins];

        for &v in &self.values {
            let bin = ((v - min_val) / bin_width).floor() as usize;
            let bin = bin.min(self.num_bins - 1);
            counts[bin] += 1;
        }

        let result: Vec<Value> = counts
            .iter()
            .enumerate()
            .filter(|(_, c)| **c > 0)
            .map(|(i, count)| {
                let lower = min_val + i as f64 * bin_width;
                let upper = min_val + (i + 1) as f64 * bin_width;
                Value::array(vec![
                    Value::float64(lower),
                    Value::float64(upper),
                    Value::int64(*count as i64),
                ])
            })
            .collect();

        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct HistogramFunction {
    num_bins: usize,
}

impl Default for HistogramFunction {
    fn default() -> Self {
        Self { num_bins: 10 }
    }
}

impl HistogramFunction {
    pub fn new(num_bins: usize) -> Self {
        Self { num_bins }
    }
}

impl AggregateFunction for HistogramFunction {
    fn name(&self) -> &str {
        "HISTOGRAM"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Unknown,
        )))))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(HistogramAccumulator::new(self.num_bins))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileExactLowAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl Default for QuantileExactLowAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileExactLowAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }
}

impl Accumulator for QuantileExactLowAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(v) = numeric_value_to_f64(value)? {
            self.values.push(v);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "QUANTILE_EXACT_LOW merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        let idx = ((sorted.len() as f64 * self.percentile).floor() as usize).min(sorted.len() - 1);
        Ok(Value::float64(sorted[idx]))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileExactLowFunction {
    percentile: f64,
}

impl Default for QuantileExactLowFunction {
    fn default() -> Self {
        Self { percentile: 0.5 }
    }
}

impl AggregateFunction for QuantileExactLowFunction {
    fn name(&self) -> &str {
        "QUANTILE_EXACT_LOW"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileExactLowAccumulator::new(self.percentile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileExactHighAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl Default for QuantileExactHighAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileExactHighAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }
}

impl Accumulator for QuantileExactHighAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(v) = numeric_value_to_f64(value)? {
            self.values.push(v);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "QUANTILE_EXACT_HIGH merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        let idx = ((sorted.len() as f64 * self.percentile).ceil() as usize).min(sorted.len() - 1);
        Ok(Value::float64(sorted[idx]))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileExactHighFunction {
    percentile: f64,
}

impl Default for QuantileExactHighFunction {
    fn default() -> Self {
        Self { percentile: 0.5 }
    }
}

impl AggregateFunction for QuantileExactHighFunction {
    fn name(&self) -> &str {
        "QUANTILE_EXACT_HIGH"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileExactHighAccumulator::new(self.percentile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileDDAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl Default for QuantileDDAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileDDAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }
}

impl Accumulator for QuantileDDAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(v) = numeric_value_to_f64(value)? {
            self.values.push(v);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "QUANTILE_DD merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        let idx = ((sorted.len() as f64 - 1.0) * self.percentile).round() as usize;
        Ok(Value::float64(sorted[idx.min(sorted.len() - 1)]))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileDDFunction {
    percentile: f64,
}

impl Default for QuantileDDFunction {
    fn default() -> Self {
        Self { percentile: 0.5 }
    }
}

impl AggregateFunction for QuantileDDFunction {
    fn name(&self) -> &str {
        "QUANTILE_DD"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileDDAccumulator::new(self.percentile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileGKAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl Default for QuantileGKAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileGKAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }
}

impl Accumulator for QuantileGKAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(v) = numeric_value_to_f64(value)? {
            self.values.push(v);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "QUANTILE_GK merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        let idx = ((sorted.len() as f64 - 1.0) * self.percentile).round() as usize;
        Ok(Value::float64(sorted[idx.min(sorted.len() - 1)]))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileGKFunction {
    percentile: f64,
}

impl Default for QuantileGKFunction {
    fn default() -> Self {
        Self { percentile: 0.5 }
    }
}

impl AggregateFunction for QuantileGKFunction {
    fn name(&self) -> &str {
        "QUANTILE_GK"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileGKAccumulator::new(self.percentile))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileInterpolatedWeightedAccumulator {
    values: Vec<(f64, f64)>,
    percentile: f64,
}

impl Default for QuantileInterpolatedWeightedAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileInterpolatedWeightedAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }
}

impl Accumulator for QuantileInterpolatedWeightedAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        if let Some(arr) = value.as_array() {
            if arr.len() >= 2 {
                if let (Some(v), Some(w)) = (
                    numeric_value_to_f64(&arr[0])?,
                    numeric_value_to_f64(&arr[1])?,
                ) {
                    self.values.push((v, w));
                }
            }
        } else if let Some(v) = numeric_value_to_f64(value)? {
            self.values.push((v, 1.0));
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "QUANTILE_INTERPOLATED_WEIGHTED merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

        let total_weight: f64 = sorted.iter().map(|(_, w)| w).sum();
        let target = total_weight * self.percentile;
        let mut cumulative = 0.0;

        for &(val, weight) in &sorted {
            cumulative += weight;
            if cumulative >= target {
                return Ok(Value::float64(val));
            }
        }
        Ok(Value::float64(
            sorted.last().map(|(v, _)| *v).unwrap_or(0.0),
        ))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileInterpolatedWeightedFunction {
    percentile: f64,
}

impl Default for QuantileInterpolatedWeightedFunction {
    fn default() -> Self {
        Self { percentile: 0.5 }
    }
}

impl AggregateFunction for QuantileInterpolatedWeightedFunction {
    fn name(&self) -> &str {
        "QUANTILE_INTERPOLATED_WEIGHTED"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileInterpolatedWeightedAccumulator::new(
            self.percentile,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct QuantileBFloat16WeightedAccumulator {
    values: Vec<(f64, f64)>,
    percentile: f64,
}

impl Default for QuantileBFloat16WeightedAccumulator {
    fn default() -> Self {
        Self::new(0.5)
    }
}

impl QuantileBFloat16WeightedAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }
}

impl Accumulator for QuantileBFloat16WeightedAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        if let Some(arr) = value.as_array() {
            if arr.len() >= 2 {
                if let (Some(v), Some(w)) = (
                    numeric_value_to_f64(&arr[0])?,
                    numeric_value_to_f64(&arr[1])?,
                ) {
                    self.values.push((v, w));
                }
            }
        } else if let Some(v) = numeric_value_to_f64(value)? {
            self.values.push((v, 1.0));
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "QUANTILE_BFLOAT16_WEIGHTED merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

        let total_weight: f64 = sorted.iter().map(|(_, w)| w).sum();
        let target = total_weight * self.percentile;
        let mut cumulative = 0.0;

        for &(val, weight) in &sorted {
            cumulative += weight;
            if cumulative >= target {
                return Ok(Value::float64(val));
            }
        }
        Ok(Value::float64(
            sorted.last().map(|(v, _)| *v).unwrap_or(0.0),
        ))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct QuantileBFloat16WeightedFunction {
    percentile: f64,
}

impl Default for QuantileBFloat16WeightedFunction {
    fn default() -> Self {
        Self { percentile: 0.5 }
    }
}

impl AggregateFunction for QuantileBFloat16WeightedFunction {
    fn name(&self) -> &str {
        "QUANTILE_BFLOAT16_WEIGHTED"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(QuantileBFloat16WeightedAccumulator::new(self.percentile))
    }
}

#[derive(Debug, Clone)]
pub struct SumStateAccumulator {
    sum: Value,
}

impl Default for SumStateAccumulator {
    fn default() -> Self {
        Self {
            sum: Value::int64(0),
        }
    }
}

impl Accumulator for SumStateAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        self.sum = if let (Some(a), Some(b)) = (self.sum.as_i64(), value.as_i64()) {
            Value::int64(a + b)
        } else if let (Some(a), Some(b)) = (self.sum.as_i64(), value.as_f64()) {
            Value::float64(a as f64 + b)
        } else if let (Some(a), Some(b)) = (self.sum.as_f64(), value.as_i64()) {
            Value::float64(a + b as f64)
        } else if let (Some(a), Some(b)) = (self.sum.as_f64(), value.as_f64()) {
            Value::float64(a + b)
        } else if self.sum.as_i64() == Some(0) {
            value.clone()
        } else {
            return Err(Error::TypeMismatch {
                expected: "numeric".to_string(),
                actual: value.data_type().to_string(),
            });
        };
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "SUMSTATE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.sum.clone())
    }

    fn reset(&mut self) {
        self.sum = Value::int64(0);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SumStateFunction;

impl AggregateFunction for SumStateFunction {
    fn name(&self) -> &str {
        "SUMSTATE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumStateAccumulator::default())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SumMergeFunction;

impl AggregateFunction for SumMergeFunction {
    fn name(&self) -> &str {
        "SUMMERGE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumStateAccumulator::default())
    }
}

#[derive(Debug, Clone)]
pub struct AvgStateAccumulator {
    sum: Value,
    count: i64,
}

impl Default for AvgStateAccumulator {
    fn default() -> Self {
        Self {
            sum: Value::int64(0),
            count: 0,
        }
    }
}

impl Accumulator for AvgStateAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        self.sum = if let (Some(a), Some(b)) = (self.sum.as_i64(), value.as_i64()) {
            Value::int64(a + b)
        } else if let (Some(a), Some(b)) = (self.sum.as_i64(), value.as_f64()) {
            Value::float64(a as f64 + b)
        } else if let (Some(a), Some(b)) = (self.sum.as_f64(), value.as_i64()) {
            Value::float64(a + b as f64)
        } else if let (Some(a), Some(b)) = (self.sum.as_f64(), value.as_f64()) {
            Value::float64(a + b)
        } else if self.sum.as_i64() == Some(0) {
            value.clone()
        } else {
            return Err(Error::TypeMismatch {
                expected: "numeric".to_string(),
                actual: value.data_type().to_string(),
            });
        };
        self.count += 1;
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "AVGSTATE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count == 0 {
            return Ok(Value::null());
        }
        let sum_f64 = self
            .sum
            .as_f64()
            .unwrap_or_else(|| self.sum.as_i64().map(|i| i as f64).unwrap_or(0.0));
        Ok(Value::float64(sum_f64 / self.count as f64))
    }

    fn reset(&mut self) {
        self.sum = Value::int64(0);
        self.count = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AvgStateFunction;

impl AggregateFunction for AvgStateFunction {
    fn name(&self) -> &str {
        "AVGSTATE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgStateAccumulator::default())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AvgMergeFunction;

impl AggregateFunction for AvgMergeFunction {
    fn name(&self) -> &str {
        "AVGMERGE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgStateAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct CountStateAccumulator {
    count: i64,
}

impl Accumulator for CountStateAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if !value.is_null() {
            self.count += 1;
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "COUNTSTATE merge not implemented".to_string(),
        ))
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

#[derive(Debug, Default, Clone, Copy)]
pub struct CountStateFunction;

impl AggregateFunction for CountStateFunction {
    fn name(&self) -> &str {
        "COUNTSTATE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountStateAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct CountMergeAccumulator {
    sum: i64,
}

impl Accumulator for CountMergeAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        if let Some(i) = value.as_i64() {
            self.sum += i;
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "COUNTMERGE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::int64(self.sum))
    }

    fn reset(&mut self) {
        self.sum = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CountMergeFunction;

impl AggregateFunction for CountMergeFunction {
    fn name(&self) -> &str {
        "COUNTMERGE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountMergeAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct MinStateAccumulator {
    min: Option<Value>,
}

impl MinStateAccumulator {
    fn compare_values(a: &Value, b: &Value) -> Result<i32> {
        if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
            return Ok(x.cmp(&y) as i32);
        }
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return if x < y {
                Ok(-1)
            } else if x > y {
                Ok(1)
            } else {
                Ok(0)
            };
        }
        if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
            return Ok(x.cmp(y) as i32);
        }
        Err(Error::InternalError(format!(
            "Cannot compare values: {:?} vs {:?}",
            a, b
        )))
    }
}

impl Accumulator for MinStateAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        self.min = match &self.min {
            None => Some(value.clone()),
            Some(current) => {
                let cmp = Self::compare_values(value, current)?;
                if cmp < 0 {
                    Some(value.clone())
                } else {
                    self.min.clone()
                }
            }
        };
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MINSTATE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.min.clone().unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.min = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MinStateFunction;

impl AggregateFunction for MinStateFunction {
    fn name(&self) -> &str {
        "MINSTATE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Unknown))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MinStateAccumulator::default())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MinMergeFunction;

impl AggregateFunction for MinMergeFunction {
    fn name(&self) -> &str {
        "MINMERGE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Unknown))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MinStateAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct MaxStateAccumulator {
    max: Option<Value>,
}

impl MaxStateAccumulator {
    fn compare_values(a: &Value, b: &Value) -> Result<i32> {
        if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
            return Ok(x.cmp(&y) as i32);
        }
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return if x < y {
                Ok(-1)
            } else if x > y {
                Ok(1)
            } else {
                Ok(0)
            };
        }
        if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
            return Ok(x.cmp(y) as i32);
        }
        Err(Error::InternalError(format!(
            "Cannot compare values: {:?} vs {:?}",
            a, b
        )))
    }
}

impl Accumulator for MaxStateAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        self.max = match &self.max {
            None => Some(value.clone()),
            Some(current) => {
                let cmp = Self::compare_values(value, current)?;
                if cmp > 0 {
                    Some(value.clone())
                } else {
                    self.max.clone()
                }
            }
        };
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MAXSTATE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.max.clone().unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.max = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MaxStateFunction;

impl AggregateFunction for MaxStateFunction {
    fn name(&self) -> &str {
        "MAXSTATE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Unknown))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MaxStateAccumulator::default())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MaxMergeFunction;

impl AggregateFunction for MaxMergeFunction {
    fn name(&self) -> &str {
        "MAXMERGE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Unknown))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MaxStateAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct UniqStateAccumulator {
    values: std::collections::HashSet<String>,
}

impl Accumulator for UniqStateAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        self.values.insert(format!("{:?}", value));
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "UNIQSTATE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::int64(self.values.len() as i64))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UniqStateFunction;

impl AggregateFunction for UniqStateFunction {
    fn name(&self) -> &str {
        "UNIQSTATE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(UniqStateAccumulator::default())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UniqMergeFunction;

impl AggregateFunction for UniqMergeFunction {
    fn name(&self) -> &str {
        "UNIQMERGE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumStateAccumulator::default())
    }
}
