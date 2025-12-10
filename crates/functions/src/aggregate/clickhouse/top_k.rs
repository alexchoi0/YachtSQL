use std::cmp::Ordering;
use std::collections::HashMap;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};

use super::super::{Accumulator, AggregateFunction};
use super::common::{numeric_value_to_f64, value_to_string};

#[derive(Debug, Clone)]
pub struct TopKAccumulator {
    counts: HashMap<String, i64>,
    k: usize,
}

impl TopKAccumulator {
    pub fn new(k: i64) -> Self {
        Self {
            counts: HashMap::new(),
            k: k.max(1) as usize,
        }
    }
}

impl Accumulator for TopKAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let key = value_to_string(value);
        *self.counts.entry(key).or_insert(0) += 1;
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for TopKAccumulator merge"))?;
        for (key, count) in &other.counts {
            *self.counts.entry(key.clone()).or_insert(0) += count;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        let mut items: Vec<_> = self.counts.iter().collect();
        items.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));

        let result: Vec<Value> = items
            .into_iter()
            .take(self.k)
            .map(|(key, _)| Value::string(key.clone()))
            .collect();

        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.counts.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TopKFunction {
    k: i64,
}

impl TopKFunction {
    pub fn new(k: i64) -> Self {
        Self { k }
    }
}

impl Default for TopKFunction {
    fn default() -> Self {
        Self::new(10)
    }
}

impl AggregateFunction for TopKFunction {
    fn name(&self) -> &str {
        "TOP_K"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::String)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(TopKAccumulator::new(self.k))
    }
}

#[derive(Debug, Clone)]
pub struct TopKWeightedAccumulator {
    sums: HashMap<String, f64>,
    k: usize,
}

impl TopKWeightedAccumulator {
    pub fn new(k: i64) -> Self {
        Self {
            sums: HashMap::new(),
            k: k.max(1) as usize,
        }
    }
}

impl Accumulator for TopKWeightedAccumulator {
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
                    expected: "ARRAY with 2 values [value, weight]".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY with 2 values [value, weight]".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        let weight_f64 = numeric_value_to_f64(weight)?.ok_or_else(|| Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: "NULL".to_string(),
        })?;

        let key = value_to_string(val);
        *self.sums.entry(key).or_insert(0.0) += weight_f64;

        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other.as_any().downcast_ref::<Self>().ok_or_else(|| {
            Error::internal("Invalid accumulator type for TopKWeightedAccumulator merge")
        })?;
        for (key, weight) in &other.sums {
            *self.sums.entry(key.clone()).or_insert(0.0) += weight;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        let mut items: Vec<_> = self.sums.iter().collect();
        items.sort_by(|a, b| {
            b.1.partial_cmp(a.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.0.cmp(b.0))
        });

        let result: Vec<Value> = items
            .into_iter()
            .take(self.k)
            .map(|(key, _)| Value::string(key.clone()))
            .collect();

        Ok(Value::array(result))
    }

    fn reset(&mut self) {
        self.sums.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TopKWeightedFunction {
    k: i64,
}

impl TopKWeightedFunction {
    pub fn new(k: i64) -> Self {
        Self { k }
    }
}

impl Default for TopKWeightedFunction {
    fn default() -> Self {
        Self::new(10)
    }
}

impl AggregateFunction for TopKWeightedFunction {
    fn name(&self) -> &str {
        "TOP_K_WEIGHTED"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::String)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(TopKWeightedAccumulator::new(self.k))
    }
}
