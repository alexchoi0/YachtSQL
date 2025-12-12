use std::collections::HashSet;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

use super::{Accumulator, AggregateFunction};

#[derive(Debug, Clone)]
pub struct ArrayAggAccumulator {
    values: Vec<Value>,
    distinct: bool,
    seen: HashSet<String>,
}

impl Default for ArrayAggAccumulator {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ArrayAggAccumulator {
    pub fn new(distinct: bool) -> Self {
        Self {
            values: Vec::new(),
            distinct,
            seen: HashSet::new(),
        }
    }

    fn value_key(value: &Value) -> String {
        format!("{:?}", value)
    }
}

impl Accumulator for ArrayAggAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if self.distinct {
            if value.is_null() {
                self.values.push(value.clone());
                return Ok(());
            }
            let key = Self::value_key(value);
            if self.seen.insert(key) {
                self.values.push(value.clone());
            }
        } else {
            self.values.push(value.clone());
        }

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ARRAY_AGG merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }
        Ok(Value::array(self.values.clone()))
    }

    fn reset(&mut self) {
        self.values.clear();
        self.seen.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ArrayAggFunction {
    distinct: bool,
}

impl Default for ArrayAggFunction {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ArrayAggFunction {
    pub fn new(distinct: bool) -> Self {
        Self { distinct }
    }
}

impl AggregateFunction for ArrayAggFunction {
    fn name(&self) -> &str {
        "ARRAY_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArrayAggAccumulator::new(self.distinct))
    }
}

#[derive(Debug, Clone)]
pub struct ArrayConcatAggAccumulator {
    values: Vec<Value>,
}

impl Default for ArrayConcatAggAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayConcatAggAccumulator {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }
}

impl Accumulator for ArrayConcatAggAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(arr) = value.as_array() {
            self.values.extend(arr.iter().cloned());
            Ok(())
        } else {
            Err(Error::TypeMismatch {
                expected: "ARRAY".to_string(),
                actual: value.data_type().to_string(),
            })
        }
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ARRAY_CONCAT_AGG merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
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

#[derive(Debug, Clone, Copy, Default)]
pub struct ArrayConcatAggFunction;

impl AggregateFunction for ArrayConcatAggFunction {
    fn name(&self) -> &str {
        "ARRAY_CONCAT_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ArrayConcatAggAccumulator::new())
    }
}
