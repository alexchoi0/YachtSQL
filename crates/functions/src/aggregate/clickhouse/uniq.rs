use std::collections::HashSet;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};

use super::super::{Accumulator, AggregateFunction};
use super::common::{HashableValue, value_to_string};
use crate::approximate::HyperLogLogPlusPlus;

#[derive(Debug, Clone)]
pub struct UniqAccumulator {
    estimator: HyperLogLogPlusPlus,
}

impl Default for UniqAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl UniqAccumulator {
    pub fn new() -> Self {
        Self {
            estimator: HyperLogLogPlusPlus::new(),
        }
    }
}

impl Accumulator for UniqAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        self.estimator.add(&HashableValue(value));
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| Error::internal("Invalid accumulator type for UniqAccumulator merge"))?;
        self.estimator.merge(&other.estimator);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::int64(self.estimator.estimate() as i64))
    }

    fn reset(&mut self) {
        self.estimator = HyperLogLogPlusPlus::new();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UniqFunction;

impl AggregateFunction for UniqFunction {
    fn name(&self) -> &str {
        "UNIQ"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(UniqAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct UniqExactAccumulator {
    values: HashSet<String>,
}

impl Default for UniqExactAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl UniqExactAccumulator {
    pub fn new() -> Self {
        Self {
            values: HashSet::new(),
        }
    }
}

impl Accumulator for UniqExactAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        self.values.insert(value_to_string(value));
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other.as_any().downcast_ref::<Self>().ok_or_else(|| {
            Error::internal("Invalid accumulator type for UniqExactAccumulator merge")
        })?;
        self.values.extend(other.values.iter().cloned());
        Ok(())
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
pub struct UniqExactFunction;

impl AggregateFunction for UniqExactFunction {
    fn name(&self) -> &str {
        "UNIQ_EXACT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(UniqExactAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UniqHll12Function;

impl AggregateFunction for UniqHll12Function {
    fn name(&self) -> &str {
        "UNIQ_HLL12"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(UniqAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct UniqCombinedAccumulator {
    estimator: HyperLogLogPlusPlus,
}

impl Default for UniqCombinedAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl UniqCombinedAccumulator {
    pub fn new() -> Self {
        Self {
            estimator: HyperLogLogPlusPlus::new(),
        }
    }
}

impl Accumulator for UniqCombinedAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        self.estimator.add(&HashableValue(value));
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other.as_any().downcast_ref::<Self>().ok_or_else(|| {
            Error::internal("Invalid accumulator type for UniqCombinedAccumulator merge")
        })?;
        self.estimator.merge(&other.estimator);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::int64(self.estimator.estimate() as i64))
    }

    fn reset(&mut self) {
        self.estimator = HyperLogLogPlusPlus::new();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UniqCombinedFunction;

impl AggregateFunction for UniqCombinedFunction {
    fn name(&self) -> &str {
        "UNIQ_COMBINED"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(UniqCombinedAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UniqCombined64Function;

impl AggregateFunction for UniqCombined64Function {
    fn name(&self) -> &str {
        "UNIQ_COMBINED_64"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(UniqCombinedAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct UniqThetaSketchAccumulator {
    estimator: HyperLogLogPlusPlus,
}

impl Default for UniqThetaSketchAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl UniqThetaSketchAccumulator {
    pub fn new() -> Self {
        Self {
            estimator: HyperLogLogPlusPlus::new(),
        }
    }
}

impl Accumulator for UniqThetaSketchAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        self.estimator.add(&HashableValue(value));
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other.as_any().downcast_ref::<Self>().ok_or_else(|| {
            Error::internal("Invalid accumulator type for UniqThetaSketchAccumulator merge")
        })?;
        self.estimator.merge(&other.estimator);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::int64(self.estimator.estimate() as i64))
    }

    fn reset(&mut self) {
        self.estimator = HyperLogLogPlusPlus::new();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UniqThetaSketchFunction;

impl AggregateFunction for UniqThetaSketchFunction {
    fn name(&self) -> &str {
        "UNIQ_THETA_SKETCH"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(UniqThetaSketchAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct UniqUpToAccumulator {
    values: HashSet<String>,
    threshold: usize,
}

impl Default for UniqUpToAccumulator {
    fn default() -> Self {
        Self::new(100)
    }
}

impl UniqUpToAccumulator {
    pub fn new(threshold: usize) -> Self {
        Self {
            values: HashSet::new(),
            threshold,
        }
    }
}

impl Accumulator for UniqUpToAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if self.values.len() < self.threshold {
            let key = value_to_string(value);
            self.values.insert(key);
        }

        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other.as_any().downcast_ref::<Self>().ok_or_else(|| {
            Error::internal("Invalid accumulator type for UniqUpToAccumulator merge")
        })?;
        for value in &other.values {
            if self.values.len() >= self.threshold {
                break;
            }
            self.values.insert(value.clone());
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        let count = self.values.len();
        if count >= self.threshold {
            Ok(Value::int64(self.threshold as i64 + 1))
        } else {
            Ok(Value::int64(count as i64))
        }
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct UniqUpToFunction {
    threshold: usize,
}

impl Default for UniqUpToFunction {
    fn default() -> Self {
        Self { threshold: 100 }
    }
}

impl UniqUpToFunction {
    pub fn new(threshold: usize) -> Self {
        Self { threshold }
    }
}

impl AggregateFunction for UniqUpToFunction {
    fn name(&self) -> &str {
        "UNIQ_UP_TO"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(UniqUpToAccumulator::new(self.threshold))
    }
}
