use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

use super::{Accumulator, AggregateFunction};

#[derive(Debug, Clone)]
pub struct BoolAndAccumulator {
    result: Option<bool>,
}

impl Default for BoolAndAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolAndAccumulator {
    pub fn new() -> Self {
        Self { result: None }
    }
}

impl Accumulator for BoolAndAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(b) = value.as_bool() {
            self.result = Some(match self.result {
                None => b,
                Some(prev) => prev && b,
            });
            return Ok(());
        }

        Err(Error::TypeMismatch {
            expected: "BOOLEAN".to_string(),
            actual: value.data_type().to_string(),
        })
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "BOOL_AND merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.result.map(Value::bool_val).unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.result = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct BoolOrAccumulator {
    result: Option<bool>,
}

impl Default for BoolOrAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolOrAccumulator {
    pub fn new() -> Self {
        Self { result: None }
    }
}

impl Accumulator for BoolOrAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(b) = value.as_bool() {
            self.result = Some(match self.result {
                None => b,
                Some(prev) => prev || b,
            });
            return Ok(());
        }

        Err(Error::TypeMismatch {
            expected: "BOOLEAN".to_string(),
            actual: value.data_type().to_string(),
        })
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "BOOL_OR merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.result.map(Value::bool_val).unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.result = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct BitAndAccumulator {
    result: Option<i64>,
}

impl Default for BitAndAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl BitAndAccumulator {
    pub fn new() -> Self {
        Self { result: None }
    }
}

impl Accumulator for BitAndAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(n) = value.as_i64() {
            self.result = Some(match self.result {
                None => n,
                Some(prev) => prev & n,
            });
            return Ok(());
        }

        Err(Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: value.data_type().to_string(),
        })
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "BIT_AND merge not implemented".to_string(),
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

#[derive(Debug, Clone)]
pub struct BitOrAccumulator {
    result: Option<i64>,
}

impl Default for BitOrAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl BitOrAccumulator {
    pub fn new() -> Self {
        Self { result: None }
    }
}

impl Accumulator for BitOrAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(n) = value.as_i64() {
            self.result = Some(match self.result {
                None => n,
                Some(prev) => prev | n,
            });
            return Ok(());
        }

        Err(Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: value.data_type().to_string(),
        })
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "BIT_OR merge not implemented".to_string(),
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

#[derive(Debug, Clone)]
pub struct BitXorAccumulator {
    result: Option<i64>,
}

impl Default for BitXorAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl BitXorAccumulator {
    pub fn new() -> Self {
        Self { result: None }
    }
}

impl Accumulator for BitXorAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(n) = value.as_i64() {
            self.result = Some(match self.result {
                None => n,
                Some(prev) => prev ^ n,
            });
            return Ok(());
        }

        Err(Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: value.data_type().to_string(),
        })
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "BIT_XOR merge not implemented".to_string(),
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
pub struct BoolAndFunction;

impl AggregateFunction for BoolAndFunction {
    fn name(&self) -> &str {
        "BOOL_AND"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Bool]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Bool)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BoolAndAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BoolOrFunction;

impl AggregateFunction for BoolOrFunction {
    fn name(&self) -> &str {
        "BOOL_OR"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Bool]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Bool)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BoolOrAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EveryFunction;

impl AggregateFunction for EveryFunction {
    fn name(&self) -> &str {
        "EVERY"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Bool]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Bool)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BoolAndAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BitAndFunction;

impl AggregateFunction for BitAndFunction {
    fn name(&self) -> &str {
        "BIT_AND"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Int64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BitAndAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BitOrFunction;

impl AggregateFunction for BitOrFunction {
    fn name(&self) -> &str {
        "BIT_OR"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Int64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BitOrAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BitXorFunction;

impl AggregateFunction for BitXorFunction {
    fn name(&self) -> &str {
        "BIT_XOR"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Int64]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BitXorAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct LogicalAndFunction;

impl AggregateFunction for LogicalAndFunction {
    fn name(&self) -> &str {
        "LOGICAL_AND"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Bool]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Bool)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BoolAndAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct LogicalOrFunction;

impl AggregateFunction for LogicalOrFunction {
    fn name(&self) -> &str {
        "LOGICAL_OR"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Bool]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Bool)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(BoolOrAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct AnyValueAccumulator {
    value: Option<Value>,
}

impl Default for AnyValueAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyValueAccumulator {
    pub fn new() -> Self {
        Self { value: None }
    }
}

impl Accumulator for AnyValueAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if self.value.is_none() && !value.is_null() {
            self.value = Some(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "ANY_VALUE merge not implemented".to_string(),
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
pub struct AnyValueFunction;

impl AggregateFunction for AnyValueFunction {
    fn name(&self) -> &str {
        "ANY_VALUE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Unknown)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AnyValueAccumulator::new())
    }
}
