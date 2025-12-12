use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

use super::{Accumulator, AggregateFunction};

#[derive(Debug, Clone, Default)]
pub struct CountIfAccumulator {
    count: i64,
}

impl CountIfAccumulator {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountIfAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(b) = value.as_bool() {
            if b {
                self.count += 1;
            }

            return Ok(());
        }

        Err(Error::TypeMismatch {
            expected: "BOOL".to_string(),
            actual: value.data_type().to_string(),
        })
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "COUNTIF merge not implemented".to_string(),
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
pub struct CountIfFunction;

impl AggregateFunction for CountIfFunction {
    fn name(&self) -> &str {
        "COUNTIF"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Bool]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountIfAccumulator::new())
    }
}
