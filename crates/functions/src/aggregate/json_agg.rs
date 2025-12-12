use serde_json::{Map, Value as JsonValue};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

use super::{Accumulator, AggregateFunction};
use crate::json::aggregates::{json_key_to_string, sql_value_to_json};

#[derive(Debug, Clone)]
pub struct JsonAggAccumulator {
    values: Vec<JsonValue>,
}

impl Default for JsonAggAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonAggAccumulator {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }
}

impl Accumulator for JsonAggAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        let json_value = sql_value_to_json(value);
        self.values.push(json_value);
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "JSON_AGG merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let json_array = JsonValue::Array(self.values.clone());
        Ok(Value::json(json_array))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct JsonObjectAggAccumulator {
    entries: Map<String, JsonValue>,
}

impl Default for JsonObjectAggAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObjectAggAccumulator {
    pub fn new() -> Self {
        Self {
            entries: Map::new(),
        }
    }
}

impl Accumulator for JsonObjectAggAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(values) = value.as_array() {
            if values.len() == 2 {
                if let Some(key) = json_key_to_string(&values[0]) {
                    let json_value = sql_value_to_json(&values[1]);
                    self.entries.insert(key, json_value);
                }
                Ok(())
            } else {
                Err(Error::TypeMismatch {
                    expected: "ARRAY with 2 values (key, value)".to_string(),
                    actual: value.data_type().to_string(),
                })
            }
        } else {
            Err(Error::TypeMismatch {
                expected: "ARRAY with 2 values (key, value)".to_string(),
                actual: value.data_type().to_string(),
            })
        }
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "JSON_OBJECT_AGG merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.entries.is_empty() {
            return Ok(Value::null());
        }

        let json_object = JsonValue::Object(self.entries.clone());
        Ok(Value::json(json_object))
    }

    fn reset(&mut self) {
        self.entries.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct JsonAggFunction;

impl AggregateFunction for JsonAggFunction {
    fn name(&self) -> &str {
        "JSON_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Json)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(JsonAggAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct JsonbAggFunction;

impl AggregateFunction for JsonbAggFunction {
    fn name(&self) -> &str {
        "JSONB_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Json)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(JsonAggAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct JsonObjectAggFunction;

impl AggregateFunction for JsonObjectAggFunction {
    fn name(&self) -> &str {
        "JSON_OBJECT_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Json)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(JsonObjectAggAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct JsonbObjectAggFunction;

impl AggregateFunction for JsonbObjectAggFunction {
    fn name(&self) -> &str {
        "JSONB_OBJECT_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Json)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(JsonObjectAggAccumulator::new())
    }
}
