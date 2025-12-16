use std::fmt::Debug;

use yachtsql_core::error::Result;
use yachtsql_core::types::{DataType, Value};

pub trait AggregateFunction: Debug + Send + Sync {
    fn name(&self) -> &str;

    fn arg_types(&self) -> &[DataType];

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

    fn is_variadic(&self) -> bool {
        false
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator>;
}

pub trait Accumulator: Debug + Send {
    fn accumulate(&mut self, value: &Value) -> Result<()>;

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()>;

    fn finalize(&self) -> Result<Value>;

    fn reset(&mut self);

    fn as_any(&self) -> &dyn std::any::Any;
}

pub mod approximate;
pub mod array_agg;
pub mod bigquery;
pub mod boolean_bitwise;
pub mod conditional;
pub mod json_agg;
pub mod statistical;
pub mod string_agg;
pub mod window_functions;
