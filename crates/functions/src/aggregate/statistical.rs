use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

use super::{Accumulator, AggregateFunction};

const NUMERIC_ARG: &[DataType] = &[DataType::Float64];
const DYNAMIC_ARG: &[DataType] = &[DataType::Unknown];

#[derive(Debug, Clone)]
pub struct StdDevPopAccumulator {
    count: usize,
    mean: f64,
    m2: f64,
}

impl Default for StdDevPopAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl StdDevPopAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }
}

impl Accumulator for StdDevPopAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let x = value.as_f64().ok_or_else(|| Error::TypeMismatch {
            expected: "FLOAT64 or numeric type".to_string(),
            actual: value.data_type().to_string(),
        })?;

        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "STDDEV_POP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count == 0 {
            return Ok(Value::null());
        }
        if self.count == 1 {
            return Ok(Value::float64(0.0));
        }
        let variance = self.m2 / self.count as f64;
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

#[derive(Debug, Clone)]
pub struct StdDevSampAccumulator {
    count: usize,
    mean: f64,
    m2: f64,
}

impl Default for StdDevSampAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl StdDevSampAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }
}

impl Accumulator for StdDevSampAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let x = value.as_f64().ok_or_else(|| Error::TypeMismatch {
            expected: "FLOAT64 or numeric type".to_string(),
            actual: value.data_type().to_string(),
        })?;

        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "STDDEV_SAMP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count <= 1 {
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

#[derive(Debug, Clone)]
pub struct VarPopAccumulator {
    count: usize,
    mean: f64,
    m2: f64,
}

impl Default for VarPopAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl VarPopAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }
}

impl Accumulator for VarPopAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let x = value.as_f64().ok_or_else(|| Error::TypeMismatch {
            expected: "FLOAT64 or numeric type".to_string(),
            actual: value.data_type().to_string(),
        })?;

        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "VAR_POP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count == 0 {
            return Ok(Value::null());
        }
        Ok(Value::float64(self.m2 / self.count as f64))
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

#[derive(Debug, Clone)]
pub struct VarSampAccumulator {
    count: usize,
    mean: f64,
    m2: f64,
}

impl Default for VarSampAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl VarSampAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }
}

impl Accumulator for VarSampAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let x = value.as_f64().ok_or_else(|| Error::TypeMismatch {
            expected: "FLOAT64 or numeric type".to_string(),
            actual: value.data_type().to_string(),
        })?;

        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "VAR_SAMP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count <= 1 {
            return Ok(Value::null());
        }
        Ok(Value::float64(self.m2 / (self.count - 1) as f64))
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

#[derive(Debug, Clone)]
pub struct MedianAccumulator {
    values: Vec<f64>,
}

impl Default for MedianAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl MedianAccumulator {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }
}

impl Accumulator for MedianAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        let x = value.as_f64().ok_or_else(|| Error::TypeMismatch {
            expected: "FLOAT64 or numeric type".to_string(),
            actual: value.data_type().to_string(),
        })?;
        self.values.push(x);
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MEDIAN merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let len = sorted.len();
        if len % 2 == 1 {
            Ok(Value::float64(sorted[len / 2]))
        } else {
            let mid = len / 2;
            Ok(Value::float64((sorted[mid - 1] + sorted[mid]) / 2.0))
        }
    }

    fn reset(&mut self) {
        self.values.clear();
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct ModeAccumulator {
    counts: HashMap<String, (usize, Value)>,
}

impl ModeAccumulator {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
        }
    }

    fn key_for(value: &Value) -> String {
        if let Some(s) = value.as_str() {
            format!("str:{}", s)
        } else if let Some(b) = value.as_bytes() {
            format!("bytes:{:?}", b)
        } else if let Some(u) = value.as_uuid() {
            format!("uuid:{}", u)
        } else if let Some(j) = value.as_json() {
            format!("json:{}", j)
        } else {
            format!("{}::{:?}", value.data_type(), value)
        }
    }
}

impl Accumulator for ModeAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let key = Self::key_for(value);
        let entry = self.counts.entry(key).or_insert((0, value.clone()));
        entry.0 += 1;
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MODE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        let mut best: Option<(&Value, usize)> = None;
        for (count, value) in self.counts.values() {
            match best {
                None => best = Some((value, *count)),
                Some((_, best_count)) if *count > best_count => best = Some((value, *count)),
                _ => {}
            }
        }
        Ok(best
            .map(|(value, _)| value.clone())
            .unwrap_or(Value::null()))
    }

    fn reset(&mut self) {
        self.counts.clear();
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

macro_rules! simple_numeric_aggregate {
    ($name:ident, $display:expr, $return_ty:expr, $acc:ty) => {
        #[derive(Debug, Default, Clone, Copy)]
        pub struct $name;

        impl AggregateFunction for $name {
            fn name(&self) -> &str {
                $display
            }

            fn arg_types(&self) -> &[DataType] {
                NUMERIC_ARG
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok($return_ty)
            }

            fn create_accumulator(&self) -> Box<dyn Accumulator> {
                Box::new(<$acc>::new())
            }
        }
    };
}

simple_numeric_aggregate!(
    StddevPopFunction,
    "STDDEV_POP",
    DataType::Float64,
    StdDevPopAccumulator
);
simple_numeric_aggregate!(
    StddevSampFunction,
    "STDDEV_SAMP",
    DataType::Float64,
    StdDevSampAccumulator
);
simple_numeric_aggregate!(
    StddevFunction,
    "STDDEV",
    DataType::Float64,
    StdDevSampAccumulator
);
simple_numeric_aggregate!(
    VarPopFunction,
    "VAR_POP",
    DataType::Float64,
    VarPopAccumulator
);
simple_numeric_aggregate!(
    VarSampFunction,
    "VAR_SAMP",
    DataType::Float64,
    VarSampAccumulator
);
simple_numeric_aggregate!(
    VarianceFunction,
    "VARIANCE",
    DataType::Float64,
    VarSampAccumulator
);
simple_numeric_aggregate!(
    MedianFunction,
    "MEDIAN",
    DataType::Float64,
    MedianAccumulator
);

#[derive(Debug, Default, Clone, Copy)]
pub struct ModeFunction;

impl AggregateFunction for ModeFunction {
    fn name(&self) -> &str {
        "MODE"
    }

    fn arg_types(&self) -> &[DataType] {
        DYNAMIC_ARG
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Unknown)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ModeAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct CorrAccumulator {
    count: usize,
    mean_x: f64,
    mean_y: f64,
    m2_x: f64,
    m2_y: f64,
    coproduct: f64,
}

impl Default for CorrAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl CorrAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean_x: 0.0,
            mean_y: 0.0,
            m2_x: 0.0,
            m2_y: 0.0,
            coproduct: 0.0,
        }
    }
}

impl Accumulator for CorrAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (x, y) = if let Some(values) = value.as_array() {
            if values.len() == 2 {
                let x_val = values[0].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[0].data_type().to_string(),
                })?;
                let y_val = values[1].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[1].data_type().to_string(),
                })?;
                (x_val, y_val)
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY with 2 FLOAT64 values".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY with 2 FLOAT64 values".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        self.count += 1;
        let n = self.count as f64;

        let delta_x = x - self.mean_x;
        let delta_y = y - self.mean_y;

        self.mean_x += delta_x / n;
        self.mean_y += delta_y / n;

        let delta_x2 = x - self.mean_x;
        let delta_y2 = y - self.mean_y;

        self.m2_x += delta_x * delta_x2;
        self.m2_y += delta_y * delta_y2;
        self.coproduct += delta_x * delta_y2;

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "CORR merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count < 2 {
            return Ok(Value::null());
        }

        let var_x = self.m2_x / self.count as f64;
        let var_y = self.m2_y / self.count as f64;

        if var_x == 0.0 || var_y == 0.0 {
            return Ok(Value::null());
        }

        let corr = self.coproduct / (self.count as f64 * var_x.sqrt() * var_y.sqrt());
        Ok(Value::float64(corr))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.mean_x = 0.0;
        self.mean_y = 0.0;
        self.m2_x = 0.0;
        self.m2_y = 0.0;
        self.coproduct = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct CovarPopAccumulator {
    count: usize,
    mean_x: f64,
    mean_y: f64,
    coproduct: f64,
}

impl Default for CovarPopAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl CovarPopAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean_x: 0.0,
            mean_y: 0.0,
            coproduct: 0.0,
        }
    }
}

impl Accumulator for CovarPopAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (x, y) = if let Some(values) = value.as_array() {
            if values.len() == 2 {
                let x_val = values[0].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[0].data_type().to_string(),
                })?;
                let y_val = values[1].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[1].data_type().to_string(),
                })?;
                (x_val, y_val)
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY with 2 FLOAT64 values".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY with 2 FLOAT64 values".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        self.count += 1;
        let n = self.count as f64;

        let delta_x = x - self.mean_x;
        let delta_y = y - self.mean_y;

        self.mean_x += delta_x / n;
        self.mean_y += delta_y / n;

        let delta_y2 = y - self.mean_y;
        self.coproduct += delta_x * delta_y2;

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "COVAR_POP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count == 0 {
            return Ok(Value::null());
        }
        Ok(Value::float64(self.coproduct / self.count as f64))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.mean_x = 0.0;
        self.mean_y = 0.0;
        self.coproduct = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct CovarSampAccumulator {
    count: usize,
    mean_x: f64,
    mean_y: f64,
    coproduct: f64,
}

impl Default for CovarSampAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl CovarSampAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean_x: 0.0,
            mean_y: 0.0,
            coproduct: 0.0,
        }
    }
}

impl Accumulator for CovarSampAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (x, y) = if let Some(values) = value.as_array() {
            if values.len() == 2 {
                let x_val = values[0].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[0].data_type().to_string(),
                })?;
                let y_val = values[1].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[1].data_type().to_string(),
                })?;
                (x_val, y_val)
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY with 2 FLOAT64 values".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY with 2 FLOAT64 values".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        self.count += 1;
        let n = self.count as f64;

        let delta_x = x - self.mean_x;
        let delta_y = y - self.mean_y;

        self.mean_x += delta_x / n;
        self.mean_y += delta_y / n;

        let delta_y2 = y - self.mean_y;
        self.coproduct += delta_x * delta_y2;

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "COVAR_SAMP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count <= 1 {
            return Ok(Value::null());
        }
        Ok(Value::float64(self.coproduct / (self.count - 1) as f64))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.mean_x = 0.0;
        self.mean_y = 0.0;
        self.coproduct = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct PercentileContAccumulator {
    values: Vec<f64>,
    percentile: f64,
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
        if value.is_null() {
            return Ok(());
        }
        let x = value.as_f64().ok_or_else(|| Error::TypeMismatch {
            expected: "FLOAT64 or numeric type".to_string(),
            actual: value.data_type().to_string(),
        })?;
        self.values.push(x);
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "PERCENTILE_CONT merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = sorted.len();
        let index = self.percentile * (n - 1) as f64;
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;

        if lower == upper {
            Ok(Value::float64(sorted[lower]))
        } else {
            let fraction = index - lower as f64;
            let interpolated = sorted[lower] * (1.0 - fraction) + sorted[upper] * fraction;
            Ok(Value::float64(interpolated))
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
pub struct PercentileDiscAccumulator {
    values: Vec<f64>,
    percentile: f64,
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
        if value.is_null() {
            return Ok(());
        }
        let x = value.as_f64().ok_or_else(|| Error::TypeMismatch {
            expected: "FLOAT64 or numeric type".to_string(),
            actual: value.data_type().to_string(),
        })?;
        self.values.push(x);
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "PERCENTILE_DISC merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = sorted.len();
        let index = (self.percentile * n as f64).ceil() as usize - 1;
        let index = index.min(n - 1);

        Ok(Value::float64(sorted[index]))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CorrFunction;

impl AggregateFunction for CorrFunction {
    fn name(&self) -> &str {
        "CORR"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CorrAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CovarPopFunction;

impl AggregateFunction for CovarPopFunction {
    fn name(&self) -> &str {
        "COVAR_POP"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CovarPopAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CovarSampFunction;

impl AggregateFunction for CovarSampFunction {
    fn name(&self) -> &str {
        "COVAR_SAMP"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CovarSampAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct RegrSlopeAccumulator {
    count: usize,
    sum_x: f64,
    sum_y: f64,
    sum_xx: f64,
    sum_xy: f64,
}

impl Default for RegrSlopeAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl RegrSlopeAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum_x: 0.0,
            sum_y: 0.0,
            sum_xx: 0.0,
            sum_xy: 0.0,
        }
    }
}

impl Accumulator for RegrSlopeAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (y, x) = if let Some(values) = value.as_array() {
            if values.len() == 2 {
                let y_val = values[0].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[0].data_type().to_string(),
                })?;
                let x_val = values[1].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[1].data_type().to_string(),
                })?;
                (y_val, x_val)
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY with 2 FLOAT64 values".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY with 2 FLOAT64 values".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        self.count += 1;
        self.sum_x += x;
        self.sum_y += y;
        self.sum_xx += x * x;
        self.sum_xy += x * y;

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "REGR_SLOPE merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count < 2 {
            return Ok(Value::null());
        }

        let n = self.count as f64;
        let numerator = n * self.sum_xy - self.sum_x * self.sum_y;
        let denominator = n * self.sum_xx - self.sum_x * self.sum_x;

        if denominator.abs() < f64::EPSILON {
            return Ok(Value::null());
        }

        Ok(Value::float64(numerator / denominator))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.sum_x = 0.0;
        self.sum_y = 0.0;
        self.sum_xx = 0.0;
        self.sum_xy = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct RegrInterceptAccumulator {
    count: usize,
    sum_x: f64,
    sum_y: f64,
    sum_xx: f64,
    sum_xy: f64,
}

impl Default for RegrInterceptAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl RegrInterceptAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum_x: 0.0,
            sum_y: 0.0,
            sum_xx: 0.0,
            sum_xy: 0.0,
        }
    }
}

impl Accumulator for RegrInterceptAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (x, y) = if let Some(values) = value.as_array() {
            if values.len() == 2 {
                let x_val = values[0].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[0].data_type().to_string(),
                })?;
                let y_val = values[1].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[1].data_type().to_string(),
                })?;
                (x_val, y_val)
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY with 2 FLOAT64 values".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY with 2 FLOAT64 values".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        self.count += 1;
        self.sum_x += x;
        self.sum_y += y;
        self.sum_xx += x * x;
        self.sum_xy += x * y;

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "REGR_INTERCEPT merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count < 2 {
            return Ok(Value::null());
        }

        let n = self.count as f64;
        let slope_num = n * self.sum_xy - self.sum_x * self.sum_y;
        let slope_denom = n * self.sum_xx - self.sum_x * self.sum_x;

        if slope_denom.abs() < f64::EPSILON {
            return Ok(Value::null());
        }

        let slope = slope_num / slope_denom;
        let mean_x = self.sum_x / n;
        let mean_y = self.sum_y / n;
        let intercept = mean_y - slope * mean_x;

        Ok(Value::float64(intercept))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.sum_x = 0.0;
        self.sum_y = 0.0;
        self.sum_xx = 0.0;
        self.sum_xy = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct RegrR2Accumulator {
    count: usize,
    sum_x: f64,
    sum_y: f64,
    sum_xx: f64,
    sum_yy: f64,
    sum_xy: f64,
}

impl Default for RegrR2Accumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl RegrR2Accumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum_x: 0.0,
            sum_y: 0.0,
            sum_xx: 0.0,
            sum_yy: 0.0,
            sum_xy: 0.0,
        }
    }
}

impl Accumulator for RegrR2Accumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (x, y) = if let Some(values) = value.as_array() {
            if values.len() == 2 {
                let x_val = values[0].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[0].data_type().to_string(),
                })?;
                let y_val = values[1].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: values[1].data_type().to_string(),
                })?;
                (x_val, y_val)
            } else {
                return Err(Error::TypeMismatch {
                    expected: "ARRAY with 2 FLOAT64 values".to_string(),
                    actual: value.data_type().to_string(),
                });
            }
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY with 2 FLOAT64 values".to_string(),
                actual: value.data_type().to_string(),
            });
        };

        self.count += 1;
        self.sum_x += x;
        self.sum_y += y;
        self.sum_xx += x * x;
        self.sum_yy += y * y;
        self.sum_xy += x * y;

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "REGR_R2 merge not implemented".to_string(),
        ))
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

        let r_squared = (numerator * numerator) / (denom_x * denom_y);
        Ok(Value::float64(r_squared))
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

#[derive(Debug, Default, Clone, Copy)]
pub struct RegrSlopeFunction;

impl AggregateFunction for RegrSlopeFunction {
    fn name(&self) -> &str {
        "REGR_SLOPE"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RegrSlopeAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RegrInterceptFunction;

impl AggregateFunction for RegrInterceptFunction {
    fn name(&self) -> &str {
        "REGR_INTERCEPT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RegrInterceptAccumulator::new())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RegrR2Function;

impl AggregateFunction for RegrR2Function {
    fn name(&self) -> &str {
        "REGR_R2"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(RegrR2Accumulator::new())
    }
}

#[derive(Debug, Clone, Default)]
pub struct CountAccumulator {
    count: i64,
}

impl Accumulator for CountAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if !value.is_null() {
            self.count += 1;
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "COUNT merge not implemented".to_string(),
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
pub struct CountFunction;

impl AggregateFunction for CountFunction {
    fn name(&self) -> &str {
        "COUNT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountAccumulator::default())
    }
}

#[derive(Debug, Clone)]
pub struct SumAccumulator {
    sum: Value,

    has_values: bool,

    numeric_scale: Option<u32>,
}

impl Default for SumAccumulator {
    fn default() -> Self {
        Self {
            sum: Value::int64(0),
            has_values: false,
            numeric_scale: None,
        }
    }
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        self.has_values = true;
        self.sum = if let (Some(a), Some(b)) = (self.sum.as_numeric(), value.as_numeric()) {
            if self.numeric_scale.is_none() {
                self.numeric_scale = Some(b.scale());
            }
            Value::numeric(a + b)
        } else if let (Some(a), Some(b)) = (self.sum.as_i64(), value.as_i64()) {
            Value::int64(a + b)
        } else if let (Some(a), Some(b)) = (self.sum.as_i64(), value.as_f64()) {
            Value::float64(a as f64 + b)
        } else if let (Some(a), Some(b)) = (self.sum.as_f64(), value.as_i64()) {
            Value::float64(a + b as f64)
        } else if let (Some(a), Some(b)) = (self.sum.as_f64(), value.as_f64()) {
            Value::float64(a + b)
        } else if self.sum.as_i64() == Some(0) {
            if let Some(dec) = value.as_numeric() {
                self.numeric_scale = Some(dec.scale());
            }
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
            "SUM merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if !self.has_values {
            return Ok(Value::null());
        }

        if let (Some(dec), Some(scale)) = (self.sum.as_numeric(), self.numeric_scale) {
            let rescaled = dec
                .round_dp_with_strategy(scale, rust_decimal::RoundingStrategy::MidpointNearestEven);
            return Ok(Value::numeric(rescaled));
        }
        Ok(self.sum.clone())
    }

    fn reset(&mut self) {
        self.sum = Value::int64(0);
        self.has_values = false;
        self.numeric_scale = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SumFunction;

impl AggregateFunction for SumFunction {
    fn name(&self) -> &str {
        "SUM"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(first_type) = arg_types.first() {
            match first_type {
                DataType::Numeric(_) => Ok(first_type.clone()),
                DataType::Int64 => Ok(DataType::Int64),
                _ => Ok(DataType::Float64),
            }
        } else {
            Ok(DataType::Float64)
        }
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumAccumulator::default())
    }
}

#[derive(Debug, Clone)]
pub struct AvgAccumulator {
    sum: Value,
    count: i64,

    numeric_scale: Option<u32>,
}

impl Default for AvgAccumulator {
    fn default() -> Self {
        Self {
            sum: Value::int64(0),
            count: 0,
            numeric_scale: None,
        }
    }
}

impl Accumulator for AvgAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        self.sum = if let (Some(a), Some(b)) = (self.sum.as_numeric(), value.as_numeric()) {
            if self.numeric_scale.is_none() {
                self.numeric_scale = Some(b.scale());
            }
            Value::numeric(a + b)
        } else if let (Some(a), Some(b)) = (self.sum.as_i64(), value.as_i64()) {
            Value::int64(a + b)
        } else if let (Some(a), Some(b)) = (self.sum.as_i64(), value.as_f64()) {
            Value::float64(a as f64 + b)
        } else if let (Some(a), Some(b)) = (self.sum.as_f64(), value.as_i64()) {
            Value::float64(a + b as f64)
        } else if let (Some(a), Some(b)) = (self.sum.as_f64(), value.as_f64()) {
            Value::float64(a + b)
        } else if self.sum.as_i64() == Some(0) {
            if let Some(dec) = value.as_numeric() {
                self.numeric_scale = Some(dec.scale());
            }
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
            "AVG merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count == 0 {
            return Ok(Value::null());
        }

        if let Some(s) = self.sum.as_i64() {
            return Ok(Value::float64(s as f64 / self.count as f64));
        }

        if let Some(s) = self.sum.as_f64() {
            return Ok(Value::float64(s / self.count as f64));
        }

        if let Some(s) = self.sum.as_numeric() {
            use rust_decimal::Decimal;
            let count_decimal = Decimal::from(self.count);
            let mut result = s / count_decimal;

            if let Some(scale) = self.numeric_scale {
                result.rescale(scale);
                return Ok(Value::numeric(result));
            }
            return Ok(Value::numeric(result));
        }

        Ok(Value::null())
    }

    fn reset(&mut self) {
        self.sum = Value::int64(0);
        self.count = 0;
        self.numeric_scale = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AvgFunction;

impl AggregateFunction for AvgFunction {
    fn name(&self) -> &str {
        "AVG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(first_type) = arg_types.first() {
            match first_type {
                DataType::Numeric(_) => Ok(first_type.clone()),
                _ => Ok(DataType::Float64),
            }
        } else {
            Ok(DataType::Float64)
        }
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct MinAccumulator {
    min: Option<Value>,
}

impl Accumulator for MinAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(current_min) = &self.min {
            if Self::compare_values(value, current_min)? < 0 {
                self.min = Some(value.clone());
            }
        } else {
            self.min = Some(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MIN merge not implemented".to_string(),
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

impl MinAccumulator {
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

        if let (Some(x), Some(y)) = (a.as_date(), b.as_date()) {
            return Ok(x.cmp(&y) as i32);
        }

        if let (Some(x), Some(y)) = (a.as_timestamp(), b.as_timestamp()) {
            return Ok(x.cmp(&y) as i32);
        }

        if let (Some(x), Some(y)) = (a.as_datetime(), b.as_datetime()) {
            return Ok(x.cmp(&y) as i32);
        }

        if let (Some(x), Some(y)) = (a.as_time(), b.as_time()) {
            return Ok(x.cmp(&y) as i32);
        }

        Err(Error::InternalError(format!(
            "Cannot compare values: {:?} vs {:?}",
            a, b
        )))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MinFunction;

impl AggregateFunction for MinFunction {
    fn name(&self) -> &str {
        "MIN"
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
        Box::new(MinAccumulator::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct MaxAccumulator {
    max: Option<Value>,
}

impl Accumulator for MaxAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if let Some(current_max) = &self.max {
            if Self::compare_values(value, current_max)? > 0 {
                self.max = Some(value.clone());
            }
        } else {
            self.max = Some(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "MAX merge not implemented".to_string(),
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

impl MaxAccumulator {
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

        if let (Some(x), Some(y)) = (a.as_date(), b.as_date()) {
            return Ok(x.cmp(&y) as i32);
        }

        if let (Some(x), Some(y)) = (a.as_timestamp(), b.as_timestamp()) {
            return Ok(x.cmp(&y) as i32);
        }

        if let (Some(x), Some(y)) = (a.as_datetime(), b.as_datetime()) {
            return Ok(x.cmp(&y) as i32);
        }

        if let (Some(x), Some(y)) = (a.as_time(), b.as_time()) {
            return Ok(x.cmp(&y) as i32);
        }

        Err(Error::InternalError(format!(
            "Cannot compare values: {:?} vs {:?}",
            a, b
        )))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MaxFunction;

impl AggregateFunction for MaxFunction {
    fn name(&self) -> &str {
        "MAX"
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
        Box::new(MaxAccumulator::default())
    }
}

#[derive(Debug, Clone)]
pub struct SkewPopAccumulator {
    count: usize,
    mean: f64,
    m2: f64,
    m3: f64,
}

impl Default for SkewPopAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SkewPopAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            m3: 0.0,
        }
    }
}

impl Accumulator for SkewPopAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let x = value.as_f64().ok_or_else(|| Error::TypeMismatch {
            expected: "FLOAT64 or numeric type".to_string(),
            actual: value.data_type().to_string(),
        })?;

        let n1 = self.count as f64;
        self.count += 1;
        let n = self.count as f64;

        let delta = x - self.mean;
        let delta_n = delta / n;
        let term1 = delta * delta_n * n1;

        self.mean += delta_n;
        self.m3 += term1 * delta_n * (n - 2.0) - 3.0 * delta_n * self.m2;
        self.m2 += term1;

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "SKEW_POP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count < 3 {
            return Ok(Value::null());
        }
        let n = self.count as f64;
        let variance = self.m2 / n;
        if variance == 0.0 {
            return Ok(Value::float64(0.0));
        }
        let skewness = (self.m3 / n) / variance.powf(1.5);
        Ok(Value::float64(skewness))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
        self.m3 = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct SkewSampAccumulator {
    count: usize,
    mean: f64,
    m2: f64,
    m3: f64,
}

impl Default for SkewSampAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SkewSampAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            m3: 0.0,
        }
    }
}

impl Accumulator for SkewSampAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let x = value.as_f64().ok_or_else(|| Error::TypeMismatch {
            expected: "FLOAT64 or numeric type".to_string(),
            actual: value.data_type().to_string(),
        })?;

        let n1 = self.count as f64;
        self.count += 1;
        let n = self.count as f64;

        let delta = x - self.mean;
        let delta_n = delta / n;
        let term1 = delta * delta_n * n1;

        self.mean += delta_n;
        self.m3 += term1 * delta_n * (n - 2.0) - 3.0 * delta_n * self.m2;
        self.m2 += term1;

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "SKEW_SAMP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count < 3 {
            return Ok(Value::null());
        }
        let n = self.count as f64;
        let m2 = self.m2;
        let m3 = self.m3;
        if m2 == 0.0 {
            return Ok(Value::float64(0.0));
        }
        let skewness = (n.sqrt() * (n - 1.0).sqrt() / (n - 2.0)) * (m3 / m2.powf(1.5));
        Ok(Value::float64(skewness))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
        self.m3 = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct KurtPopAccumulator {
    count: usize,
    mean: f64,
    m2: f64,
    m4: f64,
}

impl Default for KurtPopAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl KurtPopAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            m4: 0.0,
        }
    }
}

impl Accumulator for KurtPopAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let x = value.as_f64().ok_or_else(|| Error::TypeMismatch {
            expected: "FLOAT64 or numeric type".to_string(),
            actual: value.data_type().to_string(),
        })?;

        let n1 = self.count as f64;
        self.count += 1;
        let n = self.count as f64;

        let delta = x - self.mean;
        let delta_n = delta / n;
        let delta_n2 = delta_n * delta_n;
        let term1 = delta * delta_n * n1;

        self.mean += delta_n;
        self.m4 += term1 * delta_n2 * (n * n - 3.0 * n + 3.0) + 6.0 * delta_n2 * self.m2
            - 4.0 * delta_n * 0.0;
        self.m2 += term1;

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "KURT_POP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count < 4 {
            return Ok(Value::null());
        }
        let n = self.count as f64;
        let variance = self.m2 / n;
        if variance == 0.0 {
            return Ok(Value::float64(0.0));
        }
        let kurtosis = (n * self.m4) / (self.m2 * self.m2) - 3.0;
        Ok(Value::float64(kurtosis))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
        self.m4 = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct KurtSampAccumulator {
    count: usize,
    mean: f64,
    m2: f64,
    m4: f64,
}

impl Default for KurtSampAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl KurtSampAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            m4: 0.0,
        }
    }
}

impl Accumulator for KurtSampAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let x = value.as_f64().ok_or_else(|| Error::TypeMismatch {
            expected: "FLOAT64 or numeric type".to_string(),
            actual: value.data_type().to_string(),
        })?;

        let n1 = self.count as f64;
        self.count += 1;
        let n = self.count as f64;

        let delta = x - self.mean;
        let delta_n = delta / n;
        let delta_n2 = delta_n * delta_n;
        let term1 = delta * delta_n * n1;

        self.mean += delta_n;
        self.m4 += term1 * delta_n2 * (n * n - 3.0 * n + 3.0) + 6.0 * delta_n2 * self.m2
            - 4.0 * delta_n * 0.0;
        self.m2 += term1;

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "KURT_SAMP merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.count < 4 {
            return Ok(Value::null());
        }
        let n = self.count as f64;
        if self.m2 == 0.0 {
            return Ok(Value::float64(0.0));
        }
        let g2 = (n * self.m4) / (self.m2 * self.m2) - 3.0;
        let kurtosis = ((n - 1.0) / ((n - 2.0) * (n - 3.0))) * ((n + 1.0) * g2 + 6.0);
        Ok(Value::float64(kurtosis))
    }

    fn reset(&mut self) {
        self.count = 0;
        self.mean = 0.0;
        self.m2 = 0.0;
        self.m4 = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct AvgWeightedAccumulator {
    weighted_sum: f64,
    total_weight: f64,
}

impl Default for AvgWeightedAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AvgWeightedAccumulator {
    pub fn new() -> Self {
        Self {
            weighted_sum: 0.0,
            total_weight: 0.0,
        }
    }
}

impl Accumulator for AvgWeightedAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let (val, weight) = if let Some(arr) = value.as_array() {
            if arr.len() == 2 {
                let v = arr[0].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: arr[0].data_type().to_string(),
                })?;
                let w = arr[1].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: arr[1].data_type().to_string(),
                })?;
                (v, w)
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

        self.weighted_sum += val * weight;
        self.total_weight += weight;
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "AVG_WEIGHTED merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.total_weight == 0.0 {
            return Ok(Value::null());
        }
        Ok(Value::float64(self.weighted_sum / self.total_weight))
    }

    fn reset(&mut self) {
        self.weighted_sum = 0.0;
        self.total_weight = 0.0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

simple_numeric_aggregate!(
    SkewPopFunction,
    "SKEW_POP",
    DataType::Float64,
    SkewPopAccumulator
);
simple_numeric_aggregate!(
    SkewSampFunction,
    "SKEW_SAMP",
    DataType::Float64,
    SkewSampAccumulator
);
simple_numeric_aggregate!(
    KurtPopFunction,
    "KURT_POP",
    DataType::Float64,
    KurtPopAccumulator
);
simple_numeric_aggregate!(
    KurtSampFunction,
    "KURT_SAMP",
    DataType::Float64,
    KurtSampAccumulator
);

#[derive(Debug, Default, Clone, Copy)]
pub struct AvgWeightedFunction;

impl AggregateFunction for AvgWeightedFunction {
    fn name(&self) -> &str {
        "AVG_WEIGHTED"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgWeightedAccumulator::new())
    }
}
