use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use indexmap::IndexMap;
use rust_decimal::prelude::ToPrimitive;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

use super::{Accumulator, AggregateFunction};
use crate::approximate::{HyperLogLogPlusPlus, TDigest};

#[derive(Debug, Clone)]
pub struct ApproxCountDistinctAccumulator {
    estimator: HyperLogLogPlusPlus,
}

impl Default for ApproxCountDistinctAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl ApproxCountDistinctAccumulator {
    pub fn new() -> Self {
        Self {
            estimator: HyperLogLogPlusPlus::new(),
        }
    }
}

impl Accumulator for ApproxCountDistinctAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        self.estimator.add(&HashableValue(value));
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "APPROX_COUNT_DISTINCT merge not implemented".to_string(),
        ))
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
pub struct ApproxCountDistinctFunction;

impl AggregateFunction for ApproxCountDistinctFunction {
    fn name(&self) -> &str {
        "APPROX_COUNT_DISTINCT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ApproxCountDistinctAccumulator::new())
    }
}

#[derive(Debug, Clone)]
pub struct ApproxQuantilesAccumulator {
    digest: TDigest,
    num_quantiles: i64,
}

impl ApproxQuantilesAccumulator {
    pub fn new(num_quantiles: i64) -> Self {
        Self {
            digest: TDigest::new(),
            num_quantiles,
        }
    }
}

impl Accumulator for ApproxQuantilesAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if let Some(number) = numeric_value_to_f64(value)? {
            self.digest.add(number);
        }
        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "APPROX_QUANTILES merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.digest.is_empty() {
            return Ok(Value::null());
        }

        let mut digest_clone = self.digest.clone();

        let mut quantiles = Vec::with_capacity((self.num_quantiles + 1) as usize);
        for i in 0..=self.num_quantiles {
            let q = i as f64 / self.num_quantiles as f64;
            quantiles.push(Value::float64(digest_clone.quantile(q)));
        }

        Ok(Value::array(quantiles))
    }

    fn reset(&mut self) {
        self.digest = TDigest::new();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ApproxQuantilesFunction {
    num_quantiles: i64,
}

impl ApproxQuantilesFunction {
    pub fn new(num_quantiles: i64) -> Self {
        Self { num_quantiles }
    }
}

impl Default for ApproxQuantilesFunction {
    fn default() -> Self {
        Self::new(100)
    }
}

impl AggregateFunction for ApproxQuantilesFunction {
    fn name(&self) -> &str {
        "APPROX_QUANTILES"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Float64)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ApproxQuantilesAccumulator::new(self.num_quantiles))
    }
}

#[derive(Debug, Clone)]
pub struct ApproxTopCountAccumulator {
    counts: HashMap<String, i64>,
    k: usize,
}

impl ApproxTopCountAccumulator {
    pub fn new(k: i64) -> Self {
        Self {
            counts: HashMap::new(),
            k: k as usize,
        }
    }
}

impl Accumulator for ApproxTopCountAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let key = value_to_string(value);

        if let Some(count) = self.counts.get_mut(&key) {
            *count += 1;
        } else if self.counts.len() < self.k {
            self.counts.insert(key, 1);
        } else if let Some((min_key, min_count)) = find_min_entry(&self.counts) {
            self.counts.remove(&min_key);
            self.counts.insert(key, min_count + 1);
        }

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "APPROX_TOP_COUNT merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.counts.is_empty() {
            return Ok(Value::array(vec![]));
        }

        let result = counts_to_struct_array(self.counts.clone());
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
pub struct ApproxTopCountFunction {
    k: i64,
}

impl ApproxTopCountFunction {
    pub fn new(k: i64) -> Self {
        Self { k }
    }
}

impl Default for ApproxTopCountFunction {
    fn default() -> Self {
        Self::new(10)
    }
}

impl AggregateFunction for ApproxTopCountFunction {
    fn name(&self) -> &str {
        "APPROX_TOP_COUNT"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ApproxTopCountAccumulator::new(self.k))
    }
}

#[derive(Debug, Clone)]
pub struct ApproxTopSumAccumulator {
    sums: HashMap<String, f64>,
    k: usize,
}

impl ApproxTopSumAccumulator {
    pub fn new(k: i64) -> Self {
        Self {
            sums: HashMap::new(),
            k: k as usize,
        }
    }
}

impl Accumulator for ApproxTopSumAccumulator {
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

        let weight_f64 = extract_numeric_weight(weight)?;
        let key = value_to_string(val);

        if let Some(sum) = self.sums.get_mut(&key) {
            *sum += weight_f64;
        } else if self.sums.len() < self.k {
            self.sums.insert(key, weight_f64);
        } else if let Some((min_key, min_sum)) = find_min_entry_f64(&self.sums) {
            self.sums.remove(&min_key);
            self.sums.insert(key, min_sum + weight_f64);
        }

        Ok(())
    }

    fn merge(&mut self, _other: &dyn Accumulator) -> Result<()> {
        Err(Error::unsupported_feature(
            "APPROX_TOP_SUM merge not implemented".to_string(),
        ))
    }

    fn finalize(&self) -> Result<Value> {
        if self.sums.is_empty() {
            return Ok(Value::array(vec![]));
        }

        let result = sums_to_struct_array(self.sums.clone());
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
pub struct ApproxTopSumFunction {
    k: i64,
}

impl ApproxTopSumFunction {
    pub fn new(k: i64) -> Self {
        Self { k }
    }
}

impl Default for ApproxTopSumFunction {
    fn default() -> Self {
        Self::new(10)
    }
}

impl AggregateFunction for ApproxTopSumFunction {
    fn name(&self) -> &str {
        "APPROX_TOP_SUM"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown, DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Unknown)))
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ApproxTopSumAccumulator::new(self.k))
    }
}

fn numeric_value_to_f64(value: &Value) -> Result<Option<f64>> {
    if value.is_null() {
        return Ok(None);
    }

    if let Some(i) = value.as_i64() {
        return Ok(Some(i as f64));
    }

    if let Some(f) = value.as_f64() {
        return Ok(Some(f));
    }

    if let Some(d) = value.as_numeric() {
        return d.to_f64().map(Some).ok_or_else(|| Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: "NUMERIC (out of range)".to_string(),
        });
    }

    Err(Error::TypeMismatch {
        expected: "NUMERIC".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn extract_numeric_weight(weight: &Value) -> Result<f64> {
    numeric_value_to_f64(weight)?.ok_or_else(|| Error::TypeMismatch {
        expected: "NUMERIC".to_string(),
        actual: "NULL".to_string(),
    })
}

fn value_to_string(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        return s.to_string();
    }
    if let Some(i) = value.as_i64() {
        return i.to_string();
    }
    if let Some(f) = value.as_f64() {
        return f.to_string();
    }
    if let Some(b) = value.as_bool() {
        return b.to_string();
    }
    if let Some(d) = value.as_date() {
        return d.to_string();
    }
    if let Some(ts) = value.as_timestamp() {
        return ts.to_string();
    }

    if let Some(d) = value.as_numeric() {
        d.to_string()
    } else if let Some(u) = value.as_uuid() {
        u.to_string()
    } else {
        format!("{:?}", value)
    }
}

fn find_min_entry(map: &HashMap<String, i64>) -> Option<(String, i64)> {
    map.iter()
        .min_by(|a, b| a.1.cmp(b.1))
        .map(|(k, v)| (k.clone(), *v))
}

fn find_min_entry_f64(map: &HashMap<String, f64>) -> Option<(String, f64)> {
    map.iter()
        .min_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(Ordering::Equal))
        .map(|(k, v)| (k.clone(), *v))
}

fn counts_to_struct_array(counts: HashMap<String, i64>) -> Vec<Value> {
    let mut items: Vec<_> = counts.into_iter().collect();

    items.sort_by(
        |(a_key, a_count), (b_key, b_count)| match b_count.cmp(a_count) {
            Ordering::Equal => a_key.cmp(b_key),
            ordering => ordering,
        },
    );

    items
        .into_iter()
        .map(|(key, count)| {
            let mut fields = IndexMap::new();
            fields.insert("value".to_string(), Value::string(key));
            fields.insert("count".to_string(), Value::int64(count));
            Value::struct_val(fields)
        })
        .collect()
}

fn sums_to_struct_array(sums: HashMap<String, f64>) -> Vec<Value> {
    let mut items: Vec<_> = sums.into_iter().collect();

    items.sort_by(|(a_key, a_sum), (b_key, b_sum)| {
        match b_sum.partial_cmp(a_sum).unwrap_or(Ordering::Equal) {
            Ordering::Equal => a_key.cmp(b_key),
            ordering => ordering,
        }
    });

    items
        .into_iter()
        .map(|(key, sum)| {
            let mut fields = IndexMap::new();
            fields.insert("value".to_string(), Value::string(key));
            fields.insert("sum".to_string(), Value::float64(sum));
            Value::struct_val(fields)
        })
        .collect()
}

struct HashableValue<'a>(&'a Value);

impl<'a> Hash for HashableValue<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.0.is_null() {
            0_u8.hash(state);
            return;
        }

        if let Some(b) = self.0.as_bool() {
            b.hash(state);
            return;
        }

        if let Some(i) = self.0.as_i64() {
            i.hash(state);
            return;
        }

        if let Some(f) = self.0.as_f64() {
            f.to_bits().hash(state);
            return;
        }

        if let Some(s) = self.0.as_str() {
            s.hash(state);
            return;
        }

        if let Some(date) = self.0.as_date() {
            date.to_string().hash(state);
            return;
        }

        if let Some(dt) = self.0.as_datetime() {
            dt.timestamp_nanos_opt().unwrap_or(0).hash(state);
            return;
        }

        if let Some(time) = self.0.as_time() {
            time.to_string().hash(state);
            return;
        }

        if let Some(ts) = self.0.as_timestamp() {
            ts.timestamp_nanos_opt().unwrap_or(0).hash(state);
            return;
        }

        if let Some(d) = self.0.as_numeric() {
            d.to_string().hash(state);
        } else if let Some(bytes) = self.0.as_bytes() {
            bytes.hash(state);
        } else if let Some(fields) = self.0.as_struct() {
            fields.len().hash(state);
            for (key, value) in fields.iter() {
                key.hash(state);
                HashableValue(value).hash(state);
            }
        } else if let Some(items) = self.0.as_array() {
            items.len().hash(state);
            for item in items.iter() {
                HashableValue(item).hash(state);
            }
        } else if let Some(wkt) = self.0.as_geography() {
            wkt.hash(state);
        } else if let Some(json) = self.0.as_json() {
            json.to_string().hash(state);
        } else if let Some(uuid) = self.0.as_uuid() {
            uuid.as_bytes().hash(state);
        }
    }
}
