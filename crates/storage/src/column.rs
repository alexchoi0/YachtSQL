use aligned_vec::{AVec, ConstAlign};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, IntervalValue, RangeValue, Value};

use crate::NullBitmap;

pub type A64 = ConstAlign<64>;

fn serialize_avec_i64<S>(
    data: &AVec<i64, A64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    data.as_slice().serialize(serializer)
}

fn deserialize_avec_i64<'de, D>(deserializer: D) -> std::result::Result<AVec<i64, A64>, D::Error>
where
    D: Deserializer<'de>,
{
    let vec = Vec::<i64>::deserialize(deserializer)?;
    Ok(AVec::from_iter(64, vec))
}

fn serialize_avec_f64<S>(
    data: &AVec<f64, A64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    data.as_slice().serialize(serializer)
}

fn deserialize_avec_f64<'de, D>(deserializer: D) -> std::result::Result<AVec<f64, A64>, D::Error>
where
    D: Deserializer<'de>,
{
    let vec = Vec::<f64>::deserialize(deserializer)?;
    Ok(AVec::from_iter(64, vec))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Column {
    Bool {
        data: Vec<bool>,
        nulls: NullBitmap,
    },
    Int64 {
        #[serde(
            serialize_with = "serialize_avec_i64",
            deserialize_with = "deserialize_avec_i64"
        )]
        data: AVec<i64, A64>,
        nulls: NullBitmap,
    },
    Float64 {
        #[serde(
            serialize_with = "serialize_avec_f64",
            deserialize_with = "deserialize_avec_f64"
        )]
        data: AVec<f64, A64>,
        nulls: NullBitmap,
    },
    Numeric {
        data: Vec<Decimal>,
        nulls: NullBitmap,
    },
    String {
        data: Vec<String>,
        nulls: NullBitmap,
    },
    Bytes {
        data: Vec<Vec<u8>>,
        nulls: NullBitmap,
    },
    Date {
        data: Vec<NaiveDate>,
        nulls: NullBitmap,
    },
    Time {
        data: Vec<NaiveTime>,
        nulls: NullBitmap,
    },
    DateTime {
        data: Vec<chrono::NaiveDateTime>,
        nulls: NullBitmap,
    },
    Timestamp {
        data: Vec<DateTime<Utc>>,
        nulls: NullBitmap,
    },
    Json {
        data: Vec<serde_json::Value>,
        nulls: NullBitmap,
    },
    Array {
        data: Vec<Vec<Value>>,
        nulls: NullBitmap,
        element_type: DataType,
    },
    Struct {
        data: Vec<Vec<(String, Value)>>,
        nulls: NullBitmap,
        fields: Vec<(String, DataType)>,
    },
    Geography {
        data: Vec<String>,
        nulls: NullBitmap,
    },
    Interval {
        data: Vec<IntervalValue>,
        nulls: NullBitmap,
    },
    Range {
        data: Vec<RangeValue>,
        nulls: NullBitmap,
        element_type: DataType,
    },
}

impl Column {
    pub fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Bool => Column::Bool {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Int64 => Column::Int64 {
                data: AVec::new(64),
                nulls: NullBitmap::new(),
            },
            DataType::Float64 => Column::Float64 {
                data: AVec::new(64),
                nulls: NullBitmap::new(),
            },
            DataType::Numeric(_) | DataType::BigNumeric => Column::Numeric {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::String | DataType::Unknown => Column::String {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Bytes => Column::Bytes {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Date => Column::Date {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Time => Column::Time {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::DateTime => Column::DateTime {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Timestamp => Column::Timestamp {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Json => Column::Json {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Array(elem_type) => Column::Array {
                data: Vec::new(),
                nulls: NullBitmap::new(),
                element_type: (**elem_type).clone(),
            },
            DataType::Struct(fields) => Column::Struct {
                data: Vec::new(),
                nulls: NullBitmap::new(),
                fields: fields
                    .iter()
                    .map(|f| (f.name.clone(), f.data_type.clone()))
                    .collect(),
            },
            DataType::Geography => Column::Geography {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Interval => Column::Interval {
                data: Vec::new(),
                nulls: NullBitmap::new(),
            },
            DataType::Range(elem_type) => Column::Range {
                data: Vec::new(),
                nulls: NullBitmap::new(),
                element_type: (**elem_type).clone(),
            },
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Column::Bool { data, .. } => data.len(),
            Column::Int64 { data, .. } => data.len(),
            Column::Float64 { data, .. } => data.len(),
            Column::Numeric { data, .. } => data.len(),
            Column::String { data, .. } => data.len(),
            Column::Bytes { data, .. } => data.len(),
            Column::Date { data, .. } => data.len(),
            Column::Time { data, .. } => data.len(),
            Column::DateTime { data, .. } => data.len(),
            Column::Timestamp { data, .. } => data.len(),
            Column::Json { data, .. } => data.len(),
            Column::Array { data, .. } => data.len(),
            Column::Struct { data, .. } => data.len(),
            Column::Geography { data, .. } => data.len(),
            Column::Interval { data, .. } => data.len(),
            Column::Range { data, .. } => data.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Column::Bool { .. } => DataType::Bool,
            Column::Int64 { .. } => DataType::Int64,
            Column::Float64 { .. } => DataType::Float64,
            Column::Numeric { .. } => DataType::Numeric(None),
            Column::String { .. } => DataType::String,
            Column::Bytes { .. } => DataType::Bytes,
            Column::Date { .. } => DataType::Date,
            Column::Time { .. } => DataType::Time,
            Column::DateTime { .. } => DataType::DateTime,
            Column::Timestamp { .. } => DataType::Timestamp,
            Column::Json { .. } => DataType::Json,
            Column::Array { element_type, .. } => DataType::Array(Box::new(element_type.clone())),
            Column::Struct { fields, .. } => {
                let struct_fields = fields
                    .iter()
                    .map(|(name, dt)| yachtsql_common::types::StructField {
                        name: name.clone(),
                        data_type: dt.clone(),
                    })
                    .collect();
                DataType::Struct(struct_fields)
            }
            Column::Geography { .. } => DataType::Geography,
            Column::Interval { .. } => DataType::Interval,
            Column::Range { element_type, .. } => DataType::Range(Box::new(element_type.clone())),
        }
    }

    pub fn count_null(&self) -> usize {
        match self {
            Column::Bool { nulls, .. }
            | Column::Int64 { nulls, .. }
            | Column::Float64 { nulls, .. }
            | Column::Numeric { nulls, .. }
            | Column::String { nulls, .. }
            | Column::Bytes { nulls, .. }
            | Column::Date { nulls, .. }
            | Column::Time { nulls, .. }
            | Column::DateTime { nulls, .. }
            | Column::Timestamp { nulls, .. }
            | Column::Json { nulls, .. }
            | Column::Array { nulls, .. }
            | Column::Struct { nulls, .. }
            | Column::Geography { nulls, .. }
            | Column::Interval { nulls, .. }
            | Column::Range { nulls, .. } => nulls.count_null(),
        }
    }

    pub fn count_valid(&self) -> usize {
        self.len() - self.count_null()
    }

    pub fn sum(&self) -> Option<f64> {
        match self {
            Column::Int64 { data, nulls } => {
                let mut sum: i64 = 0;
                let mut has_value = false;
                let chunks = data.chunks_exact(64);
                let remainder = chunks.remainder();

                for (&bitmap_word, chunk) in nulls.words().iter().zip(chunks) {
                    if bitmap_word == 0 {
                        sum += chunk.iter().sum::<i64>();
                        has_value = true;
                    } else if bitmap_word != u64::MAX {
                        let mut valid_mask = !bitmap_word;
                        while valid_mask != 0 {
                            let bit = valid_mask.trailing_zeros() as usize;
                            sum += chunk[bit];
                            has_value = true;
                            valid_mask &= valid_mask - 1;
                        }
                    }
                }

                if !remainder.is_empty() {
                    let last_word = nulls.words().last().copied().unwrap_or(0);
                    if last_word == 0 {
                        sum += remainder.iter().sum::<i64>();
                        has_value = true;
                    } else {
                        for (i, &val) in remainder.iter().enumerate() {
                            if (last_word >> i) & 1 == 0 {
                                sum += val;
                                has_value = true;
                            }
                        }
                    }
                }

                if has_value { Some(sum as f64) } else { None }
            }
            Column::Float64 { data, nulls } => {
                let mut sum: f64 = 0.0;
                let mut has_value = false;
                let chunks = data.chunks_exact(64);
                let remainder = chunks.remainder();

                for (&bitmap_word, chunk) in nulls.words().iter().zip(chunks) {
                    if bitmap_word == 0 {
                        sum += chunk.iter().sum::<f64>();
                        has_value = true;
                    } else if bitmap_word != u64::MAX {
                        let mut valid_mask = !bitmap_word;
                        while valid_mask != 0 {
                            let bit = valid_mask.trailing_zeros() as usize;
                            sum += chunk[bit];
                            has_value = true;
                            valid_mask &= valid_mask - 1;
                        }
                    }
                }

                if !remainder.is_empty() {
                    let last_word = nulls.words().last().copied().unwrap_or(0);
                    if last_word == 0 {
                        sum += remainder.iter().sum::<f64>();
                        has_value = true;
                    } else {
                        for (i, &val) in remainder.iter().enumerate() {
                            if (last_word >> i) & 1 == 0 {
                                sum += val;
                                has_value = true;
                            }
                        }
                    }
                }

                if has_value { Some(sum) } else { None }
            }
            Column::Numeric { data, nulls } => {
                use rust_decimal::prelude::ToPrimitive;
                let null_count = nulls.count_null();
                if null_count == data.len() {
                    None
                } else if null_count == 0 {
                    Some(data.iter().filter_map(|v| v.to_f64()).sum())
                } else {
                    let sum: f64 = data
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| !nulls.is_null(*i))
                        .filter_map(|(_, v)| v.to_f64())
                        .sum();
                    Some(sum)
                }
            }
            Column::Bool { .. }
            | Column::String { .. }
            | Column::Bytes { .. }
            | Column::Date { .. }
            | Column::Time { .. }
            | Column::DateTime { .. }
            | Column::Timestamp { .. }
            | Column::Json { .. }
            | Column::Array { .. }
            | Column::Struct { .. }
            | Column::Geography { .. }
            | Column::Interval { .. }
            | Column::Range { .. } => None,
        }
    }

    pub fn min(&self) -> Option<Value> {
        match self {
            Column::Int64 { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, &v)| v)
                .min()
                .map(Value::Int64),
            Column::Float64 { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, &v)| v)
                .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .map(Value::float64),
            Column::Numeric { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .cloned()
                .map(Value::Numeric),
            Column::String { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .cloned()
                .map(Value::String),
            Column::Date { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .copied()
                .map(Value::Date),
            Column::Time { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .copied()
                .map(Value::Time),
            Column::DateTime { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .copied()
                .map(Value::DateTime),
            Column::Timestamp { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .min()
                .copied()
                .map(Value::Timestamp),
            Column::Bool { .. }
            | Column::Bytes { .. }
            | Column::Json { .. }
            | Column::Array { .. }
            | Column::Struct { .. }
            | Column::Geography { .. }
            | Column::Interval { .. }
            | Column::Range { .. } => None,
        }
    }

    pub fn max(&self) -> Option<Value> {
        match self {
            Column::Int64 { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, &v)| v)
                .max()
                .map(Value::Int64),
            Column::Float64 { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, &v)| v)
                .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .map(Value::float64),
            Column::Numeric { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .cloned()
                .map(Value::Numeric),
            Column::String { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .cloned()
                .map(Value::String),
            Column::Date { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .copied()
                .map(Value::Date),
            Column::Time { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .copied()
                .map(Value::Time),
            Column::DateTime { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .copied()
                .map(Value::DateTime),
            Column::Timestamp { data, nulls } => data
                .iter()
                .enumerate()
                .filter(|(i, _)| !nulls.is_null(*i))
                .map(|(_, v)| v)
                .max()
                .copied()
                .map(Value::Timestamp),
            Column::Bool { .. }
            | Column::Bytes { .. }
            | Column::Json { .. }
            | Column::Array { .. }
            | Column::Struct { .. }
            | Column::Geography { .. }
            | Column::Interval { .. }
            | Column::Range { .. } => None,
        }
    }

    pub fn is_null(&self, index: usize) -> bool {
        match self {
            Column::Bool { nulls, .. } => nulls.is_null(index),
            Column::Int64 { nulls, .. } => nulls.is_null(index),
            Column::Float64 { nulls, .. } => nulls.is_null(index),
            Column::Numeric { nulls, .. } => nulls.is_null(index),
            Column::String { nulls, .. } => nulls.is_null(index),
            Column::Bytes { nulls, .. } => nulls.is_null(index),
            Column::Date { nulls, .. } => nulls.is_null(index),
            Column::Time { nulls, .. } => nulls.is_null(index),
            Column::DateTime { nulls, .. } => nulls.is_null(index),
            Column::Timestamp { nulls, .. } => nulls.is_null(index),
            Column::Json { nulls, .. } => nulls.is_null(index),
            Column::Array { nulls, .. } => nulls.is_null(index),
            Column::Struct { nulls, .. } => nulls.is_null(index),
            Column::Geography { nulls, .. } => nulls.is_null(index),
            Column::Interval { nulls, .. } => nulls.is_null(index),
            Column::Range { nulls, .. } => nulls.is_null(index),
        }
    }

    pub fn get(&self, index: usize) -> Result<Value> {
        if index >= self.len() {
            return Err(Error::invalid_query(format!(
                "Column index {} out of bounds (len: {})",
                index,
                self.len()
            )));
        }
        if self.is_null(index) {
            return Ok(Value::Null);
        }

        Ok(match self {
            Column::Bool { data, .. } => Value::Bool(data[index]),
            Column::Int64 { data, .. } => Value::Int64(data[index]),
            Column::Float64 { data, .. } => Value::float64(data[index]),
            Column::Numeric { data, .. } => Value::Numeric(data[index]),
            Column::String { data, .. } => Value::String(data[index].clone()),
            Column::Bytes { data, .. } => Value::Bytes(data[index].clone()),
            Column::Date { data, .. } => Value::Date(data[index]),
            Column::Time { data, .. } => Value::Time(data[index]),
            Column::DateTime { data, .. } => Value::DateTime(data[index]),
            Column::Timestamp { data, .. } => Value::Timestamp(data[index]),
            Column::Json { data, .. } => Value::Json(data[index].clone()),
            Column::Array { data, .. } => Value::Array(data[index].clone()),
            Column::Struct { data, .. } => Value::Struct(data[index].clone()),
            Column::Geography { data, .. } => Value::Geography(data[index].clone()),
            Column::Interval { data, .. } => Value::Interval(data[index].clone()),
            Column::Range { data, .. } => Value::Range(data[index].clone()),
        })
    }

    pub fn get_value(&self, index: usize) -> Value {
        if index >= self.len() || self.is_null(index) {
            return Value::Null;
        }

        match self {
            Column::Bool { data, .. } => Value::Bool(data[index]),
            Column::Int64 { data, .. } => Value::Int64(data[index]),
            Column::Float64 { data, .. } => Value::float64(data[index]),
            Column::Numeric { data, .. } => Value::Numeric(data[index]),
            Column::String { data, .. } => Value::String(data[index].clone()),
            Column::Bytes { data, .. } => Value::Bytes(data[index].clone()),
            Column::Date { data, .. } => Value::Date(data[index]),
            Column::Time { data, .. } => Value::Time(data[index]),
            Column::DateTime { data, .. } => Value::DateTime(data[index]),
            Column::Timestamp { data, .. } => Value::Timestamp(data[index]),
            Column::Json { data, .. } => Value::Json(data[index].clone()),
            Column::Array { data, .. } => Value::Array(data[index].clone()),
            Column::Struct { data, .. } => Value::Struct(data[index].clone()),
            Column::Geography { data, .. } => Value::Geography(data[index].clone()),
            Column::Interval { data, .. } => Value::Interval(data[index].clone()),
            Column::Range { data, .. } => Value::Range(data[index].clone()),
        }
    }

    pub fn push(&mut self, value: Value) -> Result<()> {
        match (self, value) {
            (Column::Bool { data, nulls }, Value::Null) => {
                data.push(false);
                nulls.push(true);
            }
            (Column::Bool { data, nulls }, Value::Bool(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Bool { data, nulls }, Value::String(v)) => {
                let b = matches!(v.to_uppercase().as_str(), "TRUE" | "1" | "YES");
                data.push(b);
                nulls.push(false);
            }
            (Column::Bool { data, nulls }, Value::Int64(v)) => {
                data.push(v != 0);
                nulls.push(false);
            }
            (Column::Int64 { data, nulls }, Value::Null) => {
                data.push(0);
                nulls.push(true);
            }
            (Column::Int64 { data, nulls }, Value::Int64(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Int64 { data, nulls }, Value::Float64(v)) => {
                data.push(v.0 as i64);
                nulls.push(false);
            }
            (Column::Int64 { data, nulls }, Value::String(v)) => {
                let n = v.parse::<i64>().unwrap_or(0);
                data.push(n);
                nulls.push(false);
            }
            (Column::Float64 { data, nulls }, Value::Null) => {
                data.push(0.0);
                nulls.push(true);
            }
            (Column::Float64 { data, nulls }, Value::Float64(v)) => {
                data.push(v.0);
                nulls.push(false);
            }
            (Column::Float64 { data, nulls }, Value::Int64(v)) => {
                data.push(v as f64);
                nulls.push(false);
            }
            (Column::Float64 { data, nulls }, Value::Numeric(v)) => {
                use rust_decimal::prelude::ToPrimitive;
                data.push(v.to_f64().unwrap_or(0.0));
                nulls.push(false);
            }
            (Column::Numeric { data, nulls }, Value::Null) => {
                data.push(Decimal::ZERO);
                nulls.push(true);
            }
            (Column::Numeric { data, nulls }, Value::Numeric(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Numeric { data, nulls }, Value::Float64(v)) => {
                data.push(Decimal::from_f64_retain(v.0).unwrap_or(Decimal::ZERO));
                nulls.push(false);
            }
            (Column::Numeric { data, nulls }, Value::Int64(v)) => {
                data.push(Decimal::from(v));
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Null) => {
                data.push(String::new());
                nulls.push(true);
            }
            (Column::String { data, nulls }, Value::String(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Int64(v)) => {
                data.push(v.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Float64(v)) => {
                data.push(v.0.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Date(v)) => {
                data.push(v.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::DateTime(v)) => {
                data.push(v.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Timestamp(v)) => {
                data.push(v.to_rfc3339());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Time(v)) => {
                data.push(v.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Bool(v)) => {
                data.push(if v {
                    "true".to_string()
                } else {
                    "false".to_string()
                });
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Struct(fields)) => {
                let s = format!("{:?}", fields);
                data.push(s);
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Array(arr)) => {
                let s = format!("{:?}", arr);
                data.push(s);
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Numeric(v)) => {
                data.push(v.to_string());
                nulls.push(false);
            }
            (Column::String { data, nulls }, Value::Bytes(v)) => {
                let s = String::from_utf8_lossy(&v).to_string();
                data.push(s);
                nulls.push(false);
            }
            (Column::Bytes { data, nulls }, Value::Null) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (Column::Bytes { data, nulls }, Value::Bytes(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Date { data, nulls }, Value::Null) => {
                data.push(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                nulls.push(true);
            }
            (Column::Date { data, nulls }, Value::Date(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Time { data, nulls }, Value::Null) => {
                data.push(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                nulls.push(true);
            }
            (Column::Time { data, nulls }, Value::Time(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::DateTime { data, nulls }, Value::Null) => {
                data.push(chrono::DateTime::UNIX_EPOCH.naive_utc());
                nulls.push(true);
            }
            (Column::DateTime { data, nulls }, Value::DateTime(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Timestamp { data, nulls }, Value::Null) => {
                data.push(DateTime::UNIX_EPOCH);
                nulls.push(true);
            }
            (Column::Timestamp { data, nulls }, Value::Timestamp(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Json { data, nulls }, Value::Null) => {
                data.push(serde_json::Value::Null);
                nulls.push(true);
            }
            (Column::Json { data, nulls }, Value::Json(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Json { data, nulls }, Value::String(v)) => {
                let json_val = serde_json::from_str(&v).unwrap_or(serde_json::Value::String(v));
                data.push(json_val);
                nulls.push(false);
            }
            (Column::Array { data, nulls, .. }, Value::Null) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (Column::Array { data, nulls, .. }, Value::Array(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Struct { data, nulls, .. }, Value::Null) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (Column::Struct { data, nulls, .. }, Value::Struct(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Geography { data, nulls }, Value::Null) => {
                data.push(String::new());
                nulls.push(true);
            }
            (Column::Geography { data, nulls }, Value::Geography(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Interval { data, nulls }, Value::Null) => {
                data.push(IntervalValue {
                    months: 0,
                    days: 0,
                    nanos: 0,
                });
                nulls.push(true);
            }
            (Column::Interval { data, nulls }, Value::Interval(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Range { data, nulls, .. }, Value::Null) => {
                data.push(RangeValue::new(None, None));
                nulls.push(true);
            }
            (Column::Range { data, nulls, .. }, Value::Range(v)) => {
                data.push(v);
                nulls.push(false);
            }
            (Column::Bool { data, nulls }, Value::Default) => {
                data.push(false);
                nulls.push(true);
            }
            (Column::Int64 { data, nulls }, Value::Default) => {
                data.push(0);
                nulls.push(true);
            }
            (Column::Float64 { data, nulls }, Value::Default) => {
                data.push(0.0);
                nulls.push(true);
            }
            (Column::Numeric { data, nulls }, Value::Default) => {
                data.push(Decimal::ZERO);
                nulls.push(true);
            }
            (Column::String { data, nulls }, Value::Default) => {
                data.push(String::new());
                nulls.push(true);
            }
            (Column::Bytes { data, nulls }, Value::Default) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (Column::Date { data, nulls }, Value::Default) => {
                data.push(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                nulls.push(true);
            }
            (Column::Time { data, nulls }, Value::Default) => {
                data.push(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                nulls.push(true);
            }
            (Column::Timestamp { data, nulls }, Value::Default) => {
                data.push(DateTime::UNIX_EPOCH);
                nulls.push(true);
            }
            (Column::DateTime { data, nulls }, Value::Default) => {
                data.push(NaiveDateTime::default());
                nulls.push(true);
            }
            (Column::Interval { data, nulls }, Value::Default) => {
                data.push(IntervalValue::new(0, 0, 0));
                nulls.push(true);
            }
            (Column::Json { data, nulls }, Value::Default) => {
                data.push(serde_json::Value::Null);
                nulls.push(true);
            }
            (Column::Array { data, nulls, .. }, Value::Default) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (Column::Struct { data, nulls, .. }, Value::Default) => {
                data.push(Vec::new());
                nulls.push(true);
            }
            (Column::Geography { data, nulls }, Value::Default) => {
                data.push(String::new());
                nulls.push(true);
            }
            (Column::Range { data, nulls, .. }, Value::Default) => {
                data.push(RangeValue::new(None, None));
                nulls.push(true);
            }
            (col, val) => {
                return Err(Error::type_mismatch(format!(
                    "Cannot push {:?} to column of type {:?}",
                    val.data_type(),
                    col.data_type()
                )));
            }
        }
        Ok(())
    }

    pub fn set(&mut self, index: usize, value: Value) -> Result<()> {
        match (self, value) {
            (Column::Bool { data, nulls }, Value::Null) => {
                data[index] = false;
                nulls.set(index, true);
            }
            (Column::Bool { data, nulls }, Value::Bool(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Int64 { data, nulls }, Value::Null) => {
                data[index] = 0;
                nulls.set(index, true);
            }
            (Column::Int64 { data, nulls }, Value::Int64(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Float64 { data, nulls }, Value::Null) => {
                data[index] = 0.0;
                nulls.set(index, true);
            }
            (Column::Float64 { data, nulls }, Value::Float64(v)) => {
                data[index] = v.0;
                nulls.set(index, false);
            }
            (Column::Numeric { data, nulls }, Value::Null) => {
                data[index] = Decimal::ZERO;
                nulls.set(index, true);
            }
            (Column::Numeric { data, nulls }, Value::Numeric(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::String { data, nulls }, Value::Null) => {
                data[index] = String::new();
                nulls.set(index, true);
            }
            (Column::String { data, nulls }, Value::String(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Bytes { data, nulls }, Value::Null) => {
                data[index] = Vec::new();
                nulls.set(index, true);
            }
            (Column::Bytes { data, nulls }, Value::Bytes(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Date { data, nulls }, Value::Null) => {
                data[index] = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                nulls.set(index, true);
            }
            (Column::Date { data, nulls }, Value::Date(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Time { data, nulls }, Value::Null) => {
                data[index] = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
                nulls.set(index, true);
            }
            (Column::Time { data, nulls }, Value::Time(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::DateTime { data, nulls }, Value::Null) => {
                data[index] = chrono::DateTime::UNIX_EPOCH.naive_utc();
                nulls.set(index, true);
            }
            (Column::DateTime { data, nulls }, Value::DateTime(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Timestamp { data, nulls }, Value::Null) => {
                data[index] = DateTime::UNIX_EPOCH;
                nulls.set(index, true);
            }
            (Column::Timestamp { data, nulls }, Value::Timestamp(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Json { data, nulls }, Value::Null) => {
                data[index] = serde_json::Value::Null;
                nulls.set(index, true);
            }
            (Column::Json { data, nulls }, Value::Json(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Array { data, nulls, .. }, Value::Null) => {
                data[index] = Vec::new();
                nulls.set(index, true);
            }
            (Column::Array { data, nulls, .. }, Value::Array(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Struct { data, nulls, .. }, Value::Null) => {
                data[index] = Vec::new();
                nulls.set(index, true);
            }
            (Column::Struct { data, nulls, .. }, Value::Struct(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Geography { data, nulls }, Value::Null) => {
                data[index] = String::new();
                nulls.set(index, true);
            }
            (Column::Geography { data, nulls }, Value::Geography(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Interval { data, nulls }, Value::Null) => {
                data[index] = IntervalValue {
                    months: 0,
                    days: 0,
                    nanos: 0,
                };
                nulls.set(index, true);
            }
            (Column::Interval { data, nulls }, Value::Interval(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (Column::Range { data, nulls, .. }, Value::Null) => {
                data[index] = RangeValue::new(None, None);
                nulls.set(index, true);
            }
            (Column::Range { data, nulls, .. }, Value::Range(v)) => {
                data[index] = v;
                nulls.set(index, false);
            }
            (col, val) => {
                return Err(Error::type_mismatch(format!(
                    "Cannot set {:?} in column of type {:?}",
                    val.data_type(),
                    col.data_type()
                )));
            }
        }
        Ok(())
    }

    pub fn remove(&mut self, index: usize) {
        match self {
            Column::Bool { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Int64 { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Float64 { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Numeric { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::String { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Bytes { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Date { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Time { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::DateTime { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Timestamp { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Json { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Array { data, nulls, .. } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Struct { data, nulls, .. } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Geography { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Interval { data, nulls } => {
                data.remove(index);
                nulls.remove(index);
            }
            Column::Range { data, nulls, .. } => {
                data.remove(index);
                nulls.remove(index);
            }
        }
    }

    pub fn clear(&mut self) {
        match self {
            Column::Bool { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Int64 { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Float64 { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Numeric { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::String { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Bytes { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Date { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Time { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::DateTime { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Timestamp { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Json { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Array { data, nulls, .. } => {
                data.clear();
                nulls.clear();
            }
            Column::Struct { data, nulls, .. } => {
                data.clear();
                nulls.clear();
            }
            Column::Geography { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Interval { data, nulls } => {
                data.clear();
                nulls.clear();
            }
            Column::Range { data, nulls, .. } => {
                data.clear();
                nulls.clear();
            }
        }
    }
}
