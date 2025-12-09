use std::str::FromStr;

use aligned_vec::AVec;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use indexmap::IndexMap;
use rust_decimal::Decimal;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Interval, Value};

use crate::NullBitmap;

#[derive(Debug, Clone)]
pub enum Column {
    Bool {
        data: AVec<u8>,
        nulls: NullBitmap,
    },
    Int64 {
        data: AVec<i64>,
        nulls: NullBitmap,
    },

    Unknown {
        nulls: NullBitmap,
    },
    Float64 {
        data: AVec<f64>,
        nulls: NullBitmap,
    },
    Numeric {
        data: Vec<Decimal>,
        nulls: NullBitmap,
        precision_scale: Option<(u8, u8)>,
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
    DateTime {
        data: Vec<DateTime<Utc>>,
        nulls: NullBitmap,
    },
    Time {
        data: Vec<NaiveTime>,
        nulls: NullBitmap,
    },
    Timestamp {
        data: Vec<DateTime<Utc>>,
        nulls: NullBitmap,
    },
    Uuid {
        data: Vec<uuid::Uuid>,
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
    Vector {
        data: Vec<Vec<Value>>,
        nulls: NullBitmap,
        dimensions: usize,
    },
    Struct {
        data: Vec<IndexMap<String, Value>>,
        nulls: NullBitmap,
    },
    Geography {
        data: Vec<String>,
        nulls: NullBitmap,
    },
    Interval {
        data: Vec<Interval>,
        nulls: NullBitmap,
    },

    Hstore {
        data: Vec<IndexMap<String, Option<String>>>,
        nulls: NullBitmap,
    },
    MacAddr {
        data: Vec<yachtsql_core::types::MacAddress>,
        nulls: NullBitmap,
    },
    MacAddr8 {
        data: Vec<yachtsql_core::types::MacAddress>,
        nulls: NullBitmap,
    },
    Inet {
        data: Vec<yachtsql_core::types::network::InetAddr>,
        nulls: NullBitmap,
    },
    Cidr {
        data: Vec<yachtsql_core::types::network::CidrAddr>,
        nulls: NullBitmap,
    },
    Enum {
        data: Vec<String>,
        nulls: NullBitmap,
        type_name: String,
        labels: Vec<String>,
    },

    Point {
        data: Vec<yachtsql_core::types::PgPoint>,
        nulls: NullBitmap,
    },

    PgBox {
        data: Vec<yachtsql_core::types::PgBox>,
        nulls: NullBitmap,
    },

    Circle {
        data: Vec<yachtsql_core::types::PgCircle>,
        nulls: NullBitmap,
    },

    Map {
        data: Vec<Vec<(Value, Value)>>,
        nulls: NullBitmap,
        key_type: DataType,
        value_type: DataType,
    },

    Range {
        data: Vec<yachtsql_core::types::Range>,
        nulls: NullBitmap,
        range_type: yachtsql_core::types::RangeType,
    },
}

impl Column {
    pub fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Bool => Column::Bool {
                data: AVec::with_capacity(64, capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Int64 => Column::Int64 {
                data: AVec::with_capacity(64, capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Float64 => Column::Float64 {
                data: AVec::with_capacity(64, capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::String => Column::String {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Bytes => Column::Bytes {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Date => Column::Date {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::DateTime => Column::DateTime {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Time => Column::Time {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Timestamp | DataType::TimestampTz => Column::Timestamp {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Array(elem_type) => Column::Array {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
                element_type: *elem_type.clone(),
            },
            DataType::Vector(dims) => Column::Vector {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
                dimensions: *dims,
            },
            DataType::Struct(_) => Column::Struct {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Numeric(precision_scale) => Column::Numeric {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
                precision_scale: *precision_scale,
            },
            DataType::BigNumeric => Column::Numeric {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
                precision_scale: None,
            },
            DataType::Uuid => Column::Uuid {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Json => Column::Json {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Unknown => Column::Unknown {
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Geography => Column::Geography {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Interval => Column::Interval {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Hstore => Column::Hstore {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::MacAddr => Column::MacAddr {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::MacAddr8 => Column::MacAddr8 {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Inet => Column::Inet {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Cidr => Column::Cidr {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Enum { type_name, labels } => Column::Enum {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
                type_name: type_name.clone(),
                labels: labels.clone(),
            },

            DataType::Custom(_) => Column::Struct {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Point => Column::Point {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::PgBox => Column::PgBox {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Circle => Column::Circle {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
            },
            DataType::Map(key_type, value_type) => Column::Map {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
                key_type: *key_type.clone(),
                value_type: *value_type.clone(),
            },
            DataType::Range(range_type) => Column::Range {
                data: Vec::with_capacity(capacity),
                nulls: NullBitmap::new_valid(0),
                range_type: range_type.clone(),
            },
            _ => unimplemented!("Complex types not yet supported: {:?}", data_type),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Column::Bool { .. } => DataType::Bool,
            Column::Int64 { .. } => DataType::Int64,
            Column::Unknown { .. } => DataType::Unknown,
            Column::Float64 { .. } => DataType::Float64,
            Column::Numeric {
                precision_scale, ..
            } => DataType::Numeric(*precision_scale),
            Column::String { .. } => DataType::String,
            Column::Bytes { .. } => DataType::Bytes,
            Column::Date { .. } => DataType::Date,
            Column::DateTime { .. } => DataType::DateTime,
            Column::Time { .. } => DataType::Time,
            Column::Timestamp { .. } => DataType::Timestamp,
            Column::Uuid { .. } => DataType::Uuid,
            Column::Json { .. } => DataType::Json,
            Column::Array { element_type, .. } => DataType::Array(Box::new(element_type.clone())),
            Column::Vector { dimensions, .. } => DataType::Vector(*dimensions),
            Column::Struct { data, .. } => {
                if let Some(first_struct) = data.first() {
                    let fields = first_struct
                        .iter()
                        .map(|(name, value)| yachtsql_core::types::StructField {
                            name: name.clone(),
                            data_type: value.data_type(),
                        })
                        .collect();
                    DataType::Struct(fields)
                } else {
                    DataType::Struct(vec![])
                }
            }
            Column::Geography { .. } => DataType::Geography,
            Column::Interval { .. } => DataType::Interval,
            Column::Hstore { .. } => DataType::Hstore,
            Column::MacAddr { .. } => DataType::MacAddr,
            Column::MacAddr8 { .. } => DataType::MacAddr8,
            Column::Inet { .. } => DataType::Inet,
            Column::Cidr { .. } => DataType::Cidr,
            Column::Enum {
                type_name, labels, ..
            } => DataType::Enum {
                type_name: type_name.clone(),
                labels: labels.clone(),
            },
            Column::Point { .. } => DataType::Point,
            Column::PgBox { .. } => DataType::PgBox,
            Column::Circle { .. } => DataType::Circle,
            Column::Map {
                key_type,
                value_type,
                ..
            } => DataType::Map(Box::new(key_type.clone()), Box::new(value_type.clone())),
            Column::Range { range_type, .. } => DataType::Range(range_type.clone()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Column::Bool { nulls, .. } => nulls.len(),
            Column::Int64 { nulls, .. } => nulls.len(),
            Column::Unknown { nulls } => nulls.len(),
            Column::Float64 { nulls, .. } => nulls.len(),
            Column::Numeric { nulls, .. } => nulls.len(),
            Column::String { nulls, .. } => nulls.len(),
            Column::Bytes { nulls, .. } => nulls.len(),
            Column::Date { nulls, .. } => nulls.len(),
            Column::DateTime { nulls, .. } => nulls.len(),
            Column::Time { nulls, .. } => nulls.len(),
            Column::Timestamp { nulls, .. } => nulls.len(),
            Column::Uuid { nulls, .. } => nulls.len(),
            Column::Json { nulls, .. } => nulls.len(),
            Column::Array { nulls, .. } => nulls.len(),
            Column::Vector { nulls, .. } => nulls.len(),
            Column::Struct { nulls, .. } => nulls.len(),
            Column::Geography { nulls, .. } => nulls.len(),
            Column::Interval { nulls, .. } => nulls.len(),
            Column::Hstore { nulls, .. } => nulls.len(),
            Column::MacAddr { nulls, .. } => nulls.len(),
            Column::MacAddr8 { nulls, .. } => nulls.len(),
            Column::Inet { nulls, .. } => nulls.len(),
            Column::Cidr { nulls, .. } => nulls.len(),
            Column::Enum { nulls, .. } => nulls.len(),
            Column::Point { nulls, .. } => nulls.len(),
            Column::PgBox { nulls, .. } => nulls.len(),
            Column::Circle { nulls, .. } => nulls.len(),
            Column::Map { nulls, .. } => nulls.len(),
            Column::Range { nulls, .. } => nulls.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn nulls(&self) -> &NullBitmap {
        match self {
            Column::Bool { nulls, .. } => nulls,
            Column::Int64 { nulls, .. } => nulls,
            Column::Unknown { nulls } => nulls,
            Column::Float64 { nulls, .. } => nulls,
            Column::Numeric { nulls, .. } => nulls,
            Column::String { nulls, .. } => nulls,
            Column::Bytes { nulls, .. } => nulls,
            Column::Date { nulls, .. } => nulls,
            Column::DateTime { nulls, .. } => nulls,
            Column::Time { nulls, .. } => nulls,
            Column::Timestamp { nulls, .. } => nulls,
            Column::Uuid { nulls, .. } => nulls,
            Column::Json { nulls, .. } => nulls,
            Column::Array { nulls, .. } => nulls,
            Column::Vector { nulls, .. } => nulls,
            Column::Struct { nulls, .. } => nulls,
            Column::Geography { nulls, .. } => nulls,
            Column::Interval { nulls, .. } => nulls,
            Column::Hstore { nulls, .. } => nulls,
            Column::MacAddr { nulls, .. } => nulls,
            Column::MacAddr8 { nulls, .. } => nulls,
            Column::Inet { nulls, .. } => nulls,
            Column::Cidr { nulls, .. } => nulls,
            Column::Enum { nulls, .. } => nulls,
            Column::Point { nulls, .. } => nulls,
            Column::PgBox { nulls, .. } => nulls,
            Column::Circle { nulls, .. } => nulls,
            Column::Map { nulls, .. } => nulls,
            Column::Range { nulls, .. } => nulls,
        }
    }

    pub fn null_count(&self) -> usize {
        self.nulls().count_null()
    }

    fn validate_vector_value(vector: &[Value], expected_dims: usize) -> Result<()> {
        if vector.len() != expected_dims {
            return Err(Error::invalid_query(format!(
                "Vector dimension mismatch: expected {}, got {}",
                expected_dims,
                vector.len()
            )));
        }

        if let Some((i, elem)) = vector.iter().enumerate().find(|(_, v)| !v.is_float64()) {
            return Err(Error::invalid_query(format!(
                "Vector element {} must be FLOAT64, got {:?}",
                i, elem
            )));
        }

        Ok(())
    }

    pub fn push(&mut self, value: Value) -> Result<()> {
        if value.is_null() {
            return self.push_null();
        }

        match self {
            Column::Bool { data, nulls } => {
                if let Some(v) = value.as_bool() {
                    data.push(if v { 1 } else { 0 });
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Int64 { data, nulls } => {
                if let Some(v) = value.as_i64() {
                    data.push(v);
                    nulls.push(true);
                    Ok(())
                } else if let Some(n) = value.as_numeric() {
                    if n.fract().is_zero()
                        && let Ok(v) = i64::try_from(n)
                    {
                        data.push(v);
                        nulls.push(true);
                        return Ok(());
                    }
                    Err(Error::invalid_query(format!(
                        "Cannot convert NUMERIC {} to INT64",
                        n
                    )))
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Float64 { data, nulls } => {
                if let Some(f) = value.as_f64() {
                    data.push(f);
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Numeric { data, nulls, .. } => {
                if let Some(d) = value.as_numeric() {
                    data.push(d);
                    nulls.push(true);
                    Ok(())
                } else if let Some(f) = value.as_f64() {
                    if let Ok(decimal) = Decimal::try_from(f) {
                        data.push(decimal);
                        nulls.push(true);
                        Ok(())
                    } else {
                        Err(Error::invalid_query(format!(
                            "Cannot convert {} to Numeric",
                            f
                        )))
                    }
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::String { data, nulls } => {
                if let Some(s) = value.as_str() {
                    data.push(s.to_string());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Bytes { data, nulls } => {
                if let Some(v) = value.as_bytes() {
                    data.push(v.to_vec());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Unknown { nulls: _ } => Err(Error::invalid_query(
                "Unknown column type can only contain NULL values".to_string(),
            )),
            Column::Date { data, nulls } => {
                if let Some(v) = value.as_date() {
                    data.push(v);
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Timestamp { data, nulls } => {
                if let Some(v) = value.as_timestamp() {
                    data.push(v);
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::DateTime { data, nulls } => {
                if let Some(v) = value.as_datetime() {
                    data.push(v);
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Time { data, nulls } => {
                if let Some(v) = value.as_time() {
                    data.push(v);
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Uuid { data, nulls } => {
                if let Some(v) = value.as_uuid() {
                    data.push(*v);
                    nulls.push(true);
                    Ok(())
                } else if let Some(s) = value.as_str() {
                    match uuid::Uuid::parse_str(s) {
                        Ok(uuid_val) => {
                            data.push(uuid_val);
                            nulls.push(true);
                            Ok(())
                        }
                        Err(e) => Err(Error::invalid_query(format!(
                            "Invalid UUID string '{}': {}",
                            s, e
                        ))),
                    }
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Json { data, nulls } => {
                if let Some(v) = value.as_json() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else if let Some(s) = value.as_str() {
                    match serde_json::from_str(s) {
                        Ok(json_val) => {
                            data.push(json_val);
                            nulls.push(true);
                            Ok(())
                        }
                        Err(e) => Err(Error::InvalidOperation(format!(
                            "Invalid JSON string: {}",
                            e
                        ))),
                    }
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Array {
                data,
                nulls,
                element_type: _,
            } => {
                if let Some(v) = value.as_array() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Vector {
                data,
                nulls,
                dimensions,
            } => {
                if let Some(v) = value.as_array() {
                    Self::validate_vector_value(v, *dimensions)?;
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else if let Some(v) = value.as_vector() {
                    if *dimensions != 0 && v.len() != *dimensions {
                        return Err(Error::invalid_query(format!(
                            "VECTOR dimension mismatch: expected {}, got {}",
                            dimensions,
                            v.len()
                        )));
                    }
                    let as_values: Vec<Value> = v.iter().map(|f| Value::float64(*f)).collect();
                    data.push(as_values);
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected VECTOR({}), got {}",
                        dimensions,
                        value.data_type()
                    )))
                }
            }
            Column::Geography { data, nulls } => {
                if let Some(v) = value.as_geography() {
                    data.push(v.to_string());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Struct { data, nulls } => {
                if let Some(v) = value.as_struct() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Interval { data, nulls } => {
                if let Some(v) = value.as_interval() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Hstore { data, nulls } => {
                if let Some(v) = value.as_hstore() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::MacAddr { data, nulls } => {
                if let Some(v) = value.as_macaddr() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::MacAddr8 { data, nulls } => {
                if let Some(v) = value.as_macaddr8() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Inet { data, nulls } => {
                if let Some(v) = value.as_inet() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else if let Some(s) = value.as_str() {
                    match yachtsql_core::types::network::InetAddr::from_str(s) {
                        Ok(inet) => {
                            data.push(inet);
                            nulls.push(true);
                            Ok(())
                        }
                        Err(e) => Err(Error::invalid_query(format!(
                            "cannot parse '{}' as INET: {}",
                            s, e
                        ))),
                    }
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Cidr { data, nulls } => {
                if let Some(v) = value.as_cidr() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else if let Some(s) = value.as_str() {
                    match yachtsql_core::types::network::CidrAddr::from_str(s) {
                        Ok(cidr) => {
                            data.push(cidr);
                            nulls.push(true);
                            Ok(())
                        }
                        Err(e) => Err(Error::invalid_query(format!(
                            "cannot parse '{}' as CIDR: {}",
                            s, e
                        ))),
                    }
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Enum { data, nulls, .. } => {
                if let Some(s) = value.as_str() {
                    data.push(s.to_string());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Point { data, nulls } => {
                if let Some(v) = value.as_point() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::PgBox { data, nulls } => {
                if let Some(v) = value.as_pgbox() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Circle { data, nulls } => {
                if let Some(v) = value.as_circle() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Map { data, nulls, .. } => {
                if let Some(v) = value.as_map() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Range { data, nulls, .. } => {
                if let Some(v) = value.as_range() {
                    data.push(v.clone());
                    nulls.push(true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
        }
    }

    fn push_null(&mut self) -> Result<()> {
        match self {
            Column::Bool { data, nulls } => {
                data.push(0);
                nulls.push(false);
            }
            Column::Int64 { data, nulls } => {
                data.push(0);
                nulls.push(false);
            }
            Column::Float64 { data, nulls } => {
                data.push(0.0);
                nulls.push(false);
            }
            Column::Numeric { data, nulls, .. } => {
                data.push(Decimal::ZERO);
                nulls.push(false);
            }
            Column::String { data, nulls } => {
                data.push(String::new());
                nulls.push(false);
            }
            Column::Bytes { data, nulls } => {
                data.push(Vec::new());
                nulls.push(false);
            }
            Column::Unknown { nulls } => {
                nulls.push(false);
            }
            Column::Date { data, nulls } => {
                data.push(NaiveDate::default());
                nulls.push(false);
            }
            Column::DateTime { data, nulls } => {
                data.push(DateTime::<Utc>::default());
                nulls.push(false);
            }
            Column::Time { data, nulls } => {
                data.push(NaiveTime::default());
                nulls.push(false);
            }
            Column::Timestamp { data, nulls } => {
                data.push(DateTime::<Utc>::default());
                nulls.push(false);
            }
            Column::Uuid { data, nulls } => {
                data.push(uuid::Uuid::nil());
                nulls.push(false);
            }
            Column::Json { data, nulls } => {
                data.push(serde_json::Value::Null);
                nulls.push(false);
            }
            Column::Array { data, nulls, .. } => {
                data.push(Vec::new());
                nulls.push(false);
            }
            Column::Vector {
                data,
                nulls,
                dimensions,
            } => {
                data.push(vec![Value::float64(0.0); *dimensions]);
                nulls.push(false);
            }
            Column::Geography { data, nulls } => {
                data.push(String::new());
                nulls.push(false);
            }
            Column::Struct { data, nulls } => {
                data.push(IndexMap::new());
                nulls.push(false);
            }
            Column::Interval { data, nulls } => {
                data.push(Interval::default());
                nulls.push(false);
            }
            Column::Hstore { data, nulls } => {
                data.push(IndexMap::new());
                nulls.push(false);
            }
            Column::MacAddr { data, nulls } => {
                data.push(yachtsql_core::types::MacAddress::new_macaddr([
                    0, 0, 0, 0, 0, 0,
                ]));
                nulls.push(false);
            }
            Column::MacAddr8 { data, nulls } => {
                data.push(yachtsql_core::types::MacAddress::new_macaddr8([
                    0, 0, 0, 0, 0, 0, 0, 0,
                ]));
                nulls.push(false);
            }
            Column::Inet { data, nulls } => {
                data.push(yachtsql_core::types::network::InetAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
                ));
                nulls.push(false);
            }
            Column::Cidr { data, nulls } => {
                data.push(
                    yachtsql_core::types::network::CidrAddr::new(
                        std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
                        0,
                    )
                    .unwrap(),
                );
                nulls.push(false);
            }
            Column::Enum { data, nulls, .. } => {
                data.push(String::new());
                nulls.push(false);
            }
            Column::Point { data, nulls } => {
                data.push(yachtsql_core::types::PgPoint::new(0.0, 0.0));
                nulls.push(false);
            }
            Column::PgBox { data, nulls } => {
                data.push(yachtsql_core::types::PgBox::new(
                    yachtsql_core::types::PgPoint::new(0.0, 0.0),
                    yachtsql_core::types::PgPoint::new(0.0, 0.0),
                ));
                nulls.push(false);
            }
            Column::Circle { data, nulls } => {
                data.push(yachtsql_core::types::PgCircle::new(
                    yachtsql_core::types::PgPoint::new(0.0, 0.0),
                    0.0,
                ));
                nulls.push(false);
            }
            Column::Map { data, nulls, .. } => {
                data.push(Vec::new());
                nulls.push(false);
            }
            Column::Range {
                data,
                nulls,
                range_type,
            } => {
                data.push(yachtsql_core::types::Range {
                    range_type: range_type.clone(),
                    lower: None,
                    upper: None,
                    lower_inclusive: false,
                    upper_inclusive: false,
                });
                nulls.push(false);
            }
        }
        Ok(())
    }

    pub fn update(&mut self, index: usize, value: Value) -> Result<()> {
        if index >= self.len() {
            return Err(Error::InvalidOperation(format!(
                "Index {} out of bounds",
                index
            )));
        }

        if value.is_null() {
            return self.update_null(index);
        }

        match self {
            Column::Bool { data, nulls } => {
                if let Some(v) = value.as_bool() {
                    data[index] = if v { 1 } else { 0 };
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Int64 { data, nulls } => {
                if let Some(v) = value.as_i64() {
                    data[index] = v;
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Float64 { data, nulls } => {
                if let Some(f) = value.as_f64() {
                    data[index] = f;
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Numeric { data, nulls, .. } => {
                if let Some(d) = value.as_numeric() {
                    data[index] = d;
                    nulls.set(index, true);
                    Ok(())
                } else if let Some(f) = value.as_f64() {
                    if let Ok(decimal) = Decimal::try_from(f) {
                        data[index] = decimal;
                        nulls.set(index, true);
                        Ok(())
                    } else {
                        Err(Error::invalid_query(format!(
                            "Cannot convert {} to Numeric",
                            f
                        )))
                    }
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::String { data, nulls } => {
                if let Some(s) = value.as_str() {
                    data[index] = s.to_string();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Bytes { data, nulls } => {
                if let Some(v) = value.as_bytes() {
                    data[index] = v.to_vec();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Unknown { .. } => Err(Error::invalid_query(
                "Unknown column type can only contain NULL values".to_string(),
            )),
            Column::Date { data, nulls } => {
                if let Some(v) = value.as_date() {
                    data[index] = v;
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Timestamp { data, nulls } => {
                if let Some(v) = value.as_timestamp() {
                    data[index] = v;
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::DateTime { data, nulls } => {
                if let Some(v) = value.as_datetime() {
                    data[index] = v;
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Time { data, nulls } => {
                if let Some(v) = value.as_time() {
                    data[index] = v;
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Uuid { data, nulls } => {
                if let Some(v) = value.as_uuid() {
                    data[index] = *v;
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Json { data, nulls } => {
                if let Some(v) = value.as_json() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else if let Some(s) = value.as_str() {
                    match serde_json::from_str(s) {
                        Ok(json_val) => {
                            data[index] = json_val;
                            nulls.set(index, true);
                            Ok(())
                        }
                        Err(e) => Err(Error::InvalidOperation(format!(
                            "Invalid JSON string: {}",
                            e
                        ))),
                    }
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Array {
                data,
                nulls,
                element_type: _,
            } => {
                if let Some(v) = value.as_array() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Vector {
                data,
                nulls,
                dimensions,
            } => {
                if let Some(v) = value.as_array() {
                    Self::validate_vector_value(v, *dimensions)?;
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else if let Some(v) = value.as_vector() {
                    if *dimensions != 0 && v.len() != *dimensions {
                        return Err(Error::invalid_query(format!(
                            "VECTOR dimension mismatch: expected {}, got {}",
                            dimensions,
                            v.len()
                        )));
                    }
                    let as_values: Vec<Value> = v.iter().map(|f| Value::float64(*f)).collect();
                    data[index] = as_values;
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected VECTOR({}), got {}",
                        dimensions,
                        value.data_type()
                    )))
                }
            }
            Column::Geography { data, nulls } => {
                if let Some(v) = value.as_geography() {
                    data[index] = v.to_string();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Struct { data, nulls } => {
                if let Some(v) = value.as_struct() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Interval { data, nulls } => {
                if let Some(v) = value.as_interval() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Hstore { data, nulls } => {
                if let Some(v) = value.as_hstore() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::MacAddr { data, nulls } => {
                if let Some(v) = value.as_macaddr() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::MacAddr8 { data, nulls } => {
                if let Some(v) = value.as_macaddr8() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Inet { data, nulls } => {
                if let Some(v) = value.as_inet() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Cidr { data, nulls } => {
                if let Some(v) = value.as_cidr() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Enum { data, nulls, .. } => {
                if let Some(s) = value.as_str() {
                    data[index] = s.to_string();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Point { data, nulls } => {
                if let Some(v) = value.as_point() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::PgBox { data, nulls } => {
                if let Some(v) = value.as_pgbox() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Circle { data, nulls } => {
                if let Some(v) = value.as_circle() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Map { data, nulls, .. } => {
                if let Some(v) = value.as_map() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
            Column::Range { data, nulls, .. } => {
                if let Some(v) = value.as_range() {
                    data[index] = v.clone();
                    nulls.set(index, true);
                    Ok(())
                } else {
                    Err(Error::invalid_query(format!(
                        "type mismatch: expected {}, got {}",
                        self.data_type(),
                        value.data_type()
                    )))
                }
            }
        }
    }

    fn update_null(&mut self, index: usize) -> Result<()> {
        match self {
            Column::Bool { data, nulls } => {
                data[index] = 0;
                nulls.set(index, false);
            }
            Column::Int64 { data, nulls } => {
                data[index] = 0;
                nulls.set(index, false);
            }
            Column::Float64 { data, nulls } => {
                data[index] = 0.0;
                nulls.set(index, false);
            }
            Column::Numeric { data, nulls, .. } => {
                data[index] = Decimal::ZERO;
                nulls.set(index, false);
            }
            Column::String { data, nulls } => {
                data[index] = String::new();
                nulls.set(index, false);
            }
            Column::Bytes { data, nulls } => {
                data[index] = Vec::new();
                nulls.set(index, false);
            }
            Column::Unknown { nulls } => {
                nulls.set(index, false);
            }
            Column::Date { data, nulls } => {
                data[index] = NaiveDate::default();
                nulls.set(index, false);
            }
            Column::DateTime { data, nulls } => {
                data[index] = DateTime::<Utc>::default();
                nulls.set(index, false);
            }
            Column::Time { data, nulls } => {
                data[index] = NaiveTime::default();
                nulls.set(index, false);
            }
            Column::Timestamp { data, nulls } => {
                data[index] = DateTime::<Utc>::default();
                nulls.set(index, false);
            }
            Column::Uuid { data, nulls } => {
                data[index] = uuid::Uuid::nil();
                nulls.set(index, false);
            }
            Column::Json { data, nulls } => {
                data[index] = serde_json::Value::Null;
                nulls.set(index, false);
            }
            Column::Array { data, nulls, .. } => {
                data[index] = Vec::new();
                nulls.set(index, false);
            }
            Column::Vector {
                data,
                nulls,
                dimensions,
            } => {
                data[index] = vec![Value::float64(0.0); *dimensions];
                nulls.set(index, false);
            }
            Column::Geography { data, nulls } => {
                data[index] = String::new();
                nulls.set(index, false);
            }
            Column::Struct { data, nulls } => {
                data[index] = IndexMap::new();
                nulls.set(index, false);
            }
            Column::Interval { data, nulls } => {
                data[index] = Interval::default();
                nulls.set(index, false);
            }
            Column::Hstore { data, nulls } => {
                data[index] = IndexMap::new();
                nulls.set(index, false);
            }
            Column::MacAddr { data, nulls } => {
                data[index] = yachtsql_core::types::MacAddress::new_macaddr([0, 0, 0, 0, 0, 0]);
                nulls.set(index, false);
            }
            Column::MacAddr8 { data, nulls } => {
                data[index] =
                    yachtsql_core::types::MacAddress::new_macaddr8([0, 0, 0, 0, 0, 0, 0, 0]);
                nulls.set(index, false);
            }
            Column::Inet { data, nulls } => {
                data[index] = yachtsql_core::types::network::InetAddr::new(std::net::IpAddr::V4(
                    std::net::Ipv4Addr::new(0, 0, 0, 0),
                ));
                nulls.set(index, false);
            }
            Column::Cidr { data, nulls } => {
                data[index] = yachtsql_core::types::network::CidrAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
                    0,
                )
                .unwrap();
                nulls.set(index, false);
            }
            Column::Enum { data, nulls, .. } => {
                data[index] = String::new();
                nulls.set(index, false);
            }
            Column::Point { data, nulls } => {
                data[index] = yachtsql_core::types::PgPoint::new(0.0, 0.0);
                nulls.set(index, false);
            }
            Column::PgBox { data, nulls } => {
                data[index] = yachtsql_core::types::PgBox::new(
                    yachtsql_core::types::PgPoint::new(0.0, 0.0),
                    yachtsql_core::types::PgPoint::new(0.0, 0.0),
                );
                nulls.set(index, false);
            }
            Column::Circle { data, nulls } => {
                data[index] = yachtsql_core::types::PgCircle::new(
                    yachtsql_core::types::PgPoint::new(0.0, 0.0),
                    0.0,
                );
                nulls.set(index, false);
            }
            Column::Map { data, nulls, .. } => {
                data[index] = Vec::new();
                nulls.set(index, false);
            }
            Column::Range {
                data,
                nulls,
                range_type,
            } => {
                data[index] = yachtsql_core::types::Range {
                    range_type: range_type.clone(),
                    lower: None,
                    upper: None,
                    lower_inclusive: false,
                    upper_inclusive: false,
                };
                nulls.set(index, false);
            }
        }
        Ok(())
    }

    pub fn clear(&mut self) {
        match self {
            Column::Bool { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Int64 { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Unknown { nulls } => {
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Float64 { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Numeric { data, nulls, .. } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::String { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Bytes { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Date { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::DateTime { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Time { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Timestamp { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Uuid { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Json { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Array { data, nulls, .. } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Vector { data, nulls, .. } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Struct { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Geography { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Interval { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Hstore { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::MacAddr { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::MacAddr8 { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Inet { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Cidr { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Enum { data, nulls, .. } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Point { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::PgBox { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Circle { data, nulls } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Map { data, nulls, .. } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
            Column::Range { data, nulls, .. } => {
                data.clear();
                *nulls = NullBitmap::new_valid(0);
            }
        }
    }

    pub fn get(&self, index: usize) -> Result<Value> {
        if index >= self.len() {
            return Err(Error::InvalidOperation(format!(
                "Index {} out of bounds for column of length {}",
                index,
                self.len()
            )));
        }

        let is_valid = self.nulls().is_valid(index);
        if !is_valid {
            return Ok(Value::null());
        }

        match self {
            Column::Bool { data, .. } => Ok(Value::bool_val(data[index] != 0)),
            Column::Int64 { data, .. } => Ok(Value::int64(data[index])),
            Column::Unknown { .. } => Ok(Value::null()),
            Column::Float64 { data, .. } => Ok(Value::float64(data[index])),
            Column::Numeric { data, .. } => Ok(Value::numeric(data[index])),
            Column::String { data, .. } => Ok(Value::string(data[index].clone())),
            Column::Bytes { data, .. } => Ok(Value::bytes(data[index].clone())),
            Column::Date { data, .. } => Ok(Value::date(data[index])),
            Column::DateTime { data, .. } => Ok(Value::datetime(data[index])),
            Column::Time { data, .. } => Ok(Value::time(data[index])),
            Column::Timestamp { data, .. } => Ok(Value::timestamp(data[index])),
            Column::Uuid { data, .. } => Ok(Value::uuid(data[index])),
            Column::Json { data, .. } => Ok(Value::json(data[index].clone())),
            Column::Array { data, .. } => Ok(Value::array(data[index].clone())),
            Column::Vector { data, .. } => Ok(Value::array(data[index].clone())),
            Column::Struct { data, .. } => Ok(Value::struct_val(data[index].clone())),
            Column::Geography { data, .. } => Ok(Value::geography(data[index].clone())),
            Column::Interval { data, .. } => Ok(Value::interval(data[index].clone())),
            Column::Hstore { data, .. } => Ok(Value::hstore(data[index].clone())),
            Column::MacAddr { data, .. } => Ok(Value::macaddr(data[index].clone())),
            Column::MacAddr8 { data, .. } => Ok(Value::macaddr8(data[index].clone())),
            Column::Inet { data, .. } => Ok(Value::inet(data[index].clone())),
            Column::Cidr { data, .. } => Ok(Value::cidr(data[index].clone())),
            Column::Enum { data, .. } => Ok(Value::string(data[index].clone())),
            Column::Point { data, .. } => Ok(Value::point(data[index].clone())),
            Column::PgBox { data, .. } => Ok(Value::pgbox(data[index].clone())),
            Column::Circle { data, .. } => Ok(Value::circle(data[index].clone())),
            Column::Map { data, .. } => Ok(Value::map(data[index].clone())),
            Column::Range { data, .. } => Ok(Value::range(data[index].clone())),
        }
    }

    pub fn slice(&self, start: usize, len: usize) -> Result<Column> {
        if start + len > self.len() {
            return Err(Error::InvalidOperation("Slice out of bounds".to_string()));
        }

        let nulls = self.nulls().slice(start, len);

        match self {
            Column::Bool { data, .. } => {
                let mut sliced_data = AVec::with_capacity(64, len);
                sliced_data.extend_from_slice(&data[start..start + len]);
                Ok(Column::Bool {
                    data: sliced_data,
                    nulls,
                })
            }
            Column::Int64 { data, .. } => {
                let mut sliced_data = AVec::with_capacity(64, len);
                sliced_data.extend_from_slice(&data[start..start + len]);
                Ok(Column::Int64 {
                    data: sliced_data,
                    nulls,
                })
            }
            Column::Float64 { data, .. } => {
                let mut sliced_data = AVec::with_capacity(64, len);
                sliced_data.extend_from_slice(&data[start..start + len]);
                Ok(Column::Float64 {
                    data: sliced_data,
                    nulls,
                })
            }
            Column::Numeric {
                data,
                precision_scale,
                ..
            } => Ok(Column::Numeric {
                data: data[start..start + len].to_vec(),
                nulls,
                precision_scale: *precision_scale,
            }),
            Column::String { data, .. } => Ok(Column::String {
                data: data[start..start + len].to_vec(),
                nulls,
            }),
            Column::Bytes { data, .. } => Ok(Column::Bytes {
                data: data[start..start + len].to_vec(),
                nulls,
            }),
            Column::Uuid { data, .. } => Ok(Column::Uuid {
                data: data[start..start + len].to_vec(),
                nulls,
            }),
            Column::Json { data, .. } => Ok(Column::Json {
                data: data[start..start + len].to_vec(),
                nulls,
            }),
            Column::Geography { data, .. } => Ok(Column::Geography {
                data: data[start..start + len].to_vec(),
                nulls,
            }),
            _ => unimplemented!("Slice not yet implemented for this type"),
        }
    }

    pub fn gather(&self, indices: &[usize]) -> Result<Column> {
        let nulls = self.nulls().gather(indices);

        macro_rules! gather_avec {
            ($data:expr, $variant:ident) => {{
                let mut gathered_data = AVec::with_capacity(64, indices.len());
                for &idx in indices {
                    gathered_data.push($data[idx]);
                }
                Ok(Column::$variant {
                    data: gathered_data,
                    nulls,
                })
            }};
        }

        macro_rules! gather_copy {
            ($data:expr, $variant:ident) => {{
                let gathered_data = indices.iter().map(|&idx| $data[idx]).collect();
                Ok(Column::$variant {
                    data: gathered_data,
                    nulls,
                })
            }};
        }

        macro_rules! gather_clone {
            ($data:expr, $variant:ident) => {{
                let gathered_data = indices.iter().map(|&idx| $data[idx].clone()).collect();
                Ok(Column::$variant {
                    data: gathered_data,
                    nulls,
                })
            }};
        }

        match self {
            Column::Bool { data, .. } => gather_avec!(data, Bool),
            Column::Int64 { data, .. } => gather_avec!(data, Int64),
            Column::Unknown { .. } => Ok(Column::Unknown { nulls }),
            Column::Float64 { data, .. } => gather_avec!(data, Float64),
            Column::Numeric {
                data,
                precision_scale,
                ..
            } => {
                let gathered_data = indices.iter().map(|&idx| data[idx]).collect();
                Ok(Column::Numeric {
                    data: gathered_data,
                    nulls,
                    precision_scale: *precision_scale,
                })
            }
            Column::String { data, .. } => gather_clone!(data, String),
            Column::Bytes { data, .. } => gather_clone!(data, Bytes),
            Column::Date { data, .. } => gather_copy!(data, Date),
            Column::DateTime { data, .. } => gather_copy!(data, DateTime),
            Column::Time { data, .. } => gather_copy!(data, Time),
            Column::Timestamp { data, .. } => gather_copy!(data, Timestamp),
            Column::Uuid { data, .. } => gather_copy!(data, Uuid),
            Column::Json { data, .. } => gather_clone!(data, Json),
            Column::Array {
                data, element_type, ..
            } => {
                let gathered_data = indices.iter().map(|&idx| data[idx].clone()).collect();
                Ok(Column::Array {
                    data: gathered_data,
                    nulls,
                    element_type: element_type.clone(),
                })
            }
            Column::Vector {
                data, dimensions, ..
            } => {
                let gathered_data = indices.iter().map(|&idx| data[idx].clone()).collect();
                Ok(Column::Vector {
                    data: gathered_data,
                    nulls,
                    dimensions: *dimensions,
                })
            }
            Column::Struct { data, .. } => {
                let gathered_data = indices.iter().map(|&idx| data[idx].clone()).collect();
                Ok(Column::Struct {
                    data: gathered_data,
                    nulls,
                })
            }
            Column::Geography { data, .. } => gather_clone!(data, Geography),
            Column::Interval { data, .. } => gather_clone!(data, Interval),
            Column::Hstore { data, .. } => gather_clone!(data, Hstore),
            Column::MacAddr { data, .. } => gather_clone!(data, MacAddr),
            Column::MacAddr8 { data, .. } => gather_clone!(data, MacAddr8),
            Column::Inet { data, .. } => gather_clone!(data, Inet),
            Column::Cidr { data, .. } => gather_clone!(data, Cidr),
            Column::Enum {
                data,
                type_name,
                labels,
                ..
            } => {
                let gathered_data = indices.iter().map(|&idx| data[idx].clone()).collect();
                Ok(Column::Enum {
                    data: gathered_data,
                    nulls,
                    type_name: type_name.clone(),
                    labels: labels.clone(),
                })
            }
            Column::Point { data, .. } => gather_clone!(data, Point),
            Column::PgBox { data, .. } => gather_clone!(data, PgBox),
            Column::Circle { data, .. } => gather_clone!(data, Circle),
            Column::Map {
                data,
                key_type,
                value_type,
                ..
            } => {
                let gathered_data = indices.iter().map(|&idx| data[idx].clone()).collect();
                Ok(Column::Map {
                    data: gathered_data,
                    nulls,
                    key_type: key_type.clone(),
                    value_type: value_type.clone(),
                })
            }
            Column::Range {
                data, range_type, ..
            } => {
                let gathered_data = indices.iter().map(|&idx| data[idx].clone()).collect();
                Ok(Column::Range {
                    data: gathered_data,
                    nulls,
                    range_type: range_type.clone(),
                })
            }
        }
    }

    pub fn append(&mut self, other: &Column) -> Result<()> {
        if self.data_type() != other.data_type() {
            return Err(Error::type_mismatch(self.data_type(), other.data_type()));
        }

        match (self, other) {
            (
                Column::Int64 {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::Int64 {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Float64 {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::Float64 {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::String {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::String {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Date {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::Date {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::DateTime {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::DateTime {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Time {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::Time {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Timestamp {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::Timestamp {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Bytes {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::Bytes {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Bool {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::Bool {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Numeric {
                    data: self_data,
                    nulls: self_nulls,
                    ..
                },
                Column::Numeric {
                    data: other_data,
                    nulls: other_nulls,
                    ..
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Uuid {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::Uuid {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Json {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::Json {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Array {
                    data: self_data,
                    nulls: self_nulls,
                    element_type: self_elem_type,
                },
                Column::Array {
                    data: other_data,
                    nulls: other_nulls,
                    element_type: other_elem_type,
                },
            ) => {
                if self_elem_type != other_elem_type {
                    return Err(Error::InvalidOperation(format!(
                        "Cannot append Array columns with different element types: {:?} vs {:?}",
                        self_elem_type, other_elem_type
                    )));
                }
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Vector {
                    data: self_data,
                    nulls: self_nulls,
                    dimensions: self_dims,
                },
                Column::Vector {
                    data: other_data,
                    nulls: other_nulls,
                    dimensions: other_dims,
                },
            ) => {
                if self_dims != other_dims {
                    return Err(Error::InvalidOperation(format!(
                        "Cannot append Vector columns with different dimensions: {} vs {}",
                        self_dims, other_dims
                    )));
                }
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Geography {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::Geography {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Struct {
                    data: self_data,
                    nulls: self_nulls,
                },
                Column::Struct {
                    data: other_data,
                    nulls: other_nulls,
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            (
                Column::Map {
                    data: self_data,
                    nulls: self_nulls,
                    ..
                },
                Column::Map {
                    data: other_data,
                    nulls: other_nulls,
                    ..
                },
            ) => {
                self_data.extend_from_slice(other_data);
                self_nulls.append(other_nulls);
            }
            _ => unimplemented!("Append not yet implemented for this type combination"),
        }

        Ok(())
    }

    pub fn enum_ordinal(&self, index: usize) -> Option<usize> {
        match self {
            Column::Enum {
                data,
                labels,
                nulls,
                ..
            } => {
                if !nulls.is_valid(index) {
                    return None;
                }
                let value = &data[index];
                labels.iter().position(|l| l == value)
            }
            _ => None,
        }
    }

    pub fn is_enum(&self) -> bool {
        matches!(self, Column::Enum { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_creation() {
        let col = Column::new(&DataType::Int64, 10);
        assert_eq!(col.data_type(), DataType::Int64);
        assert_eq!(col.len(), 0);
        assert!(col.is_empty());
    }

    #[test]
    fn test_push_and_get() {
        let mut col = Column::new(&DataType::Int64, 10);
        col.push(Value::int64(42)).unwrap();
        col.push(Value::null()).unwrap();
        col.push(Value::int64(100)).unwrap();

        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0).unwrap(), Value::int64(42));
        assert_eq!(col.get(1).unwrap(), Value::null());
        assert_eq!(col.get(2).unwrap(), Value::int64(100));
    }

    #[test]
    fn test_null_count() {
        let mut col = Column::new(&DataType::Int64, 10);
        col.push(Value::int64(1)).unwrap();
        col.push(Value::null()).unwrap();
        col.push(Value::null()).unwrap();
        col.push(Value::int64(2)).unwrap();

        assert_eq!(col.null_count(), 2);
    }

    #[test]
    fn test_slice() {
        let mut col = Column::new(&DataType::Int64, 10);
        for i in 0..10 {
            col.push(Value::int64(i)).unwrap();
        }

        let sliced = col.slice(2, 5).unwrap();
        assert_eq!(sliced.len(), 5);
        assert_eq!(sliced.get(0).unwrap(), Value::int64(2));
        assert_eq!(sliced.get(4).unwrap(), Value::int64(6));
    }

    #[test]
    fn test_gather() {
        let mut col = Column::new(&DataType::Int64, 10);
        for i in 0..10 {
            col.push(Value::int64(i)).unwrap();
        }

        let gathered = col.gather(&[0, 2, 5, 9]).unwrap();
        assert_eq!(gathered.len(), 4);
        assert_eq!(gathered.get(0).unwrap(), Value::int64(0));
        assert_eq!(gathered.get(1).unwrap(), Value::int64(2));
        assert_eq!(gathered.get(2).unwrap(), Value::int64(5));
        assert_eq!(gathered.get(3).unwrap(), Value::int64(9));
    }
}
