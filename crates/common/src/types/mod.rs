pub mod small_value;

use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Unknown,
    Bool,
    Int64,
    Float64,
    Numeric(Option<(u8, u8)>),
    BigNumeric,
    String,
    Bytes,
    Date,
    DateTime,
    Time,
    Timestamp,
    Geography,
    Json,
    Struct(Vec<StructField>),
    Array(Box<DataType>),
    Interval,
    Range(Box<DataType>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StructField {
    pub name: String,
    pub data_type: DataType,
}

impl DataType {
    pub fn to_bq_type(&self) -> String {
        match self {
            DataType::Unknown => "STRING".to_string(),
            DataType::Bool => "BOOLEAN".to_string(),
            DataType::Int64 => "INT64".to_string(),
            DataType::Float64 => "FLOAT64".to_string(),
            DataType::Numeric(_) => "NUMERIC".to_string(),
            DataType::BigNumeric => "BIGNUMERIC".to_string(),
            DataType::String => "STRING".to_string(),
            DataType::Bytes => "BYTES".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::DateTime => "DATETIME".to_string(),
            DataType::Time => "TIME".to_string(),
            DataType::Timestamp => "TIMESTAMP".to_string(),
            DataType::Geography => "GEOGRAPHY".to_string(),
            DataType::Json => "JSON".to_string(),
            DataType::Struct(_) => "STRUCT".to_string(),
            DataType::Array(inner) => format!("ARRAY<{}>", inner.to_bq_type()),
            DataType::Interval => "INTERVAL".to_string(),
            DataType::Range(_) => "STRING".to_string(),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Unknown => write!(f, "UNKNOWN"),
            DataType::Bool => write!(f, "BOOL"),
            DataType::Int64 => write!(f, "INT64"),
            DataType::Float64 => write!(f, "FLOAT64"),
            DataType::Numeric(None) => write!(f, "NUMERIC"),
            DataType::Numeric(Some((p, s))) => write!(f, "NUMERIC({}, {})", p, s),
            DataType::BigNumeric => write!(f, "BIGNUMERIC"),
            DataType::String => write!(f, "STRING"),
            DataType::Bytes => write!(f, "BYTES"),
            DataType::Date => write!(f, "DATE"),
            DataType::DateTime => write!(f, "DATETIME"),
            DataType::Time => write!(f, "TIME"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Geography => write!(f, "GEOGRAPHY"),
            DataType::Json => write!(f, "JSON"),
            DataType::Struct(fields) => {
                write!(f, "STRUCT<")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} {}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            DataType::Array(inner) => write!(f, "ARRAY<{}>", inner),
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::Range(inner) => write!(f, "RANGE<{}>", inner),
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Int64(i64),
    Float64(ordered_float::OrderedFloat<f64>),
    Numeric(Decimal),
    BigNumeric(Decimal),
    String(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(chrono::NaiveDateTime),
    Timestamp(DateTime<Utc>),
    Json(serde_json::Value),
    Array(Vec<Value>),
    Struct(Vec<(String, Value)>),
    Geography(String),
    Interval(IntervalValue),
    Range(RangeValue),
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntervalValue {
    pub months: i32,
    pub days: i32,
    pub nanos: i64,
}

impl IntervalValue {
    pub const MICROS_PER_SECOND: i64 = 1_000_000;
    pub const MICROS_PER_MINUTE: i64 = 60 * Self::MICROS_PER_SECOND;
    pub const MICROS_PER_HOUR: i64 = 60 * Self::MICROS_PER_MINUTE;
    pub const NANOS_PER_MICRO: i64 = 1_000;

    pub fn new(months: i32, days: i32, micros: i64) -> Self {
        Self {
            months,
            days,
            nanos: micros * Self::NANOS_PER_MICRO,
        }
    }

    pub fn from_months(months: i32) -> Self {
        Self {
            months,
            days: 0,
            nanos: 0,
        }
    }

    pub fn from_days(days: i32) -> Self {
        Self {
            months: 0,
            days,
            nanos: 0,
        }
    }

    pub fn from_hours(hours: i64) -> Self {
        Self {
            months: 0,
            days: 0,
            nanos: hours * Self::MICROS_PER_HOUR * Self::NANOS_PER_MICRO,
        }
    }
}

pub type Interval = IntervalValue;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RangeValue {
    pub start: Option<Box<Value>>,
    pub end: Option<Box<Value>>,
}

impl RangeValue {
    pub fn new(start: Option<Value>, end: Option<Value>) -> Self {
        Self {
            start: start.map(Box::new),
            end: end.map(Box::new),
        }
    }

    pub fn start(&self) -> Option<&Value> {
        self.start.as_deref()
    }

    pub fn end(&self) -> Option<&Value> {
        self.end.as_deref()
    }

    pub fn contains(&self, value: &Value) -> bool {
        let after_start = match &self.start {
            Some(start) => value >= start.as_ref(),
            None => true,
        };
        let before_end = match &self.end {
            Some(end) => value < end.as_ref(),
            None => true,
        };
        after_start && before_end
    }

    pub fn overlaps(&self, other: &RangeValue) -> bool {
        let self_start = self.start.as_deref();
        let self_end = self.end.as_deref();
        let other_start = other.start.as_deref();
        let other_end = other.end.as_deref();

        let start_before_other_end = match (self_start, other_end) {
            (Some(s), Some(e)) => s < e,
            _ => true,
        };
        let other_start_before_end = match (other_start, self_end) {
            (Some(s), Some(e)) => s < e,
            _ => true,
        };
        start_before_other_end && other_start_before_end
    }

    pub fn intersect(&self, other: &RangeValue) -> Option<RangeValue> {
        if !self.overlaps(other) {
            return None;
        }

        let new_start = match (&self.start, &other.start) {
            (Some(a), Some(b)) => Some(if a.as_ref() > b.as_ref() {
                a.as_ref().clone()
            } else {
                b.as_ref().clone()
            }),
            (Some(a), None) => Some(a.as_ref().clone()),
            (None, Some(b)) => Some(b.as_ref().clone()),
            (None, None) => None,
        };

        let new_end = match (&self.end, &other.end) {
            (Some(a), Some(b)) => Some(if a.as_ref() < b.as_ref() {
                a.as_ref().clone()
            } else {
                b.as_ref().clone()
            }),
            (Some(a), None) => Some(a.as_ref().clone()),
            (None, Some(b)) => Some(b.as_ref().clone()),
            (None, None) => None,
        };

        Some(RangeValue::new(new_start, new_end))
    }

    pub fn element_type(&self) -> DataType {
        self.start
            .as_ref()
            .map(|v| v.data_type())
            .or_else(|| self.end.as_ref().map(|v| v.data_type()))
            .unwrap_or(DataType::Unknown)
    }
}

impl Value {
    pub fn null() -> Self {
        Value::Null
    }

    pub fn bool_val(v: bool) -> Self {
        Value::Bool(v)
    }

    pub fn int64(v: i64) -> Self {
        Value::Int64(v)
    }

    pub fn float64(v: f64) -> Self {
        Value::Float64(ordered_float::OrderedFloat(v))
    }

    pub fn numeric(v: Decimal) -> Self {
        Value::Numeric(v)
    }

    pub fn string(v: impl Into<String>) -> Self {
        Value::String(v.into())
    }

    pub fn bytes(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }

    pub fn date(v: NaiveDate) -> Self {
        Value::Date(v)
    }

    pub fn time(v: NaiveTime) -> Self {
        Value::Time(v)
    }

    pub fn datetime(v: chrono::NaiveDateTime) -> Self {
        Value::DateTime(v)
    }

    pub fn timestamp(v: DateTime<Utc>) -> Self {
        Value::Timestamp(v)
    }

    pub fn json(v: serde_json::Value) -> Self {
        Value::Json(v)
    }

    pub fn array(v: Vec<Value>) -> Self {
        Value::Array(v)
    }

    pub fn struct_val(v: Vec<(String, Value)>) -> Self {
        Value::Struct(v)
    }

    pub fn geography(v: impl Into<String>) -> Self {
        Value::Geography(v.into())
    }

    pub fn interval(v: IntervalValue) -> Self {
        Value::Interval(v)
    }

    pub fn interval_from_parts(months: i32, days: i32, nanos: i64) -> Self {
        Value::Interval(IntervalValue {
            months,
            days,
            nanos,
        })
    }

    pub fn range(start: Option<Value>, end: Option<Value>) -> Self {
        Value::Range(RangeValue::new(start, end))
    }

    pub fn range_val(v: RangeValue) -> Self {
        Value::Range(v)
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Unknown,
            Value::Bool(_) => DataType::Bool,
            Value::Int64(_) => DataType::Int64,
            Value::Float64(_) => DataType::Float64,
            Value::Numeric(_) => DataType::Numeric(None),
            Value::BigNumeric(_) => DataType::BigNumeric,
            Value::String(_) => DataType::String,
            Value::Bytes(_) => DataType::Bytes,
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::DateTime(_) => DataType::DateTime,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Json(_) => DataType::Json,
            Value::Array(elements) => {
                let elem_type = elements
                    .first()
                    .map(|v| v.data_type())
                    .unwrap_or(DataType::Unknown);
                DataType::Array(Box::new(elem_type))
            }
            Value::Struct(fields) => {
                let struct_fields = fields
                    .iter()
                    .map(|(name, val)| StructField {
                        name: name.clone(),
                        data_type: val.data_type(),
                    })
                    .collect();
                DataType::Struct(struct_fields)
            }
            Value::Geography(_) => DataType::Geography,
            Value::Interval(_) => DataType::Interval,
            Value::Range(r) => DataType::Range(Box::new(r.element_type())),
            Value::Default => DataType::Unknown,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(v) => Some(v.0),
            Value::Int64(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_numeric(&self) -> Option<Decimal> {
        match self {
            Value::Numeric(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_numeric_ref(&self) -> Option<&Decimal> {
        match self {
            Value::Numeric(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<NaiveDate> {
        match self {
            Value::Date(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_time(&self) -> Option<NaiveTime> {
        match self {
            Value::Time(t) => Some(*t),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<chrono::NaiveDateTime> {
        match self {
            Value::DateTime(dt) => Some(*dt),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<DateTime<Utc>> {
        match self {
            Value::Timestamp(ts) => Some(*ts),
            _ => None,
        }
    }

    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            Value::Json(j) => Some(j),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_struct(&self) -> Option<&[(String, Value)]> {
        match self {
            Value::Struct(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_geography(&self) -> Option<&str> {
        match self {
            Value::Geography(g) => Some(g),
            _ => None,
        }
    }

    pub fn as_interval(&self) -> Option<&IntervalValue> {
        match self {
            Value::Interval(i) => Some(i),
            _ => None,
        }
    }

    pub fn as_range(&self) -> Option<&RangeValue> {
        match self {
            Value::Range(r) => Some(r),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_bytes(self) -> Option<Vec<u8>> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn into_array(self) -> Option<Vec<Value>> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int64(i) => serde_json::json!(i),
            Value::Float64(f) => serde_json::json!(f.into_inner()),
            Value::Numeric(d) => serde_json::Value::String(d.to_string()),
            Value::BigNumeric(d) => serde_json::Value::String(d.to_string()),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b,
            )),
            Value::Date(d) => serde_json::Value::String(d.to_string()),
            Value::Time(t) => serde_json::Value::String(t.to_string()),
            Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
            Value::Timestamp(ts) => serde_json::Value::String(ts.to_string()),
            Value::Json(j) => j.clone(),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| v.to_json()).collect())
            }
            Value::Struct(fields) => {
                let obj: serde_json::Map<String, serde_json::Value> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect();
                serde_json::Value::Object(obj)
            }
            Value::Geography(g) => serde_json::Value::String(g.clone()),
            Value::Interval(i) => serde_json::Value::String(format!("{:?}", i)),
            Value::Range(r) => serde_json::Value::String(format!("{:?}", r)),
            Value::Default => serde_json::Value::Null,
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Numeric(v) => write!(f, "{}", v),
            Value::BigNumeric(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "'{}'", v),
            Value::Bytes(v) => write!(f, "b'{}'", hex::encode(v)),
            Value::Date(v) => write!(f, "DATE '{}'", v),
            Value::Time(v) => write!(f, "TIME '{}'", v),
            Value::DateTime(v) => write!(f, "DATETIME '{}'", v),
            Value::Timestamp(v) => {
                write!(f, "TIMESTAMP '{}'", v.format("%Y-%m-%d %H:%M:%S%.6f UTC"))
            }
            Value::Json(v) => write!(f, "JSON '{}'", v),
            Value::Array(v) => {
                write!(f, "[")?;
                for (i, elem) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", elem)?;
                }
                write!(f, "]")
            }
            Value::Struct(fields) => {
                write!(f, "STRUCT(")?;
                for (i, (name, val)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {:?}", name, val)?;
                }
                write!(f, ")")
            }
            Value::Geography(v) => write!(f, "GEOGRAPHY '{}'", v),
            Value::Interval(v) => write!(
                f,
                "INTERVAL {} months {} days {} nanos",
                v.months, v.days, v.nanos
            ),
            Value::Range(r) => {
                write!(f, "RANGE(")?;
                match &r.start {
                    Some(s) => write!(f, "{:?}", s)?,
                    None => write!(f, "NULL")?,
                }
                write!(f, ", ")?;
                match &r.end {
                    Some(e) => write!(f, "{:?}", e)?,
                    None => write!(f, "NULL")?,
                }
                write!(f, ")")
            }
            Value::Default => write!(f, "DEFAULT"),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Numeric(v) => write!(f, "{}", v),
            Value::BigNumeric(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
            Value::Bytes(v) => write!(f, "{}", hex::encode(v)),
            Value::Date(v) => write!(f, "{}", v),
            Value::Time(v) => write!(f, "{}", v),
            Value::DateTime(v) => write!(f, "{}", v),
            Value::Timestamp(v) => write!(f, "{}", v.format("%Y-%m-%d %H:%M:%S%.6f UTC")),
            Value::Json(v) => write!(f, "{}", v),
            Value::Array(v) => {
                write!(f, "[")?;
                for (i, elem) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", elem)?;
                }
                write!(f, "]")
            }
            Value::Struct(fields) => {
                write!(f, "{{")?;
                for (i, (name, val)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", name, val)?;
                }
                write!(f, "}}")
            }
            Value::Geography(v) => write!(f, "{}", v),
            Value::Interval(v) => write!(f, "{}-{} {}", v.months, v.days, v.nanos),
            Value::Range(r) => {
                write!(f, "[")?;
                match &r.start {
                    Some(s) => write!(f, "{}", s)?,
                    None => write!(f, "UNBOUNDED")?,
                }
                write!(f, ", ")?;
                match &r.end {
                    Some(e) => write!(f, "{}", e)?,
                    None => write!(f, "UNBOUNDED")?,
                }
                write!(f, ")")
            }
            Value::Default => write!(f, "DEFAULT"),
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Bool(v) => v.hash(state),
            Value::Int64(v) => v.hash(state),
            Value::Float64(v) => v.hash(state),
            Value::Numeric(v) => v.hash(state),
            Value::BigNumeric(v) => v.hash(state),
            Value::String(v) => v.hash(state),
            Value::Bytes(v) => v.hash(state),
            Value::Date(v) => v.hash(state),
            Value::Time(v) => v.hash(state),
            Value::DateTime(v) => v.hash(state),
            Value::Timestamp(v) => v.hash(state),
            Value::Json(v) => v.to_string().hash(state),
            Value::Array(v) => {
                for elem in v {
                    elem.hash(state);
                }
            }
            Value::Struct(fields) => {
                for (name, val) in fields {
                    name.hash(state);
                    val.hash(state);
                }
            }
            Value::Geography(v) => v.hash(state),
            Value::Interval(v) => {
                v.months.hash(state);
                v.days.hash(state);
                v.nanos.hash(state);
            }
            Value::Range(r) => r.hash(state),
            Value::Default => {}
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        if self.is_null() && other.is_null() {
            return Ordering::Equal;
        }
        if self.is_null() {
            return Ordering::Greater;
        }
        if other.is_null() {
            return Ordering::Less;
        }

        match (self, other) {
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.cmp(b),
            (Value::Numeric(a), Value::Numeric(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}
