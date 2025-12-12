use std::hash::{Hash, Hasher};
use std::sync::LazyLock;

use rust_decimal::prelude::ToPrimitive;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

pub(crate) static ARRAY_OF_UNKNOWN: LazyLock<Vec<DataType>> =
    LazyLock::new(|| vec![DataType::Array(Box::new(DataType::Unknown))]);
pub(crate) static ARRAY_OF_INT64: LazyLock<Vec<DataType>> =
    LazyLock::new(|| vec![DataType::Array(Box::new(DataType::Int64))]);

pub(crate) fn numeric_value_to_f64(value: &Value) -> Result<Option<f64>> {
    if value.is_null() {
        Ok(None)
    } else if let Some(i) = value.as_i64() {
        Ok(Some(i as f64))
    } else if let Some(f) = value.as_f64() {
        Ok(Some(f))
    } else if let Some(d) = value.as_numeric() {
        d.to_f64().map(Some).ok_or_else(|| Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: "NUMERIC (out of range)".to_string(),
        })
    } else {
        Err(Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: value.data_type().to_string(),
        })
    }
}

pub(crate) fn value_to_string(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        s.to_string()
    } else if let Some(i) = value.as_i64() {
        i.to_string()
    } else if let Some(f) = value.as_f64() {
        f.to_string()
    } else if let Some(b) = value.as_bool() {
        b.to_string()
    } else if let Some(d) = value.as_numeric() {
        d.to_string()
    } else if let Some(d) = value.as_date() {
        d.to_string()
    } else if let Some(ts) = value.as_timestamp() {
        ts.to_string()
    } else if let Some(u) = value.as_uuid() {
        u.to_string()
    } else {
        format!("{:?}", value)
    }
}

pub(crate) struct HashableValue<'a>(pub &'a Value);

impl<'a> Hash for HashableValue<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let value = self.0;
        if value.is_null() {
            0_u8.hash(state);
        } else if let Some(b) = value.as_bool() {
            b.hash(state);
        } else if let Some(i) = value.as_i64() {
            i.hash(state);
        } else if let Some(f) = value.as_f64() {
            f.to_bits().hash(state);
        } else if let Some(d) = value.as_numeric() {
            d.to_string().hash(state);
        } else if let Some(s) = value.as_str() {
            s.hash(state);
        } else if let Some(bytes) = value.as_bytes() {
            bytes.hash(state);
        } else if let Some(date) = value.as_date() {
            date.to_string().hash(state);
        } else if let Some(dt) = value.as_datetime() {
            dt.timestamp_nanos_opt().unwrap_or(0).hash(state);
        } else if let Some(time) = value.as_time() {
            time.to_string().hash(state);
        } else if let Some(ts) = value.as_timestamp() {
            ts.timestamp_nanos_opt().unwrap_or(0).hash(state);
        } else if let Some(fields) = value.as_struct() {
            fields.len().hash(state);
            for (key, value) in fields {
                key.hash(state);
                HashableValue(value).hash(state);
            }
        } else if let Some(items) = value.as_array() {
            items.len().hash(state);
            for item in items {
                HashableValue(item).hash(state);
            }
        } else if let Some(wkt) = value.as_geography() {
            wkt.hash(state);
        } else if let Some(json) = value.as_json() {
            json.to_string().hash(state);
        } else if let Some(uuid) = value.as_uuid() {
            uuid.as_bytes().hash(state);
        }
    }
}
