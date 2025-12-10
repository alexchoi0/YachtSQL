use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use rust_decimal::Decimal;
use serde_json;

use crate::error::{Error, Result};
use crate::types::coercion::CoercionRules;
use crate::types::{DataType, Value};

impl Value {
    pub fn from_json(json: serde_json::Value, data_type: &DataType) -> Result<Self> {
        match (json, data_type) {
            (serde_json::Value::Null, _) => Ok(Value::null()),
            (serde_json::Value::Bool(b), DataType::Bool) => Ok(Value::bool_val(b)),
            (serde_json::Value::Number(n), DataType::Int64) => {
                Ok(Value::int64(n.as_i64().ok_or_else(|| {
                    Error::type_mismatch("INT64", format!("{}", n))
                })?))
            }
            (serde_json::Value::Number(n), DataType::Float64) => {
                Ok(Value::float64(n.as_f64().ok_or_else(|| {
                    Error::type_mismatch("FLOAT64", format!("{}", n))
                })?))
            }
            (serde_json::Value::Number(n), DataType::Numeric(_)) => {
                let decimal = if let Some(f) = n.as_f64() {
                    Decimal::try_from(f)
                        .map_err(|_| Error::type_mismatch("NUMERIC", format!("{}", n)))?
                } else if let Some(i) = n.as_i64() {
                    Decimal::from(i)
                } else {
                    return Err(Error::type_mismatch("NUMERIC", format!("{}", n)));
                };
                Ok(Value::numeric(decimal))
            }
            (serde_json::Value::String(s), DataType::String) => Ok(Value::string(s)),
            _ => Err(Error::type_mismatch(
                format!("{}", data_type),
                "incompatible JSON value",
            )),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        use crate::types::small_value::ValueTag;

        unsafe {
            let tag = self.tag();

            if tag < crate::types::TAG_HEAP_START {
                let small = self.as_inline();
                match small.tag() {
                    ValueTag::Null => serde_json::Value::Null,
                    ValueTag::Bool => {
                        serde_json::Value::Bool(small.as_bool().expect("tag matched Bool"))
                    }
                    ValueTag::Int64 => serde_json::Value::Number(
                        small.as_int64().expect("tag matched Int64").into(),
                    ),
                    ValueTag::Float64 => serde_json::Value::Number(
                        serde_json::Number::from_f64(
                            small.as_float64().expect("tag matched Float64"),
                        )
                        .unwrap_or_else(|| 0.into()),
                    ),
                    ValueTag::SmallString => serde_json::Value::String(
                        small.as_str().expect("tag matched SmallString").to_string(),
                    ),
                    ValueTag::Date => {
                        let days = small.as_date().expect("tag matched Date");
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                            .expect("1970-01-01 is a valid date");
                        let date = epoch + chrono::Duration::days(days as i64);
                        serde_json::Value::String(date.to_string())
                    }
                    ValueTag::Time => {
                        let nanos = small.as_int64().expect("tag matched Time");
                        let time = chrono::NaiveTime::from_hms_opt(0, 0, 0)
                            .expect("00:00:00 is a valid time")
                            + chrono::Duration::nanoseconds(nanos);
                        serde_json::Value::String(time.to_string())
                    }
                    ValueTag::DateTime => {
                        let micros = small.as_datetime().expect("tag matched DateTime");
                        let dt = chrono::DateTime::from_timestamp_micros(micros)
                            .expect("valid timestamp micros from internal storage");
                        serde_json::Value::String(dt.to_rfc3339())
                    }
                    ValueTag::Timestamp => {
                        let micros = small.as_timestamp().expect("tag matched Timestamp");
                        let dt = chrono::DateTime::from_timestamp_micros(micros)
                            .expect("valid timestamp micros from internal storage");
                        serde_json::Value::String(dt.to_rfc3339())
                    }
                }
            } else {
                let heap = self.as_heap();
                match tag {
                    crate::types::TAG_LARGE_STRING => {
                        let s = &*(heap.ptr as *const String);
                        serde_json::Value::String(s.clone())
                    }
                    crate::types::TAG_NUMERIC => {
                        let d = &*(heap.ptr as *const Decimal);
                        if let Ok(f) = d.to_string().parse::<f64>() {
                            serde_json::Value::Number(
                                serde_json::Number::from_f64(f).unwrap_or_else(|| 0.into()),
                            )
                        } else {
                            serde_json::Value::String(d.to_string())
                        }
                    }
                    crate::types::TAG_BYTES => {
                        let b = &*(heap.ptr as *const Vec<u8>);
                        serde_json::Value::String(BASE64_STANDARD.encode(b))
                    }
                    crate::types::TAG_GEOGRAPHY => {
                        let wkt = &*(heap.ptr as *const String);
                        serde_json::Value::String(wkt.clone())
                    }
                    crate::types::TAG_STRUCT => {
                        let map = &*(heap.ptr as *const indexmap::IndexMap<String, Value>);
                        let obj: serde_json::Map<String, serde_json::Value> =
                            map.iter().map(|(k, v)| (k.clone(), v.to_json())).collect();
                        serde_json::Value::Object(obj)
                    }
                    crate::types::TAG_ARRAY => {
                        let arr = &*(heap.ptr as *const Vec<Value>);
                        serde_json::Value::Array(arr.iter().map(|v| v.to_json()).collect())
                    }
                    crate::types::TAG_JSON => {
                        let j = &*(heap.ptr as *const serde_json::Value);
                        j.clone()
                    }
                    crate::types::TAG_UUID => {
                        let u = &*(heap.ptr as *const uuid::Uuid);
                        serde_json::Value::String(crate::types::format_uuid_string(u))
                    }
                    crate::types::TAG_DEFAULT => serde_json::Value::String("DEFAULT".to_string()),
                    crate::types::TAG_IPV4 => {
                        let ip = &*(heap.ptr as *const crate::types::IPv4Addr);
                        serde_json::Value::String(format!("ipv4:{}", ip.0))
                    }
                    crate::types::TAG_IPV6 => {
                        let ip = &*(heap.ptr as *const crate::types::IPv6Addr);
                        serde_json::Value::String(format!("ipv6:{}", ip.0))
                    }
                    crate::types::TAG_DATE32 => {
                        let d = &*(heap.ptr as *const crate::types::Date32Value);
                        serde_json::Value::Number(d.0.into())
                    }
                    crate::types::TAG_GEO_POINT => {
                        let p = &*(heap.ptr as *const crate::types::GeoPointValue);
                        serde_json::Value::String(format!("point:{},{}", p.x, p.y))
                    }
                    crate::types::TAG_GEO_RING => {
                        let r = &*(heap.ptr as *const crate::types::GeoRingValue);
                        serde_json::Value::String(format!("ring:{:?}", r))
                    }
                    crate::types::TAG_GEO_POLYGON => {
                        let p = &*(heap.ptr as *const crate::types::GeoPolygonValue);
                        serde_json::Value::String(format!("polygon:{:?}", p))
                    }
                    crate::types::TAG_GEO_MULTIPOLYGON => {
                        let mp = &*(heap.ptr as *const crate::types::GeoMultiPolygonValue);
                        serde_json::Value::String(format!("multipolygon:{:?}", mp))
                    }
                    crate::types::TAG_FIXED_STRING => {
                        let fs = &*(heap.ptr as *const crate::types::FixedStringData);
                        serde_json::Value::String(format!(
                            "fixedstring:{}:{}",
                            fs.length,
                            BASE64_STANDARD.encode(&fs.data)
                        ))
                    }
                    _ => serde_json::Value::Null,
                }
            }
        }
    }
}

pub fn can_coerce(from: &DataType, to: &DataType) -> bool {
    CoercionRules::can_implicitly_coerce(from, to)
}

pub fn coerce_value(value: Value, target_type: &DataType) -> Result<Value> {
    CoercionRules::coerce_value(value, target_type)
}
