use chrono::TimeZone;
use rust_decimal::Decimal;

use crate::error::{Error, Result};
use crate::types::{DataType, Range, RangeType, Value};

fn parse_range_string(s: &str, range_type: RangeType) -> Result<Value> {
    let s = s.trim();

    if s == "empty" || s.is_empty() {
        return Ok(Value::range(Range {
            range_type,
            lower: None,
            upper: None,
            lower_inclusive: false,
            upper_inclusive: false,
        }));
    }

    if s.len() < 3 {
        return Err(Error::invalid_query(format!(
            "Invalid range format: '{}'",
            s
        )));
    }

    let lower_inclusive = s.starts_with('[');
    let upper_inclusive = s.ends_with(']');

    let inner = &s[1..s.len() - 1];
    let comma_pos = inner
        .find(',')
        .ok_or_else(|| Error::invalid_query(format!("Invalid range format (no comma): '{}'", s)))?;

    let lower_str = inner[..comma_pos].trim();
    let upper_str = inner[comma_pos + 1..].trim();

    let lower = if lower_str.is_empty() {
        None
    } else {
        Some(parse_range_bound(lower_str, &range_type)?)
    };

    let upper = if upper_str.is_empty() {
        None
    } else {
        Some(parse_range_bound(upper_str, &range_type)?)
    };

    Ok(Value::range(Range {
        range_type,
        lower,
        upper,
        lower_inclusive,
        upper_inclusive,
    }))
}

fn parse_range_bound(s: &str, range_type: &RangeType) -> Result<Value> {
    match range_type {
        RangeType::Int4Range | RangeType::Int8Range => {
            let val: i64 = s
                .parse()
                .map_err(|_| Error::invalid_query(format!("Invalid integer in range: '{}'", s)))?;
            Ok(Value::int64(val))
        }
        RangeType::NumRange => {
            let val: f64 = s
                .parse()
                .map_err(|_| Error::invalid_query(format!("Invalid number in range: '{}'", s)))?;
            Ok(Value::float64(val))
        }
        RangeType::DateRange => {
            use chrono::NaiveDate;
            let date = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                .map_err(|_| Error::invalid_query(format!("Invalid date in range: '{}'", s)))?;
            Ok(Value::date(date))
        }
        RangeType::TsRange | RangeType::TsTzRange => {
            use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
            let s = s.trim();
            let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| {
                    NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                })
                .or_else(|_| {
                    DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%z").map(|dt| dt.naive_utc())
                })
                .or_else(|_| {
                    let s_no_tz =
                        s.trim_end_matches(|c: char| c == '+' || c == '-' || c.is_numeric());
                    NaiveDateTime::parse_from_str(s_no_tz.trim(), "%Y-%m-%d %H:%M:%S")
                })
                .map_err(|_| {
                    Error::invalid_query(format!("Invalid timestamp in range: '{}'", s))
                })?;
            Ok(Value::timestamp(Utc.from_utc_datetime(&dt)))
        }
    }
}

fn apply_numeric_precision(value: Decimal, precision_scale: Option<(u8, u8)>) -> Result<Decimal> {
    let Some((precision, scale)) = precision_scale else {
        return Ok(value);
    };

    if precision == 0 {
        return Err(Error::invalid_query("NUMERIC precision must be at least 1"));
    }
    if scale > precision {
        return Err(Error::invalid_query(format!(
            "NUMERIC scale ({}) cannot exceed precision ({})",
            scale, precision
        )));
    }

    let rounded = value.round_dp(scale.into());

    let abs_value = rounded.abs();

    let integer_part = abs_value.trunc();
    let integer_digits = if integer_part.is_zero() {
        0
    } else {
        let int_str = integer_part.to_string();

        int_str.trim_start_matches('-').len()
    };

    let max_integer_digits = precision.saturating_sub(scale) as usize;

    if integer_digits > max_integer_digits {
        return Err(Error::invalid_query(format!(
            "NUMERIC value {} exceeds precision ({}, {}): has {} integer digits, max {} allowed",
            value, precision, scale, integer_digits, max_integer_digits
        )));
    }

    Ok(rounded)
}

pub struct CoercionRules;

impl CoercionRules {
    pub fn can_implicitly_coerce(from_type: &DataType, to_type: &DataType) -> bool {
        if from_type == to_type {
            return true;
        }

        match (from_type, to_type) {
            (DataType::Int64, DataType::Float64) => true,
            (DataType::Int64, DataType::Numeric(_)) => true,

            (DataType::Float64, DataType::Numeric(_)) => true,
            (DataType::Numeric(_), DataType::Float64) => true,

            (DataType::Numeric(_), DataType::Numeric(_)) => true,

            (DataType::String, DataType::Json) => true,

            (DataType::String, DataType::Time) => true,
            (DataType::String, DataType::Timestamp) => true,
            (DataType::String, DataType::TimestampTz) => true,
            (DataType::String, DataType::Date) => true,
            (DataType::String, DataType::Interval) => true,

            (DataType::String, DataType::MacAddr) => true,
            (DataType::String, DataType::MacAddr8) => true,

            (DataType::Timestamp, DataType::TimestampTz) => true,
            (DataType::TimestampTz, DataType::Timestamp) => true,

            (DataType::String, DataType::Uuid) => true,

            (DataType::String, DataType::Inet) => true,
            (DataType::String, DataType::Cidr) => true,

            (DataType::String, DataType::Hstore) => true,

            (DataType::String, DataType::Enum { .. }) => true,

            (DataType::Array(inner), DataType::Vector(_)) if **inner == DataType::Float64 => true,

            (DataType::String, DataType::Vector(_)) => true,

            (DataType::String, DataType::Range(_)) => true,

            (DataType::Array(from_elem), DataType::Array(to_elem)) => {
                if **to_elem == DataType::Unknown || **from_elem == DataType::Unknown {
                    return true;
                }

                Self::can_implicitly_coerce(from_elem, to_elem)
            }

            (DataType::Struct(_), DataType::Custom(_)) => true,

            (DataType::Custom(from_name), DataType::Custom(to_name)) => from_name == to_name,

            (DataType::Point, DataType::Point) => true,
            (DataType::PgBox, DataType::PgBox) => true,
            (DataType::Circle, DataType::Circle) => true,

            _ => false,
        }
    }

    pub fn find_common_type(types: &[DataType]) -> Result<DataType> {
        if types.is_empty() {
            return Err(Error::invalid_query(
                "Cannot find common type for empty type list",
            ));
        }

        let mut common = types[0].clone();

        for ty in &types[1..] {
            common = Self::get_common_type_pair(&common, ty)?;
        }

        Ok(common)
    }

    fn get_common_type_pair(type1: &DataType, type2: &DataType) -> Result<DataType> {
        if type1 == type2 {
            return Ok(type1.clone());
        }

        match (type1, type2) {
            (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
                Ok(DataType::Float64)
            }
            (DataType::Int64, DataType::Numeric(p)) | (DataType::Numeric(p), DataType::Int64) => {
                Ok(DataType::Numeric(*p))
            }

            (DataType::Float64, DataType::Numeric(p))
            | (DataType::Numeric(p), DataType::Float64) => Ok(DataType::Numeric(*p)),

            (DataType::Numeric(p1), DataType::Numeric(p2)) => match (p1, p2) {
                (None, _) | (_, None) => Ok(DataType::Numeric(None)),
                (Some((prec1, scale1)), Some((prec2, scale2))) => Ok(DataType::Numeric(Some((
                    (*prec1).max(*prec2),
                    (*scale1).max(*scale2),
                )))),
            },

            _ => Err(Error::type_coercion_error(
                type1,
                type2,
                "no implicit coercion path exists",
            )),
        }
    }

    pub fn coerce_value(value: Value, target_type: &DataType) -> Result<Value> {
        if value == Value::null() {
            return Ok(Value::null());
        }

        let source_type = value.data_type();

        if !Self::can_implicitly_coerce(&source_type, target_type) {
            return Err(Error::type_coercion_error(
                &source_type,
                target_type,
                "implicit coercion not allowed",
            ));
        }

        if &source_type == target_type {
            return Ok(value);
        }

        match (&source_type, target_type) {
            (DataType::Int64, DataType::Float64) => {
                if let Some(i) = value.as_i64() {
                    Ok(Value::float64(i as f64))
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::Int64, DataType::Numeric(precision_scale)) => {
                if let Some(i) = value.as_i64() {
                    let decimal = Decimal::from(i);
                    apply_numeric_precision(decimal, *precision_scale).map(Value::numeric)
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::Float64, DataType::Numeric(precision_scale)) => {
                if let Some(f) = value.as_f64() {
                    match Decimal::try_from(f) {
                        Ok(d) => apply_numeric_precision(d, *precision_scale).map(Value::numeric),
                        Err(_) => Err(Error::type_coercion_error(
                            &DataType::Float64,
                            target_type,
                            "float value cannot be represented as NUMERIC",
                        )),
                    }
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::Numeric(_), DataType::Float64) => {
                if let Some(d) = value.as_numeric() {
                    use rust_decimal::prelude::ToPrimitive;
                    d.to_f64()
                        .ok_or_else(|| {
                            Error::type_coercion_error(
                                DataType::Numeric(None),
                                target_type,
                                "NUMERIC value out of range for FLOAT64",
                            )
                        })
                        .map(Value::float64)
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::Numeric(_), DataType::Numeric(precision_scale)) => {
                if let Some(d) = value.as_numeric() {
                    apply_numeric_precision(d, *precision_scale).map(Value::numeric)
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::Json) => {
                if let Some(s) = value.as_str() {
                    match serde_json::from_str(s) {
                        Ok(json_val) => Ok(Value::json(json_val)),
                        Err(e) => Err(Error::invalid_query(format!("Invalid JSON string: {}", e))),
                    }
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::Time) => {
                if let Some(s) = value.as_str() {
                    use chrono::NaiveTime;

                    let time = NaiveTime::parse_from_str(s, "%H:%M:%S")
                        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
                        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M"))
                        .map_err(|e| {
                            Error::invalid_query(format!("Invalid TIME string '{}': {}", s, e))
                        })?;
                    Ok(Value::time(time))
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::Timestamp) => {
                if let Some(s) = value.as_str() {
                    use chrono::{DateTime, NaiveDateTime};

                    let dt = DateTime::parse_from_rfc3339(s)
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .or_else(|_| {
                            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                .or_else(|_| {
                                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                                })
                                .map(|ndt| chrono::Utc.from_utc_datetime(&ndt))
                        })
                        .map_err(|e| {
                            Error::invalid_query(format!("Invalid TIMESTAMP string '{}': {}", s, e))
                        })?;
                    Ok(Value::timestamp(dt))
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::TimestampTz) => {
                if let Some(s) = value.as_str() {
                    use chrono::{DateTime, NaiveDateTime};
                    let dt = DateTime::parse_from_rfc3339(s)
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .or_else(|_| {
                            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                .or_else(|_| {
                                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                                })
                                .map(|ndt| chrono::Utc.from_utc_datetime(&ndt))
                        })
                        .map_err(|e| {
                            Error::invalid_query(format!(
                                "Invalid TIMESTAMPTZ string '{}': {}",
                                s, e
                            ))
                        })?;
                    Ok(Value::timestamp(dt))
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::Date) => {
                if let Some(s) = value.as_str() {
                    use chrono::NaiveDate;
                    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|e| {
                        Error::invalid_query(format!("Invalid DATE string '{}': {}", s, e))
                    })?;
                    Ok(Value::date(date))
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::Interval) => Err(Error::unsupported_feature(
                "STRING to INTERVAL coercion not yet implemented",
            )),

            (DataType::String, DataType::Uuid) => {
                if let Some(s) = value.as_str() {
                    uuid::Uuid::parse_str(s).map(Value::uuid).map_err(|e| {
                        Error::invalid_query(format!("Invalid UUID string '{}': {}", s, e))
                    })
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::Inet) => {
                if let Some(s) = value.as_str() {
                    Value::inet_from_str(s).map_err(|e| {
                        Error::invalid_query(format!("Invalid INET string '{}': {}", s, e))
                    })
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::MacAddr) => {
                if let Some(s) = value.as_str() {
                    use crate::types::MacAddress;
                    MacAddress::parse(s, false)
                        .map(Value::macaddr)
                        .ok_or_else(|| {
                            Error::invalid_query(format!("Invalid MACADDR string '{}'", s))
                        })
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::Cidr) => {
                if let Some(s) = value.as_str() {
                    Value::cidr_from_str(s).map_err(|e| {
                        Error::invalid_query(format!("Invalid CIDR string '{}': {}", s, e))
                    })
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::Hstore) => {
                if let Some(s) = value.as_str() {
                    Value::hstore_from_str(s).map_err(|e| {
                        Error::invalid_query(format!("Invalid HSTORE string '{}': {}", s, e))
                    })
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::MacAddr8) => {
                if let Some(s) = value.as_str() {
                    use crate::types::MacAddress;

                    MacAddress::parse(s, true)
                        .or_else(|| MacAddress::parse(s, false).map(|mac| mac.to_eui64()))
                        .map(Value::macaddr8)
                        .ok_or_else(|| {
                            Error::invalid_query(format!("Invalid MACADDR8 string '{}'", s))
                        })
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::Enum { type_name, labels }) => {
                if let Some(str_val) = value.as_str() {
                    if labels.contains(&str_val.to_string()) {
                        Ok(value)
                    } else {
                        Err(Error::InvalidQuery(format!(
                            "invalid input value for enum {}: \"{}\"",
                            type_name, str_val
                        )))
                    }
                } else if value.is_null() {
                    Ok(value)
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "expected string value for enum",
                    ))
                }
            }

            (DataType::Timestamp, DataType::TimestampTz) => Ok(value),

            (DataType::TimestampTz, DataType::Timestamp) => Ok(value),

            (DataType::Array(_), DataType::Vector(_)) => Ok(value),

            (DataType::String, DataType::Vector(_)) => {
                if let Some(s) = value.as_str() {
                    let s = s.trim();
                    let inner = if s.starts_with('[') && s.ends_with(']') {
                        &s[1..s.len() - 1]
                    } else {
                        s
                    };
                    let values: std::result::Result<Vec<f64>, _> = inner
                        .split(',')
                        .map(|part| part.trim().parse::<f64>())
                        .collect();
                    match values {
                        Ok(v) => Ok(Value::vector(v)),
                        Err(_) => Err(Error::invalid_query(format!(
                            "Invalid VECTOR string '{}'",
                            s
                        ))),
                    }
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::String, DataType::Range(range_type)) => {
                if let Some(s) = value.as_str() {
                    parse_range_string(s, range_type.clone())
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value extraction failed",
                    ))
                }
            }

            (DataType::Array(_from_elem), DataType::Array(to_elem)) => {
                if let Some(arr) = value.as_array() {
                    let coerced_elements: Result<Vec<Value>> = arr
                        .iter()
                        .map(|elem| Self::coerce_value(elem.clone(), to_elem))
                        .collect();
                    Ok(Value::array(coerced_elements?))
                } else {
                    Err(Error::type_coercion_error(
                        &source_type,
                        target_type,
                        "value is not an array",
                    ))
                }
            }

            (DataType::Struct(_), DataType::Custom(_)) => Ok(value),

            (DataType::Custom(_), DataType::Custom(_)) => Ok(value),

            _ => Err(Error::type_coercion_error(
                &source_type,
                target_type,
                "unexpected coercion path",
            )),
        }
    }

    pub fn coerce_values_to_common_type(values: &[Value]) -> Result<(DataType, Vec<Value>)> {
        if values.is_empty() {
            return Ok((DataType::Int64, vec![]));
        }

        let types: Vec<DataType> = values
            .iter()
            .filter(|v| **v != Value::null())
            .map(|v| v.data_type())
            .collect();

        if types.is_empty() {
            return Ok((DataType::Int64, values.to_vec()));
        }

        let common_type = Self::find_common_type(&types)?;

        let coerced_values: Result<Vec<Value>> = values
            .iter()
            .map(|v| Self::coerce_value(v.clone(), &common_type))
            .collect();

        Ok((common_type, coerced_values?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_implicitly_coerce_same_type() {
        assert!(CoercionRules::can_implicitly_coerce(
            &DataType::Int64,
            &DataType::Int64
        ));
        assert!(CoercionRules::can_implicitly_coerce(
            &DataType::Float64,
            &DataType::Float64
        ));
    }

    #[test]
    fn test_can_implicitly_coerce_int64_to_float64() {
        assert!(CoercionRules::can_implicitly_coerce(
            &DataType::Int64,
            &DataType::Float64
        ));
    }

    #[test]
    fn test_cannot_implicitly_coerce_float64_to_int64() {
        assert!(!CoercionRules::can_implicitly_coerce(
            &DataType::Float64,
            &DataType::Int64
        ));
    }

    #[test]
    fn test_can_implicitly_coerce_int64_to_numeric() {
        assert!(CoercionRules::can_implicitly_coerce(
            &DataType::Int64,
            &DataType::Numeric(None)
        ));
    }

    #[test]
    fn test_can_implicitly_coerce_float64_to_numeric() {
        assert!(CoercionRules::can_implicitly_coerce(
            &DataType::Float64,
            &DataType::Numeric(None)
        ));
    }

    #[test]
    fn test_find_common_type_int64_float64() {
        let types = vec![DataType::Int64, DataType::Float64];
        assert_eq!(
            CoercionRules::find_common_type(&types).unwrap(),
            DataType::Float64
        );
    }

    #[test]
    fn test_find_common_type_int64_numeric() {
        let types = vec![DataType::Int64, DataType::Numeric(None)];
        assert_eq!(
            CoercionRules::find_common_type(&types).unwrap(),
            DataType::Numeric(None)
        );
    }

    #[test]
    fn test_find_common_type_mixed() {
        let types = vec![DataType::Int64, DataType::Float64, DataType::Numeric(None)];
        assert_eq!(
            CoercionRules::find_common_type(&types).unwrap(),
            DataType::Numeric(None)
        );
    }

    #[test]
    fn test_coerce_value_int64_to_float64() {
        let value = Value::int64(42);
        let result = CoercionRules::coerce_value(value, &DataType::Float64).unwrap();
        assert_eq!(result, Value::float64(42.0));
    }

    #[test]
    fn test_coerce_value_int64_to_numeric() {
        let value = Value::int64(42);
        let result = CoercionRules::coerce_value(value, &DataType::Numeric(None)).unwrap();
        assert_eq!(result, Value::numeric(Decimal::from(42)));
    }

    #[test]
    fn test_coerce_value_null() {
        let value = Value::null();
        let result = CoercionRules::coerce_value(value, &DataType::Float64).unwrap();
        assert_eq!(result, Value::null());
    }

    #[test]
    fn test_coerce_value_invalid() {
        #[allow(clippy::approx_constant)]
        let value = Value::float64(3.14);
        let result = CoercionRules::coerce_value(value, &DataType::Int64);
        assert!(result.is_err());
    }

    #[test]
    fn test_can_implicitly_coerce_array_float64_to_vector() {
        let array_type = DataType::Array(Box::new(DataType::Float64));
        let vector_type = DataType::Vector(3);
        assert!(CoercionRules::can_implicitly_coerce(
            &array_type,
            &vector_type
        ));
    }

    #[test]
    fn test_coerce_value_array_to_vector() {
        let value = Value::array(vec![Value::float64(1.0), Value::float64(2.0)]);
        let result = CoercionRules::coerce_value(value, &DataType::Vector(2)).unwrap();
        assert_eq!(
            result,
            Value::array(vec![Value::float64(1.0), Value::float64(2.0)])
        );
    }

    #[test]
    fn test_can_implicitly_coerce_point_to_point() {
        assert!(CoercionRules::can_implicitly_coerce(
            &DataType::Point,
            &DataType::Point
        ));
    }

    #[test]
    fn test_coerce_value_point_to_point() {
        use crate::types::PgPoint;
        let point = PgPoint::new(1.0, 2.0);
        let value = Value::point(point.clone());
        let result = CoercionRules::coerce_value(value.clone(), &DataType::Point).unwrap();
        assert_eq!(result, value);
    }
}
