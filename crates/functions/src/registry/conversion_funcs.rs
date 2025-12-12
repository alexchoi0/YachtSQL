use std::rc::Rc;

use rust_decimal::prelude::ToPrimitive;
use yachtsql_common::error::Error;
use yachtsql_common::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_cast_functions(registry);
    register_oracle_conversions(registry);
    register_json_conversions(registry);
}

fn register_cast_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "CAST".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CAST".to_string(),
            arg_types: vec![],
            return_type: DataType::Unknown,
            variadic: true,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "CAST requires exactly 2 arguments: value and target type".to_string(),
                    ));
                }

                if let Some(type_name) = args[1].as_str() {
                    cast_value(&args[0], type_name)
                } else {
                    Err(Error::invalid_query(
                        "CAST target type must be a string".to_string(),
                    ))
                }
            },
        }),
    );

    registry.register_scalar(
        "TRY_CAST".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TRY_CAST".to_string(),
            arg_types: vec![],
            return_type: DataType::Unknown,
            variadic: true,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "TRY_CAST requires exactly 2 arguments: value and target type".to_string(),
                    ));
                }

                if let Some(type_name) = args[1].as_str() {
                    match cast_value(&args[0], type_name) {
                        Ok(val) => Ok(val),
                        Err(_) => Ok(Value::null()),
                    }
                } else {
                    Ok(Value::null())
                }
            },
        }),
    );

    registry.register_scalar(
        "CONVERT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CONVERT".to_string(),
            arg_types: vec![],
            return_type: DataType::Unknown,
            variadic: true,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "CONVERT requires exactly 2 arguments: target type and value".to_string(),
                    ));
                }

                if let Some(type_name) = args[0].as_str() {
                    cast_value(&args[1], type_name)
                } else {
                    Err(Error::invalid_query(
                        "CONVERT target type must be a string".to_string(),
                    ))
                }
            },
        }),
    );
}

fn register_oracle_conversions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TO_CHAR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_CHAR".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::invalid_query(
                        "TO_CHAR requires 1 or 2 arguments".to_string(),
                    ));
                }

                let format = if args.len() == 2 {
                    if args[1].is_null() {
                        return Ok(Value::null());
                    } else if let Some(f) = args[1].as_str() {
                        Some(f)
                    } else {
                        return Err(Error::TypeMismatch {
                            expected: "STRING".to_string(),
                            actual: args[1].data_type().to_string(),
                        });
                    }
                } else {
                    None
                };

                to_char(&args[0], format)
            },
        }),
    );

    registry.register_scalar(
        "TO_NUMBER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_NUMBER".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(s) = args[0].as_str() {
                    let cleaned = s.trim().replace(',', "");
                    cleaned
                        .parse::<f64>()
                        .map(Value::float64)
                        .or_else(|_| cleaned.parse::<i64>().map(Value::int64))
                        .map_err(|_| {
                            Error::invalid_query(format!("Cannot convert '{}' to number", s))
                        })
                } else if let Some(i) = args[0].as_i64() {
                    Ok(Value::int64(i))
                } else if let Some(f) = args[0].as_f64() {
                    Ok(Value::float64(f))
                } else if let Some(d) = args[0].as_numeric() {
                    Ok(Value::numeric(d))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING or NUMERIC".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "TO_DATE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_DATE".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Date,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::invalid_query(
                        "TO_DATE requires 1 or 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(s) = args[0].as_str() {
                    let format = if args.len() == 2 {
                        if args[1].is_null() {
                            return Ok(Value::null());
                        } else if let Some(f) = args[1].as_str() {
                            f
                        } else {
                            return Err(Error::TypeMismatch {
                                expected: "STRING".to_string(),
                                actual: args[1].data_type().to_string(),
                            });
                        }
                    } else {
                        "%Y-%m-%d"
                    };

                    chrono::NaiveDate::parse_from_str(s, format)
                        .map(Value::date)
                        .map_err(|e| {
                            Error::invalid_query(format!(
                                "Failed to parse date '{}' with format '{}': {}",
                                s, format, e
                            ))
                        })
                } else if let Some(d) = args[0].as_date() {
                    Ok(Value::date(d))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING or DATE".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );
}

fn register_json_conversions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "PARSE_JSON".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "PARSE_JSON".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Json,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(s) = args[0].as_str() {
                    serde_json::from_str(s)
                        .map(Value::json)
                        .map_err(|e| Error::invalid_query(format!("Invalid JSON: {}", e)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "TO_JSON_STRING".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_JSON_STRING".to_string(),
            arg_types: vec![DataType::Json],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(j) = args[0].as_json() {
                    Ok(Value::string(j.to_string()))
                } else if let Some(s) = args[0].as_str() {
                    Ok(Value::string(s.to_string()))
                } else {
                    let json_val = value_to_json(&args[0])?;
                    Ok(Value::string(json_val.to_string()))
                }
            },
        }),
    );
}

fn cast_value(value: &Value, target_type: &str) -> yachtsql_common::error::Result<Value> {
    let target = target_type.to_uppercase();

    if value.is_null() {
        return Ok(Value::null());
    }

    match target.as_str() {
        "STRING" | "VARCHAR" | "TEXT" | "CHAR" => to_char(value, None),
        "INT" | "INT64" | "INTEGER" | "BIGINT" => to_int64(value),
        "FLOAT" | "FLOAT64" | "DOUBLE" | "REAL" => to_float64(value),
        "BOOL" | "BOOLEAN" => to_bool(value),
        "DATE" => to_date(value),
        "TIMESTAMP" => to_timestamp(value),
        "JSON" => to_json(value),
        _ => Err(Error::invalid_query(format!(
            "Unsupported cast target type: {}",
            target_type
        ))),
    }
}

fn to_char(value: &Value, _format: Option<&str>) -> yachtsql_common::error::Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(s) = value.as_str() {
        return Ok(Value::string(s.to_string()));
    }
    if let Some(i) = value.as_i64() {
        return Ok(Value::string(i.to_string()));
    }
    if let Some(f) = value.as_f64() {
        return Ok(Value::string(f.to_string()));
    }
    if let Some(d) = value.as_numeric() {
        return Ok(Value::string(d.to_string()));
    }
    if let Some(b) = value.as_bool() {
        return Ok(Value::string(b.to_string()));
    }
    if let Some(d) = value.as_date() {
        return Ok(Value::string(d.to_string()));
    }
    if let Some(ts) = value.as_timestamp() {
        return Ok(Value::string(ts.to_rfc3339()));
    }

    if let Some(j) = value.as_json() {
        return Ok(Value::string(j.to_string()));
    }
    if let Some(u) = value.as_uuid() {
        return Ok(Value::string(u.hyphenated().to_string()));
    }

    Err(Error::TypeMismatch {
        expected: "convertible type".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn to_int64(value: &Value) -> yachtsql_common::error::Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(i) = value.as_i64() {
        return Ok(Value::int64(i));
    }
    if let Some(f) = value.as_f64() {
        return Ok(Value::int64(f as i64));
    }
    if let Some(d) = value.as_numeric() {
        return d.to_i64().map(Value::int64).ok_or_else(|| {
            Error::invalid_query("Numeric value out of range for INT64".to_string())
        });
    }
    if let Some(b) = value.as_bool() {
        return Ok(Value::int64(if b { 1 } else { 0 }));
    }
    if let Some(s) = value.as_str() {
        return s
            .trim()
            .parse::<i64>()
            .map(Value::int64)
            .map_err(|_| Error::invalid_query(format!("Cannot convert '{}' to INT64", s)));
    }

    Err(Error::TypeMismatch {
        expected: "numeric or string".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn to_float64(value: &Value) -> yachtsql_common::error::Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(f) = value.as_f64() {
        return Ok(Value::float64(f));
    }
    if let Some(i) = value.as_i64() {
        return Ok(Value::float64(i as f64));
    }
    if let Some(d) = value.as_numeric() {
        return d.to_f64().map(Value::float64).ok_or_else(|| {
            Error::invalid_query("Numeric value out of range for FLOAT64".to_string())
        });
    }
    if let Some(b) = value.as_bool() {
        return Ok(Value::float64(if b { 1.0 } else { 0.0 }));
    }
    if let Some(s) = value.as_str() {
        return s
            .trim()
            .parse::<f64>()
            .map(Value::float64)
            .map_err(|_| Error::invalid_query(format!("Cannot convert '{}' to FLOAT64", s)));
    }

    Err(Error::TypeMismatch {
        expected: "numeric or string".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn to_bool(value: &Value) -> yachtsql_common::error::Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(b) = value.as_bool() {
        return Ok(Value::bool_val(b));
    }
    if let Some(i) = value.as_i64() {
        return Ok(Value::bool_val(i != 0));
    }
    if let Some(f) = value.as_f64() {
        return Ok(Value::bool_val(f != 0.0));
    }
    if let Some(s) = value.as_str() {
        let lower = s.to_lowercase();
        return match lower.as_str() {
            "true" | "t" | "yes" | "y" | "1" => Ok(Value::bool_val(true)),
            "false" | "f" | "no" | "n" | "0" => Ok(Value::bool_val(false)),
            _ => Err(Error::invalid_query(format!(
                "Cannot convert '{}' to BOOLEAN",
                s
            ))),
        };
    }

    Err(Error::TypeMismatch {
        expected: "boolean, numeric, or string".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn to_date(value: &Value) -> yachtsql_common::error::Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(d) = value.as_date() {
        return Ok(Value::date(d));
    }
    if let Some(ts) = value.as_timestamp() {
        return Ok(Value::date(ts.date_naive()));
    }
    if let Some(s) = value.as_str() {
        return chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map(Value::date)
            .map_err(|e| Error::invalid_query(format!("Failed to parse date '{}': {}", s, e)));
    }

    Err(Error::TypeMismatch {
        expected: "date, timestamp, or string".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn to_timestamp(value: &Value) -> yachtsql_common::error::Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(ts) = value.as_timestamp() {
        return Ok(Value::timestamp(ts));
    }
    if let Some(d) = value.as_date() {
        let dt = d.and_hms_opt(0, 0, 0).ok_or_else(|| {
            Error::invalid_query("Failed to convert date to timestamp".to_string())
        })?;
        return Ok(Value::timestamp(
            chrono::DateTime::from_naive_utc_and_offset(dt, chrono::Utc),
        ));
    }
    if let Some(s) = value.as_str() {
        return s
            .parse::<chrono::DateTime<chrono::Utc>>()
            .map(Value::timestamp)
            .map_err(|e| {
                Error::invalid_query(format!("Failed to parse timestamp '{}': {}", s, e))
            });
    }
    if let Some(i) = value.as_i64() {
        return chrono::DateTime::from_timestamp(i, 0)
            .map(Value::timestamp)
            .ok_or_else(|| Error::invalid_query("Invalid timestamp value".to_string()));
    }

    Err(Error::TypeMismatch {
        expected: "timestamp, date, string, or int64".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn to_json(value: &Value) -> yachtsql_common::error::Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(j) = value.as_json() {
        return Ok(Value::json(j.clone()));
    }
    if let Some(s) = value.as_str() {
        return serde_json::from_str(s)
            .map(Value::json)
            .map_err(|e| Error::invalid_query(format!("Invalid JSON: {}", e)));
    }

    let json_val = value_to_json(value)?;
    Ok(Value::json(json_val))
}

fn value_to_json(value: &Value) -> yachtsql_common::error::Result<serde_json::Value> {
    if value.is_null() {
        return Ok(serde_json::Value::Null);
    }

    if let Some(b) = value.as_bool() {
        return Ok(serde_json::Value::Bool(b));
    }
    if let Some(i) = value.as_i64() {
        return Ok(serde_json::Value::Number(i.into()));
    }
    if let Some(f) = value.as_f64() {
        return serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .ok_or_else(|| Error::invalid_query("Invalid float value for JSON".to_string()));
    }
    if let Some(s) = value.as_str() {
        return Ok(serde_json::Value::String(s.to_string()));
    }

    if let Some(j) = value.as_json() {
        return Ok(j.clone());
    }
    if let Some(arr) = value.as_array() {
        let json_arr: Result<Vec<_>, _> = arr.iter().map(value_to_json).collect();
        return Ok(serde_json::Value::Array(json_arr?));
    }

    Err(Error::invalid_query(format!(
        "Cannot convert {} to JSON",
        value.data_type()
    )))
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

    #[test]
    fn test_cast_to_int() {
        assert_eq!(
            cast_value(&Value::string("123".to_string()), "INT64").unwrap(),
            Value::int64(123)
        );
        assert_eq!(
            cast_value(&Value::float64(45.7), "INT64").unwrap(),
            Value::int64(45)
        );
        assert_eq!(
            cast_value(&Value::bool_val(true), "INT").unwrap(),
            Value::int64(1)
        );
        assert_eq!(
            cast_value(&Value::bool_val(false), "INT").unwrap(),
            Value::int64(0)
        );
    }

    #[test]
    fn test_cast_to_string() {
        assert_eq!(
            cast_value(&Value::int64(123), "STRING").unwrap(),
            Value::string("123".to_string())
        );
        assert_eq!(
            cast_value(&Value::float64(3.14), "VARCHAR").unwrap(),
            Value::string("3.14".to_string())
        );
        assert_eq!(
            cast_value(&Value::bool_val(true), "TEXT").unwrap(),
            Value::string("true".to_string())
        );
    }

    #[test]
    fn test_cast_to_float() {
        assert_eq!(
            cast_value(&Value::string("3.14".to_string()), "FLOAT64").unwrap(),
            Value::float64(3.14)
        );
        assert_eq!(
            cast_value(&Value::int64(42), "DOUBLE").unwrap(),
            Value::float64(42.0)
        );
    }

    #[test]
    fn test_cast_to_bool() {
        assert_eq!(
            cast_value(&Value::int64(1), "BOOL").unwrap(),
            Value::bool_val(true)
        );
        assert_eq!(
            cast_value(&Value::int64(0), "BOOLEAN").unwrap(),
            Value::bool_val(false)
        );
        assert_eq!(
            cast_value(&Value::string("true".to_string()), "BOOL").unwrap(),
            Value::bool_val(true)
        );
        assert_eq!(
            cast_value(&Value::string("false".to_string()), "BOOL").unwrap(),
            Value::bool_val(false)
        );
    }

    #[test]
    fn test_cast_null() {
        assert_eq!(cast_value(&Value::null(), "STRING").unwrap(), Value::null());
        assert_eq!(cast_value(&Value::null(), "INT64").unwrap(), Value::null());
        assert_eq!(
            cast_value(&Value::null(), "FLOAT64").unwrap(),
            Value::null()
        );
        assert_eq!(cast_value(&Value::null(), "BOOL").unwrap(), Value::null());
    }

    #[test]
    fn test_cast_invalid_type() {
        let result = cast_value(&Value::int64(42), "INVALID_TYPE");
        assert!(result.is_err());
    }

    #[test]
    fn test_to_number() {
        let result = to_int64(&Value::string("123".to_string())).unwrap();
        assert_eq!(result, Value::int64(123));

        let result = to_float64(&Value::string("3.14".to_string())).unwrap();
        assert_eq!(result, Value::float64(3.14));
    }

    #[test]
    fn test_to_char() {
        assert_eq!(
            to_char(&Value::int64(42), None).unwrap(),
            Value::string("42".to_string())
        );
        assert_eq!(
            to_char(&Value::float64(3.14), None).unwrap(),
            Value::string("3.14".to_string())
        );
        assert_eq!(to_char(&Value::null(), None).unwrap(), Value::null());
    }

    #[test]
    fn test_value_to_json() {
        let result = value_to_json(&Value::int64(42)).unwrap();
        assert_eq!(result, serde_json::Value::Number(42.into()));

        let result = value_to_json(&Value::string("test".to_string())).unwrap();
        assert_eq!(result, serde_json::Value::String("test".to_string()));

        let result = value_to_json(&Value::null()).unwrap();
        assert_eq!(result, serde_json::Value::Null);

        let result = value_to_json(&Value::bool_val(true)).unwrap();
        assert_eq!(result, serde_json::Value::Bool(true));
    }
}
