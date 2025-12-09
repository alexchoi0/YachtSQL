use std::rc::Rc;

use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ROUNDBANKERS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ROUNDBANKERS".to_string(),
            arg_types: vec![DataType::Float64],
            return_type: DataType::Float64,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::invalid_query(
                        "roundBankers requires 1 or 2 arguments",
                    ));
                }
                let value = extract_f64(&args[0])?;
                let decimals = if args.len() == 2 {
                    extract_i64(&args[1])?
                } else {
                    0
                };
                let rounded = bankers_round(value, decimals as i32);
                Ok(Value::float64(rounded))
            },
        }),
    );

    registry.register_scalar(
        "ROUNDDOWN".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ROUNDDOWN".to_string(),
            arg_types: vec![
                DataType::Float64,
                DataType::Array(Box::new(DataType::Float64)),
            ],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query("roundDown requires 2 arguments"));
                }
                let value = extract_f64(&args[0])?;
                let thresholds = args[1].as_array().ok_or_else(|| {
                    Error::invalid_query("roundDown: second argument must be an array")
                })?;

                let mut sorted_thresholds: Vec<f64> = thresholds
                    .iter()
                    .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                    .collect();
                sorted_thresholds
                    .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

                let result = sorted_thresholds
                    .iter()
                    .rev()
                    .find(|&&t| t <= value)
                    .copied()
                    .unwrap_or(sorted_thresholds.first().copied().unwrap_or(0.0));

                Ok(Value::float64(result))
            },
        }),
    );

    registry.register_scalar(
        "ROUNDTOEXP2".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ROUNDTOEXP2".to_string(),
            arg_types: vec![DataType::Float64],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("roundToExp2 requires 1 argument"));
                }
                let value = extract_f64(&args[0])?;
                if value <= 0.0 {
                    return Ok(Value::int64(0));
                }
                let exp = (value.log2().floor()) as u32;
                let result = 1i64 << exp;
                Ok(Value::int64(result))
            },
        }),
    );

    registry.register_scalar(
        "ROUNDDURATION".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ROUNDDURATION".to_string(),
            arg_types: vec![DataType::Float64],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("roundDuration requires 1 argument"));
                }
                let value = extract_f64(&args[0])? as i64;
                let thresholds = [
                    0, 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000,
                    36000, 86400, 172800, 604800, 2592000, 31536000,
                ];
                let result = thresholds
                    .iter()
                    .rev()
                    .find(|&&t| t <= value)
                    .copied()
                    .unwrap_or(0);
                Ok(Value::int64(result))
            },
        }),
    );

    registry.register_scalar(
        "ROUNDAGE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ROUNDAGE".to_string(),
            arg_types: vec![DataType::Float64],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("roundAge requires 1 argument"));
                }
                let age = extract_f64(&args[0])? as i64;
                let thresholds = [0, 1, 18, 25, 35, 45, 55];
                let result = thresholds
                    .iter()
                    .rev()
                    .find(|&&t| t <= age)
                    .copied()
                    .unwrap_or(0);
                Ok(Value::int64(result))
            },
        }),
    );
}

fn extract_f64(value: &Value) -> Result<f64, Error> {
    if value.is_null() {
        return Err(Error::invalid_query("Expected numeric value, got NULL"));
    }
    if let Some(f) = value.as_f64() {
        return Ok(f);
    }
    if let Some(i) = value.as_i64() {
        return Ok(i as f64);
    }
    if let Some(n) = value.as_numeric() {
        return n
            .to_string()
            .parse()
            .map_err(|_| Error::invalid_query("Failed to parse numeric"));
    }
    Err(Error::TypeMismatch {
        expected: "FLOAT64".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn extract_i64(value: &Value) -> Result<i64, Error> {
    if value.is_null() {
        return Err(Error::invalid_query("Expected integer value, got NULL"));
    }
    if let Some(i) = value.as_i64() {
        return Ok(i);
    }
    if let Some(f) = value.as_f64() {
        return Ok(f as i64);
    }
    Err(Error::TypeMismatch {
        expected: "INT64".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn bankers_round(value: f64, decimals: i32) -> f64 {
    let multiplier = 10_f64.powi(decimals);
    let scaled = value * multiplier;
    let floor_scaled = scaled.floor();
    let frac = scaled - floor_scaled;

    let rounded = if (frac - 0.5).abs() < 1e-10 {
        if floor_scaled as i64 % 2 == 0 {
            floor_scaled
        } else {
            floor_scaled + 1.0
        }
    } else {
        scaled.round()
    };

    rounded / multiplier
}
