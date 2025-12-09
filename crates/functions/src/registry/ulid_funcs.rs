use std::rc::Rc;

use chrono::DateTime;
use ulid::Ulid;
use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "GENERATEULID".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "GENERATEULID".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: false,
            evaluator: |_| {
                let ulid = Ulid::new();
                Ok(Value::string(ulid.to_string()))
            },
        }),
    );

    registry.register_scalar(
        "ULIDSTRINGTODATETIME".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ULIDSTRINGTODATETIME".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Timestamp,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::invalid_query(
                        "ULIDStringToDateTime requires 1 or 2 arguments",
                    ));
                }
                let s = args[0].as_str().ok_or_else(|| {
                    Error::invalid_query("ULIDStringToDateTime: argument must be a string")
                })?;
                let ulid = Ulid::from_string(s).map_err(|_| {
                    Error::invalid_query(format!(
                        "ULIDStringToDateTime: invalid ULID string: {}",
                        s
                    ))
                })?;
                let timestamp_ms = ulid.timestamp_ms();
                let secs = (timestamp_ms / 1000) as i64;
                let nsecs = ((timestamp_ms % 1000) * 1_000_000) as u32;
                let dt = DateTime::from_timestamp(secs, nsecs).ok_or_else(|| {
                    Error::invalid_query("ULIDStringToDateTime: invalid timestamp")
                })?;
                Ok(Value::timestamp(dt))
            },
        }),
    );

    registry.register_scalar(
        "ULIDTODATETIME".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ULIDTODATETIME".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Timestamp,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("ULIDToDateTime requires 1 argument"));
                }
                let s = args[0].as_str().ok_or_else(|| {
                    Error::invalid_query("ULIDToDateTime: argument must be a string")
                })?;
                let ulid = Ulid::from_string(s).map_err(|_| {
                    Error::invalid_query(format!("ULIDToDateTime: invalid ULID string: {}", s))
                })?;
                let timestamp_ms = ulid.timestamp_ms();
                let secs = (timestamp_ms / 1000) as i64;
                let nsecs = ((timestamp_ms % 1000) * 1_000_000) as u32;
                let dt = DateTime::from_timestamp(secs, nsecs)
                    .ok_or_else(|| Error::invalid_query("ULIDToDateTime: invalid timestamp"))?;
                Ok(Value::timestamp(dt))
            },
        }),
    );

    registry.register_scalar(
        "TOULID".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOULID".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("toULID requires 1 argument"));
                }
                let s = args[0]
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("toULID: argument must be a string"))?;
                let ulid = Ulid::from_string(s).map_err(|_| {
                    Error::invalid_query(format!("toULID: invalid ULID string: {}", s))
                })?;
                Ok(Value::string(ulid.to_string()))
            },
        }),
    );
}
