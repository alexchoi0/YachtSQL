use std::rc::Rc;
use std::sync::LazyLock;

use uuid::Uuid;
use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

static SERVER_UUID: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);

pub(super) fn register(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "GENERATEUUIDV4".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "GENERATEUUIDV4".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: false,
            evaluator: |_| {
                let uuid = Uuid::new_v4();
                Ok(Value::string(uuid.to_string()))
            },
        }),
    );

    registry.register_scalar(
        "TOUUID".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOUUID".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("toUUID requires 1 argument"));
                }
                let s = args[0]
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("toUUID: argument must be a string"))?;
                let uuid = Uuid::parse_str(s).map_err(|_| {
                    Error::invalid_query(format!("toUUID: invalid UUID string: {}", s))
                })?;
                Ok(Value::string(uuid.to_string()))
            },
        }),
    );

    registry.register_scalar(
        "TOUUIDORNULL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOUUIDORNULL".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("toUUIDOrNull requires 1 argument"));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| {
                    Error::invalid_query("toUUIDOrNull: argument must be a string")
                })?;
                match Uuid::parse_str(s) {
                    Ok(uuid) => Ok(Value::string(uuid.to_string())),
                    Err(_) => Ok(Value::null()),
                }
            },
        }),
    );

    registry.register_scalar(
        "TOUUIDORZERO".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOUUIDORZERO".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("toUUIDOrZero requires 1 argument"));
                }
                if args[0].is_null() {
                    return Ok(Value::string(
                        "00000000-0000-0000-0000-000000000000".to_string(),
                    ));
                }
                let s = args[0].as_str().ok_or_else(|| {
                    Error::invalid_query("toUUIDOrZero: argument must be a string")
                })?;
                match Uuid::parse_str(s) {
                    Ok(uuid) => Ok(Value::string(uuid.to_string())),
                    Err(_) => Ok(Value::string(
                        "00000000-0000-0000-0000-000000000000".to_string(),
                    )),
                }
            },
        }),
    );

    registry.register_scalar(
        "UUIDSTRINGTONUM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "UUIDSTRINGTONUM".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Bytes,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("UUIDStringToNum requires 1 argument"));
                }
                let s = args[0].as_str().ok_or_else(|| {
                    Error::invalid_query("UUIDStringToNum: argument must be a string")
                })?;
                let uuid = Uuid::parse_str(s).map_err(|_| {
                    Error::invalid_query(format!("UUIDStringToNum: invalid UUID string: {}", s))
                })?;
                Ok(Value::bytes(uuid.as_bytes().to_vec()))
            },
        }),
    );

    registry.register_scalar(
        "UUIDNUMTOSTRING".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "UUIDNUMTOSTRING".to_string(),
            arg_types: vec![DataType::Bytes],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("UUIDNumToString requires 1 argument"));
                }
                let bytes = args[0].as_bytes().ok_or_else(|| {
                    Error::invalid_query("UUIDNumToString: argument must be bytes")
                })?;
                if bytes.len() != 16 {
                    return Err(Error::invalid_query(
                        "UUIDNumToString: bytes must be exactly 16 bytes",
                    ));
                }
                let uuid_bytes: [u8; 16] = bytes
                    .try_into()
                    .map_err(|_| Error::invalid_query("UUIDNumToString: invalid bytes"))?;
                let uuid = Uuid::from_bytes(uuid_bytes);
                Ok(Value::string(uuid.to_string()))
            },
        }),
    );

    registry.register_scalar(
        "SERVERUUID".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SERVERUUID".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: false,
            evaluator: |_| Ok(Value::string(SERVER_UUID.to_string())),
        }),
    );

    registry.register_scalar(
        "EMPTY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "EMPTY".to_string(),
            arg_types: vec![DataType::Unknown],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("empty requires 1 argument"));
                }
                if args[0].is_null() {
                    return Ok(Value::int64(1));
                }
                if let Some(s) = args[0].as_str() {
                    if s == "00000000-0000-0000-0000-000000000000" {
                        return Ok(Value::int64(1));
                    }
                    return Ok(Value::int64(if s.is_empty() { 1 } else { 0 }));
                }
                if let Some(arr) = args[0].as_array() {
                    return Ok(Value::int64(if arr.is_empty() { 1 } else { 0 }));
                }
                Ok(Value::int64(0))
            },
        }),
    );

    registry.register_scalar(
        "NOTEMPTY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NOTEMPTY".to_string(),
            arg_types: vec![DataType::Unknown],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("notEmpty requires 1 argument"));
                }
                if args[0].is_null() {
                    return Ok(Value::int64(0));
                }
                if let Some(s) = args[0].as_str() {
                    if s == "00000000-0000-0000-0000-000000000000" {
                        return Ok(Value::int64(0));
                    }
                    return Ok(Value::int64(if s.is_empty() { 0 } else { 1 }));
                }
                if let Some(arr) = args[0].as_array() {
                    return Ok(Value::int64(if arr.is_empty() { 0 } else { 1 }));
                }
                Ok(Value::int64(1))
            },
        }),
    );
}
