use std::rc::Rc;

use regex::Regex;
use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_regex_contains(registry);
    register_regex_replace(registry);
    register_regex_extract(registry);
    register_regex_extract_all(registry);
}

fn register_regex_contains(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "REGEXP_CONTAINS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "REGEXP_CONTAINS".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(text), Some(pattern)) = (args[0].as_str(), args[1].as_str()) {
                    let regex = Regex::new(pattern).map_err(|e| {
                        Error::invalid_query(format!("Invalid regex pattern: {}", e))
                    })?;
                    Ok(Value::bool_val(regex.is_match(text)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    })
                }
            },
        }),
    );
}

fn register_regex_replace(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "REGEXP_REPLACE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "REGEXP_REPLACE".to_string(),
            arg_types: vec![DataType::String, DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() || args[2].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(text), Some(pattern), Some(replacement)) =
                    (args[0].as_str(), args[1].as_str(), args[2].as_str())
                {
                    let regex = Regex::new(pattern).map_err(|e| {
                        Error::invalid_query(format!("Invalid regex pattern: {}", e))
                    })?;
                    Ok(Value::string(
                        regex.replace_all(text, replacement).to_string(),
                    ))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING, STRING, STRING".to_string(),
                        actual: format!(
                            "{}, {}, {}",
                            args[0].data_type(),
                            args[1].data_type(),
                            args[2].data_type()
                        ),
                    })
                }
            },
        }),
    );
}

fn register_regex_extract(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "REGEXP_EXTRACT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "REGEXP_EXTRACT".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(text), Some(pattern)) = (args[0].as_str(), args[1].as_str()) {
                    let regex = Regex::new(pattern).map_err(|e| {
                        Error::invalid_query(format!("Invalid regex pattern: {}", e))
                    })?;

                    if let Some(caps) = regex.captures(text) {
                        let match_str = if caps.len() > 1 {
                            caps.get(1)
                                .ok_or_else(|| Error::internal("Regex capture group 1 missing"))?
                                .as_str()
                        } else {
                            caps.get(0)
                                .ok_or_else(|| Error::internal("Regex full match missing"))?
                                .as_str()
                        };
                        Ok(Value::string(match_str.to_string()))
                    } else {
                        Ok(Value::null())
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    })
                }
            },
        }),
    );
}

fn register_regex_extract_all(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "REGEXP_EXTRACT_ALL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "REGEXP_EXTRACT_ALL".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(text), Some(pattern)) = (args[0].as_str(), args[1].as_str()) {
                    let regex = Regex::new(pattern).map_err(|e| {
                        Error::invalid_query(format!("Invalid regex pattern: {}", e))
                    })?;

                    let results: Vec<Value> = regex
                        .captures_iter(text)
                        .map(|caps| {
                            if caps.len() > 1 {
                                Value::string(caps.get(1).unwrap().as_str().to_string())
                            } else {
                                Value::string(caps.get(0).unwrap().as_str().to_string())
                            }
                        })
                        .collect();
                    Ok(Value::array(results))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{}, {}", args[0].data_type(), args[1].data_type()),
                    })
                }
            },
        }),
    );
}
