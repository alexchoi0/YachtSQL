use std::rc::Rc;

use yachtsql_common::error::Error;
use yachtsql_common::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_array_concat(registry);
    register_array_mutators(registry);
    register_array_element_ops(registry);
    register_array_string_conversion(registry);
}

fn register_array_concat(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAY_CONCAT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAY_CONCAT".to_string(),
            arg_types: vec![],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: true,
            evaluator: |args| {
                let mut combined = Vec::new();
                for value in args {
                    if value.is_null() {
                        continue;
                    }
                    if let Some(values) = value.as_array() {
                        combined.extend_from_slice(values);
                    } else {
                        return Err(Error::TypeMismatch {
                            expected: "ARRAY".to_string(),
                            actual: value.data_type().to_string(),
                        });
                    }
                }
                Ok(Value::array(combined))
            },
        }),
    );
}

fn register_array_mutators(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAY_REVERSE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAY_REVERSE".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(values) = args[0].as_array() {
                    let mut reversed = values.to_vec();
                    reversed.reverse();
                    Ok(Value::array(reversed))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "ARRAY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ARRAY_LENGTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAY_LENGTH".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(values) = args[0].as_array() {
                    Ok(Value::int64(values.len() as i64))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "ARRAY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "CARDINALITY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CARDINALITY".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(values) = args[0].as_array() {
                    Ok(Value::int64(values.len() as i64))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "ARRAY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ARRAY_CAT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAY_CAT".to_string(),
            arg_types: vec![],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: true,
            evaluator: |args| {
                let mut combined = Vec::new();
                for value in args {
                    if value.is_null() {
                        continue;
                    }
                    if let Some(values) = value.as_array() {
                        combined.extend_from_slice(values);
                    } else {
                        return Err(Error::TypeMismatch {
                            expected: "ARRAY".to_string(),
                            actual: value.data_type().to_string(),
                        });
                    }
                }
                Ok(Value::array(combined))
            },
        }),
    );
}

fn register_array_element_ops(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAY_APPEND".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAY_APPEND".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::Unknown,
            ],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(values) = args[0].as_array() {
                    let mut result = values.to_vec();
                    result.push(args[1].clone());
                    Ok(Value::array(result))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "ARRAY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ARRAY_PREPEND".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAY_PREPEND".to_string(),
            arg_types: vec![
                DataType::Unknown,
                DataType::Array(Box::new(DataType::Unknown)),
            ],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                if args[1].is_null() {
                    return Ok(Value::null());
                }

                if let Some(values) = args[1].as_array() {
                    let mut result = vec![args[0].clone()];
                    result.extend_from_slice(values);
                    Ok(Value::array(result))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "ARRAY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ARRAY_POSITION".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAY_POSITION".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::Unknown,
            ],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(values) = args[0].as_array() {
                    let needle = &args[1];
                    for (idx, val) in values.iter().enumerate() {
                        if values_equal(val, needle) {
                            return Ok(Value::int64((idx + 1) as i64));
                        }
                    }
                    Ok(Value::null())
                } else {
                    Err(Error::TypeMismatch {
                        expected: "ARRAY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ARRAY_REMOVE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAY_REMOVE".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::Unknown,
            ],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(values) = args[0].as_array() {
                    let needle = &args[1];
                    let result: Vec<Value> = values
                        .iter()
                        .filter(|v| !values_equal(v, needle))
                        .cloned()
                        .collect();
                    Ok(Value::array(result))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "ARRAY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ARRAY_REPLACE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAY_REPLACE".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::Unknown,
                DataType::Unknown,
            ],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(values) = args[0].as_array() {
                    let find = &args[1];
                    let replace = &args[2];
                    let result: Vec<Value> = values
                        .iter()
                        .map(|v| {
                            if values_equal(v, find) {
                                replace.clone()
                            } else {
                                v.clone()
                            }
                        })
                        .collect();
                    Ok(Value::array(result))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "ARRAY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );
}

fn register_array_string_conversion(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAY_TO_STRING".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAY_TO_STRING".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::String,
            ],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let delimiter = args[1].as_str().unwrap_or(",");

                if let Some(values) = args[0].as_array() {
                    let strings: Vec<String> = values
                        .iter()
                        .filter(|v| !v.is_null())
                        .map(value_to_string)
                        .collect();
                    Ok(Value::string(strings.join(delimiter)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "ARRAY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "STRING_TO_ARRAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "STRING_TO_ARRAY".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                let input = args[0].as_str().unwrap_or("");
                let delimiter = args[1].as_str().unwrap_or(",");

                if delimiter.is_empty() {
                    let chars: Vec<Value> = input
                        .chars()
                        .map(|c| Value::string(c.to_string()))
                        .collect();
                    return Ok(Value::array(chars));
                }

                let parts: Vec<Value> = input
                    .split(delimiter)
                    .map(|s| Value::string(s.to_string()))
                    .collect();
                Ok(Value::array(parts))
            },
        }),
    );
}

fn values_equal(a: &Value, b: &Value) -> bool {
    if a.is_null() && b.is_null() {
        return true;
    }
    if a.is_null() || b.is_null() {
        return false;
    }

    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x == y;
    }

    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return (x - y).abs() < f64::EPSILON;
    }

    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return x == y;
    }

    if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
        return x == y;
    }

    false
}

fn value_to_string(v: &Value) -> String {
    if let Some(s) = v.as_str() {
        s.to_string()
    } else if let Some(n) = v.as_i64() {
        n.to_string()
    } else if let Some(f) = v.as_f64() {
        f.to_string()
    } else if let Some(b) = v.as_bool() {
        b.to_string()
    } else {
        String::new()
    }
}
