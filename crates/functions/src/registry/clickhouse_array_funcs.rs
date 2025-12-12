use std::collections::HashSet;
use std::rc::Rc;

use rust_decimal::prelude::ToPrimitive;
use yachtsql_common::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_array_membership(registry);
    register_array_search(registry);
    register_array_manipulation(registry);
    register_array_aggregation(registry);
}

fn register_array_membership(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAYHAS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYHAS".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::Unknown,
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::bool_val(false));
                };

                let needle = &args[1];
                if needle.is_null() {
                    return Ok(Value::bool_val(false));
                }

                for val in arr.iter() {
                    if values_equal(val, needle) {
                        return Ok(Value::bool_val(true));
                    }
                }

                Ok(Value::bool_val(false))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYHASANY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYHASANY".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::Array(Box::new(DataType::Unknown)),
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                let Some(arr1) = args[0].as_array() else {
                    return Ok(Value::bool_val(false));
                };
                let Some(arr2) = args[1].as_array() else {
                    return Ok(Value::bool_val(false));
                };

                for needle in arr2.iter() {
                    for haystack in arr1.iter() {
                        if values_equal(haystack, needle) {
                            return Ok(Value::bool_val(true));
                        }
                    }
                }

                Ok(Value::bool_val(false))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYHASALL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYHASALL".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::Array(Box::new(DataType::Unknown)),
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                let Some(arr1) = args[0].as_array() else {
                    return Ok(Value::bool_val(false));
                };
                let Some(arr2) = args[1].as_array() else {
                    return Ok(Value::bool_val(false));
                };

                for needle in arr2.iter() {
                    let mut found = false;
                    for haystack in arr1.iter() {
                        if values_equal(haystack, needle) {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        return Ok(Value::bool_val(false));
                    }
                }

                Ok(Value::bool_val(true))
            },
        }),
    );
}

fn register_array_search(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAYINDEXOF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYINDEXOF".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::Unknown,
            ],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::int64(0));
                };

                let needle = &args[1];
                if needle.is_null() {
                    return Ok(Value::int64(0));
                }

                for (idx, val) in arr.iter().enumerate() {
                    if values_equal(val, needle) {
                        return Ok(Value::int64((idx + 1) as i64));
                    }
                }

                Ok(Value::int64(0))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYCOUNT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYCOUNT".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::Unknown,
            ],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::int64(0));
                };

                let needle = &args[1];
                if needle.is_null() {
                    return Ok(Value::int64(0));
                }

                let count = arr.iter().filter(|val| values_equal(val, needle)).count();

                Ok(Value::int64(count as i64))
            },
        }),
    );
}

fn register_array_manipulation(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAYSLICE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYSLICE".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::Int64,
                DataType::Int64,
            ],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::null());
                };

                let offset = if let Some(n) = args[1].as_i64() {
                    n
                } else {
                    return Ok(Value::null());
                };

                let length = if let Some(n) = args[2].as_i64() {
                    n
                } else {
                    return Ok(Value::null());
                };

                if arr.is_empty() || length <= 0 {
                    return Ok(Value::array(vec![]));
                }

                let start = if offset >= 0 {
                    (offset - 1).max(0) as usize
                } else {
                    let abs_offset = (-offset) as usize;
                    if abs_offset > arr.len() {
                        0
                    } else {
                        arr.len() - abs_offset
                    }
                };

                let end = (start + length as usize).min(arr.len());

                if start >= arr.len() {
                    return Ok(Value::array(vec![]));
                }

                Ok(Value::array(arr[start..end].to_vec()))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYDISTINCT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYDISTINCT".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::null());
                };

                let mut seen = HashSet::new();
                let mut result = Vec::new();

                for val in arr.iter() {
                    let key = format!("{:?}", val);
                    if seen.insert(key) {
                        result.push(val.clone());
                    }
                }

                Ok(Value::array(result))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYFLATTEN".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYFLATTEN".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::null());
                };

                let mut result = Vec::new();
                flatten_recursive(arr, &mut result);

                Ok(Value::array(result))
            },
        }),
    );
}

fn register_array_aggregation(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAYSUM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYSUM".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::null());
                };

                let mut sum = 0.0;
                for val in arr.iter() {
                    if let Some(n) = val.as_i64() {
                        sum += n as f64;
                    } else if let Some(f) = val.as_f64() {
                        sum += f;
                    } else if let Some(d) = val.as_numeric() {
                        if let Some(f) = d.to_f64() {
                            sum += f;
                        }
                    }
                }

                Ok(Value::float64(sum))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYMIN".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYMIN".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::null());
                };

                let mut min: Option<f64> = None;
                for val in arr.iter() {
                    let num_opt = if let Some(n) = val.as_i64() {
                        Some(n as f64)
                    } else if let Some(f) = val.as_f64() {
                        Some(f)
                    } else if let Some(d) = val.as_numeric() {
                        d.to_f64()
                    } else {
                        None
                    };

                    if let Some(num) = num_opt {
                        min = Some(min.map_or(num, |m| m.min(num)));
                    }
                }

                match min {
                    Some(val) => Ok(Value::float64(val)),
                    None => Ok(Value::null()),
                }
            },
        }),
    );

    registry.register_scalar(
        "ARRAYMAX".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYMAX".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::null());
                };

                let mut max: Option<f64> = None;
                for val in arr.iter() {
                    let num_opt = if let Some(n) = val.as_i64() {
                        Some(n as f64)
                    } else if let Some(f) = val.as_f64() {
                        Some(f)
                    } else if let Some(d) = val.as_numeric() {
                        d.to_f64()
                    } else {
                        None
                    };

                    if let Some(num) = num_opt {
                        max = Some(max.map_or(num, |m| m.max(num)));
                    }
                }

                match max {
                    Some(val) => Ok(Value::float64(val)),
                    None => Ok(Value::null()),
                }
            },
        }),
    );

    registry.register_scalar(
        "ARRAYAVG".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYAVG".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::null());
                };

                let mut sum = 0.0;
                let mut count = 0;

                for val in arr.iter() {
                    if let Some(n) = val.as_i64() {
                        sum += n as f64;
                        count += 1;
                    } else if let Some(f) = val.as_f64() {
                        sum += f;
                        count += 1;
                    } else if let Some(d) = val.as_numeric() {
                        if let Some(f) = d.to_f64() {
                            sum += f;
                            count += 1;
                        }
                    }
                }

                if count == 0 {
                    Ok(Value::null())
                } else {
                    Ok(Value::float64(sum / count as f64))
                }
            },
        }),
    );
}

fn values_equal(a: &Value, b: &Value) -> bool {
    if a.is_null() && b.is_null() {
        return true;
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

fn flatten_recursive(arr: &[Value], result: &mut Vec<Value>) {
    for val in arr {
        if let Some(nested) = val.as_array() {
            flatten_recursive(nested, result);
        } else {
            result.push(val.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_has() {
        let arr = Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYHAS").unwrap();

        let result = func.evaluate(&[arr.clone(), Value::int64(2)]).unwrap();
        assert_eq!(result, Value::bool_val(true));

        let result = func.evaluate(&[arr, Value::int64(5)]).unwrap();
        assert_eq!(result, Value::bool_val(false));
    }

    #[test]
    fn test_array_index_of() {
        let arr = Value::array(vec![
            Value::string("a".to_string()),
            Value::string("b".to_string()),
            Value::string("c".to_string()),
        ]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYINDEXOF").unwrap();

        let result = func
            .evaluate(&[arr.clone(), Value::string("b".to_string())])
            .unwrap();
        assert_eq!(result, Value::int64(2));

        let result = func
            .evaluate(&[arr, Value::string("d".to_string())])
            .unwrap();
        assert_eq!(result, Value::int64(0));
    }

    #[test]
    fn test_array_slice() {
        let arr = Value::array(vec![
            Value::int64(1),
            Value::int64(2),
            Value::int64(3),
            Value::int64(4),
            Value::int64(5),
        ]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYSLICE").unwrap();

        let result = func
            .evaluate(&[arr.clone(), Value::int64(2), Value::int64(3)])
            .unwrap();
        assert_eq!(
            result,
            Value::array(vec![Value::int64(2), Value::int64(3), Value::int64(4)])
        );
    }

    #[test]
    fn test_array_distinct() {
        let arr = Value::array(vec![
            Value::int64(1),
            Value::int64(2),
            Value::int64(2),
            Value::int64(3),
            Value::int64(1),
        ]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYDISTINCT").unwrap();

        let result = func.evaluate(&[arr]).unwrap();
        if let Some(vals) = result.as_array() {
            assert_eq!(vals.len(), 3);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_array_sum() {
        let arr = Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYSUM").unwrap();

        let result = func.evaluate(&[arr]).unwrap();
        assert_eq!(result, Value::float64(6.0));
    }
}
