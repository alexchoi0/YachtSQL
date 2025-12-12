use std::cmp::Ordering;
use std::rc::Rc;

use rust_decimal::prelude::ToPrimitive;
use yachtsql_common::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_filter_functions(registry);
    register_transform_functions(registry);
    register_predicate_functions(registry);
    register_sort_functions(registry);
}

fn register_filter_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAYFILTER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYFILTER".to_string(),
            arg_types: vec![
                DataType::Unknown,
                DataType::Array(Box::new(DataType::Unknown)),
            ],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Ok(Value::null());
                }

                let Some(arr) = args[1].as_array() else {
                    return Ok(Value::null());
                };

                let mut result = Vec::new();
                for val in arr.iter() {
                    if should_include_in_filter(val) {
                        result.push(val.clone());
                    }
                }

                Ok(Value::array(result))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYFIRST".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYFIRST".to_string(),
            arg_types: vec![
                DataType::Unknown,
                DataType::Array(Box::new(DataType::Unknown)),
            ],
            return_type: DataType::Unknown,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Ok(Value::null());
                }

                let Some(arr) = args[1].as_array() else {
                    return Ok(Value::null());
                };

                for val in arr.iter() {
                    if should_include_in_filter(val) {
                        return Ok(val.clone());
                    }
                }

                Ok(Value::null())
            },
        }),
    );
}

fn register_transform_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAYMAP".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYMAP".to_string(),
            arg_types: vec![
                DataType::Unknown,
                DataType::Array(Box::new(DataType::Unknown)),
            ],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Ok(Value::null());
                }

                let Some(arr) = args[1].as_array() else {
                    return Ok(Value::null());
                };

                let result: Vec<Value> = arr.iter().map(apply_transform).collect();

                Ok(Value::array(result))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYCOMPACT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYCOMPACT".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::null());
                };

                if arr.is_empty() {
                    return Ok(Value::array(vec![]));
                }

                let mut result = vec![arr[0].clone()];

                for i in 1..arr.len() {
                    if !values_equal(&arr[i], &arr[i - 1]) {
                        result.push(arr[i].clone());
                    }
                }

                Ok(Value::array(result))
            },
        }),
    );
}

fn register_predicate_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAYEXISTS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYEXISTS".to_string(),
            arg_types: vec![
                DataType::Unknown,
                DataType::Array(Box::new(DataType::Unknown)),
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Ok(Value::bool_val(false));
                }

                let Some(arr) = args[1].as_array() else {
                    return Ok(Value::bool_val(false));
                };

                for val in arr.iter() {
                    if should_include_in_filter(val) {
                        return Ok(Value::bool_val(true));
                    }
                }

                Ok(Value::bool_val(false))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYALL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYALL".to_string(),
            arg_types: vec![
                DataType::Unknown,
                DataType::Array(Box::new(DataType::Unknown)),
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Ok(Value::bool_val(true));
                }

                let Some(arr) = args[1].as_array() else {
                    return Ok(Value::bool_val(true));
                };

                if arr.is_empty() {
                    return Ok(Value::bool_val(true));
                }

                for val in arr.iter() {
                    if !should_include_in_filter(val) {
                        return Ok(Value::bool_val(false));
                    }
                }

                Ok(Value::bool_val(true))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYFIRSTINDEX".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYFIRSTINDEX".to_string(),
            arg_types: vec![
                DataType::Unknown,
                DataType::Array(Box::new(DataType::Unknown)),
            ],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.len() < 2 {
                    return Ok(Value::int64(0));
                }

                let Some(arr) = args[1].as_array() else {
                    return Ok(Value::int64(0));
                };

                for (idx, val) in arr.iter().enumerate() {
                    if should_include_in_filter(val) {
                        return Ok(Value::int64((idx + 1) as i64));
                    }
                }

                Ok(Value::int64(0))
            },
        }),
    );
}

fn register_sort_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ARRAYSORT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYSORT".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::null());
                };

                let mut sorted = arr.to_vec();
                sorted.sort_by(compare_values);

                Ok(Value::array(sorted))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYREVERSESORT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYREVERSESORT".to_string(),
            arg_types: vec![DataType::Array(Box::new(DataType::Unknown))],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::null());
                };

                let mut sorted = arr.to_vec();
                sorted.sort_by(|a, b| compare_values(b, a));

                Ok(Value::array(sorted))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYZIP".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYZIP".to_string(),
            arg_types: vec![],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Ok(Value::array(vec![]));
                }

                let arrays: Vec<Vec<Value>> = args
                    .iter()
                    .filter_map(|arg| arg.as_array().map(|arr| arr.to_vec()))
                    .collect();

                if arrays.is_empty() {
                    return Ok(Value::array(vec![]));
                }

                let max_len = arrays.iter().map(|arr| arr.len()).max().unwrap_or(0);
                let mut result = Vec::new();

                for i in 0..max_len {
                    let mut tuple = Vec::new();
                    for arr in &arrays {
                        if i < arr.len() {
                            tuple.push(arr[i].clone());
                        } else {
                            tuple.push(Value::null());
                        }
                    }
                    result.push(Value::array(tuple));
                }

                Ok(Value::array(result))
            },
        }),
    );

    registry.register_scalar(
        "ARRAYRESIZE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ARRAYRESIZE".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::Unknown)),
                DataType::Int64,
                DataType::Unknown,
            ],
            return_type: DataType::Array(Box::new(DataType::Unknown)),
            variadic: false,
            evaluator: |args| {
                let Some(arr) = args[0].as_array() else {
                    return Ok(Value::null());
                };

                let size = if let Some(s) = args[1].as_i64() {
                    s
                } else {
                    return Ok(Value::null());
                };

                let fill_value = if args.len() > 2 {
                    args[2].clone()
                } else {
                    Value::null()
                };

                let new_size = size.max(0) as usize;
                let mut result = arr.to_vec();

                if new_size < result.len() {
                    result.truncate(new_size);
                } else {
                    while result.len() < new_size {
                        result.push(fill_value.clone());
                    }
                }

                Ok(Value::array(result))
            },
        }),
    );
}

fn should_include_in_filter(val: &Value) -> bool {
    if val.is_null() {
        return false;
    }
    if let Some(b) = val.as_bool() {
        return b;
    }
    if let Some(i) = val.as_i64() {
        return i != 0;
    }
    if let Some(f) = val.as_f64() {
        return f != 0.0;
    }
    true
}

fn apply_transform(val: &Value) -> Value {
    val.clone()
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

fn compare_values(a: &Value, b: &Value) -> Ordering {
    if a.is_null() && b.is_null() {
        return Ordering::Equal;
    }
    if a.is_null() {
        return Ordering::Less;
    }
    if b.is_null() {
        return Ordering::Greater;
    }

    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x.cmp(&y);
    }
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
    }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return x.cmp(y);
    }
    if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
        return x.cmp(&y);
    }

    if let (Some(x), Some(y)) = (a.as_numeric(), b.as_numeric()) {
        return x.cmp(&y);
    }

    if let Some(x) = a.as_i64() {
        if let Some(y) = b.as_f64() {
            return (x as f64).partial_cmp(&y).unwrap_or(Ordering::Equal);
        }
        if let Some(y) = b.as_numeric() {
            if let Some(y_f64) = y.to_f64() {
                return (x as f64).partial_cmp(&y_f64).unwrap_or(Ordering::Equal);
            }
        }
    }

    if let Some(x) = a.as_f64() {
        if let Some(y) = b.as_i64() {
            return x.partial_cmp(&(y as f64)).unwrap_or(Ordering::Equal);
        }
    }

    if let Some(x) = a.as_numeric() {
        if let Some(y) = b.as_i64() {
            if let Some(x_f64) = x.to_f64() {
                return x_f64.partial_cmp(&(y as f64)).unwrap_or(Ordering::Equal);
            }
        }
    }

    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_filter() {
        let arr = Value::array(vec![
            Value::int64(1),
            Value::int64(0),
            Value::int64(2),
            Value::null(),
            Value::int64(3),
        ]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYFILTER").unwrap();

        let result = func.evaluate(&[Value::null(), arr]).unwrap();

        if let Some(vals) = result.as_array() {
            assert_eq!(vals.len(), 3);
            assert_eq!(vals[0], Value::int64(1));
            assert_eq!(vals[1], Value::int64(2));
            assert_eq!(vals[2], Value::int64(3));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_array_exists() {
        let arr = Value::array(vec![Value::int64(0), Value::int64(0), Value::int64(5)]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYEXISTS").unwrap();

        let result = func.evaluate(&[Value::null(), arr]).unwrap();
        assert_eq!(result, Value::bool_val(true));
    }

    #[test]
    fn test_array_all() {
        let arr1 = Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]);

        let arr2 = Value::array(vec![Value::int64(1), Value::int64(0), Value::int64(3)]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYALL").unwrap();

        let result1 = func.evaluate(&[Value::null(), arr1]).unwrap();
        assert_eq!(result1, Value::bool_val(true));

        let result2 = func.evaluate(&[Value::null(), arr2]).unwrap();
        assert_eq!(result2, Value::bool_val(false));
    }

    #[test]
    fn test_array_sort() {
        let arr = Value::array(vec![Value::int64(3), Value::int64(1), Value::int64(2)]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYSORT").unwrap();

        let result = func.evaluate(&[arr]).unwrap();

        if let Some(vals) = result.as_array() {
            assert_eq!(vals[0], Value::int64(1));
            assert_eq!(vals[1], Value::int64(2));
            assert_eq!(vals[2], Value::int64(3));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_array_compact() {
        let arr = Value::array(vec![
            Value::int64(1),
            Value::int64(1),
            Value::int64(2),
            Value::int64(2),
            Value::int64(2),
            Value::int64(3),
        ]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYCOMPACT").unwrap();

        let result = func.evaluate(&[arr]).unwrap();

        if let Some(vals) = result.as_array() {
            assert_eq!(vals.len(), 3);
            assert_eq!(vals[0], Value::int64(1));
            assert_eq!(vals[1], Value::int64(2));
            assert_eq!(vals[2], Value::int64(3));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_array_zip() {
        let arr1 = Value::array(vec![Value::int64(1), Value::int64(2)]);
        let arr2 = Value::array(vec![
            Value::string("a".to_string()),
            Value::string("b".to_string()),
        ]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYZIP").unwrap();

        let result = func.evaluate(&[arr1, arr2]).unwrap();

        if let Some(vals) = result.as_array() {
            assert_eq!(vals.len(), 2);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_array_resize() {
        let arr = Value::array(vec![Value::int64(1), Value::int64(2)]);

        let registry = FunctionRegistry::new();
        let func = registry.get_scalar("ARRAYRESIZE").unwrap();

        let result = func
            .evaluate(&[arr, Value::int64(5), Value::int64(0)])
            .unwrap();

        if let Some(vals) = result.as_array() {
            assert_eq!(vals.len(), 5);
            assert_eq!(vals[4], Value::int64(0));
        } else {
            panic!("Expected array result");
        }
    }
}
