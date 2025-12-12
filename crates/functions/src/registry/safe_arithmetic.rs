use std::rc::Rc;

use yachtsql_common::error::Error;
use yachtsql_common::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_safe_add(registry);
    register_safe_subtract(registry);
    register_safe_multiply(registry);
    register_safe_divide(registry);
    register_safe_negate(registry);
}

fn register_safe_add(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "SAFE_ADD".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SAFE_ADD".to_string(),
            arg_types: vec![DataType::Int64, DataType::Int64],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(a), Some(b)) = (args[0].as_i64(), args[1].as_i64()) {
                    match a.checked_add(b) {
                        Some(result) => return Ok(Value::int64(result)),
                        None => return Ok(Value::null()),
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_f64(), args[1].as_f64()) {
                    let is_int_a = a.fract() == 0.0 && a.is_finite();
                    let is_int_b = b.fract() == 0.0 && b.is_finite();
                    let in_range_a = a >= i64::MIN as f64 && a <= i64::MAX as f64;
                    let in_range_b = b >= i64::MIN as f64 && b <= i64::MAX as f64;

                    if is_int_a && is_int_b && in_range_a && in_range_b {
                        let a_int = a as i64;
                        let b_int = b as i64;
                        match a_int.checked_add(b_int) {
                            Some(result) => return Ok(Value::int64(result)),
                            None => return Ok(Value::null()),
                        }
                    } else {
                        let result = a + b;
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_i64(), args[1].as_f64()) {
                    if b.fract() == 0.0
                        && b.is_finite()
                        && b >= i64::MIN as f64
                        && b <= i64::MAX as f64
                    {
                        let b_int = b as i64;
                        match a.checked_add(b_int) {
                            Some(result) => return Ok(Value::int64(result)),
                            None => return Ok(Value::null()),
                        }
                    } else {
                        let result = (a as f64) + b;
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_f64(), args[1].as_i64()) {
                    if a.fract() == 0.0
                        && a.is_finite()
                        && a >= i64::MIN as f64
                        && a <= i64::MAX as f64
                    {
                        let a_int = a as i64;
                        match a_int.checked_add(b) {
                            Some(result) => return Ok(Value::int64(result)),
                            None => return Ok(Value::null()),
                        }
                    } else {
                        let result = a + (b as f64);
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                Err(Error::TypeMismatch {
                    expected: "NUMERIC".to_string(),
                    actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );
}

fn register_safe_subtract(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "SAFE_SUBTRACT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SAFE_SUBTRACT".to_string(),
            arg_types: vec![DataType::Int64, DataType::Int64],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(a), Some(b)) = (args[0].as_i64(), args[1].as_i64()) {
                    match a.checked_sub(b) {
                        Some(result) => return Ok(Value::int64(result)),
                        None => return Ok(Value::null()),
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_f64(), args[1].as_f64()) {
                    let is_int_a = a.fract() == 0.0 && a.is_finite();
                    let is_int_b = b.fract() == 0.0 && b.is_finite();
                    let in_range_a = a >= i64::MIN as f64 && a <= i64::MAX as f64;
                    let in_range_b = b >= i64::MIN as f64 && b <= i64::MAX as f64;

                    if is_int_a && is_int_b && in_range_a && in_range_b {
                        let a_int = a as i64;
                        let b_int = b as i64;
                        match a_int.checked_sub(b_int) {
                            Some(result) => return Ok(Value::int64(result)),
                            None => return Ok(Value::null()),
                        }
                    } else {
                        let result = a - b;
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_i64(), args[1].as_f64()) {
                    if b.fract() == 0.0
                        && b.is_finite()
                        && b >= i64::MIN as f64
                        && b <= i64::MAX as f64
                    {
                        let b_int = b as i64;
                        match a.checked_sub(b_int) {
                            Some(result) => return Ok(Value::int64(result)),
                            None => return Ok(Value::null()),
                        }
                    } else {
                        let result = (a as f64) - b;
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_f64(), args[1].as_i64()) {
                    if a.fract() == 0.0
                        && a.is_finite()
                        && a >= i64::MIN as f64
                        && a <= i64::MAX as f64
                    {
                        let a_int = a as i64;
                        match a_int.checked_sub(b) {
                            Some(result) => return Ok(Value::int64(result)),
                            None => return Ok(Value::null()),
                        }
                    } else {
                        let result = a - (b as f64);
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                Err(Error::TypeMismatch {
                    expected: "NUMERIC".to_string(),
                    actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );
}

fn register_safe_multiply(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "SAFE_MULTIPLY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SAFE_MULTIPLY".to_string(),
            arg_types: vec![DataType::Int64, DataType::Int64],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(a), Some(b)) = (args[0].as_i64(), args[1].as_i64()) {
                    match a.checked_mul(b) {
                        Some(result) => return Ok(Value::int64(result)),
                        None => return Ok(Value::null()),
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_f64(), args[1].as_f64()) {
                    let is_int_a = a.fract() == 0.0 && a.is_finite();
                    let is_int_b = b.fract() == 0.0 && b.is_finite();
                    let in_range_a = a >= i64::MIN as f64 && a <= i64::MAX as f64;
                    let in_range_b = b >= i64::MIN as f64 && b <= i64::MAX as f64;

                    if is_int_a && is_int_b && in_range_a && in_range_b {
                        let a_int = a as i64;
                        let b_int = b as i64;
                        match a_int.checked_mul(b_int) {
                            Some(result) => return Ok(Value::int64(result)),
                            None => return Ok(Value::null()),
                        }
                    } else {
                        let result = a * b;
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_i64(), args[1].as_f64()) {
                    if b.fract() == 0.0
                        && b.is_finite()
                        && b >= i64::MIN as f64
                        && b <= i64::MAX as f64
                    {
                        let b_int = b as i64;
                        match a.checked_mul(b_int) {
                            Some(result) => return Ok(Value::int64(result)),
                            None => return Ok(Value::null()),
                        }
                    } else {
                        let result = (a as f64) * b;
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_f64(), args[1].as_i64()) {
                    if a.fract() == 0.0
                        && a.is_finite()
                        && a >= i64::MIN as f64
                        && a <= i64::MAX as f64
                    {
                        let a_int = a as i64;
                        match a_int.checked_mul(b) {
                            Some(result) => return Ok(Value::int64(result)),
                            None => return Ok(Value::null()),
                        }
                    } else {
                        let result = a * (b as f64);
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                Err(Error::TypeMismatch {
                    expected: "NUMERIC".to_string(),
                    actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );
}

fn register_safe_divide(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "SAFE_DIVIDE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SAFE_DIVIDE".to_string(),
            arg_types: vec![DataType::Float64, DataType::Float64],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }

                if let (Some(a), Some(b)) = (args[0].as_i64(), args[1].as_i64()) {
                    if b == 0 {
                        return Ok(Value::null());
                    } else {
                        return Ok(Value::float64(a as f64 / b as f64));
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_f64(), args[1].as_f64()) {
                    if b == 0.0 {
                        return Ok(Value::null());
                    } else {
                        let result = a / b;
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_i64(), args[1].as_f64()) {
                    if b == 0.0 {
                        return Ok(Value::null());
                    } else {
                        let result = (a as f64) / b;
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                if let (Some(a), Some(b)) = (args[0].as_f64(), args[1].as_i64()) {
                    if b == 0 {
                        return Ok(Value::null());
                    } else {
                        let result = a / (b as f64);
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                Err(Error::TypeMismatch {
                    expected: "NUMERIC".to_string(),
                    actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                })
            },
        }),
    );
}

fn register_safe_negate(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "SAFE_NEGATE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SAFE_NEGATE".to_string(),
            arg_types: vec![DataType::Int64],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }

                if let Some(i) = args[0].as_i64() {
                    match i.checked_neg() {
                        Some(result) => return Ok(Value::int64(result)),
                        None => return Ok(Value::null()),
                    }
                }

                if let Some(f) = args[0].as_f64() {
                    if f.fract() == 0.0
                        && f.is_finite()
                        && f >= i64::MIN as f64
                        && f <= i64::MAX as f64
                    {
                        let f_int = f as i64;
                        match f_int.checked_neg() {
                            Some(result) => return Ok(Value::int64(result)),
                            None => return Ok(Value::null()),
                        }
                    } else {
                        let result = -f;
                        if result.is_finite() {
                            return Ok(Value::float64(result));
                        } else {
                            return Ok(Value::null());
                        }
                    }
                }

                Err(Error::TypeMismatch {
                    expected: "NUMERIC".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            },
        }),
    );
}
