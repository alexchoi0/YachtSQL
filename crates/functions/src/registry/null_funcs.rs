use std::rc::Rc;

use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_sql_standard(registry);
    register_oracle_functions(registry);
    register_mysql_functions(registry);
    register_predicates(registry);
}

fn register_sql_standard(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "COALESCE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "COALESCE".to_string(),
            arg_types: vec![],
            return_type: DataType::Unknown,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "COALESCE requires at least one argument".to_string(),
                    ));
                }

                for arg in args {
                    if !arg.is_null() {
                        return Ok(arg.clone());
                    }
                }
                Ok(Value::null())
            },
        }),
    );

    registry.register_scalar(
        "NULLIF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NULLIF".to_string(),
            arg_types: vec![],
            return_type: DataType::Unknown,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "NULLIF requires exactly 2 arguments".to_string(),
                    ));
                }

                if values_equal(&args[0], &args[1]) {
                    Ok(Value::null())
                } else {
                    Ok(args[0].clone())
                }
            },
        }),
    );
}

fn register_oracle_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "NVL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NVL".to_string(),
            arg_types: vec![],
            return_type: DataType::Unknown,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "NVL requires exactly 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() {
                    Ok(args[1].clone())
                } else {
                    Ok(args[0].clone())
                }
            },
        }),
    );

    registry.register_scalar(
        "NVL2".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NVL2".to_string(),
            arg_types: vec![],
            return_type: DataType::Unknown,
            variadic: false,
            evaluator: |args| {
                if args.len() != 3 {
                    return Err(Error::invalid_query(
                        "NVL2 requires exactly 3 arguments".to_string(),
                    ));
                }

                if args[0].is_null() {
                    Ok(args[2].clone())
                } else {
                    Ok(args[1].clone())
                }
            },
        }),
    );
}

fn register_mysql_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "IFNULL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IFNULL".to_string(),
            arg_types: vec![],
            return_type: DataType::Unknown,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "IFNULL requires exactly 2 arguments".to_string(),
                    ));
                }

                if args[0].is_null() {
                    Ok(args[1].clone())
                } else {
                    Ok(args[0].clone())
                }
            },
        }),
    );

    registry.register_scalar(
        "ISNULL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ISNULL".to_string(),
            arg_types: vec![],
            return_type: DataType::Unknown,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::invalid_query(
                        "ISNULL requires 1 or 2 arguments".to_string(),
                    ));
                }

                if args.len() == 1 {
                    Ok(Value::bool_val(args[0].is_null()))
                } else if args[0].is_null() {
                    Ok(args[1].clone())
                } else {
                    Ok(args[0].clone())
                }
            },
        }),
    );
}

fn register_predicates(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "IS_NULL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IS_NULL".to_string(),
            arg_types: vec![],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "IS_NULL requires exactly 1 argument".to_string(),
                    ));
                }

                Ok(Value::bool_val(args[0].is_null()))
            },
        }),
    );

    registry.register_scalar(
        "ASSUMENOTNULL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ASSUMENOTNULL".to_string(),
            arg_types: vec![],
            return_type: DataType::Unknown,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "assumeNotNull requires exactly 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Err(Error::invalid_query(
                        "assumeNotNull: value is NULL".to_string(),
                    ));
                }
                Ok(args[0].clone())
            },
        }),
    );

    registry.register_scalar(
        "TONULLABLE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TONULLABLE".to_string(),
            arg_types: vec![],
            return_type: DataType::Unknown,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "toNullable requires exactly 1 argument".to_string(),
                    ));
                }
                Ok(args[0].clone())
            },
        }),
    );

    registry.register_scalar(
        "ISZEROORULL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ISZEROORULL".to_string(),
            arg_types: vec![],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "isZeroOrNull requires exactly 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::bool_val(true));
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::bool_val(i == 0));
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::bool_val(f == 0.0));
                }
                Ok(Value::bool_val(false))
            },
        }),
    );

    registry.register_scalar(
        "ISZEROORNULL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ISZEROORNULL".to_string(),
            arg_types: vec![],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "isZeroOrNull requires exactly 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::bool_val(true));
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::bool_val(i == 0));
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::bool_val(f == 0.0));
                }
                Ok(Value::bool_val(false))
            },
        }),
    );

    registry.register_scalar(
        "IS_NOT_NULL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IS_NOT_NULL".to_string(),
            arg_types: vec![],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "IS_NOT_NULL requires exactly 1 argument".to_string(),
                    ));
                }

                Ok(Value::bool_val(!args[0].is_null()))
            },
        }),
    );

    registry.register_scalar(
        "ISNOTNULL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ISNOTNULL".to_string(),
            arg_types: vec![],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "ISNOTNULL requires exactly 1 argument".to_string(),
                    ));
                }

                Ok(Value::bool_val(!args[0].is_null()))
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

    if let (Some(a), Some(b)) = (a.as_bool(), b.as_bool()) {
        return a == b;
    }
    if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
        return a == b;
    }
    if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
        return (a - b).abs() < f64::EPSILON;
    }

    if let Some(a) = a.as_i64() {
        if let Some(b) = b.as_f64() {
            return (a as f64 - b).abs() < f64::EPSILON;
        }
    }
    if let Some(a) = a.as_f64() {
        if let Some(b) = b.as_i64() {
            return (a - b as f64).abs() < f64::EPSILON;
        }
    }
    if let (Some(a), Some(b)) = (a.as_str(), b.as_str()) {
        return a == b;
    }
    if let (Some(a), Some(b)) = (a.as_date(), b.as_date()) {
        return a == b;
    }
    if let (Some(a), Some(b)) = (a.as_timestamp(), b.as_timestamp()) {
        return a == b;
    }

    false
}
