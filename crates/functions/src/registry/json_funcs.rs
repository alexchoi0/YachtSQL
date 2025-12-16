use std::rc::Rc;

use yachtsql_core::error::Error;
use yachtsql_core::types::DataType;

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_json_extraction(registry);
    register_json_construction(registry);
    register_json_predicates(registry);
    register_json_utilities(registry);
}

fn register_json_extraction(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "JSON_EXTRACT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_EXTRACT".to_string(),
            arg_types: vec![DataType::Json, DataType::String],
            return_type: DataType::Json,
            variadic: false,
            evaluator: |args| {
                crate::json::extract::json_extract(&args[0], args[1].as_str().unwrap_or(""))
            },
        }),
    );

    registry.register_scalar(
        "JSON_EXTRACT_SCALAR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_EXTRACT_SCALAR".to_string(),
            arg_types: vec![DataType::Json, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                crate::json::extract::json_value(&args[0], args[1].as_str().unwrap_or(""))
            },
        }),
    );

    registry.register_scalar(
        "JSON_VALUE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_VALUE".to_string(),
            arg_types: vec![DataType::Json, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                crate::json::extract::json_value(&args[0], args[1].as_str().unwrap_or(""))
            },
        }),
    );

    registry.register_scalar(
        "JSON_QUERY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_QUERY".to_string(),
            arg_types: vec![DataType::Json, DataType::String],
            return_type: DataType::Json,
            variadic: false,
            evaluator: |args| {
                crate::json::extract::json_query(&args[0], args[1].as_str().unwrap_or(""))
            },
        }),
    );
}

fn register_json_construction(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "JSON_ARRAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_ARRAY".to_string(),
            arg_types: vec![],
            return_type: DataType::Json,
            variadic: true,
            evaluator: |args| crate::json::builder::json_array(args.to_vec()),
        }),
    );

    registry.register_scalar(
        "JSON_OBJECT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_OBJECT".to_string(),
            arg_types: vec![],
            return_type: DataType::Json,
            variadic: true,
            evaluator: |args| {
                if args.len() % 2 != 0 {
                    return Err(Error::invalid_query(
                        "JSON_OBJECT requires an even number of arguments (key-value pairs)"
                            .to_string(),
                    ));
                }

                let mut pairs = Vec::new();
                for chunk in args.chunks(2) {
                    let key = if let Some(s) = chunk[0].as_str() {
                        s.to_string()
                    } else {
                        return Err(Error::invalid_query(format!(
                            "JSON_OBJECT keys must be strings, got {:?}",
                            chunk[0].data_type()
                        )));
                    };
                    let value = chunk[1].clone();
                    pairs.push((key, value));
                }

                crate::json::builder::json_object(pairs)
            },
        }),
    );

    registry.register_scalar(
        "TO_JSON".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_JSON".to_string(),
            arg_types: vec![],
            return_type: DataType::Json,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "TO_JSON requires at least one argument",
                    ));
                }
                crate::json::functions::to_json(&args[0])
            },
        }),
    );

    registry.register_scalar(
        "TO_JSON_STRING".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_JSON_STRING".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "TO_JSON_STRING requires at least one argument",
                    ));
                }
                crate::json::functions::to_json_string(&args[0])
            },
        }),
    );

    registry.register_scalar(
        "PARSE_JSON".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "PARSE_JSON".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Json,
            variadic: false,
            evaluator: |args| crate::json::functions::parse_json(&args[0]),
        }),
    );

    registry.register_scalar(
        "TO_JSONB".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TO_JSONB".to_string(),
            arg_types: vec![],
            return_type: DataType::Json,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "TO_JSONB requires at least one argument",
                    ));
                }
                crate::json::functions::to_json(&args[0])
            },
        }),
    );

    registry.register_scalar(
        "JSON_BUILD_ARRAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_BUILD_ARRAY".to_string(),
            arg_types: vec![],
            return_type: DataType::Json,
            variadic: true,
            evaluator: |args| crate::json::builder::json_array(args.to_vec()),
        }),
    );

    registry.register_scalar(
        "JSONB_BUILD_ARRAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSONB_BUILD_ARRAY".to_string(),
            arg_types: vec![],
            return_type: DataType::Json,
            variadic: true,
            evaluator: |args| crate::json::builder::json_array(args.to_vec()),
        }),
    );

    registry.register_scalar(
        "JSON_BUILD_OBJECT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_BUILD_OBJECT".to_string(),
            arg_types: vec![],
            return_type: DataType::Json,
            variadic: true,
            evaluator: |args| {
                if args.len() % 2 != 0 {
                    return Err(Error::invalid_query(
                        "JSON_BUILD_OBJECT requires an even number of arguments (key-value pairs)"
                            .to_string(),
                    ));
                }

                let mut pairs = Vec::new();
                for chunk in args.chunks(2) {
                    let key = if let Some(s) = chunk[0].as_str() {
                        s.to_string()
                    } else {
                        return Err(Error::invalid_query(format!(
                            "JSON_BUILD_OBJECT keys must be strings, got {:?}",
                            chunk[0].data_type()
                        )));
                    };
                    let value = chunk[1].clone();
                    pairs.push((key, value));
                }

                crate::json::builder::json_object(pairs)
            },
        }),
    );

    registry.register_scalar(
        "JSONB_BUILD_OBJECT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSONB_BUILD_OBJECT".to_string(),
            arg_types: vec![],
            return_type: DataType::Json,
            variadic: true,
            evaluator: |args| {
                if args.len() % 2 != 0 {
                    return Err(Error::invalid_query(
                        "JSONB_BUILD_OBJECT requires an even number of arguments (key-value pairs)"
                            .to_string(),
                    ));
                }

                let mut pairs = Vec::new();
                for chunk in args.chunks(2) {
                    let key = if let Some(s) = chunk[0].as_str() {
                        s.to_string()
                    } else {
                        return Err(Error::invalid_query(format!(
                            "JSONB_BUILD_OBJECT keys must be strings, got {:?}",
                            chunk[0].data_type()
                        )));
                    };
                    let value = chunk[1].clone();
                    pairs.push((key, value));
                }

                crate::json::builder::json_object(pairs)
            },
        }),
    );
}

fn register_json_predicates(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "JSON_EXISTS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_EXISTS".to_string(),
            arg_types: vec![DataType::Json, DataType::String],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                crate::json::predicates::json_exists(&args[0], args[1].as_str().unwrap_or(""))
            },
        }),
    );

    registry.register_scalar(
        "IS_JSON_VALUE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IS_JSON_VALUE".to_string(),
            arg_types: vec![],
            return_type: DataType::Bool,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "IS_JSON_VALUE requires at least one argument",
                    ));
                }
                crate::json::predicates::is_json_value(&args[0])
            },
        }),
    );

    registry.register_scalar(
        "IS_JSON_ARRAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IS_JSON_ARRAY".to_string(),
            arg_types: vec![],
            return_type: DataType::Bool,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "IS_JSON_ARRAY requires at least one argument",
                    ));
                }
                crate::json::predicates::is_json_array(&args[0])
            },
        }),
    );

    registry.register_scalar(
        "IS_JSON_OBJECT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IS_JSON_OBJECT".to_string(),
            arg_types: vec![],
            return_type: DataType::Bool,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "IS_JSON_OBJECT requires at least one argument",
                    ));
                }
                crate::json::predicates::is_json_object(&args[0])
            },
        }),
    );

    registry.register_scalar(
        "IS_JSON_SCALAR".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IS_JSON_SCALAR".to_string(),
            arg_types: vec![],
            return_type: DataType::Bool,
            variadic: true,
            evaluator: |args| {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "IS_JSON_SCALAR requires at least one argument",
                    ));
                }
                crate::json::predicates::is_json_scalar(&args[0])
            },
        }),
    );
}

fn register_json_utilities(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "JSON_KEYS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_KEYS".to_string(),
            arg_types: vec![DataType::Json],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: false,
            evaluator: |args| crate::json::json_keys(&args[0], None),
        }),
    );

    registry.register_scalar(
        "JSON_LENGTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_LENGTH".to_string(),
            arg_types: vec![DataType::Json],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| crate::json::json_length(&args[0]),
        }),
    );

    registry.register_scalar(
        "JSON_TYPE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_TYPE".to_string(),
            arg_types: vec![DataType::Json],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| crate::json::json_type(&args[0]),
        }),
    );

    registry.register_scalar(
        "JSON_TYPEOF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_TYPEOF".to_string(),
            arg_types: vec![DataType::Json],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| crate::json::json_type(&args[0]),
        }),
    );

    registry.register_scalar(
        "JSONB_TYPEOF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSONB_TYPEOF".to_string(),
            arg_types: vec![DataType::Json],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| crate::json::json_type(&args[0]),
        }),
    );

    registry.register_scalar(
        "JSON_ARRAY_LENGTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSON_ARRAY_LENGTH".to_string(),
            arg_types: vec![DataType::Json],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| crate::json::json_length(&args[0]),
        }),
    );

    registry.register_scalar(
        "JSONB_ARRAY_LENGTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JSONB_ARRAY_LENGTH".to_string(),
            arg_types: vec![DataType::Json],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| crate::json::json_length(&args[0]),
        }),
    );
}
