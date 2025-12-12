use std::rc::Rc;

use yachtsql_common::types::DataType;

use super::FunctionRegistry;
use crate::hstore;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_constructors(registry);
    register_operators(registry);
    register_utility(registry);
}

fn register_constructors(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "HSTORE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE".to_string(),
            arg_types: vec![
                DataType::Array(Box::new(DataType::String)),
                DataType::Array(Box::new(DataType::String)),
            ],
            return_type: DataType::Hstore,
            variadic: false,
            evaluator: |args| hstore::hstore_from_arrays(&args[0], &args[1]),
        }),
    );
}

fn register_operators(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "HSTORE_GET".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_GET".to_string(),
            arg_types: vec![DataType::Hstore, DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| hstore::hstore_get(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "HSTORE_EXISTS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_EXISTS".to_string(),
            arg_types: vec![DataType::Hstore, DataType::String],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| hstore::hstore_exists(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "HSTORE_EXISTS_ALL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_EXISTS_ALL".to_string(),
            arg_types: vec![
                DataType::Hstore,
                DataType::Array(Box::new(DataType::String)),
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| hstore::hstore_exists_all(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "HSTORE_EXISTS_ANY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_EXISTS_ANY".to_string(),
            arg_types: vec![
                DataType::Hstore,
                DataType::Array(Box::new(DataType::String)),
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| hstore::hstore_exists_any(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "HSTORE_CONCAT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_CONCAT".to_string(),
            arg_types: vec![DataType::Hstore, DataType::Hstore],
            return_type: DataType::Hstore,
            variadic: false,
            evaluator: |args| hstore::hstore_concat(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "HSTORE_DELETE_KEY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_DELETE_KEY".to_string(),
            arg_types: vec![DataType::Hstore, DataType::String],
            return_type: DataType::Hstore,
            variadic: false,
            evaluator: |args| hstore::hstore_delete_key(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "HSTORE_DELETE_KEYS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_DELETE_KEYS".to_string(),
            arg_types: vec![
                DataType::Hstore,
                DataType::Array(Box::new(DataType::String)),
            ],
            return_type: DataType::Hstore,
            variadic: false,
            evaluator: |args| hstore::hstore_delete_keys(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "HSTORE_CONTAINS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_CONTAINS".to_string(),
            arg_types: vec![DataType::Hstore, DataType::Hstore],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| hstore::hstore_contains(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "HSTORE_CONTAINED_BY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_CONTAINED_BY".to_string(),
            arg_types: vec![DataType::Hstore, DataType::Hstore],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| hstore::hstore_contained_by(&args[0], &args[1]),
        }),
    );
}

fn register_utility(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "AKEYS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "AKEYS".to_string(),
            arg_types: vec![DataType::Hstore],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: false,
            evaluator: |args| hstore::hstore_akeys(&args[0]),
        }),
    );

    registry.register_scalar(
        "AVALS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "AVALS".to_string(),
            arg_types: vec![DataType::Hstore],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: false,
            evaluator: |args| hstore::hstore_avals(&args[0]),
        }),
    );

    registry.register_scalar(
        "SKEYS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SKEYS".to_string(),
            arg_types: vec![DataType::Hstore],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: false,
            evaluator: |args| hstore::hstore_akeys(&args[0]),
        }),
    );

    registry.register_scalar(
        "SVALS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SVALS".to_string(),
            arg_types: vec![DataType::Hstore],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: false,
            evaluator: |args| hstore::hstore_avals(&args[0]),
        }),
    );

    registry.register_scalar(
        "HSTORE_TO_JSON".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_TO_JSON".to_string(),
            arg_types: vec![DataType::Hstore],
            return_type: DataType::Json,
            variadic: false,
            evaluator: |args| hstore::hstore_to_json(&args[0]),
        }),
    );

    registry.register_scalar(
        "HSTORE_TO_JSONB".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_TO_JSONB".to_string(),
            arg_types: vec![DataType::Hstore],
            return_type: DataType::Json,
            variadic: false,
            evaluator: |args| hstore::hstore_to_jsonb(&args[0]),
        }),
    );

    registry.register_scalar(
        "HSTORE_TO_ARRAY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_TO_ARRAY".to_string(),
            arg_types: vec![DataType::Hstore],
            return_type: DataType::Array(Box::new(DataType::String)),
            variadic: false,
            evaluator: |args| hstore::hstore_to_array(&args[0]),
        }),
    );

    registry.register_scalar(
        "HSTORE_TO_MATRIX".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HSTORE_TO_MATRIX".to_string(),
            arg_types: vec![DataType::Hstore],
            return_type: DataType::Array(Box::new(DataType::Array(Box::new(DataType::String)))),
            variadic: false,
            evaluator: |args| hstore::hstore_to_matrix(&args[0]),
        }),
    );

    registry.register_scalar(
        "SLICE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SLICE".to_string(),
            arg_types: vec![
                DataType::Hstore,
                DataType::Array(Box::new(DataType::String)),
            ],
            return_type: DataType::Hstore,
            variadic: false,
            evaluator: |args| hstore::hstore_slice(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "DEFINED".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DEFINED".to_string(),
            arg_types: vec![DataType::Hstore, DataType::String],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| hstore::hstore_defined(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "DELETE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DELETE".to_string(),
            arg_types: vec![DataType::Hstore, DataType::String],
            return_type: DataType::Hstore,
            variadic: false,
            evaluator: |args| hstore::hstore_delete(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "EXIST".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "EXIST".to_string(),
            arg_types: vec![DataType::Hstore, DataType::String],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| hstore::hstore_exist(&args[0], &args[1]),
        }),
    );
}
