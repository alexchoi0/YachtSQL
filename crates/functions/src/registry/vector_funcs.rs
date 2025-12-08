use std::rc::Rc;

use yachtsql_core::types::DataType;

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;
use crate::vector;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_distance_functions(registry);
    register_arithmetic_functions(registry);
    register_utility_functions(registry);
}

fn register_distance_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "L2_DISTANCE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "L2_DISTANCE".to_string(),
            arg_types: vec![DataType::Vector(0), DataType::Vector(0)],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| vector::l2_distance(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "COSINE_DISTANCE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "COSINE_DISTANCE".to_string(),
            arg_types: vec![DataType::Vector(0), DataType::Vector(0)],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| vector::cosine_distance(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "COSINE_SIMILARITY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "COSINE_SIMILARITY".to_string(),
            arg_types: vec![DataType::Vector(0), DataType::Vector(0)],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| vector::cosine_similarity(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "INNER_PRODUCT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INNER_PRODUCT".to_string(),
            arg_types: vec![DataType::Vector(0), DataType::Vector(0)],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| vector::inner_product(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "L1_DISTANCE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "L1_DISTANCE".to_string(),
            arg_types: vec![DataType::Vector(0), DataType::Vector(0)],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| vector::l1_distance(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "NEGATIVE_INNER_PRODUCT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NEGATIVE_INNER_PRODUCT".to_string(),
            arg_types: vec![DataType::Vector(0), DataType::Vector(0)],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| vector::negative_inner_product(&args[0], &args[1]),
        }),
    );
}

fn register_arithmetic_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "VECTOR_ADD".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "VECTOR_ADD".to_string(),
            arg_types: vec![DataType::Vector(0), DataType::Vector(0)],
            return_type: DataType::Vector(0),
            variadic: false,
            evaluator: |args| vector::vector_add(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "VECTOR_SUBTRACT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "VECTOR_SUBTRACT".to_string(),
            arg_types: vec![DataType::Vector(0), DataType::Vector(0)],
            return_type: DataType::Vector(0),
            variadic: false,
            evaluator: |args| vector::vector_subtract(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "VECTOR_MULTIPLY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "VECTOR_MULTIPLY".to_string(),
            arg_types: vec![DataType::Vector(0), DataType::Float64],
            return_type: DataType::Vector(0),
            variadic: false,
            evaluator: |args| vector::vector_scalar_multiply(&args[0], &args[1]),
        }),
    );
}

fn register_utility_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "VECTOR_NORM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "VECTOR_NORM".to_string(),
            arg_types: vec![DataType::Vector(0)],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| vector::vector_norm(&args[0]),
        }),
    );

    registry.register_scalar(
        "L2_NORM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "L2_NORM".to_string(),
            arg_types: vec![DataType::Vector(0)],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| vector::vector_norm(&args[0]),
        }),
    );

    registry.register_scalar(
        "VECTOR_NORMALIZE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "VECTOR_NORMALIZE".to_string(),
            arg_types: vec![DataType::Vector(0)],
            return_type: DataType::Vector(0),
            variadic: false,
            evaluator: |args| vector::vector_normalize(&args[0]),
        }),
    );

    registry.register_scalar(
        "L2_NORMALIZE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "L2_NORMALIZE".to_string(),
            arg_types: vec![DataType::Vector(0)],
            return_type: DataType::Vector(0),
            variadic: false,
            evaluator: |args| vector::vector_normalize(&args[0]),
        }),
    );

    registry.register_scalar(
        "VECTOR_DIMS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "VECTOR_DIMS".to_string(),
            arg_types: vec![DataType::Vector(0)],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| vector::vector_dims(&args[0]),
        }),
    );
}
