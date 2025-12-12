use std::rc::Rc;

use yachtsql_common::types::DataType;

use super::FunctionRegistry;
use crate::geometric;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_constructors(registry);
    register_property_functions(registry);
    register_distance_functions(registry);
    register_containment_functions(registry);
}

fn register_constructors(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "POINT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "POINT".to_string(),
            arg_types: vec![DataType::Float64, DataType::Float64],
            return_type: DataType::Point,
            variadic: false,
            evaluator: |args| geometric::point_constructor(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "BOX".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "BOX".to_string(),
            arg_types: vec![DataType::Point, DataType::Point],
            return_type: DataType::PgBox,
            variadic: false,
            evaluator: |args| geometric::box_constructor(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "CIRCLE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CIRCLE".to_string(),
            arg_types: vec![DataType::Point, DataType::Float64],
            return_type: DataType::Circle,
            variadic: false,
            evaluator: |args| geometric::circle_constructor(&args[0], &args[1]),
        }),
    );
}

fn register_property_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "AREA".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "AREA".to_string(),
            arg_types: vec![DataType::PgBox],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| geometric::area(&args[0]),
        }),
    );

    registry.register_scalar(
        "CENTER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CENTER".to_string(),
            arg_types: vec![DataType::PgBox],
            return_type: DataType::Point,
            variadic: false,
            evaluator: |args| geometric::center(&args[0]),
        }),
    );

    registry.register_scalar(
        "DIAMETER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DIAMETER".to_string(),
            arg_types: vec![DataType::Circle],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| geometric::diameter(&args[0]),
        }),
    );

    registry.register_scalar(
        "RADIUS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RADIUS".to_string(),
            arg_types: vec![DataType::Circle],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| geometric::radius(&args[0]),
        }),
    );

    registry.register_scalar(
        "WIDTH".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "WIDTH".to_string(),
            arg_types: vec![DataType::PgBox],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| geometric::width(&args[0]),
        }),
    );

    registry.register_scalar(
        "HEIGHT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HEIGHT".to_string(),
            arg_types: vec![DataType::PgBox],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| geometric::height(&args[0]),
        }),
    );
}

fn register_distance_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "DISTANCE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "DISTANCE".to_string(),
            arg_types: vec![DataType::Point, DataType::Point],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| geometric::distance(&args[0], &args[1]),
        }),
    );
}

fn register_containment_functions(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "CONTAINS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CONTAINS".to_string(),
            arg_types: vec![DataType::PgBox, DataType::Point],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| geometric::contains(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "CONTAINED_BY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CONTAINED_BY".to_string(),
            arg_types: vec![DataType::Point, DataType::PgBox],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| geometric::contained_by(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "OVERLAPS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "OVERLAPS".to_string(),
            arg_types: vec![DataType::PgBox, DataType::PgBox],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| geometric::overlaps(&args[0], &args[1]),
        }),
    );
}
