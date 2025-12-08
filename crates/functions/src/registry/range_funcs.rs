use std::rc::Rc;

use yachtsql_core::types::{DataType, RangeType};

use super::FunctionRegistry;
use crate::range;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_constructors(registry);
    register_operations(registry);
    register_utility(registry);
}

fn register_constructors(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "INT4RANGE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INT4RANGE".to_string(),
            arg_types: vec![DataType::Int64, DataType::Int64, DataType::String],
            return_type: DataType::Range(RangeType::Int4Range),
            variadic: false,
            evaluator: |args| {
                let lower = if args[0].is_null() {
                    None
                } else {
                    Some(args[0].clone())
                };
                let upper = if args[1].is_null() {
                    None
                } else {
                    Some(args[1].clone())
                };
                let bounds = args[2].as_str().unwrap_or("[)");
                range::make_range(RangeType::Int4Range, lower, upper, bounds)
            },
        }),
    );

    registry.register_scalar(
        "INT8RANGE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INT8RANGE".to_string(),
            arg_types: vec![DataType::Int64, DataType::Int64, DataType::String],
            return_type: DataType::Range(RangeType::Int8Range),
            variadic: false,
            evaluator: |args| {
                let lower = if args[0].is_null() {
                    None
                } else {
                    Some(args[0].clone())
                };
                let upper = if args[1].is_null() {
                    None
                } else {
                    Some(args[1].clone())
                };
                let bounds = args[2].as_str().unwrap_or("[)");
                range::make_range(RangeType::Int8Range, lower, upper, bounds)
            },
        }),
    );
}

fn register_operations(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "RANGE_CONTAINS_ELEM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_CONTAINS_ELEM".to_string(),
            arg_types: vec![DataType::Range(RangeType::Int4Range), DataType::Int64],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_contains(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "RANGE_CONTAINS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_CONTAINS".to_string(),
            arg_types: vec![
                DataType::Range(RangeType::Int4Range),
                DataType::Range(RangeType::Int4Range),
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_contains_range(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "RANGE_OVERLAPS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_OVERLAPS".to_string(),
            arg_types: vec![
                DataType::Range(RangeType::Int4Range),
                DataType::Range(RangeType::Int4Range),
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_overlaps(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "RANGE_UNION".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_UNION".to_string(),
            arg_types: vec![
                DataType::Range(RangeType::Int4Range),
                DataType::Range(RangeType::Int4Range),
            ],
            return_type: DataType::Range(RangeType::Int4Range),
            variadic: false,
            evaluator: |args| range::range_union(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "RANGE_INTERSECTION".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_INTERSECTION".to_string(),
            arg_types: vec![
                DataType::Range(RangeType::Int4Range),
                DataType::Range(RangeType::Int4Range),
            ],
            return_type: DataType::Range(RangeType::Int4Range),
            variadic: false,
            evaluator: |args| range::range_intersection(&args[0], &args[1]),
        }),
    );
}

fn register_utility(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "ISEMPTY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ISEMPTY".to_string(),
            arg_types: vec![DataType::Range(RangeType::Int4Range)],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_is_empty(&args[0]),
        }),
    );

    registry.register_scalar(
        "RANGE_ISEMPTY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_ISEMPTY".to_string(),
            arg_types: vec![DataType::Range(RangeType::Int4Range)],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_is_empty(&args[0]),
        }),
    );

    registry.register_scalar(
        "RANGE_LOWER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_LOWER".to_string(),
            arg_types: vec![DataType::Range(RangeType::Int4Range)],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| range::range_lower(&args[0]),
        }),
    );

    registry.register_scalar(
        "RANGE_UPPER".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_UPPER".to_string(),
            arg_types: vec![DataType::Range(RangeType::Int4Range)],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| range::range_upper(&args[0]),
        }),
    );

    registry.register_scalar(
        "LOWER_INC".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LOWER_INC".to_string(),
            arg_types: vec![DataType::Range(RangeType::Int4Range)],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_lower_inc(&args[0]),
        }),
    );

    registry.register_scalar(
        "UPPER_INC".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "UPPER_INC".to_string(),
            arg_types: vec![DataType::Range(RangeType::Int4Range)],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_upper_inc(&args[0]),
        }),
    );

    registry.register_scalar(
        "LOWER_INF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LOWER_INF".to_string(),
            arg_types: vec![DataType::Range(RangeType::Int4Range)],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_lower_inf(&args[0]),
        }),
    );

    registry.register_scalar(
        "UPPER_INF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "UPPER_INF".to_string(),
            arg_types: vec![DataType::Range(RangeType::Int4Range)],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_upper_inf(&args[0]),
        }),
    );

    registry.register_scalar(
        "RANGE_MERGE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_MERGE".to_string(),
            arg_types: vec![
                DataType::Range(RangeType::Int4Range),
                DataType::Range(RangeType::Int4Range),
            ],
            return_type: DataType::Range(RangeType::Int4Range),
            variadic: false,
            evaluator: |args| range::range_merge(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "RANGE_ADJACENT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_ADJACENT".to_string(),
            arg_types: vec![
                DataType::Range(RangeType::Int4Range),
                DataType::Range(RangeType::Int4Range),
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_adjacent(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "RANGE_STRICTLY_LEFT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_STRICTLY_LEFT".to_string(),
            arg_types: vec![
                DataType::Range(RangeType::Int4Range),
                DataType::Range(RangeType::Int4Range),
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_strictly_left(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "RANGE_STRICTLY_RIGHT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_STRICTLY_RIGHT".to_string(),
            arg_types: vec![
                DataType::Range(RangeType::Int4Range),
                DataType::Range(RangeType::Int4Range),
            ],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| range::range_strictly_right(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "RANGE_DIFFERENCE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANGE_DIFFERENCE".to_string(),
            arg_types: vec![
                DataType::Range(RangeType::Int4Range),
                DataType::Range(RangeType::Int4Range),
            ],
            return_type: DataType::Range(RangeType::Int4Range),
            variadic: false,
            evaluator: |args| range::range_difference(&args[0], &args[1]),
        }),
    );
}
