use std::rc::Rc;

use yachtsql_common::types::DataType;

use super::FunctionRegistry;
use crate::interval;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_constructor(registry);
    register_arithmetic(registry);
    register_utility(registry);
}

fn register_constructor(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "MAKE_INTERVAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "MAKE_INTERVAL".to_string(),
            arg_types: vec![
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Float64,
            ],
            return_type: DataType::Interval,
            variadic: false,
            evaluator: |args| {
                let years = args[0].as_i64().unwrap_or(0) as i32;
                let months = args[1].as_i64().unwrap_or(0) as i32;
                let days = args[2].as_i64().unwrap_or(0) as i32;
                let hours = args[3].as_i64().unwrap_or(0) as i32;
                let minutes = args[4].as_i64().unwrap_or(0) as i32;
                let seconds = args[5].as_f64().unwrap_or(0.0);
                interval::make_interval(years, months, days, hours, minutes, seconds)
            },
        }),
    );
}

fn register_arithmetic(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "INTERVAL_ADD".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INTERVAL_ADD".to_string(),
            arg_types: vec![DataType::Interval, DataType::Interval],
            return_type: DataType::Interval,
            variadic: false,
            evaluator: |args| interval::interval_add(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "INTERVAL_SUBTRACT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INTERVAL_SUBTRACT".to_string(),
            arg_types: vec![DataType::Interval, DataType::Interval],
            return_type: DataType::Interval,
            variadic: false,
            evaluator: |args| interval::interval_subtract(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "INTERVAL_MULTIPLY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INTERVAL_MULTIPLY".to_string(),
            arg_types: vec![DataType::Interval, DataType::Float64],
            return_type: DataType::Interval,
            variadic: false,
            evaluator: |args| interval::interval_multiply(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "INTERVAL_DIVIDE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INTERVAL_DIVIDE".to_string(),
            arg_types: vec![DataType::Interval, DataType::Float64],
            return_type: DataType::Interval,
            variadic: false,
            evaluator: |args| interval::interval_divide(&args[0], &args[1]),
        }),
    );

    registry.register_scalar(
        "INTERVAL_NEGATE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INTERVAL_NEGATE".to_string(),
            arg_types: vec![DataType::Interval],
            return_type: DataType::Interval,
            variadic: false,
            evaluator: |args| interval::interval_negate(&args[0]),
        }),
    );
}

fn register_utility(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "JUSTIFY_DAYS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JUSTIFY_DAYS".to_string(),
            arg_types: vec![DataType::Interval],
            return_type: DataType::Interval,
            variadic: false,
            evaluator: |args| interval::justify_days(&args[0]),
        }),
    );

    registry.register_scalar(
        "JUSTIFY_HOURS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JUSTIFY_HOURS".to_string(),
            arg_types: vec![DataType::Interval],
            return_type: DataType::Interval,
            variadic: false,
            evaluator: |args| interval::justify_hours(&args[0]),
        }),
    );

    registry.register_scalar(
        "JUSTIFY_INTERVAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "JUSTIFY_INTERVAL".to_string(),
            arg_types: vec![DataType::Interval],
            return_type: DataType::Interval,
            variadic: false,
            evaluator: |args| interval::justify_interval(&args[0]),
        }),
    );

    registry.register_scalar(
        "INTERVAL_TO_SECONDS".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INTERVAL_TO_SECONDS".to_string(),
            arg_types: vec![DataType::Interval],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| interval::interval_to_seconds(&args[0]),
        }),
    );
}
