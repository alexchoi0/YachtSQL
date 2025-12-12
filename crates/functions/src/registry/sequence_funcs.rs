use std::rc::Rc;

use yachtsql_common::error::Error;
use yachtsql_common::types::DataType;

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "NEXTVAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NEXTVAL".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |_args| {
                Err(Error::InternalError(
                    "NEXTVAL must be evaluated by executor".to_string(),
                ))
            },
        }),
    );

    registry.register_scalar(
        "CURRVAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "CURRVAL".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |_args| {
                Err(Error::InternalError(
                    "CURRVAL must be evaluated by executor".to_string(),
                ))
            },
        }),
    );

    registry.register_scalar(
        "SETVAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SETVAL".to_string(),
            arg_types: vec![DataType::String, DataType::Int64],
            return_type: DataType::Int64,
            variadic: true,
            evaluator: |_args| {
                Err(Error::InternalError(
                    "SETVAL must be evaluated by executor".to_string(),
                ))
            },
        }),
    );

    registry.register_scalar(
        "LASTVAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "LASTVAL".to_string(),
            arg_types: vec![],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |_args| {
                Err(Error::InternalError(
                    "LASTVAL must be evaluated by executor".to_string(),
                ))
            },
        }),
    );
}
