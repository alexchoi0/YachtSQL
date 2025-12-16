use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_ir::FunctionName;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_introspection_function(
        name: &FunctionName,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            FunctionName::ToTypeName => Self::eval_to_type_name(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown introspection function: {}",
                name.as_str()
            ))),
        }
    }

    pub(super) fn evaluate_security_function(
        name: &FunctionName,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            FunctionName::SessionUser | FunctionName::CurrentUser => {
                Ok(Value::string("default".to_string()))
            }
            FunctionName::SafeConvertBytesToString => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "SAFE_CONVERT_BYTES_TO_STRING requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(bytes) = val.as_bytes() {
                    match String::from_utf8(bytes.to_vec()) {
                        Ok(s) => Ok(Value::string(s)),
                        Err(_) => Ok(Value::null()),
                    }
                } else if let Some(s) = val.as_str() {
                    Ok(Value::string(s.to_string()))
                } else {
                    Ok(Value::null())
                }
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unknown security function: {}",
                name.as_str()
            ))),
        }
    }

    fn eval_to_type_name(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "toTypeName requires 1 argument".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let type_name = Self::value_type_name(&val);
        Ok(Value::string(type_name.to_string()))
    }

    fn value_type_name(val: &Value) -> &'static str {
        if val.is_null() {
            "Nullable(Nothing)"
        } else if val.is_int64() {
            "Int64"
        } else if val.is_float64() {
            "Float64"
        } else if val.is_bool() {
            "UInt8"
        } else if val.is_string() {
            "String"
        } else if val.is_numeric() {
            "Decimal"
        } else if val.is_array() {
            "Array"
        } else if val.as_struct().is_some() {
            "Tuple"
        } else if val.is_map() {
            "Map"
        } else if val.as_uuid().is_some() {
            "UUID"
        } else if val.is_json() {
            "JSON"
        } else if val.as_date().is_some() {
            "Date"
        } else if val.as_time().is_some() || val.as_timestamp().is_some() {
            "DateTime"
        } else if val.as_bytes().is_some() {
            "String"
        } else {
            "Unknown"
        }
    }
}
