mod clickhouse_json;
mod is_json_predicate;
mod is_not_json_predicate;
mod json_array;
mod json_exists;
mod json_extract;
mod json_extract_path;
mod json_keys;
mod json_length;
mod json_object;
mod json_query;
mod json_type;
mod json_value;
mod jsonb_function;
mod parse_json;
mod to_json;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_json_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "JSON_EXTRACT" | "JSON_EXTRACT_JSON" => {
                Self::evaluate_json_extract(args, batch, row_idx)
            }
            "JSON_VALUE" | "JSON_VALUE_TEXT" | "JSON_EXTRACT_SCALAR" => {
                Self::evaluate_json_value(args, batch, row_idx)
            }
            "JSON_QUERY" => Self::evaluate_json_query(args, batch, row_idx),
            "JSON_EXISTS" => Self::evaluate_json_exists(args, batch, row_idx),
            "JSON_ARRAY" | "JSON_BUILD_ARRAY" => Self::evaluate_json_array(args, batch, row_idx),
            "JSON_OBJECT" | "JSON_BUILD_OBJECT" => Self::evaluate_json_object(args, batch, row_idx),
            "TO_JSON" | "TO_JSONB" => Self::evaluate_to_json(args, batch, row_idx),
            "TO_JSON_STRING" => Self::evaluate_to_json_string(args, batch, row_idx),
            "PARSE_JSON" => Self::evaluate_parse_json(args, batch, row_idx),
            "JSON_KEYS" | "JSON_EXTRACT_KEYS" => Self::evaluate_json_keys(args, batch, row_idx),
            "JSON_OBJECT_KEYS" => Self::evaluate_json_object_keys(args, batch, row_idx),
            "JSON_LENGTH" | "JSON_ARRAY_LENGTH" => Self::evaluate_json_length(args, batch, row_idx),
            "JSON_TYPE" | "JSON_TYPEOF" => Self::evaluate_json_type(args, batch, row_idx),
            "JSON_EXTRACT_STRING" => Self::evaluate_json_extract_string(args, batch, row_idx),
            "JSON_EXTRACT_INT" => Self::evaluate_json_extract_int(args, batch, row_idx),
            "JSON_EXTRACT_FLOAT" => Self::evaluate_json_extract_float(args, batch, row_idx),
            "JSON_EXTRACT_BOOL" => Self::evaluate_json_extract_bool(args, batch, row_idx),
            "JSON_EXTRACT_RAW" => Self::evaluate_json_extract_raw(args, batch, row_idx),
            "JSON_EXTRACT_ARRAY_RAW" => Self::evaluate_json_extract_array_raw(args, batch, row_idx),
            "JSON_EXTRACT_KEYS_AND_VALUES" => {
                Self::evaluate_json_extract_keys_and_values(args, batch, row_idx)
            }
            "JSON_HAS" => Self::evaluate_json_has(args, batch, row_idx),
            "JSON_STRIP_NULLS" => Self::evaluate_json_strip_nulls(args, batch, row_idx),
            "JSON_EXTRACT_ARRAY" | "JSON_QUERY_ARRAY" => {
                Self::evaluate_json_extract_array(args, batch, row_idx)
            }
            "JSON_EXTRACT_STRING_ARRAY" | "JSON_VALUE_ARRAY" => {
                Self::evaluate_json_value_array(args, batch, row_idx)
            }
            "JSON_SET" => Self::evaluate_json_set(args, batch, row_idx),
            "JSON_REMOVE" => Self::evaluate_json_remove(args, batch, row_idx),
            "LAX_BOOL" => Self::evaluate_lax_bool(args, batch, row_idx),
            "LAX_INT64" => Self::evaluate_lax_int64(args, batch, row_idx),
            "LAX_FLOAT64" => Self::evaluate_lax_float64(args, batch, row_idx),
            "LAX_STRING" => Self::evaluate_lax_string(args, batch, row_idx),
            "BOOL" => Self::evaluate_strict_bool(args, batch, row_idx),
            "INT64" => Self::evaluate_strict_int64(args, batch, row_idx),
            "FLOAT64" => Self::evaluate_strict_float64(args, batch, row_idx),
            "STRING" => Self::evaluate_strict_string(args, batch, row_idx),
            "IS_JSON_VALUE" | "IS_JSON_ARRAY" | "IS_JSON_OBJECT" | "IS_JSON_SCALAR" => {
                Self::evaluate_is_json_predicate(name, args, batch, row_idx)
            }
            "IS_NOT_JSON_VALUE" | "IS_NOT_JSON_ARRAY" | "IS_NOT_JSON_OBJECT"
            | "IS_NOT_JSON_SCALAR" => {
                Self::evaluate_is_not_json_predicate(name, args, batch, row_idx)
            }
            name if name.starts_with("JSONB_") => {
                Self::evaluate_jsonb_function(name, args, batch, row_idx)
            }
            name if name.starts_with("JSON_EXTRACT_PATH") => {
                Self::evaluate_json_extract_path(name, args, batch, row_idx)
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unknown JSON function: {}",
                name
            ))),
        }
    }

    fn evaluate_json_strip_nulls(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "JSON_STRIP_NULLS requires at least 1 argument".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::json_strip_nulls(&json_val)
    }

    fn evaluate_json_extract_array(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::invalid_query(
                "JSON_EXTRACT_ARRAY requires 1 or 2 arguments".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path = if args.len() > 1 {
            Self::evaluate_expr(&args[1], batch, row_idx)?
                .as_str()
                .map(|s| s.to_string())
        } else {
            None
        };
        yachtsql_functions::json::json_extract_array(&json_val, path.as_deref())
    }

    fn evaluate_json_value_array(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::invalid_query(
                "JSON_VALUE_ARRAY requires 1 or 2 arguments".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path = if args.len() > 1 {
            Self::evaluate_expr(&args[1], batch, row_idx)?
                .as_str()
                .map(|s| s.to_string())
        } else {
            None
        };
        yachtsql_functions::json::json_value_array(&json_val, path.as_deref())
    }

    fn evaluate_json_set(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 3 {
            return Err(Error::invalid_query(
                "JSON_SET requires 3 arguments".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let new_val = Self::evaluate_expr(&args[2], batch, row_idx)?;
        let path_str = path
            .as_str()
            .ok_or_else(|| Error::invalid_query("JSON_SET path must be a string".to_string()))?;
        yachtsql_functions::json::json_set(&json_val, path_str, &new_val)
    }

    fn evaluate_json_remove(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "JSON_REMOVE requires 2 arguments".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let path_str = path
            .as_str()
            .ok_or_else(|| Error::invalid_query("JSON_REMOVE path must be a string".to_string()))?;
        yachtsql_functions::json::json_remove(&json_val, path_str)
    }

    fn evaluate_lax_bool(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "LAX_BOOL requires 1 argument".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::lax_bool(&json_val)
    }

    fn evaluate_lax_int64(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "LAX_INT64 requires 1 argument".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::lax_int64(&json_val)
    }

    fn evaluate_lax_float64(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "LAX_FLOAT64 requires 1 argument".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::lax_float64(&json_val)
    }

    fn evaluate_lax_string(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "LAX_STRING requires 1 argument".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::lax_string(&json_val)
    }

    fn evaluate_strict_bool(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query("BOOL requires 1 argument".to_string()));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::strict_bool(&json_val)
    }

    fn evaluate_strict_int64(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "INT64 requires 1 argument".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::strict_int64(&json_val)
    }

    fn evaluate_strict_float64(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "FLOAT64 requires 1 argument".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::strict_float64(&json_val)
    }

    fn evaluate_strict_string(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "STRING requires 1 argument".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::strict_string(&json_val)
    }
}
