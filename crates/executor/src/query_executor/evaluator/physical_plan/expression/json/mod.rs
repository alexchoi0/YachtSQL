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
            "JSON_VALUE" | "JSON_VALUE_TEXT" => Self::evaluate_json_value(args, batch, row_idx),
            "JSON_QUERY" => Self::evaluate_json_query(args, batch, row_idx),
            "JSON_EXISTS" => Self::evaluate_json_exists(args, batch, row_idx),
            "JSON_ARRAY" => Self::evaluate_json_array(args, batch, row_idx),
            "JSON_OBJECT" => Self::evaluate_json_object(args, batch, row_idx),
            "TO_JSON" | "TO_JSON_STRING" => Self::evaluate_to_json(args, batch, row_idx),
            "PARSE_JSON" => Self::evaluate_parse_json(args, batch, row_idx),
            "JSON_KEYS" => Self::evaluate_json_keys(args, batch, row_idx),
            "JSON_LENGTH" => Self::evaluate_json_length(args, batch, row_idx),
            "JSON_TYPE" => Self::evaluate_json_type(args, batch, row_idx),
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
}
