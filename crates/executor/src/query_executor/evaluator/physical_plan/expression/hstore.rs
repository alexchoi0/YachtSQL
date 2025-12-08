use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_hstore_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "HSTORE_EXISTS" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("HSTORE_EXISTS requires 2 arguments"));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let key = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if value.as_json().is_some() {
                    yachtsql_functions::json::predicates::jsonb_key_exists(&value, &key)
                } else {
                    yachtsql_functions::hstore::hstore_exists(&value, &key)
                }
            }
            "HSTORE_EXISTS_ALL" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "HSTORE_EXISTS_ALL requires 2 arguments",
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let keys = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if value.as_json().is_some() {
                    yachtsql_functions::json::predicates::jsonb_keys_all_exist(&value, &keys)
                } else {
                    yachtsql_functions::hstore::hstore_exists_all(&value, &keys)
                }
            }
            "HSTORE_EXISTS_ANY" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "HSTORE_EXISTS_ANY requires 2 arguments",
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let keys = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if value.as_json().is_some() {
                    yachtsql_functions::json::predicates::jsonb_keys_any_exist(&value, &keys)
                } else {
                    yachtsql_functions::hstore::hstore_exists_any(&value, &keys)
                }
            }
            "HSTORE_CONCAT" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("HSTORE_CONCAT requires 2 arguments"));
                }
                let left = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let right = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_concat(&left, &right)
            }
            "HSTORE_DELETE" | "HSTORE_DELETE_KEY" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("HSTORE_DELETE requires 2 arguments"));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let key = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_delete_key(&hstore, &key)
            }
            "HSTORE_DELETE_KEYS" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "HSTORE_DELETE_KEYS requires 2 arguments",
                    ));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let keys = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_delete_keys(&hstore, &keys)
            }
            "HSTORE_DELETE_HSTORE" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "HSTORE_DELETE_HSTORE requires 2 arguments",
                    ));
                }
                let left = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let right = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_delete_hstore(&left, &right)
            }
            "HSTORE_CONTAINS" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("HSTORE_CONTAINS requires 2 arguments"));
                }
                let left = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let right = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_contains(&left, &right)
            }
            "HSTORE_CONTAINED_BY" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "HSTORE_CONTAINED_BY requires 2 arguments",
                    ));
                }
                let left = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let right = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_contained_by(&left, &right)
            }
            "HSTORE_AKEYS" | "AKEYS" | "SKEYS" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("AKEYS requires 1 argument"));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_akeys(&hstore)
            }
            "HSTORE_AVALS" | "AVALS" | "SVALS" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("AVALS requires 1 argument"));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_avals(&hstore)
            }
            "HSTORE_DEFINED" | "DEFINED" | "EXIST" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("DEFINED requires 2 arguments"));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let key = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_defined(&hstore, &key)
            }
            "HSTORE_TO_JSON" | "HSTORE_TO_JSONB" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("HSTORE_TO_JSON requires 1 argument"));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_to_json(&hstore)
            }
            "HSTORE_TO_ARRAY" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("HSTORE_TO_ARRAY requires 1 argument"));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_to_array(&hstore)
            }
            "HSTORE_TO_MATRIX" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("HSTORE_TO_MATRIX requires 1 argument"));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_to_matrix(&hstore)
            }
            "HSTORE_SLICE" | "SLICE" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("HSTORE_SLICE requires 2 arguments"));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let keys = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_slice(&hstore, &keys)
            }
            "DELETE" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("DELETE requires 2 arguments"));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let key = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_delete_key(&hstore, &key)
            }
            "HSTORE" => {
                if args.len() == 2 {
                    let keys = Self::evaluate_expr(&args[0], batch, row_idx)?;
                    let values = Self::evaluate_expr(&args[1], batch, row_idx)?;
                    yachtsql_functions::hstore::hstore_from_arrays(&keys, &values)
                } else if args.len() == 1 {
                    let text = Self::evaluate_expr(&args[0], batch, row_idx)?;
                    yachtsql_functions::hstore::hstore_from_text(&text)
                } else {
                    Err(Error::invalid_query(
                        "HSTORE requires 1 or 2 arguments".to_string(),
                    ))
                }
            }
            "HSTORE_GET" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("HSTORE_GET requires 2 arguments"));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let key = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_get(&hstore, &key)
            }
            "HSTORE_GET_VALUES" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "HSTORE_GET_VALUES requires 2 arguments",
                    ));
                }
                let hstore = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let keys = Self::evaluate_expr(&args[1], batch, row_idx)?;
                yachtsql_functions::hstore::hstore_get_values(&hstore, &keys)
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unknown hstore function: {}",
                name
            ))),
        }
    }
}
