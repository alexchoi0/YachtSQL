use serde_json::Value as JsonValue;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

use super::conversion::{json_to_sql_scalar_only, json_to_sql_value};
use super::{JsonOnBehavior, JsonValueEvalOptions};
use crate::json::utils::{
    JsonPathMode, extract_and_convert, extract_via_path_array, get_json_value, json_value_to_text,
    parse_json_path, parse_json_path_with_mode,
};

macro_rules! return_null_if_null {
    ($value:expr) => {
        if $value.is_null() {
            return Ok(Value::null());
        }
    };
}

pub fn json_extract(json: &Value, path: &str) -> Result<Value> {
    return_null_if_null!(json);
    let json_val = get_json_value(json)?;
    extract_and_convert(&json_val, path, json_to_sql_value)
}

pub fn json_extract_json(json: &Value, path: &str) -> Result<Value> {
    return_null_if_null!(json);
    let json_val = get_json_value(json)?;

    if let Ok(idx) = path.parse::<usize>() {
        return match &json_val {
            JsonValue::Array(arr) => {
                if idx < arr.len() {
                    Ok(Value::json(arr[idx].clone()))
                } else {
                    Ok(Value::null())
                }
            }
            _ => Ok(Value::null()),
        };
    }

    if !path.contains('.') && !path.starts_with('$') && !path.starts_with('[') {
        return match &json_val {
            JsonValue::Object(obj) => {
                if let Some(value) = obj.get(path) {
                    Ok(Value::json(value.clone()))
                } else {
                    Ok(Value::null())
                }
            }
            _ => Ok(Value::null()),
        };
    }

    let json_path = parse_json_path(path)?;

    let matches = json_path.evaluate(&json_val).map_err(|e| {
        Error::InvalidOperation(format!("Failed to evaluate path '{}': {}", path, e))
    })?;

    if matches.is_empty() {
        return Ok(Value::null());
    }

    if matches.len() == 1 {
        Ok(Value::json(matches[0].clone()))
    } else {
        let aggregated = JsonValue::Array(matches);
        Ok(Value::json(aggregated))
    }
}

pub fn json_value(json: &Value, path: &str) -> Result<Value> {
    json_value_with_options(json, path, &JsonValueEvalOptions::default())
}

pub fn json_value_with_options(
    json: &Value,
    path: &str,
    options: &JsonValueEvalOptions,
) -> Result<Value> {
    return_null_if_null!(json);
    let json_val = get_json_value(json)?;

    let (mode, json_path) = parse_json_path_with_mode(path)?;

    let mut effective_options = options.clone();
    if matches!(mode, JsonPathMode::Strict) {
        effective_options.on_empty = JsonOnBehavior::Error;
        effective_options.on_error = JsonOnBehavior::Error;
    }

    let matches = match json_path.evaluate(&json_val) {
        Ok(matches) => matches,
        Err(e) => {
            return effective_options
                .handle_error(path, format!("Failed to evaluate path '{}': {}", path, e));
        }
    };

    if matches.is_empty() {
        return effective_options.handle_empty(path);
    }

    if matches.len() > 1 {
        return effective_options.handle_error(
            path,
            format!("JSON_VALUE path '{}' produced multiple scalar values", path),
        );
    }

    let node = matches[0].clone();
    let value = match &node {
        JsonValue::Array(_) | JsonValue::Object(_) => {
            return effective_options
                .handle_error(path, "encountered non-scalar JSON value".to_string());
        }

        _ => json_to_sql_scalar_only(&node)?,
    };

    match effective_options.convert_returning(value) {
        Ok(converted) => Ok(converted),
        Err(reason) => effective_options.handle_error(path, reason),
    }
}

pub fn json_value_text(json: &Value, path: &str) -> Result<Value> {
    return_null_if_null!(json);
    let json_val = get_json_value(json)?;

    if let Ok(idx) = path.parse::<usize>() {
        return match &json_val {
            JsonValue::Array(arr) => {
                if idx < arr.len() {
                    json_value_to_text(&arr[idx])
                } else {
                    Ok(Value::null())
                }
            }
            _ => Ok(Value::null()),
        };
    }

    if !path.contains('.') && !path.starts_with('$') && !path.starts_with('[') {
        return match &json_val {
            JsonValue::Object(obj) => {
                if let Some(value) = obj.get(path) {
                    json_value_to_text(value)
                } else {
                    Ok(Value::null())
                }
            }
            _ => Ok(Value::null()),
        };
    }

    extract_and_convert(&json_val, path, json_value_to_text)
}

pub fn json_query(json: &Value, path: &str) -> Result<Value> {
    let extracted = json_extract(json, path)?;

    if extracted.is_null() {
        return Ok(Value::null());
    }

    if let Some(json_val) = extracted.as_json() {
        match json_val {
            JsonValue::Object(_) | JsonValue::Array(_) => Ok(extracted),
            _ => Ok(Value::null()),
        }
    } else {
        Ok(Value::null())
    }
}

pub fn json_extract_path_array(json: &Value, path_array: &str) -> Result<Value> {
    extract_via_path_array(json, path_array, json_to_sql_value)
}

pub fn json_extract_path_array_text(json: &Value, path_array: &str) -> Result<Value> {
    extract_via_path_array(json, path_array, json_value_to_text)
}
