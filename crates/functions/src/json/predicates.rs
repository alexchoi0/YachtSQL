use serde_json::Value as JsonValue;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

use super::{DEFAULT_MAX_DEPTH, DEFAULT_MAX_SIZE, JsonPath, parse_json_with_limits};
use crate::json::utils::get_json_value;

macro_rules! return_null_if_null {
    ($value:expr) => {
        if $value.is_null() {
            return Ok(Value::null());
        }
    };
}

pub fn json_exists(json: &Value, path: &str) -> Result<Value> {
    return_null_if_null!(json);

    let json_val = get_json_value(json)?;

    let json_path = JsonPath::parse(path)
        .map_err(|e| Error::InvalidOperation(format!("Invalid JSON path '{}': {}", path, e)))?;

    match json_path.evaluate(&json_val) {
        Ok(matches) if !matches.is_empty() => Ok(Value::bool_val(true)),
        Ok(_) => Ok(Value::bool_val(false)),
        Err(e) => Err(Error::InvalidOperation(format!(
            "Failed to evaluate path '{}': {}",
            path, e
        ))),
    }
}

pub fn jsonb_path_exists(json: &Value, path: &str) -> Result<Value> {
    return_null_if_null!(json);

    let json_val = get_json_value(json)?;
    let json_path = JsonPath::parse(path).map_err(|e| {
        Error::InvalidOperation(format!("Invalid jsonpath expression '{}': {}", path, e))
    })?;

    let matches = json_path.evaluate(&json_val).map_err(|e| {
        Error::InvalidOperation(format!("Invalid jsonpath expression '{}': {}", path, e))
    })?;

    Ok(Value::bool_val(!matches.is_empty()))
}

pub fn jsonb_path_match(json: &Value, path: &str) -> Result<Value> {
    jsonb_path_exists(json, path)
}

pub fn jsonb_path_query_first(json: &Value, path: &str) -> Result<Value> {
    return_null_if_null!(json);

    let json_val = get_json_value(json)?;
    let json_path = JsonPath::parse(path).map_err(|e| {
        Error::InvalidOperation(format!("Invalid jsonpath expression '{}': {}", path, e))
    })?;

    let matches = json_path.evaluate(&json_val).map_err(|e| {
        Error::InvalidOperation(format!("Invalid jsonpath expression '{}': {}", path, e))
    })?;

    if let Some(first) = matches.into_iter().next() {
        Ok(Value::json(first))
    } else {
        Ok(Value::null())
    }
}

fn json_value_matches_type<F>(value: &Value, type_checker: F) -> Result<Value>
where
    F: Fn(&JsonValue) -> bool,
{
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(json_val) = value.as_json() {
        return Ok(Value::bool_val(type_checker(json_val)));
    }

    if let Some(s) = value.as_str() {
        match parse_json_with_limits(s, DEFAULT_MAX_DEPTH, DEFAULT_MAX_SIZE) {
            Ok(json_val) => return Ok(Value::bool_val(type_checker(&json_val))),
            Err(_) => return Ok(Value::bool_val(false)),
        }
    }

    Ok(Value::bool_val(false))
}

fn negate_json_predicate(result: Result<Value>) -> Result<Value> {
    let value = result?;
    if value.is_null() {
        return Ok(Value::null());
    }
    if let Some(b) = value.as_bool() {
        return Ok(Value::bool_val(!b));
    }
    unreachable!("JSON predicates only return Bool or Null")
}

pub fn is_json_value(value: &Value) -> Result<Value> {
    json_value_matches_type(value, |_| true)
}

pub fn is_json_array(value: &Value) -> Result<Value> {
    json_value_matches_type(value, |json_val| matches!(json_val, JsonValue::Array(_)))
}

pub fn is_json_object(value: &Value) -> Result<Value> {
    json_value_matches_type(value, |json_val| matches!(json_val, JsonValue::Object(_)))
}

pub fn is_json_scalar(value: &Value) -> Result<Value> {
    json_value_matches_type(value, |json_val| {
        matches!(
            json_val,
            JsonValue::String(_) | JsonValue::Number(_) | JsonValue::Bool(_) | JsonValue::Null
        )
    })
}

pub fn is_not_json_value(value: &Value) -> Result<Value> {
    negate_json_predicate(is_json_value(value))
}

pub fn is_not_json_array(value: &Value) -> Result<Value> {
    negate_json_predicate(is_json_array(value))
}

pub fn is_not_json_object(value: &Value) -> Result<Value> {
    negate_json_predicate(is_json_object(value))
}

pub fn is_not_json_scalar(value: &Value) -> Result<Value> {
    negate_json_predicate(is_json_scalar(value))
}

pub fn jsonb_key_exists(json: &Value, key: &Value) -> Result<Value> {
    return_null_if_null!(json);

    let json_val = get_json_value(json)?;
    let key_str = key
        .as_str()
        .ok_or_else(|| Error::invalid_query("Key must be a string"))?;

    let exists = match &json_val {
        JsonValue::Object(obj) => obj.contains_key(key_str),
        _ => false,
    };

    Ok(Value::bool_val(exists))
}

pub fn jsonb_keys_all_exist(json: &Value, keys: &Value) -> Result<Value> {
    return_null_if_null!(json);

    let json_val = get_json_value(json)?;
    let keys_array = keys
        .as_array()
        .ok_or_else(|| Error::invalid_query("Keys must be an array"))?;

    let result = match &json_val {
        JsonValue::Object(obj) => keys_array.iter().all(|k| {
            if let Some(key_str) = k.as_str() {
                obj.contains_key(key_str)
            } else {
                false
            }
        }),
        _ => false,
    };

    Ok(Value::bool_val(result))
}

pub fn jsonb_keys_any_exist(json: &Value, keys: &Value) -> Result<Value> {
    return_null_if_null!(json);

    let json_val = get_json_value(json)?;
    let keys_array = keys
        .as_array()
        .ok_or_else(|| Error::invalid_query("Keys must be an array"))?;

    let result = match &json_val {
        JsonValue::Object(obj) => keys_array.iter().any(|k| {
            if let Some(key_str) = k.as_str() {
                obj.contains_key(key_str)
            } else {
                false
            }
        }),
        _ => false,
    };

    Ok(Value::bool_val(result))
}
