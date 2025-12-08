use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use serde_json::{Map, Value as JsonValue};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

use super::conversion::sql_to_json_infallible;
use crate::json::extract::json_extract;
use crate::json::utils::{JsonbPathElement, get_json_value, parse_jsonb_path};

macro_rules! return_null_if_null {
    ($value:expr) => {
        if $value.is_null() {
            return Ok(Value::null());
        }
    };
}

pub fn jsonb_contains(container: &Value, subset: &Value) -> Result<Value> {
    return_null_if_null!(container);
    return_null_if_null!(subset);

    let container_json = get_json_value(container)?;
    let subset_json = get_json_value(subset)?;

    Ok(Value::bool_val(jsonb_contains_recursive(
        &container_json,
        &subset_json,
    )))
}

pub(crate) fn jsonb_contains_recursive(container: &JsonValue, subset: &JsonValue) -> bool {
    if container == subset {
        return true;
    }

    match (container, subset) {
        (JsonValue::Object(container_map), JsonValue::Object(subset_map)) => {
            subset_map.iter().all(|(key, subset_val)| {
                container_map
                    .get(key)
                    .map(|container_val| jsonb_contains_recursive(container_val, subset_val))
                    .unwrap_or(false)
            })
        }
        (JsonValue::Array(container_arr), JsonValue::Array(subset_arr)) => {
            subset_arr.iter().all(|subset_val| {
                container_arr
                    .iter()
                    .any(|container_val| jsonb_contains_recursive(container_val, subset_val))
            })
        }
        _ => false,
    }
}

pub fn jsonb_concat(left: &Value, right: &Value) -> Result<Value> {
    return_null_if_null!(left);
    return_null_if_null!(right);

    let left_json = get_json_value(left)?;
    let right_json = get_json_value(right)?;
    Ok(Value::json(*jsonb_concat_internal(&left_json, &right_json)))
}

pub(crate) fn jsonb_concat_internal(left: &JsonValue, right: &JsonValue) -> Box<JsonValue> {
    let result = match (left, right) {
        (JsonValue::Null, _) => right.clone(),
        (_, JsonValue::Null) => left.clone(),
        (JsonValue::Object(lhs), JsonValue::Object(rhs)) => {
            let mut merged = lhs.clone();
            for (key, value) in rhs {
                merged.insert(key.clone(), value.clone());
            }
            JsonValue::Object(merged)
        }
        (JsonValue::Array(lhs), JsonValue::Array(rhs)) => {
            let mut merged = lhs.clone();
            merged.extend(rhs.iter().cloned());
            JsonValue::Array(merged)
        }
        (JsonValue::Array(lhs), other) => {
            let mut merged = lhs.clone();
            merged.push(other.clone());
            JsonValue::Array(merged)
        }
        (other, JsonValue::Array(rhs)) => {
            let mut merged = Vec::with_capacity(rhs.len() + 1);
            merged.push(other.clone());
            merged.extend(rhs.iter().cloned());
            JsonValue::Array(merged)
        }
        _ => JsonValue::Array(vec![left.clone(), right.clone()]),
    };
    Box::new(result)
}

pub fn jsonb_delete(target: &Value, key_or_index: &Value) -> Result<Value> {
    return_null_if_null!(target);
    return_null_if_null!(key_or_index);

    let json_val = get_json_value(target)?;

    if let Some(key) = coerce_delete_key(key_or_index) {
        return Ok(Value::json(delete_object_key(&json_val, &key)));
    }

    if let Some(index) = coerce_delete_index(key_or_index)? {
        return Ok(Value::json(delete_array_index(&json_val, index)));
    }

    Err(Error::invalid_query(format!(
        "jsonb_delete expects TEXT key or INT index, got {:?}",
        key_or_index.data_type()
    )))
}

pub(crate) fn delete_object_key(json: &JsonValue, key: &str) -> JsonValue {
    match json {
        JsonValue::Object(map) => {
            let mut updated = map.clone();
            updated.remove(key);
            JsonValue::Object(updated)
        }
        _ => json.clone(),
    }
}

pub(crate) fn delete_array_index(json: &JsonValue, index: usize) -> JsonValue {
    match json {
        JsonValue::Array(values) => {
            if index >= values.len() {
                return JsonValue::Array(values.clone());
            }
            let mut updated = values.clone();
            updated.remove(index);
            JsonValue::Array(updated)
        }
        _ => json.clone(),
    }
}

pub(crate) fn coerce_delete_key(value: &Value) -> Option<String> {
    if let Some(s) = value.as_str() {
        return Some(s.to_string());
    }

    if let Some(j) = value.as_json() {
        if let JsonValue::String(s) = j {
            return Some(s.clone());
        }
    }

    None
}

pub(crate) fn coerce_delete_index(value: &Value) -> Result<Option<usize>> {
    if let Some(i) = value.as_i64() {
        if i >= 0 {
            return Ok(Some(i as usize));
        }
    }

    if let Some(decimal) = value.as_numeric() {
        if let Some(idx) = decimal.to_i64()
            && idx >= 0
        {
            return Ok(Some(idx as usize));
        }
        return Err(Error::InvalidOperation(format!(
            "JSON array index '{}' must be a non-negative integer",
            decimal
        )));
    }

    if let Some(f) = value.as_f64() {
        if f.fract().abs() > f64::EPSILON || f < 0.0 {
            return Err(Error::InvalidOperation(format!(
                "JSON array index '{}' must be a non-negative integer",
                f
            )));
        }
        return Ok(Some(f as usize));
    }

    Ok(None)
}

pub fn jsonb_delete_path(target: &Value, path: &Value) -> Result<Value> {
    return_null_if_null!(target);
    return_null_if_null!(path);

    let json_val = get_json_value(target)?;
    let path_tokens = coerce_delete_path_tokens(path)?;

    if path_tokens.is_empty() {
        return Ok(Value::json(json_val));
    }

    Ok(Value::json(apply_jsonb_delete_path(
        &json_val,
        &path_tokens,
    )))
}

fn coerce_delete_path_tokens(path: &Value) -> Result<Vec<JsonbPathElement>> {
    if let Some(s) = path.as_str() {
        return parse_jsonb_path(s);
    }

    if let Some(j) = path.as_json() {
        if let JsonValue::String(s) = j {
            return parse_jsonb_path(s);
        }
    }

    Err(Error::invalid_query(
        "jsonb_delete_path expects TEXT path literal (e.g. '{a,b}')".to_string(),
    ))
}

pub(crate) fn apply_jsonb_delete_path(current: &JsonValue, path: &[JsonbPathElement]) -> JsonValue {
    if path.is_empty() {
        return current.clone();
    }

    match &path[0] {
        JsonbPathElement::Key(key) => {
            if let JsonValue::Object(map) = current {
                let mut updated = map.clone();
                if path.len() == 1 {
                    updated.remove(key);
                } else if let Some(child) = updated.get(key) {
                    let next = apply_jsonb_delete_path(child, &path[1..]);
                    updated.insert(key.clone(), next);
                }
                JsonValue::Object(updated)
            } else {
                current.clone()
            }
        }
        JsonbPathElement::Index(index) => {
            if let JsonValue::Array(values) = current {
                if *index >= values.len() {
                    return JsonValue::Array(values.clone());
                }
                let mut updated = values.clone();
                if path.len() == 1 {
                    updated.remove(*index);
                } else {
                    let replacement = apply_jsonb_delete_path(&values[*index], &path[1..]);
                    updated[*index] = replacement;
                }
                JsonValue::Array(updated)
            } else {
                current.clone()
            }
        }
    }
}

pub fn jsonb_set(
    target: &Value,
    path: &Value,
    new_value: &Value,
    create_missing: Option<bool>,
) -> Result<Value> {
    return_null_if_null!(target);

    let base_json = get_json_value(target)?;

    let path_text = path
        .as_str()
        .ok_or_else(|| Error::invalid_query("jsonb_set path argument must be TEXT"))?;
    let tokens = parse_jsonb_path(path_text)?;

    let create_missing = create_missing.unwrap_or(true);
    let replacement = coerce_to_jsonb(new_value)?;

    let updated = apply_jsonb_set(&base_json, &tokens, &replacement, create_missing)?;
    Ok(Value::json(updated))
}

fn coerce_to_jsonb(value: &Value) -> Result<JsonValue> {
    if let Some(j) = value.as_json() {
        return Ok(j.clone());
    }

    if let Some(s) = value.as_str() {
        if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
            return Ok(parsed);
        } else {
            return Ok(JsonValue::String(s.to_string()));
        }
    }

    Ok(sql_to_json_infallible(value))
}

pub(crate) fn apply_jsonb_set(
    current: &JsonValue,
    path: &[JsonbPathElement],
    new_value: &JsonValue,
    create_missing: bool,
) -> Result<JsonValue> {
    if path.is_empty() {
        return Ok(new_value.clone());
    }

    match &path[0] {
        JsonbPathElement::Key(key) => {
            let mut object = match current {
                JsonValue::Object(map) => map.clone(),
                JsonValue::Null if create_missing => Map::new(),
                _ if create_missing => Map::new(),
                _ => {
                    return Err(Error::InvalidOperation(format!(
                        "jsonb_set expected object at path component '{}'",
                        key
                    )));
                }
            };

            let existing = object.get(key).cloned().unwrap_or(JsonValue::Null);
            let updated_value = if path.len() == 1 {
                new_value.clone()
            } else {
                if existing.is_null() && !create_missing {
                    return Err(Error::InvalidOperation(format!(
                        "jsonb_set missing key '{}' with create_missing=false",
                        key
                    )));
                }
                apply_jsonb_set(&existing, &path[1..], new_value, create_missing)?
            };

            object.insert(key.clone(), updated_value);
            Ok(JsonValue::Object(object))
        }
        JsonbPathElement::Index(idx) => {
            let mut array = match current {
                JsonValue::Array(arr) => arr.clone(),
                JsonValue::Null if create_missing => Vec::new(),
                _ if create_missing => Vec::new(),
                _ => {
                    return Err(Error::InvalidOperation(format!(
                        "jsonb_set expected array at index {}",
                        idx
                    )));
                }
            };

            if *idx >= array.len() {
                if !create_missing {
                    return Err(Error::InvalidOperation(format!(
                        "jsonb_set index {} out of bounds with create_missing=false",
                        idx
                    )));
                }
                while array.len() <= *idx {
                    array.push(JsonValue::Null);
                }
            }

            if path.len() == 1 {
                array[*idx] = new_value.clone();
            } else {
                let existing = array[*idx].clone();
                let updated_child =
                    apply_jsonb_set(&existing, &path[1..], new_value, create_missing)?;
                array[*idx] = updated_child;
            }

            Ok(JsonValue::Array(array))
        }
    }
}

pub fn json_keys(json: &Value, path: Option<String>) -> Result<Value> {
    return_null_if_null!(json);

    let json_val = if let Some(ref p) = path {
        let extracted = json_extract(json, p.as_str())?;
        if extracted.is_null() {
            return Ok(Value::null());
        }
        if let Some(j) = extracted.as_json() {
            j.clone()
        } else {
            return Ok(Value::null());
        }
    } else {
        get_json_value(json)?
    };

    match &json_val {
        JsonValue::Object(map) => {
            let keys: Vec<JsonValue> = map.keys().map(|k| JsonValue::String(k.clone())).collect();
            Ok(Value::json(JsonValue::Array(keys)))
        }
        _ => Ok(Value::null()),
    }
}

pub fn json_length(json: &Value) -> Result<Value> {
    return_null_if_null!(json);

    let json_val = get_json_value(json)?;

    match &json_val {
        JsonValue::Array(arr) => Ok(Value::int64(arr.len() as i64)),
        JsonValue::Object(map) => Ok(Value::int64(map.keys().len() as i64)),
        JsonValue::String(_) | JsonValue::Number(_) | JsonValue::Bool(_) => Ok(Value::int64(1)),
        JsonValue::Null => Ok(Value::null()),
    }
}

pub fn json_type(json: &Value) -> Result<Value> {
    return_null_if_null!(json);

    let json_val = get_json_value(json)?;

    let type_name = match &json_val {
        JsonValue::Object(_) => "object",
        JsonValue::Array(_) => "array",
        JsonValue::String(_) => "string",
        JsonValue::Number(_) => "number",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Null => "null",
    };

    Ok(Value::string(type_name.to_string()))
}

pub fn jsonb_insert(
    target: &Value,
    path: &Value,
    new_value: &Value,
    insert_after: bool,
) -> Result<Value> {
    return_null_if_null!(target);

    let base_json = get_json_value(target)?;

    let path_text = path
        .as_str()
        .ok_or_else(|| Error::invalid_query("jsonb_insert path argument must be TEXT"))?;
    let tokens = parse_jsonb_path(path_text)?;

    let replacement = coerce_to_jsonb(new_value)?;

    let updated = apply_jsonb_insert(&base_json, &tokens, &replacement, insert_after)?;
    Ok(Value::json(updated))
}

fn apply_jsonb_insert(
    current: &JsonValue,
    path: &[JsonbPathElement],
    new_value: &JsonValue,
    insert_after: bool,
) -> Result<JsonValue> {
    if path.is_empty() {
        return Ok(new_value.clone());
    }

    match &path[0] {
        JsonbPathElement::Key(key) => {
            let mut object = match current {
                JsonValue::Object(map) => map.clone(),
                _ => Map::new(),
            };

            if path.len() == 1 {
                object.insert(key.clone(), new_value.clone());
            } else {
                let existing = object.get(key).cloned().unwrap_or(JsonValue::Null);
                let updated_value =
                    apply_jsonb_insert(&existing, &path[1..], new_value, insert_after)?;
                object.insert(key.clone(), updated_value);
            }
            Ok(JsonValue::Object(object))
        }
        JsonbPathElement::Index(idx) => {
            let mut array = match current {
                JsonValue::Array(arr) => arr.clone(),
                _ => Vec::new(),
            };

            if path.len() == 1 {
                let insert_idx = if insert_after {
                    (*idx + 1).min(array.len())
                } else {
                    (*idx).min(array.len())
                };
                array.insert(insert_idx, new_value.clone());
            } else if *idx < array.len() {
                let existing = array[*idx].clone();
                let updated_child =
                    apply_jsonb_insert(&existing, &path[1..], new_value, insert_after)?;
                array[*idx] = updated_child;
            }

            Ok(JsonValue::Array(array))
        }
    }
}

pub fn jsonb_pretty(json: &Value) -> Result<Value> {
    return_null_if_null!(json);

    let json_val = get_json_value(json)?;

    let mut buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
    json_val
        .serialize(&mut ser)
        .map_err(|e| Error::ExecutionError(format!("Failed to pretty print JSON: {}", e)))?;

    let pretty_str = String::from_utf8(buf)
        .map_err(|e| Error::ExecutionError(format!("Invalid UTF-8 in JSON output: {}", e)))?;

    Ok(Value::string(pretty_str))
}

pub fn json_strip_nulls(json: &Value) -> Result<Value> {
    return_null_if_null!(json);

    let json_val = get_json_value(json)?;
    let stripped = strip_nulls_recursive(&json_val);

    Ok(Value::json(stripped))
}

fn strip_nulls_recursive(json: &JsonValue) -> JsonValue {
    match json {
        JsonValue::Object(map) => {
            let mut new_map = Map::new();
            for (key, value) in map {
                if !value.is_null() {
                    new_map.insert(key.clone(), strip_nulls_recursive(value));
                }
            }
            JsonValue::Object(new_map)
        }
        JsonValue::Array(arr) => JsonValue::Array(arr.iter().map(strip_nulls_recursive).collect()),
        _ => json.clone(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn jsonb_concat_merges_objects_and_arrays() {
        let left = Value::json(json!({"a": 1, "b": 2}));
        let right = Value::json(json!({"b": 3, "c": 4}));
        let merged = jsonb_concat(&left, &right).unwrap();
        assert_eq!(merged, Value::json(json!({"a":1,"b":3,"c":4})));

        let left_arr = Value::json(json!([1, 2]));
        let right_arr = Value::json(json!([3]));
        let appended = jsonb_concat(&left_arr, &right_arr).unwrap();
        assert_eq!(appended, Value::json(json!([1, 2, 3])));
    }

    #[test]
    fn jsonb_delete_removes_keys_and_indices() {
        let doc = Value::json(json!({"a":1,"b":2}));
        let result = jsonb_delete(&doc, &Value::string("a".into())).unwrap();
        assert_eq!(result, Value::json(json!({"b":2})));

        let arr = Value::json(json!([10, 20, 30]));
        let result = jsonb_delete(&arr, &Value::int64(1)).unwrap();
        assert_eq!(result, Value::json(json!([10, 30])));
    }

    #[test]
    fn jsonb_delete_path_removes_nested_values() {
        let doc = Value::json(json!({"a":{"b":{"c":1}},"keep":true}));
        let path = Value::string("{a,b}".into());
        let result = jsonb_delete_path(&doc, &path).unwrap();
        assert_eq!(result, Value::json(json!({"a":{},"keep":true})));
    }
}
