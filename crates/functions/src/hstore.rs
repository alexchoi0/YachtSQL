use indexmap::IndexMap;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

pub fn hstore_from_arrays(keys: &Value, values: &Value) -> Result<Value> {
    if keys.is_null() || values.is_null() {
        return Ok(Value::null());
    }

    let keys_array = keys.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: keys.data_type().to_string(),
    })?;

    let values_array = values.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: values.data_type().to_string(),
    })?;

    if keys_array.len() != values_array.len() {
        return Err(Error::invalid_query(format!(
            "Keys and values arrays must have the same length (keys: {}, values: {})",
            keys_array.len(),
            values_array.len()
        )));
    }

    let mut map = IndexMap::new();

    for (key_val, value_val) in keys_array.iter().zip(values_array.iter()) {
        if key_val.is_null() {
            return Err(Error::invalid_query(
                "HSTORE keys cannot be NULL".to_string(),
            ));
        }

        let key = key_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "TEXT".to_string(),
            actual: key_val.data_type().to_string(),
        })?;

        let value = if value_val.is_null() {
            None
        } else {
            Some(
                value_val
                    .as_str()
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "TEXT".to_string(),
                        actual: value_val.data_type().to_string(),
                    })?
                    .to_string(),
            )
        };

        if map.insert(key.to_string(), value).is_some() {
            return Err(Error::invalid_query(format!(
                "Duplicate key in HSTORE: {}",
                key
            )));
        }
    }

    Ok(Value::hstore(map))
}

pub fn hstore_from_text(text: &Value) -> Result<Value> {
    if text.is_null() {
        return Ok(Value::null());
    }

    let s = text.as_str().ok_or_else(|| Error::TypeMismatch {
        expected: "TEXT".to_string(),
        actual: text.data_type().to_string(),
    })?;

    let map = parse_hstore_text(s)?;
    Ok(Value::hstore(map))
}

fn parse_hstore_text(s: &str) -> Result<IndexMap<String, Option<String>>> {
    let mut map = IndexMap::new();
    let s = s.trim();

    if s.is_empty() {
        return Ok(map);
    }

    let mut pos = 0;
    let chars: Vec<char> = s.chars().collect();

    while pos < chars.len() {
        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }

        if pos >= chars.len() {
            break;
        }

        let key = if chars[pos] == '"' {
            pos += 1;
            let start = pos;
            while pos < chars.len() && chars[pos] != '"' {
                if chars[pos] == '\\' && pos + 1 < chars.len() {
                    pos += 2;
                } else {
                    pos += 1;
                }
            }
            if pos >= chars.len() {
                return Err(Error::invalid_query("Unterminated quoted key".to_string()));
            }
            let key_str: String = chars[start..pos].iter().collect();
            pos += 1;
            unescape(&key_str)
        } else {
            let start = pos;
            while pos < chars.len() && chars[pos] != '=' && !chars[pos].is_whitespace() {
                pos += 1;
            }
            chars[start..pos].iter().collect()
        };

        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }

        if pos + 1 >= chars.len() || chars[pos] != '=' || chars[pos + 1] != '>' {
            return Err(Error::invalid_query(format!(
                "Expected '=>' after key '{}', found '{}'",
                key,
                if pos < chars.len() {
                    chars[pos].to_string()
                } else {
                    "EOF".to_string()
                }
            )));
        }
        pos += 2;

        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }

        if pos >= chars.len() {
            return Err(Error::invalid_query(
                "Expected value after '=>'".to_string(),
            ));
        }

        let value = if chars[pos] == '"' {
            pos += 1;
            let start = pos;
            while pos < chars.len() && chars[pos] != '"' {
                if chars[pos] == '\\' && pos + 1 < chars.len() {
                    pos += 2;
                } else {
                    pos += 1;
                }
            }
            if pos >= chars.len() {
                return Err(Error::invalid_query(
                    "Unterminated quoted value".to_string(),
                ));
            }
            let val_str: String = chars[start..pos].iter().collect();
            pos += 1;
            Some(unescape(&val_str))
        } else {
            let start = pos;
            while pos < chars.len() && chars[pos] != ',' && !chars[pos].is_whitespace() {
                pos += 1;
            }
            let val_str: String = chars[start..pos].iter().collect();
            if val_str.to_uppercase() == "NULL" {
                None
            } else {
                Some(val_str)
            }
        };

        map.insert(key, value);

        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }

        if pos < chars.len() {
            if chars[pos] == ',' {
                pos += 1;
            } else {
                return Err(Error::invalid_query(format!(
                    "Expected ',' or end of string, found '{}'",
                    chars[pos]
                )));
            }
        }
    }

    Ok(map)
}

fn unescape(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                result.push(next);
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }
    result
}

pub fn hstore_get(hstore: &Value, key: &Value) -> Result<Value> {
    if hstore.is_null() || key.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    let key_str = key.as_str().ok_or_else(|| Error::TypeMismatch {
        expected: "TEXT".to_string(),
        actual: key.data_type().to_string(),
    })?;

    match map.get(key_str) {
        Some(Some(val)) => Ok(Value::string(val.clone())),
        Some(None) => Ok(Value::null()),
        None => Ok(Value::null()),
    }
}

pub fn hstore_exists(hstore: &Value, key: &Value) -> Result<Value> {
    if hstore.is_null() || key.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    let key_str = key.as_str().ok_or_else(|| Error::TypeMismatch {
        expected: "TEXT".to_string(),
        actual: key.data_type().to_string(),
    })?;

    Ok(Value::bool_val(map.contains_key(key_str)))
}

pub fn hstore_exists_all(hstore: &Value, keys: &Value) -> Result<Value> {
    if hstore.is_null() || keys.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    let keys_array = keys.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: keys.data_type().to_string(),
    })?;

    for key_val in keys_array.iter() {
        if key_val.is_null() {
            continue;
        }

        let key_str = key_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "TEXT".to_string(),
            actual: key_val.data_type().to_string(),
        })?;

        if !map.contains_key(key_str) {
            return Ok(Value::bool_val(false));
        }
    }

    Ok(Value::bool_val(true))
}

pub fn hstore_exists_any(hstore: &Value, keys: &Value) -> Result<Value> {
    if hstore.is_null() || keys.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    let keys_array = keys.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: keys.data_type().to_string(),
    })?;

    for key_val in keys_array.iter() {
        if key_val.is_null() {
            continue;
        }

        let key_str = key_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "TEXT".to_string(),
            actual: key_val.data_type().to_string(),
        })?;

        if map.contains_key(key_str) {
            return Ok(Value::bool_val(true));
        }
    }

    Ok(Value::bool_val(false))
}

pub fn hstore_concat(left: &Value, right: &Value) -> Result<Value> {
    if left.is_null() {
        return Ok(right.clone());
    }
    if right.is_null() {
        return Ok(left.clone());
    }

    let left_map = left.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: left.data_type().to_string(),
    })?;

    let right_map = right.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: right.data_type().to_string(),
    })?;

    let mut result = (*left_map).clone();
    for (k, v) in right_map.iter() {
        result.insert(k.clone(), v.clone());
    }

    Ok(Value::hstore(result))
}

pub fn hstore_delete_key(hstore: &Value, key: &Value) -> Result<Value> {
    if hstore.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    if key.is_null() {
        return Ok(hstore.clone());
    }

    let key_str = key.as_str().ok_or_else(|| Error::TypeMismatch {
        expected: "TEXT".to_string(),
        actual: key.data_type().to_string(),
    })?;

    let mut result = (*map).clone();
    result.shift_remove(key_str);

    Ok(Value::hstore(result))
}

pub fn hstore_delete_keys(hstore: &Value, keys: &Value) -> Result<Value> {
    if hstore.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    if keys.is_null() {
        return Ok(hstore.clone());
    }

    let keys_array = keys.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: keys.data_type().to_string(),
    })?;

    let mut result = (*map).clone();

    for key_val in keys_array.iter() {
        if key_val.is_null() {
            continue;
        }

        let key_str = key_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "TEXT".to_string(),
            actual: key_val.data_type().to_string(),
        })?;

        result.shift_remove(key_str);
    }

    Ok(Value::hstore(result))
}

pub fn hstore_contains(left: &Value, right: &Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::null());
    }

    let left_map = left.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: left.data_type().to_string(),
    })?;

    let right_map = if let Some(map) = right.as_hstore() {
        map.clone()
    } else if let Some(s) = right.as_str() {
        parse_hstore_text(s)?
    } else {
        return Err(Error::TypeMismatch {
            expected: "HSTORE or STRING".to_string(),
            actual: right.data_type().to_string(),
        });
    };

    for (k, v) in right_map.iter() {
        match left_map.get(k) {
            Some(left_val) => {
                if left_val != v {
                    return Ok(Value::bool_val(false));
                }
            }
            None => return Ok(Value::bool_val(false)),
        }
    }

    Ok(Value::bool_val(true))
}

pub fn hstore_contained_by(left: &Value, right: &Value) -> Result<Value> {
    hstore_contains(right, left)
}

pub fn hstore_akeys(hstore: &Value) -> Result<Value> {
    if hstore.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    let keys: Vec<Value> = map.keys().map(|k| Value::string(k.clone())).collect();

    Ok(Value::array(keys))
}

pub fn hstore_avals(hstore: &Value) -> Result<Value> {
    if hstore.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    let values: Vec<Value> = map
        .values()
        .map(|v| match v {
            Some(s) => Value::string(s.clone()),
            None => Value::null(),
        })
        .collect();

    Ok(Value::array(values))
}

pub fn hstore_to_json(hstore: &Value) -> Result<Value> {
    if hstore.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    let mut json_map = serde_json::Map::new();
    for (k, v) in map.iter() {
        json_map.insert(
            k.clone(),
            match v {
                Some(s) => serde_json::Value::String(s.clone()),
                None => serde_json::Value::Null,
            },
        );
    }

    Ok(Value::json(serde_json::Value::Object(json_map)))
}

pub fn hstore_to_jsonb(hstore: &Value) -> Result<Value> {
    hstore_to_json(hstore)
}

pub fn hstore_to_array(hstore: &Value) -> Result<Value> {
    if hstore.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    let mut result: Vec<Value> = Vec::with_capacity(map.len() * 2);
    for (k, v) in map.iter() {
        result.push(Value::string(k.clone()));
        match v {
            Some(s) => result.push(Value::string(s.clone())),
            None => result.push(Value::null()),
        }
    }

    Ok(Value::array(result))
}

pub fn hstore_to_matrix(hstore: &Value) -> Result<Value> {
    if hstore.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    let rows: Vec<Value> = map
        .iter()
        .map(|(k, v)| {
            let val = match v {
                Some(s) => Value::string(s.clone()),
                None => Value::null(),
            };
            Value::array(vec![Value::string(k.clone()), val])
        })
        .collect();

    Ok(Value::array(rows))
}

pub fn hstore_slice(hstore: &Value, keys: &Value) -> Result<Value> {
    if hstore.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    if keys.is_null() {
        return Ok(Value::hstore(IndexMap::new()));
    }

    let keys_array = keys.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: keys.data_type().to_string(),
    })?;

    let mut result = IndexMap::new();

    for key_val in keys_array.iter() {
        if key_val.is_null() {
            continue;
        }

        let key_str = key_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "TEXT".to_string(),
            actual: key_val.data_type().to_string(),
        })?;

        if let Some(value) = map.get(key_str) {
            result.insert(key_str.to_string(), value.clone());
        }
    }

    Ok(Value::hstore(result))
}

pub fn hstore_defined(hstore: &Value, key: &Value) -> Result<Value> {
    if hstore.is_null() || key.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    let key_str = key.as_str().ok_or_else(|| Error::TypeMismatch {
        expected: "TEXT".to_string(),
        actual: key.data_type().to_string(),
    })?;

    let is_defined = match map.get(key_str) {
        Some(Some(_)) => true,
        Some(None) => false,
        None => false,
    };

    Ok(Value::bool_val(is_defined))
}

pub fn hstore_delete(hstore: &Value, key: &Value) -> Result<Value> {
    hstore_delete_key(hstore, key)
}

pub fn hstore_exist(hstore: &Value, key: &Value) -> Result<Value> {
    hstore_exists(hstore, key)
}

pub fn hstore_delete_hstore(left: &Value, right: &Value) -> Result<Value> {
    if left.is_null() {
        return Ok(Value::null());
    }
    if right.is_null() {
        return Ok(left.clone());
    }

    let left_map = left.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: left.data_type().to_string(),
    })?;

    let right_map = right.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: right.data_type().to_string(),
    })?;

    let result: indexmap::IndexMap<String, Option<String>> = left_map
        .iter()
        .filter(|(key, left_val)| match right_map.get(*key) {
            Some(right_val) => *left_val != right_val,
            None => true,
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    Ok(Value::hstore(result))
}

pub fn hstore_get_values(hstore: &Value, keys: &Value) -> Result<Value> {
    if hstore.is_null() || keys.is_null() {
        return Ok(Value::null());
    }

    let map = hstore.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: hstore.data_type().to_string(),
    })?;

    let key_array = keys.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY<TEXT>".to_string(),
        actual: keys.data_type().to_string(),
    })?;

    let values: Vec<Value> = key_array
        .iter()
        .map(|key_val| {
            if key_val.is_null() {
                Value::null()
            } else if let Some(key_str) = key_val.as_str() {
                match map.get(key_str) {
                    Some(Some(val)) => Value::string(val.clone()),
                    Some(None) | None => Value::null(),
                }
            } else {
                Value::null()
            }
        })
        .collect();

    Ok(Value::array(values))
}

pub fn hstore_equal(left: &Value, right: &Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::null());
    }

    let left_map = left.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: left.data_type().to_string(),
    })?;

    let right_map = right.as_hstore().ok_or_else(|| Error::TypeMismatch {
        expected: "HSTORE".to_string(),
        actual: right.data_type().to_string(),
    })?;

    Ok(Value::bool_val(left_map == right_map))
}
