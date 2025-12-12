use serde_json::{Map, Number, Value as JsonValue};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

type JsonObject = Map<String, JsonValue>;
type JsonArray = Vec<JsonValue>;

pub fn sql_to_json_infallible(value: &Value) -> JsonValue {
    if value.is_null() {
        return JsonValue::Null;
    }

    if let Some(b) = value.as_bool() {
        return JsonValue::Bool(b);
    }

    if let Some(i) = value.as_i64() {
        return JsonValue::Number(Number::from(i));
    }

    if let Some(f) = value.as_f64() {
        if f.is_finite() {
            if let Some(num) = Number::from_f64(f) {
                return JsonValue::Number(num);
            }
        }
        return JsonValue::Null;
    }

    if let Some(s) = value.as_str() {
        return JsonValue::String(s.to_string());
    }

    if let Some(d) = value.as_date() {
        return JsonValue::String(d.format("%Y-%m-%d").to_string());
    }

    if let Some(dt) = value.as_datetime() {
        return JsonValue::String(dt.to_rfc3339());
    }

    if let Some(dt) = value.as_timestamp() {
        return JsonValue::String(dt.to_rfc3339());
    }

    if let Some(t) = value.as_time() {
        return JsonValue::String(t.format("%H:%M:%S").to_string());
    }

    if let Some(d) = value.as_numeric() {
        return d
            .to_string()
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null);
    }

    if let Some(b) = value.as_bytes() {
        return JsonValue::String(format!("{:?}", b));
    }

    if let Some(map) = value.as_struct() {
        let obj: JsonObject = map
            .iter()
            .map(|(k, v)| (k.clone(), sql_to_json_infallible(v)))
            .collect();
        return JsonValue::Object(obj);
    }

    if let Some(arr) = value.as_array() {
        let json_arr: JsonArray = arr.iter().map(sql_to_json_infallible).collect();
        return JsonValue::Array(json_arr);
    }

    if let Some(wkt) = value.as_geography() {
        return JsonValue::String(wkt.to_string());
    }

    if let Some(j) = value.as_json() {
        return j.clone();
    }

    if let Some(u) = value.as_uuid() {
        return JsonValue::String(u.to_string());
    }

    JsonValue::Null
}

pub fn sql_to_json_fallible(value: &Value) -> Result<JsonValue> {
    if value.is_null() {
        return Ok(JsonValue::Null);
    }

    if let Some(b) = value.as_bool() {
        return Ok(JsonValue::Bool(b));
    }

    if let Some(i) = value.as_i64() {
        return Ok(JsonValue::Number(Number::from(i)));
    }

    if let Some(f) = value.as_f64() {
        if !f.is_finite() {
            return Err(Error::InvalidOperation(format!(
                "Cannot convert non-finite float to JSON: {}",
                f
            )));
        }
        return Number::from_f64(f)
            .map(JsonValue::Number)
            .ok_or_else(|| Error::InvalidOperation(format!("Invalid float value: {}", f)));
    }

    if let Some(s) = value.as_str() {
        return Ok(JsonValue::String(s.to_string()));
    }

    if let Some(d) = value.as_date() {
        return Ok(JsonValue::String(d.to_string()));
    }

    if let Some(dt) = value.as_datetime() {
        return Ok(JsonValue::String(dt.to_rfc3339()));
    }

    if let Some(dt) = value.as_timestamp() {
        return Ok(JsonValue::String(dt.to_rfc3339()));
    }

    if let Some(t) = value.as_time() {
        return Ok(JsonValue::String(t.to_string()));
    }

    if let Some(n) = value.as_numeric() {
        return Ok(JsonValue::String(n.to_string()));
    }

    if let Some(b) = value.as_bytes() {
        return Ok(JsonValue::String(format!("{:?}", b)));
    }

    if let Some(map) = value.as_struct() {
        let mut obj = JsonObject::new();
        for (k, v) in map.iter() {
            obj.insert(k.clone(), sql_to_json_fallible(v)?);
        }
        return Ok(JsonValue::Object(obj));
    }

    if let Some(arr) = value.as_array() {
        let json_arr: Result<JsonArray> = arr.iter().map(sql_to_json_fallible).collect();
        return Ok(JsonValue::Array(json_arr?));
    }

    if let Some(wkt) = value.as_geography() {
        return Ok(JsonValue::String(wkt.to_string()));
    }

    if let Some(j) = value.as_json() {
        return Ok(j.clone());
    }

    if let Some(u) = value.as_uuid() {
        return Ok(JsonValue::String(u.to_string()));
    }

    Err(Error::invalid_query(
        "Unsupported value type in JSON context".to_string(),
    ))
}

pub fn json_to_sql_value(json: &JsonValue) -> Result<Value> {
    match json {
        JsonValue::Null => Ok(Value::null()),

        other => Ok(Value::json(other.clone())),
    }
}

pub fn json_to_sql_scalar_only(json: &JsonValue) -> Result<Value> {
    match json {
        JsonValue::String(s) => Ok(Value::string(s.clone())),
        JsonValue::Number(n) => Ok(Value::string(n.to_string())),
        JsonValue::Bool(b) => Ok(Value::string(b.to_string())),
        JsonValue::Null => Ok(Value::string("null".to_string())),
        JsonValue::Object(_) | JsonValue::Array(_) => Ok(Value::null()),
    }
}

#[deprecated(
    since = "0.1.0",
    note = "Use json_to_sql_value or json_to_sql_scalar_only for clarity"
)]
pub fn json_to_sql_scalar(json: &JsonValue) -> Result<Value> {
    json_to_sql_value(json)
}

pub fn sql_value_to_parsed_json(value: &Value) -> Result<JsonValue> {
    if let Some(j) = value.as_json() {
        return Ok(j.clone());
    }

    if let Some(s) = value.as_str() {
        return serde_json::from_str(s)
            .map_err(|e| Error::InvalidOperation(format!("Failed to parse JSON: {}", e)));
    }

    Err(Error::InvalidOperation(
        "Expected JSON value or string".to_string(),
    ))
}

pub fn sql_value_to_json_key(value: &Value) -> Option<String> {
    if value.is_null() {
        return None;
    }

    if let Some(b) = value.as_bool() {
        return Some(b.to_string());
    }

    if let Some(i) = value.as_i64() {
        return Some(i.to_string());
    }

    if let Some(f) = value.as_f64() {
        return Some(f.to_string());
    }

    if let Some(s) = value.as_str() {
        return Some(s.to_string());
    }

    if let Some(d) = value.as_date() {
        return Some(d.format("%Y-%m-%d").to_string());
    }

    if let Some(dt) = value.as_datetime() {
        return Some(dt.to_rfc3339());
    }

    if let Some(dt) = value.as_timestamp() {
        return Some(dt.to_rfc3339());
    }

    if let Some(t) = value.as_time() {
        return Some(t.format("%H:%M:%S").to_string());
    }

    if let Some(d) = value.as_numeric() {
        return Some(d.to_string());
    }

    if let Some(b) = value.as_bytes() {
        return Some(format!("{:?}", b));
    }

    if let Some(wkt) = value.as_geography() {
        return Some(wkt.to_string());
    }

    if value.as_struct().is_some() || value.as_array().is_some() || value.as_json().is_some() {
        return Some(format!("{:?}", value));
    }

    if let Some(u) = value.as_uuid() {
        return Some(u.to_string());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_to_json_infallible_basic_types() {
        assert_eq!(sql_to_json_infallible(&Value::null()), JsonValue::Null);
        assert_eq!(
            sql_to_json_infallible(&Value::bool_val(true)),
            JsonValue::Bool(true)
        );
        assert_eq!(
            sql_to_json_infallible(&Value::int64(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            sql_to_json_infallible(&Value::string("test".into())),
            serde_json::json!("test")
        );
    }

    #[test]
    fn test_sql_to_json_infallible_invalid_float() {
        assert_eq!(
            sql_to_json_infallible(&Value::float64(f64::INFINITY)),
            JsonValue::Null
        );
        assert_eq!(
            sql_to_json_infallible(&Value::float64(f64::NEG_INFINITY)),
            JsonValue::Null
        );
        assert_eq!(
            sql_to_json_infallible(&Value::float64(f64::NAN)),
            JsonValue::Null
        );
    }

    #[test]
    fn test_sql_to_json_fallible_invalid_float() {
        assert!(sql_to_json_fallible(&Value::float64(f64::INFINITY)).is_err());
        assert!(sql_to_json_fallible(&Value::float64(f64::NEG_INFINITY)).is_err());
        assert!(sql_to_json_fallible(&Value::float64(f64::NAN)).is_err());
    }

    #[test]
    fn test_sql_to_json_fallible_valid_float() {
        let result = sql_to_json_fallible(&Value::float64(std::f64::consts::E)).unwrap();
        assert!((result.as_f64().unwrap() - std::f64::consts::E).abs() < 0.00001);
    }

    #[test]
    fn test_json_to_sql_value() {
        let str_val = JsonValue::String("test".into());
        assert_eq!(
            json_to_sql_value(&str_val).unwrap(),
            Value::json(str_val.clone())
        );

        let bool_val = JsonValue::Bool(true);
        assert_eq!(
            json_to_sql_value(&bool_val).unwrap(),
            Value::json(bool_val.clone())
        );

        let num_val = serde_json::json!(42);
        assert_eq!(
            json_to_sql_value(&num_val).unwrap(),
            Value::json(num_val.clone())
        );

        let float_val = serde_json::json!(std::f64::consts::E);
        assert_eq!(
            json_to_sql_value(&float_val).unwrap(),
            Value::json(float_val.clone())
        );

        assert_eq!(json_to_sql_value(&JsonValue::Null).unwrap(), Value::null());

        let obj = serde_json::json!({"key": "value"});
        assert_eq!(json_to_sql_value(&obj).unwrap(), Value::json(obj.clone()));

        let arr = serde_json::json!([1, 2, 3]);
        assert_eq!(json_to_sql_value(&arr).unwrap(), Value::json(arr.clone()));
    }

    #[test]
    fn test_json_to_sql_scalar_only() {
        assert_eq!(
            json_to_sql_scalar_only(&JsonValue::String("test".into())).unwrap(),
            Value::string("test".into())
        );
        assert_eq!(
            json_to_sql_scalar_only(&JsonValue::Bool(true)).unwrap(),
            Value::string("true".into())
        );
        assert_eq!(
            json_to_sql_scalar_only(&serde_json::json!(42)).unwrap(),
            Value::string("42".into())
        );

        let obj = serde_json::json!({"key": "value"});
        assert_eq!(json_to_sql_scalar_only(&obj).unwrap(), Value::null());

        let arr = serde_json::json!([1, 2, 3]);
        assert_eq!(json_to_sql_scalar_only(&arr).unwrap(), Value::null());
    }

    #[test]
    fn test_sql_value_to_json_key() {
        assert_eq!(sql_value_to_json_key(&Value::null()), None);
        assert_eq!(
            sql_value_to_json_key(&Value::string("key".into())),
            Some("key".into())
        );
        assert_eq!(
            sql_value_to_json_key(&Value::int64(123)),
            Some("123".into())
        );
        assert_eq!(
            sql_value_to_json_key(&Value::bool_val(true)),
            Some("true".into())
        );
    }

    #[test]
    fn test_sql_value_to_parsed_json() {
        let json_val = serde_json::json!({"key": "value"});
        let result = sql_value_to_parsed_json(&Value::json(json_val.clone())).unwrap();
        assert_eq!(result, json_val);

        let json_str = r#"{"key": "value"}"#;
        let result = sql_value_to_parsed_json(&Value::string(json_str.into())).unwrap();
        assert_eq!(result, serde_json::json!({"key": "value"}));

        let invalid = sql_value_to_parsed_json(&Value::string("not json".into()));
        assert!(invalid.is_err());

        let invalid = sql_value_to_parsed_json(&Value::int64(123));
        assert!(invalid.is_err());
    }

    #[test]
    fn test_recursive_array_conversion() {
        let arr = Value::array(vec![
            Value::int64(1),
            Value::string("test".into()),
            Value::bool_val(true),
        ]);

        let result = sql_to_json_infallible(&arr);
        assert!(result.is_array());
        assert_eq!(result, serde_json::json!([1, "test", true]));
    }
}
