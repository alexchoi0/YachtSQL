use std::collections::HashSet;

use serde_json::{Map, Value as JsonValue};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::conversion::sql_to_json_fallible;

pub fn json_array(values: Vec<Value>) -> Result<Value> {
    let mut json_array = Vec::new();

    for value in values {
        let json_val = sql_to_json_fallible(&value)?;
        json_array.push(json_val);
    }

    Ok(Value::json(JsonValue::Array(json_array)))
}

pub fn json_object(pairs: Vec<(String, Value)>) -> Result<Value> {
    let mut json_map = Map::new();
    let mut seen_keys = HashSet::new();

    for (key, value) in pairs {
        if seen_keys.contains(&key) {
            return Err(Error::InvalidOperation(format!(
                "Duplicate key in JSON_OBJECT: '{}'",
                key
            )));
        }
        seen_keys.insert(key.clone());

        let json_val = sql_to_json_fallible(&value)?;
        json_map.insert(key, json_val);
    }

    Ok(Value::json(JsonValue::Object(json_map)))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_json_array_empty() {
        let result = json_array(vec![]).unwrap();
        assert_eq!(result, Value::json(json!([])));
    }

    #[test]
    fn test_json_array_simple() {
        let values = vec![Value::int64(1), Value::int64(2), Value::int64(3)];
        let result = json_array(values).unwrap();
        assert_eq!(result, Value::json(json!([1, 2, 3])));
    }

    #[test]
    fn test_json_array_mixed_types() {
        let values = vec![
            Value::int64(42),
            Value::string("hello".to_string()),
            Value::bool_val(true),
            Value::null(),
        ];
        let result = json_array(values).unwrap();
        assert_eq!(result, Value::json(json!([42, "hello", true, null])));
    }

    #[test]
    fn test_json_object_empty() {
        let result = json_object(vec![]).unwrap();
        assert_eq!(result, Value::json(json!({})));
    }

    #[test]
    fn test_json_object_simple() {
        let pairs = vec![
            ("name".to_string(), Value::string("Alice".to_string())),
            ("age".to_string(), Value::int64(30)),
        ];
        let result = json_object(pairs).unwrap();

        if let Some(json_val) = result.as_json() {
            if let JsonValue::Object(map) = json_val {
                assert_eq!(map.get("name"), Some(&json!("Alice")));
                assert_eq!(map.get("age"), Some(&json!(30)));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON value");
        }
    }

    #[test]
    fn test_json_object_duplicate_keys_error() {
        let pairs = vec![
            ("key".to_string(), Value::int64(1)),
            ("key".to_string(), Value::int64(2)),
        ];
        let result = json_object(pairs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Duplicate key"));
    }

    #[test]
    fn test_sql_value_to_json_types() {
        assert_eq!(sql_to_json_fallible(&Value::int64(42)).unwrap(), json!(42));
        assert_eq!(
            sql_to_json_fallible(&Value::float64(std::f64::consts::E)).unwrap(),
            json!(std::f64::consts::E)
        );
        assert_eq!(
            sql_to_json_fallible(&Value::string("test".to_string())).unwrap(),
            json!("test")
        );
        assert_eq!(
            sql_to_json_fallible(&Value::bool_val(true)).unwrap(),
            json!(true)
        );
        assert_eq!(sql_to_json_fallible(&Value::null()).unwrap(), json!(null));
    }
}
