use serde_json::Value as JsonValue;
use yachtsql_common::types::Value;

use super::conversion::{sql_to_json_infallible, sql_value_to_json_key};

pub fn sql_value_to_json(value: &Value) -> JsonValue {
    sql_to_json_infallible(value)
}

pub fn json_key_to_string(value: &Value) -> Option<String> {
    sql_value_to_json_key(value)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    #[test]
    fn test_sql_value_to_json_null() {
        let result = sql_value_to_json(&Value::null());
        assert!(result.is_null());
    }

    #[test]
    fn test_sql_value_to_json_bool() {
        let result = sql_value_to_json(&Value::bool_val(true));
        assert_eq!(result.as_bool(), Some(true));

        let result = sql_value_to_json(&Value::bool_val(false));
        assert_eq!(result.as_bool(), Some(false));
    }

    #[test]
    fn test_sql_value_to_json_int64() {
        let result = sql_value_to_json(&Value::int64(42));
        assert_eq!(result.as_i64(), Some(42));

        let result = sql_value_to_json(&Value::int64(-100));
        assert_eq!(result.as_i64(), Some(-100));
    }

    #[test]
    fn test_sql_value_to_json_float64() {
        let result = sql_value_to_json(&Value::float64(std::f64::consts::E));
        assert!((result.as_f64().unwrap() - std::f64::consts::E).abs() < 0.00001);
    }

    #[test]
    fn test_sql_value_to_json_float64_infinity() {
        let result = sql_value_to_json(&Value::float64(f64::INFINITY));
        assert!(result.is_null());

        let result = sql_value_to_json(&Value::float64(f64::NEG_INFINITY));
        assert!(result.is_null());

        let result = sql_value_to_json(&Value::float64(f64::NAN));
        assert!(result.is_null());
    }

    #[test]
    fn test_sql_value_to_json_string() {
        let result = sql_value_to_json(&Value::string("Hello, World!".to_string()));
        assert_eq!(result.as_str(), Some("Hello, World!"));
    }

    #[test]
    fn test_sql_value_to_json_date() {
        let date = NaiveDate::from_ymd_opt(2024, 10, 25).unwrap();
        let result = sql_value_to_json(&Value::date(date));
        assert_eq!(result.as_str(), Some("2024-10-25"));
    }

    #[test]
    fn test_json_key_to_string_null() {
        let result = json_key_to_string(&Value::null());
        assert_eq!(result, None);
    }

    #[test]
    fn test_json_key_to_string_bool() {
        let result = json_key_to_string(&Value::bool_val(true));
        assert_eq!(result, Some("true".to_string()));

        let result = json_key_to_string(&Value::bool_val(false));
        assert_eq!(result, Some("false".to_string()));
    }

    #[test]
    fn test_json_key_to_string_int64() {
        let result = json_key_to_string(&Value::int64(123));
        assert_eq!(result, Some("123".to_string()));
    }

    #[test]
    fn test_json_key_to_string_string() {
        let result = json_key_to_string(&Value::string("key".to_string()));
        assert_eq!(result, Some("key".to_string()));
    }

    #[test]
    fn test_sql_value_to_json_array() {
        let arr = Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]);
        let result = sql_value_to_json(&arr);
        assert!(result.is_array());
        let json_arr = result.as_array().unwrap();
        assert_eq!(json_arr.len(), 3);
        assert_eq!(json_arr[0].as_i64(), Some(1));
    }
}
