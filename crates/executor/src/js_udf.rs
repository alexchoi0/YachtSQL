use mini_v8::{MiniV8, Value as JsValue, Values};
use yachtsql_common::types::Value;

pub fn evaluate_js_function(
    js_code: &str,
    param_names: &[String],
    args: &[Value],
) -> Result<Value, String> {
    let mv8 = MiniV8::new();

    let js_args: Vec<JsValue> = args
        .iter()
        .map(|v| value_to_js(&mv8, v))
        .collect::<Result<Vec<_>, _>>()?;

    let wrapper_code = build_wrapper_function(js_code, param_names);

    let js_func: mini_v8::Function = mv8
        .eval(wrapper_code.as_str())
        .map_err(|e| format!("Failed to compile JavaScript function: {}", e))?;

    let result: JsValue = js_func
        .call_method(JsValue::Undefined, Values::from_vec(js_args))
        .map_err(|e| format!("JavaScript execution error: {}", e))?;

    js_to_value(&mv8, result)
}

fn build_wrapper_function(js_code: &str, param_names: &[String]) -> String {
    let params = param_names.join(", ");
    let trimmed = js_code.trim();

    if trimmed.starts_with("function") || trimmed.contains("=>") {
        trimmed.to_string()
    } else {
        format!("({}) => {{ {} }}", params, trimmed)
    }
}

fn value_to_js(mv8: &MiniV8, value: &Value) -> Result<JsValue, String> {
    match value {
        Value::Null => Ok(JsValue::Null),
        Value::Bool(b) => Ok(JsValue::Boolean(*b)),
        Value::Int64(n) => Ok(JsValue::Number(*n as f64)),
        Value::Float64(f) => Ok(JsValue::Number(f.into_inner())),
        Value::Numeric(d) => {
            let f: f64 = d.to_string().parse().unwrap_or(0.0);
            Ok(JsValue::Number(f))
        }
        Value::String(s) => Ok(JsValue::String(mv8.create_string(s))),
        Value::Bytes(b) => {
            let arr = mv8.create_array();
            for (i, byte) in b.iter().enumerate() {
                arr.set(i as u32, JsValue::Number(*byte as f64))
                    .map_err(|e| format!("Failed to set array element: {}", e))?;
            }
            Ok(JsValue::Array(arr))
        }
        Value::Date(d) => Ok(JsValue::String(mv8.create_string(&d.to_string()))),
        Value::Time(t) => Ok(JsValue::String(mv8.create_string(&t.to_string()))),
        Value::DateTime(dt) => Ok(JsValue::String(mv8.create_string(&dt.to_string()))),
        Value::Timestamp(ts) => Ok(JsValue::String(mv8.create_string(&ts.to_rfc3339()))),
        Value::Json(j) => json_to_js(mv8, j),
        Value::Array(arr) => {
            let js_arr = mv8.create_array();
            for (i, v) in arr.iter().enumerate() {
                let js_v = value_to_js(mv8, v)?;
                js_arr
                    .set(i as u32, js_v)
                    .map_err(|e| format!("Failed to set array element: {}", e))?;
            }
            Ok(JsValue::Array(js_arr))
        }
        Value::Struct(fields) => {
            let obj = mv8.create_object();
            for (name, v) in fields {
                let js_v = value_to_js(mv8, v)?;
                obj.set(name.as_str(), js_v)
                    .map_err(|e| format!("Failed to set object property: {}", e))?;
            }
            Ok(JsValue::Object(obj))
        }
        Value::Geography(g) => Ok(JsValue::String(mv8.create_string(g))),
        Value::Interval(i) => {
            let obj = mv8.create_object();
            obj.set("months", JsValue::Number(i.months as f64))
                .map_err(|e| format!("Failed to set interval months: {}", e))?;
            obj.set("days", JsValue::Number(i.days as f64))
                .map_err(|e| format!("Failed to set interval days: {}", e))?;
            obj.set("nanos", JsValue::Number(i.nanos as f64))
                .map_err(|e| format!("Failed to set interval nanos: {}", e))?;
            Ok(JsValue::Object(obj))
        }
    }
}

fn json_to_js(mv8: &MiniV8, json: &serde_json::Value) -> Result<JsValue, String> {
    match json {
        serde_json::Value::Null => Ok(JsValue::Null),
        serde_json::Value::Bool(b) => Ok(JsValue::Boolean(*b)),
        serde_json::Value::Number(n) => {
            let f = n.as_f64().unwrap_or(0.0);
            Ok(JsValue::Number(f))
        }
        serde_json::Value::String(s) => Ok(JsValue::String(mv8.create_string(s))),
        serde_json::Value::Array(arr) => {
            let js_arr = mv8.create_array();
            for (i, v) in arr.iter().enumerate() {
                let js_v = json_to_js(mv8, v)?;
                js_arr
                    .set(i as u32, js_v)
                    .map_err(|e| format!("Failed to set array element: {}", e))?;
            }
            Ok(JsValue::Array(js_arr))
        }
        serde_json::Value::Object(obj) => {
            let js_obj = mv8.create_object();
            for (k, v) in obj {
                let js_v = json_to_js(mv8, v)?;
                js_obj
                    .set(k.as_str(), js_v)
                    .map_err(|e| format!("Failed to set object property: {}", e))?;
            }
            Ok(JsValue::Object(js_obj))
        }
    }
}

fn js_to_value(mv8: &MiniV8, js_val: JsValue) -> Result<Value, String> {
    match js_val {
        JsValue::Undefined | JsValue::Null => Ok(Value::Null),
        JsValue::Boolean(b) => Ok(Value::Bool(b)),
        JsValue::Number(n) => {
            if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                Ok(Value::Int64(n as i64))
            } else {
                Ok(Value::Float64(ordered_float::OrderedFloat(n)))
            }
        }
        JsValue::Date(ms) => {
            let secs = (ms / 1000.0) as i64;
            let nanos = ((ms % 1000.0) * 1_000_000.0) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nanos)
                .unwrap_or(chrono::DateTime::UNIX_EPOCH);
            Ok(Value::Timestamp(dt))
        }
        JsValue::String(s) => {
            let rust_str: std::string::String = s.to_string();
            Ok(Value::String(rust_str))
        }
        JsValue::Array(arr) => {
            let len = arr.len();
            let mut values = Vec::with_capacity(len as usize);
            for i in 0..len {
                let elem: JsValue = arr
                    .get(i)
                    .map_err(|e| format!("Failed to get array element: {}", e))?;
                values.push(js_to_value(mv8, elem)?);
            }
            Ok(Value::Array(values))
        }
        JsValue::Object(obj) => {
            let keys: mini_v8::Array = mv8
                .eval("Object.keys")
                .and_then(|f: mini_v8::Function| {
                    f.call_method(JsValue::Undefined, (JsValue::Object(obj.clone()),))
                })
                .map_err(|e| format!("Failed to get object keys: {}", e))?;

            let len = keys.len();
            let mut fields = Vec::with_capacity(len as usize);
            for i in 0..len {
                let key: mini_v8::String = keys
                    .get(i)
                    .map_err(|e| format!("Failed to get key: {}", e))?;
                let key_str = key.to_string();
                let val: JsValue = obj
                    .get(key_str.as_str())
                    .map_err(|e| format!("Failed to get object value: {}", e))?;
                fields.push((key_str, js_to_value(mv8, val)?));
            }
            Ok(Value::Struct(fields))
        }
        JsValue::Function(_) => Err("Cannot convert JavaScript function to SQL value".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_addition() {
        let result = evaluate_js_function(
            "return a + b;",
            &["a".to_string(), "b".to_string()],
            &[Value::Int64(2), Value::Int64(3)],
        )
        .unwrap();
        assert_eq!(result, Value::Int64(5));
    }

    #[test]
    fn test_string_concat() {
        let result = evaluate_js_function(
            "return a + b;",
            &["a".to_string(), "b".to_string()],
            &[
                Value::String("Hello ".to_string()),
                Value::String("World".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(result, Value::String("Hello World".to_string()));
    }

    #[test]
    fn test_arrow_function() {
        let result =
            evaluate_js_function("(x) => x * 2", &["x".to_string()], &[Value::Int64(21)]).unwrap();
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn test_array_return() {
        let result = evaluate_js_function(
            "return [a, b, a + b];",
            &["a".to_string(), "b".to_string()],
            &[Value::Int64(1), Value::Int64(2)],
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Array(vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)])
        );
    }

    #[test]
    fn test_object_return() {
        let result = evaluate_js_function(
            "return { sum: a + b, product: a * b };",
            &["a".to_string(), "b".to_string()],
            &[Value::Int64(3), Value::Int64(4)],
        )
        .unwrap();
        match result {
            Value::Struct(fields) => {
                let fields_map: std::collections::HashMap<_, _> = fields.into_iter().collect();
                assert_eq!(fields_map.get("sum"), Some(&Value::Int64(7)));
                assert_eq!(fields_map.get("product"), Some(&Value::Int64(12)));
            }
            Value::Null
            | Value::Bool(_)
            | Value::Int64(_)
            | Value::Float64(_)
            | Value::Numeric(_)
            | Value::String(_)
            | Value::Bytes(_)
            | Value::Date(_)
            | Value::Time(_)
            | Value::DateTime(_)
            | Value::Timestamp(_)
            | Value::Json(_)
            | Value::Array(_)
            | Value::Geography(_)
            | Value::Interval(_) => panic!("Expected struct result"),
        }
    }

    #[test]
    fn test_null_handling() {
        let result = evaluate_js_function(
            "return a === null ? 'was null' : 'not null';",
            &["a".to_string()],
            &[Value::Null],
        )
        .unwrap();
        assert_eq!(result, Value::String("was null".to_string()));
    }

    #[test]
    fn test_float_operations() {
        let result = evaluate_js_function(
            "return Math.sqrt(x);",
            &["x".to_string()],
            &[Value::Float64(ordered_float::OrderedFloat(16.0))],
        )
        .unwrap();
        assert_eq!(result, Value::Int64(4));
    }
}
