use std::cell::RefCell;
use std::sync::Once;

use yachtsql_common::types::Value;

static V8_INIT: Once = Once::new();

fn init_v8_platform() {
    V8_INIT.call_once(|| {
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform);
        v8::V8::initialize();
    });
}

thread_local! {
    static V8_ISOLATE: RefCell<Option<v8::OwnedIsolate>> = const { RefCell::new(None) };
}

fn with_isolate<F, R>(f: F) -> R
where
    F: FnOnce(&mut v8::OwnedIsolate) -> R,
{
    init_v8_platform();

    V8_ISOLATE.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            *borrow = Some(v8::Isolate::new(v8::CreateParams::default()));
        }
        f(borrow.as_mut().unwrap())
    })
}

pub fn evaluate_js_function(
    js_code: &str,
    param_names: &[String],
    args: &[Value],
) -> Result<Value, String> {
    with_isolate(|isolate| {
        let handle_scope = &mut v8::HandleScope::new(isolate);
        let context = v8::Context::new(handle_scope, Default::default());
        let scope = &mut v8::ContextScope::new(handle_scope, context);

        for (i, (name, value)) in param_names.iter().zip(args.iter()).enumerate() {
            let js_val = value_to_js(scope, value)?;
            let key = v8::String::new(scope, name).ok_or("Failed to create parameter name")?;
            context
                .global(scope)
                .set(scope, key.into(), js_val)
                .ok_or_else(|| format!("Failed to set parameter {}", i))?;
        }

        let wrapper_code = build_wrapper_function(js_code, param_names);
        let code =
            v8::String::new(scope, &wrapper_code).ok_or("Failed to create JavaScript source")?;

        let script = v8::Script::compile(scope, code, None)
            .ok_or("Failed to compile JavaScript function")?;

        let result = script
            .run(scope)
            .ok_or("JavaScript execution returned undefined")?;

        js_to_value(scope, result)
    })
}

fn build_wrapper_function(js_code: &str, param_names: &[String]) -> String {
    let params = param_names.join(", ");
    let trimmed = js_code.trim();

    let is_arrow_fn = trimmed.starts_with('(') && trimmed.contains("=>");
    let is_function = trimmed.starts_with("function");

    if is_arrow_fn || is_function {
        format!("({})({})", trimmed, params)
    } else {
        format!("(function({}) {{ {} }})({})", params, trimmed, params)
    }
}

fn value_to_js<'s>(
    scope: &mut v8::HandleScope<'s>,
    value: &Value,
) -> Result<v8::Local<'s, v8::Value>, String> {
    match value {
        Value::Null => Ok(v8::null(scope).into()),
        Value::Bool(b) => Ok(v8::Boolean::new(scope, *b).into()),
        Value::Int64(n) => Ok(v8::Number::new(scope, *n as f64).into()),
        Value::Float64(f) => Ok(v8::Number::new(scope, f.into_inner()).into()),
        Value::Numeric(d) => {
            let f: f64 = d.to_string().parse().unwrap_or(0.0);
            Ok(v8::Number::new(scope, f).into())
        }
        Value::BigNumeric(d) => {
            let f: f64 = d.to_string().parse().unwrap_or(0.0);
            Ok(v8::Number::new(scope, f).into())
        }
        Value::String(s) => {
            let str = v8::String::new(scope, s).ok_or("Failed to create string")?;
            Ok(str.into())
        }
        Value::Bytes(b) => {
            let arr = v8::Array::new(scope, b.len() as i32);
            for (i, byte) in b.iter().enumerate() {
                let val = v8::Number::new(scope, *byte as f64);
                arr.set_index(scope, i as u32, val.into())
                    .ok_or("Failed to set array element")?;
            }
            Ok(arr.into())
        }
        Value::Date(d) => {
            let str =
                v8::String::new(scope, &d.to_string()).ok_or("Failed to create date string")?;
            Ok(str.into())
        }
        Value::Time(t) => {
            let str =
                v8::String::new(scope, &t.to_string()).ok_or("Failed to create time string")?;
            Ok(str.into())
        }
        Value::DateTime(dt) => {
            let str = v8::String::new(scope, &dt.to_string())
                .ok_or("Failed to create datetime string")?;
            Ok(str.into())
        }
        Value::Timestamp(ts) => {
            let str = v8::String::new(scope, &ts.to_rfc3339())
                .ok_or("Failed to create timestamp string")?;
            Ok(str.into())
        }
        Value::Json(j) => json_to_js(scope, j),
        Value::Array(arr) => {
            let js_arr = v8::Array::new(scope, arr.len() as i32);
            for (i, v) in arr.iter().enumerate() {
                let js_v = value_to_js(scope, v)?;
                js_arr
                    .set_index(scope, i as u32, js_v)
                    .ok_or("Failed to set array element")?;
            }
            Ok(js_arr.into())
        }
        Value::Struct(fields) => {
            let obj = v8::Object::new(scope);
            for (name, v) in fields {
                let key = v8::String::new(scope, name).ok_or("Failed to create key")?;
                let js_v = value_to_js(scope, v)?;
                obj.set(scope, key.into(), js_v)
                    .ok_or("Failed to set object property")?;
            }
            Ok(obj.into())
        }
        Value::Geography(g) => {
            let str = v8::String::new(scope, g).ok_or("Failed to create geography string")?;
            Ok(str.into())
        }
        Value::Interval(i) => {
            let obj = v8::Object::new(scope);
            let months_key = v8::String::new(scope, "months").ok_or("Failed to create key")?;
            let days_key = v8::String::new(scope, "days").ok_or("Failed to create key")?;
            let nanos_key = v8::String::new(scope, "nanos").ok_or("Failed to create key")?;
            let months_val = v8::Number::new(scope, i.months as f64);
            let days_val = v8::Number::new(scope, i.days as f64);
            let nanos_val = v8::Number::new(scope, i.nanos as f64);
            obj.set(scope, months_key.into(), months_val.into())
                .ok_or("Failed to set interval months")?;
            obj.set(scope, days_key.into(), days_val.into())
                .ok_or("Failed to set interval days")?;
            obj.set(scope, nanos_key.into(), nanos_val.into())
                .ok_or("Failed to set interval nanos")?;
            Ok(obj.into())
        }
        Value::Range(r) => {
            let obj = v8::Object::new(scope);
            let start_key = v8::String::new(scope, "start").ok_or("Failed to create key")?;
            let end_key = v8::String::new(scope, "end").ok_or("Failed to create key")?;
            let start = match r.start() {
                Some(v) => value_to_js(scope, v)?,
                None => v8::null(scope).into(),
            };
            let end = match r.end() {
                Some(v) => value_to_js(scope, v)?,
                None => v8::null(scope).into(),
            };
            obj.set(scope, start_key.into(), start)
                .ok_or("Failed to set range start")?;
            obj.set(scope, end_key.into(), end)
                .ok_or("Failed to set range end")?;
            Ok(obj.into())
        }
        Value::Default => Ok(v8::null(scope).into()),
    }
}

fn json_to_js<'s>(
    scope: &mut v8::HandleScope<'s>,
    json: &serde_json::Value,
) -> Result<v8::Local<'s, v8::Value>, String> {
    match json {
        serde_json::Value::Null => Ok(v8::null(scope).into()),
        serde_json::Value::Bool(b) => Ok(v8::Boolean::new(scope, *b).into()),
        serde_json::Value::Number(n) => {
            let f = n.as_f64().unwrap_or(0.0);
            Ok(v8::Number::new(scope, f).into())
        }
        serde_json::Value::String(s) => {
            let str = v8::String::new(scope, s).ok_or("Failed to create string")?;
            Ok(str.into())
        }
        serde_json::Value::Array(arr) => {
            let js_arr = v8::Array::new(scope, arr.len() as i32);
            for (i, v) in arr.iter().enumerate() {
                let js_v = json_to_js(scope, v)?;
                js_arr
                    .set_index(scope, i as u32, js_v)
                    .ok_or("Failed to set array element")?;
            }
            Ok(js_arr.into())
        }
        serde_json::Value::Object(obj) => {
            let js_obj = v8::Object::new(scope);
            for (k, v) in obj {
                let key = v8::String::new(scope, k).ok_or("Failed to create key")?;
                let js_v = json_to_js(scope, v)?;
                js_obj
                    .set(scope, key.into(), js_v)
                    .ok_or("Failed to set object property")?;
            }
            Ok(js_obj.into())
        }
    }
}

fn js_to_value(
    scope: &mut v8::HandleScope<'_>,
    js_val: v8::Local<v8::Value>,
) -> Result<Value, String> {
    if js_val.is_null() || js_val.is_undefined() {
        return Ok(Value::Null);
    }

    if js_val.is_boolean() {
        return Ok(Value::Bool(js_val.boolean_value(scope)));
    }

    if js_val.is_number() {
        let n = js_val.number_value(scope).unwrap_or(0.0);
        if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
            return Ok(Value::Int64(n as i64));
        }
        return Ok(Value::Float64(ordered_float::OrderedFloat(n)));
    }

    if js_val.is_string() {
        let s = js_val.to_rust_string_lossy(scope);
        return Ok(Value::String(s));
    }

    if js_val.is_array() {
        let arr =
            v8::Local::<v8::Array>::try_from(js_val).map_err(|_| "Failed to convert to array")?;
        let len = arr.length();
        let mut values = Vec::with_capacity(len as usize);
        for i in 0..len {
            let elem = arr
                .get_index(scope, i)
                .ok_or("Failed to get array element")?;
            values.push(js_to_value(scope, elem)?);
        }
        return Ok(Value::Array(values));
    }

    if js_val.is_object() {
        let obj =
            v8::Local::<v8::Object>::try_from(js_val).map_err(|_| "Failed to convert to object")?;
        let keys = obj
            .get_own_property_names(scope, v8::GetPropertyNamesArgs::default())
            .ok_or("Failed to get object keys")?;
        let len = keys.length();
        let mut fields = Vec::with_capacity(len as usize);
        for i in 0..len {
            let key = keys.get_index(scope, i).ok_or("Failed to get key")?;
            let key_str = key.to_rust_string_lossy(scope);
            let val = obj.get(scope, key).ok_or("Failed to get object value")?;
            fields.push((key_str, js_to_value(scope, val)?));
        }
        return Ok(Value::Struct(fields));
    }

    Err("Cannot convert JavaScript value to SQL value".to_string())
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
            | Value::BigNumeric(_)
            | Value::String(_)
            | Value::Bytes(_)
            | Value::Date(_)
            | Value::Time(_)
            | Value::DateTime(_)
            | Value::Timestamp(_)
            | Value::Json(_)
            | Value::Array(_)
            | Value::Geography(_)
            | Value::Interval(_)
            | Value::Range(_)
            | Value::Default => panic!("Expected struct result"),
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

    #[test]
    fn test_es6_const_let() {
        let result = evaluate_js_function(
            r#"
            const doubled = x * 2;
            let tripled = x * 3;
            return doubled + tripled;
            "#,
            &["x".to_string()],
            &[Value::Int64(5)],
        )
        .unwrap();
        assert_eq!(result, Value::Int64(25));
    }

    #[test]
    fn test_es6_arrow_functions() {
        let result = evaluate_js_function(
            r#"
            const arr = [1, 2, 3, 4, 5];
            const doubled = arr.map(x => x * 2);
            return doubled.reduce((a, b) => a + b, 0);
            "#,
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(result, Value::Int64(30));
    }

    #[test]
    fn test_es6_template_literals() {
        let result = evaluate_js_function(
            r#"
            const name = "World";
            return `Hello, ${name}!`;
            "#,
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(result, Value::String("Hello, World!".to_string()));
    }

    #[test]
    fn test_isolate_reuse() {
        for i in 0..100 {
            let result =
                evaluate_js_function("return x * 2;", &["x".to_string()], &[Value::Int64(i)])
                    .unwrap();
            assert_eq!(result, Value::Int64(i * 2));
        }
    }
}
