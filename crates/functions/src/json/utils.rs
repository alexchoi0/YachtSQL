use serde_json::Value as JsonValue;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::{DEFAULT_MAX_DEPTH, DEFAULT_MAX_SIZE, JsonPath, parse_json_with_limits};

pub(crate) fn get_json_value(value: &Value) -> Result<JsonValue> {
    if let Some(j) = value.as_json() {
        return Ok(j.clone());
    }

    if let Some(s) = value.as_str() {
        return parse_json_with_limits(s, DEFAULT_MAX_DEPTH, DEFAULT_MAX_SIZE)
            .map_err(|e| Error::InvalidOperation(format!("Invalid JSON string: {}", e)));
    }

    Err(Error::InvalidOperation(
        "Expected JSON value or string".to_string(),
    ))
}

pub(crate) fn json_value_to_text(value: &JsonValue) -> Result<Value> {
    match value {
        JsonValue::Null => Ok(Value::null()),
        JsonValue::String(s) => Ok(Value::string(s.clone())),
        JsonValue::Bool(b) => Ok(Value::string(b.to_string())),
        JsonValue::Number(num) => Ok(Value::string(num.to_string())),
        other => serde_json::to_string(other)
            .map(Value::string)
            .map_err(|e| Error::InvalidOperation(format!("Failed to stringify JSON: {}", e))),
    }
}

pub(crate) fn extract_and_convert<F>(json: &JsonValue, path: &str, converter: F) -> Result<Value>
where
    F: FnOnce(&JsonValue) -> Result<Value>,
{
    let json_path = parse_json_path(path)?;

    match json_path.evaluate(json) {
        Ok(matches) => {
            if matches.is_empty() {
                return Ok(Value::null());
            }

            if matches.len() == 1 {
                converter(&matches[0])
            } else {
                let aggregated = JsonValue::Array(matches);
                converter(&aggregated)
            }
        }
        Err(e) => Err(Error::InvalidOperation(format!(
            "Failed to evaluate path '{}': {}",
            path, e
        ))),
    }
}

pub(crate) fn parse_json_path(path: &str) -> Result<JsonPath> {
    parse_json_path_with_mode(path).map(|(_, parsed)| parsed)
}

pub(crate) fn parse_json_path_with_mode(path: &str) -> Result<(JsonPathMode, JsonPath)> {
    let (mode, normalized_path) = normalize_json_path(path);
    let json_path = JsonPath::parse(&normalized_path)
        .map_err(|e| Error::InvalidOperation(format!("Invalid JSON path '{}': {}", path, e)))?;
    Ok((mode, json_path))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonPathMode {
    Lax,
    Strict,
}

pub(crate) fn normalize_json_path(path: &str) -> (JsonPathMode, String) {
    let trimmed = path.trim_start();

    if let Some(remainder) = strip_keyword_case(trimmed, "strict") {
        return (JsonPathMode::Strict, remainder.to_owned());
    }

    if let Some(remainder) = strip_keyword_case(trimmed, "lax") {
        return (JsonPathMode::Lax, remainder.to_owned());
    }

    (JsonPathMode::Lax, trimmed.to_owned())
}

pub(crate) fn strip_keyword_case<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    if input.len() < keyword.len() {
        return None;
    }

    let (prefix, rest) = input.split_at(keyword.len());
    if prefix.eq_ignore_ascii_case(keyword) {
        Some(rest.trim_start())
    } else {
        None
    }
}

pub(crate) enum JsonbPathElement {
    Key(String),
    Index(usize),
}

pub(crate) fn parse_jsonb_path(path: &str) -> Result<Vec<JsonbPathElement>> {
    if !(path.starts_with('{') && path.ends_with('}')) {
        return Err(Error::InvalidOperation(format!(
            "jsonb_set path must use brace notation (e.g. '{{a,b}}'), got '{}'",
            path
        )));
    }

    let inner = &path[1..path.len() - 1];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = inner.chars().peekable();

    loop {
        let Some(ch) = chars.next() else {
            break;
        };
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                current.push(ch);
            }
            '\\' if in_quotes => {
                current.push(ch);
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            ',' if !in_quotes => {
                if !current.trim().is_empty() {
                    elements.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        elements.push(current.trim().to_string());
    }

    let mut tokens = Vec::with_capacity(elements.len());
    for element in elements {
        let trimmed = element.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(stripped) = trimmed.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
            tokens.push(JsonbPathElement::Key(unescape_path_component(stripped)));
        } else if let Ok(index) = trimmed.parse::<usize>() {
            tokens.push(JsonbPathElement::Index(index));
        } else {
            tokens.push(JsonbPathElement::Key(trimmed.to_string()));
        }
    }

    Ok(tokens)
}

pub(crate) fn unescape_path_component(component: &str) -> String {
    let mut result = String::with_capacity(component.len());
    let mut chars = component.chars();
    loop {
        let Some(ch) = chars.next() else {
            break;
        };
        if ch == '\\' {
            if let Some(next) = chars.next() {
                result.push(next);
            }
        } else {
            result.push(ch);
        }
    }
    result
}

pub(crate) fn extract_via_path_array<F>(
    json: &Value,
    path_array: &str,
    converter: F,
) -> Result<Value>
where
    F: FnOnce(&JsonValue) -> Result<Value>,
{
    if json.is_null() {
        return Ok(Value::null());
    }
    let json_val = get_json_value(json)?;

    let path_elements = parse_pg_text_array(path_array)?;
    let json_path_str = build_jsonpath_from_elements(&path_elements);

    extract_and_convert(&json_val, &json_path_str, converter)
}

pub(crate) fn parse_pg_text_array(array_str: &str) -> Result<Vec<String>> {
    let trimmed = array_str.trim();

    validate_pg_array_syntax(trimmed, array_str)?;

    let content = &trimmed[1..trimmed.len() - 1];
    if content.is_empty() {
        return Ok(Vec::new());
    }

    parse_array_elements(content)
}

fn validate_pg_array_syntax(trimmed: &str, original: &str) -> Result<()> {
    if trimmed == "{}" {
        return Ok(());
    }

    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return Err(Error::InvalidOperation(format!(
            "Invalid PostgreSQL array literal: must start with '{{' and end with '}}', got '{}'",
            original
        )));
    }

    Ok(())
}

fn parse_array_elements(content: &str) -> Result<Vec<String>> {
    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = content.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
            }
            ',' if !in_quotes => {
                elements.push(current.trim().to_string());
                current.clear();
            }
            '\\' if in_quotes => {
                if let Some(next_ch) = chars.next() {
                    current.push(next_ch);
                }
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.is_empty() || !elements.is_empty() {
        elements.push(current.trim().to_string());
    }

    Ok(elements)
}

fn build_jsonpath_from_elements(elements: &[String]) -> String {
    if elements.is_empty() {
        return "$".to_string();
    }

    elements
        .iter()
        .fold(String::from("$"), |mut path, element| {
            path.push_str(&format_path_element(element));
            path
        })
}

fn format_path_element(element: &str) -> String {
    if let Ok(index) = element.parse::<i64>() {
        format!("[{}]", index)
    } else {
        let escaped = escape_jsonpath_key(element);
        format!("['{}']", escaped)
    }
}

fn escape_jsonpath_key(key: &str) -> String {
    key.replace('\\', "\\\\").replace('\'', "\\'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_path_strict_prefix() {
        let (mode, normalized) = normalize_json_path("strict $.orders[0]");
        assert_eq!(mode, JsonPathMode::Strict);
        assert_eq!(normalized, "$.orders[0]");
    }

    #[test]
    fn normalize_path_default() {
        let (mode, normalized) = normalize_json_path("$.payload");
        assert_eq!(mode, JsonPathMode::Lax);
        assert_eq!(normalized, "$.payload");
    }
}
