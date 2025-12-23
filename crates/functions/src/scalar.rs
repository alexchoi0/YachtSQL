use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn upper(val: &Value) -> Result<Value> {
    match val {
        Value::String(s) => Ok(Value::String(s.to_uppercase())),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("UPPER requires a string")),
    }
}

pub fn lower(val: &Value) -> Result<Value> {
    match val {
        Value::String(s) => Ok(Value::String(s.to_lowercase())),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("LOWER requires a string")),
    }
}

pub fn length(val: &Value) -> Result<Value> {
    match val {
        Value::String(s) => Ok(Value::Int64(s.len() as i64)),
        Value::Bytes(b) => Ok(Value::Int64(b.len() as i64)),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("LENGTH requires a string or bytes")),
    }
}

pub fn trim(val: &Value) -> Result<Value> {
    match val {
        Value::String(s) => Ok(Value::String(s.trim().to_string())),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("TRIM requires a string")),
    }
}

pub fn ltrim(val: &Value) -> Result<Value> {
    match val {
        Value::String(s) => Ok(Value::String(s.trim_start().to_string())),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("LTRIM requires a string")),
    }
}

pub fn rtrim(val: &Value) -> Result<Value> {
    match val {
        Value::String(s) => Ok(Value::String(s.trim_end().to_string())),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("RTRIM requires a string")),
    }
}

pub fn substr(val: &Value, start: &Value, len: Option<&Value>) -> Result<Value> {
    match (val, start) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::Int64(start_idx)) => {
            let start = (*start_idx as usize).saturating_sub(1);
            let chars: Vec<char> = s.chars().collect();

            let result = match len {
                Some(Value::Int64(l)) => {
                    let length = *l as usize;
                    chars.into_iter().skip(start).take(length).collect()
                }
                Some(Value::Null) => return Ok(Value::Null),
                None => chars.into_iter().skip(start).collect(),
                _ => return Err(Error::type_mismatch("SUBSTR length must be an integer")),
            };
            Ok(Value::String(result))
        }
        (Value::Bytes(b), Value::Int64(start_idx)) => {
            let start = (*start_idx as usize).saturating_sub(1);

            let result = match len {
                Some(Value::Int64(l)) => {
                    let length = *l as usize;
                    b.iter().skip(start).take(length).copied().collect()
                }
                Some(Value::Null) => return Ok(Value::Null),
                None => b.iter().skip(start).copied().collect(),
                _ => return Err(Error::type_mismatch("SUBSTR length must be an integer")),
            };
            Ok(Value::Bytes(result))
        }
        _ => Err(Error::type_mismatch("SUBSTR requires (string, int, [int])")),
    }
}

pub fn concat(values: &[Value]) -> Result<Value> {
    let mut result = String::new();
    for val in values {
        match val {
            Value::Null => continue,
            Value::String(s) => result.push_str(s),
            v => result.push_str(&v.to_string()),
        }
    }
    Ok(Value::String(result))
}

pub fn replace(val: &Value, from: &Value, to: &Value) -> Result<Value> {
    match (val, from, to) {
        (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(from), Value::String(to)) => {
            Ok(Value::String(s.replace(from.as_str(), to.as_str())))
        }
        _ => Err(Error::type_mismatch("REPLACE requires strings")),
    }
}

pub fn reverse(val: &Value) -> Result<Value> {
    match val {
        Value::String(s) => Ok(Value::String(s.chars().rev().collect())),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("REVERSE requires a string")),
    }
}

pub fn left(val: &Value, n: &Value) -> Result<Value> {
    match (val, n) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::Int64(n)) => {
            let chars: String = s.chars().take(*n as usize).collect();
            Ok(Value::String(chars))
        }
        _ => Err(Error::type_mismatch("LEFT requires (string, int)")),
    }
}

pub fn right(val: &Value, n: &Value) -> Result<Value> {
    match (val, n) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::Int64(n)) => {
            let n = *n as usize;
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            Ok(Value::String(chars[start..].iter().collect()))
        }
        _ => Err(Error::type_mismatch("RIGHT requires (string, int)")),
    }
}

pub fn repeat(val: &Value, n: &Value) -> Result<Value> {
    match (val, n) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::Int64(n)) => Ok(Value::String(s.repeat(*n as usize))),
        _ => Err(Error::type_mismatch("REPEAT requires (string, int)")),
    }
}

pub fn starts_with(val: &Value, prefix: &Value) -> Result<Value> {
    match (val, prefix) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(prefix)) => {
            Ok(Value::Bool(s.starts_with(prefix.as_str())))
        }
        _ => Err(Error::type_mismatch("STARTS_WITH requires strings")),
    }
}

pub fn ends_with(val: &Value, suffix: &Value) -> Result<Value> {
    match (val, suffix) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(suffix)) => Ok(Value::Bool(s.ends_with(suffix.as_str()))),
        _ => Err(Error::type_mismatch("ENDS_WITH requires strings")),
    }
}

pub fn contains(val: &Value, substr: &Value) -> Result<Value> {
    match (val, substr) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(substr)) => Ok(Value::Bool(s.contains(substr.as_str()))),
        _ => Err(Error::type_mismatch("CONTAINS requires strings")),
    }
}

pub fn abs(val: &Value) -> Result<Value> {
    match val {
        Value::Int64(n) => Ok(Value::Int64(n.abs())),
        Value::Float64(f) => Ok(Value::Float64(ordered_float::OrderedFloat(f.abs()))),
        Value::Numeric(d) => Ok(Value::Numeric(d.abs())),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("ABS requires a number")),
    }
}

pub fn round(val: &Value, precision: Option<&Value>) -> Result<Value> {
    let precision = match precision {
        Some(Value::Int64(p)) => *p as i32,
        Some(Value::Null) => return Ok(Value::Null),
        None => 0,
        _ => return Err(Error::type_mismatch("ROUND precision must be an integer")),
    };

    match val {
        Value::Float64(f) => {
            let factor = 10_f64.powi(precision);
            Ok(Value::Float64(ordered_float::OrderedFloat(
                (f.0 * factor).round() / factor,
            )))
        }
        Value::Numeric(d) => Ok(Value::Numeric(d.round_dp(precision as u32))),
        Value::Int64(n) => Ok(Value::Int64(*n)),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("ROUND requires a number")),
    }
}

pub fn floor(val: &Value) -> Result<Value> {
    match val {
        Value::Float64(f) => Ok(Value::Float64(ordered_float::OrderedFloat(f.floor()))),
        Value::Numeric(d) => Ok(Value::Numeric(d.floor())),
        Value::Int64(n) => Ok(Value::Int64(*n)),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("FLOOR requires a number")),
    }
}

pub fn ceil(val: &Value) -> Result<Value> {
    match val {
        Value::Float64(f) => Ok(Value::Float64(ordered_float::OrderedFloat(f.ceil()))),
        Value::Numeric(d) => Ok(Value::Numeric(d.ceil())),
        Value::Int64(n) => Ok(Value::Int64(*n)),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("CEIL requires a number")),
    }
}

pub fn sqrt(val: &Value) -> Result<Value> {
    match val {
        Value::Float64(f) => {
            if f.0 < 0.0 {
                Err(Error::invalid_query("SQRT of negative number"))
            } else {
                Ok(Value::Float64(ordered_float::OrderedFloat(f.sqrt())))
            }
        }
        Value::Int64(n) => {
            if *n < 0 {
                Err(Error::invalid_query("SQRT of negative number"))
            } else {
                Ok(Value::Float64(ordered_float::OrderedFloat(
                    (*n as f64).sqrt(),
                )))
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("SQRT requires a number")),
    }
}

pub fn cbrt(val: &Value) -> Result<Value> {
    match val {
        Value::Float64(f) => Ok(Value::Float64(ordered_float::OrderedFloat(f.cbrt()))),
        Value::Int64(n) => Ok(Value::Float64(ordered_float::OrderedFloat(
            (*n as f64).cbrt(),
        ))),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("CBRT requires a number")),
    }
}

pub fn power(base: &Value, exp: &Value) -> Result<Value> {
    match (base, exp) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Float64(b), Value::Float64(e)) => {
            Ok(Value::Float64(ordered_float::OrderedFloat(b.powf(e.0))))
        }
        (Value::Int64(b), Value::Int64(e)) => {
            if *e >= 0 {
                Ok(Value::Int64(b.pow(*e as u32)))
            } else {
                Ok(Value::Float64(ordered_float::OrderedFloat(
                    (*b as f64).powi(*e as i32),
                )))
            }
        }
        _ => Err(Error::type_mismatch("POWER requires numbers")),
    }
}

pub fn modulo(a: &Value, b: &Value) -> Result<Value> {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(a), Value::Int64(b)) => {
            if *b == 0 {
                Err(Error::DivisionByZero)
            } else {
                Ok(Value::Int64(a % b))
            }
        }
        (Value::Float64(a), Value::Float64(b)) => {
            if b.0 == 0.0 {
                Err(Error::DivisionByZero)
            } else {
                Ok(Value::Float64(ordered_float::OrderedFloat(a.0 % b.0)))
            }
        }
        _ => Err(Error::type_mismatch("MOD requires numbers")),
    }
}

pub fn sign(val: &Value) -> Result<Value> {
    match val {
        Value::Int64(n) => Ok(Value::Int64(n.signum())),
        Value::Float64(f) => {
            if f.is_nan() {
                Ok(Value::Float64(ordered_float::OrderedFloat(f64::NAN)))
            } else {
                Ok(Value::Int64(if f.0 > 0.0 {
                    1
                } else if f.0 < 0.0 {
                    -1
                } else {
                    0
                }))
            }
        }
        Value::Numeric(d) => Ok(Value::Int64(if d.is_sign_positive() {
            1
        } else if d.is_sign_negative() {
            -1
        } else {
            0
        })),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("SIGN requires a number")),
    }
}

pub fn exp(val: &Value) -> Result<Value> {
    match val {
        Value::Float64(f) => Ok(Value::Float64(ordered_float::OrderedFloat(f.exp()))),
        Value::Int64(n) => Ok(Value::Float64(ordered_float::OrderedFloat(
            (*n as f64).exp(),
        ))),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("EXP requires a number")),
    }
}

pub fn ln(val: &Value) -> Result<Value> {
    match val {
        Value::Float64(f) => {
            if f.0 <= 0.0 {
                Err(Error::invalid_query("LN requires positive number"))
            } else {
                Ok(Value::Float64(ordered_float::OrderedFloat(f.ln())))
            }
        }
        Value::Int64(n) => {
            if *n <= 0 {
                Err(Error::invalid_query("LN requires positive number"))
            } else {
                Ok(Value::Float64(ordered_float::OrderedFloat(
                    (*n as f64).ln(),
                )))
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("LN requires a number")),
    }
}

pub fn log(val: &Value, base: Option<&Value>) -> Result<Value> {
    let base = match base {
        Some(Value::Float64(b)) => b.0,
        Some(Value::Int64(b)) => *b as f64,
        Some(Value::Null) => return Ok(Value::Null),
        None => std::f64::consts::E,
        _ => return Err(Error::type_mismatch("LOG base must be a number")),
    };

    match val {
        Value::Float64(f) => {
            if f.0 <= 0.0 || base <= 0.0 || base == 1.0 {
                Err(Error::invalid_query("Invalid LOG arguments"))
            } else {
                Ok(Value::Float64(ordered_float::OrderedFloat(f.log(base))))
            }
        }
        Value::Int64(n) => {
            if *n <= 0 || base <= 0.0 || base == 1.0 {
                Err(Error::invalid_query("Invalid LOG arguments"))
            } else {
                Ok(Value::Float64(ordered_float::OrderedFloat(
                    (*n as f64).log(base),
                )))
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("LOG requires a number")),
    }
}

pub fn log10(val: &Value) -> Result<Value> {
    match val {
        Value::Float64(f) => {
            if f.0 <= 0.0 {
                Err(Error::invalid_query("LOG10 requires positive number"))
            } else {
                Ok(Value::Float64(ordered_float::OrderedFloat(f.log10())))
            }
        }
        Value::Int64(n) => {
            if *n <= 0 {
                Err(Error::invalid_query("LOG10 requires positive number"))
            } else {
                Ok(Value::Float64(ordered_float::OrderedFloat(
                    (*n as f64).log10(),
                )))
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(Error::type_mismatch("LOG10 requires a number")),
    }
}

pub fn coalesce(values: &[Value]) -> Result<Value> {
    for val in values {
        if !matches!(val, Value::Null) {
            return Ok(val.clone());
        }
    }
    Ok(Value::Null)
}

pub fn ifnull(val: &Value, default: &Value) -> Result<Value> {
    match val {
        Value::Null => Ok(default.clone()),
        _ => Ok(val.clone()),
    }
}

pub fn nullif(val: &Value, other: &Value) -> Result<Value> {
    if val == other {
        Ok(Value::Null)
    } else {
        Ok(val.clone())
    }
}

pub fn if_func(condition: &Value, then_val: &Value, else_val: &Value) -> Result<Value> {
    match condition {
        Value::Bool(true) => Ok(then_val.clone()),
        Value::Bool(false) | Value::Null => Ok(else_val.clone()),
        _ => Err(Error::type_mismatch("IF condition must be a boolean")),
    }
}

pub fn current_date() -> Result<Value> {
    Ok(Value::Date(chrono::Utc::now().date_naive()))
}

pub fn current_time() -> Result<Value> {
    Ok(Value::Time(chrono::Utc::now().time()))
}

pub fn current_timestamp() -> Result<Value> {
    Ok(Value::Timestamp(chrono::Utc::now()))
}
