use std::thread;
use std::time::Duration;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

static START_TIME: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

fn get_start_time() -> std::time::Instant {
    *START_TIME.get_or_init(std::time::Instant::now)
}

pub fn hostname() -> Result<Value> {
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    Ok(Value::string(hostname))
}

pub fn fqdn() -> Result<Value> {
    hostname()
}

pub fn version() -> Result<Value> {
    Ok(Value::string("1.0.0".to_string()))
}

pub fn uptime() -> Result<Value> {
    let elapsed = get_start_time().elapsed();
    Ok(Value::int64(elapsed.as_secs() as i64))
}

pub fn timezone() -> Result<Value> {
    Ok(Value::string("UTC".to_string()))
}

pub fn current_database() -> Result<Value> {
    Ok(Value::string("default".to_string()))
}

pub fn current_user() -> Result<Value> {
    Ok(Value::string("default".to_string()))
}

pub fn is_constant(_value: Value) -> Result<Value> {
    Ok(Value::bool_val(true))
}

pub fn is_finite(value: f64) -> Result<Value> {
    Ok(Value::bool_val(value.is_finite()))
}

pub fn is_infinite(value: f64) -> Result<Value> {
    Ok(Value::bool_val(value.is_infinite()))
}

pub fn is_nan(value: f64) -> Result<Value> {
    Ok(Value::bool_val(value.is_nan()))
}

pub fn bar(value: f64, min: f64, max: f64, width: i64) -> Result<Value> {
    if max <= min {
        return Ok(Value::string("".to_string()));
    }

    let normalized = ((value - min) / (max - min)).clamp(0.0, 1.0);
    let filled = (normalized * width as f64).round() as usize;
    let bar_str = "█".repeat(filled);
    Ok(Value::string(bar_str))
}

pub fn format_readable_size(bytes: i64) -> Result<Value> {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    let result = format!("{:.2} {}", size, UNITS[unit_idx]);
    Ok(Value::string(result))
}

pub fn format_readable_quantity(value: i64) -> Result<Value> {
    const UNITS: &[&str] = &["", "thousand", "million", "billion", "trillion"];
    let mut val = value as f64;
    let mut unit_idx = 0;

    while val >= 1000.0 && unit_idx < UNITS.len() - 1 {
        val /= 1000.0;
        unit_idx += 1;
    }

    let result = if unit_idx == 0 {
        format!("{:.2}", val)
    } else {
        format!("{:.2} {}", val, UNITS[unit_idx])
    };
    Ok(Value::string(result))
}

pub fn format_readable_time_delta(seconds: i64) -> Result<Value> {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    let result = if hours > 0 {
        format!(
            "{} hour(s), {} minute(s), {} second(s)",
            hours, minutes, secs
        )
    } else if minutes > 0 {
        format!("{} minute(s), {} second(s)", minutes, secs)
    } else {
        format!("{} second(s)", secs)
    };
    Ok(Value::string(result))
}

pub fn sleep(seconds: f64) -> Result<Value> {
    let duration = Duration::from_secs_f64(seconds);
    thread::sleep(duration);
    Ok(Value::int64(0))
}

pub fn throw_if(condition: bool, message: &str) -> Result<Value> {
    if condition {
        Err(Error::invalid_query(message.to_string()))
    } else {
        Ok(Value::int64(0))
    }
}

pub fn materialize(value: Value) -> Result<Value> {
    Ok(value)
}

pub fn ignore(_args: Vec<Value>) -> Result<Value> {
    Ok(Value::int64(0))
}

pub fn identity(value: Value) -> Result<Value> {
    Ok(value)
}

pub fn get_setting(_name: &str) -> Result<Value> {
    Ok(Value::string("1".to_string()))
}

pub fn transform(
    value: Value,
    from_array: &[Value],
    to_array: &[Value],
    default: Value,
) -> Result<Value> {
    for (i, from_val) in from_array.iter().enumerate() {
        if &value == from_val {
            return if i < to_array.len() {
                Ok(to_array[i].clone())
            } else {
                Ok(default)
            };
        }
    }
    Ok(default)
}

pub fn model_evaluate(_model_name: &str, _args: Vec<Value>) -> Result<Value> {
    Ok(Value::float64(0.0))
}
