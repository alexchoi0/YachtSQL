use serde_json::Value as JsonValue;
use yachtsql_common::error::{Error, Result};

pub const DEFAULT_MAX_DEPTH: usize = 100;

pub const DEFAULT_MAX_SIZE: usize = 10 * 1024 * 1024;

pub fn parse_json_with_limits(
    input: &str,
    _max_depth: usize,
    _max_size: usize,
) -> Result<JsonValue> {
    serde_json::from_str(input).map_err(|e| Error::invalid_query(format!("Invalid JSON: {}", e)))
}
