use indexmap::IndexMap;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_extract_string(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONExtractString requires at least 2 arguments",
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let json_str = match json_val.as_str() {
            Some(s) => s,
            None => return Ok(Value::null()),
        };

        let mut path_parts: Vec<String> = Vec::new();
        for arg in &args[1..] {
            let part = Self::evaluate_expr(arg, batch, row_idx)?;
            if let Some(s) = part.as_str() {
                path_parts.push(s.to_string());
            } else if let Some(i) = part.as_i64() {
                path_parts.push(i.to_string());
            } else {
                return Err(Error::invalid_query(
                    "JSONExtractString path must be a string or integer",
                ));
            }
        }

        let parsed: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| Error::invalid_query(format!("Invalid JSON: {}", e)))?;

        let mut current = &parsed;
        for part in &path_parts {
            if let Some(obj) = current.as_object() {
                current = match obj.get(part) {
                    Some(v) => v,
                    None => return Ok(Value::string("".to_string())),
                };
            } else if let Some(arr) = current.as_array() {
                let idx: usize = part
                    .parse()
                    .map_err(|_| Error::invalid_query("Array index must be a valid integer"))?;
                current = match arr.get(idx) {
                    Some(v) => v,
                    None => return Ok(Value::string("".to_string())),
                };
            } else {
                return Ok(Value::string("".to_string()));
            }
        }

        match current.as_str() {
            Some(s) => Ok(Value::string(s.to_string())),
            None => Ok(Value::string("".to_string())),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_extract_int(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONExtractInt requires at least 2 arguments",
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let json_str = match json_val.as_str() {
            Some(s) => s,
            None => return Ok(Value::int64(0)),
        };

        let mut path_parts: Vec<String> = Vec::new();
        for arg in &args[1..] {
            let part = Self::evaluate_expr(arg, batch, row_idx)?;
            if let Some(s) = part.as_str() {
                path_parts.push(s.to_string());
            } else if let Some(i) = part.as_i64() {
                path_parts.push(i.to_string());
            }
        }

        let parsed: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| Error::invalid_query(format!("Invalid JSON: {}", e)))?;

        let mut current = &parsed;
        for part in &path_parts {
            if let Some(obj) = current.as_object() {
                current = match obj.get(part) {
                    Some(v) => v,
                    None => return Ok(Value::int64(0)),
                };
            } else if let Some(arr) = current.as_array() {
                let idx: usize = part.parse().unwrap_or(0);
                current = match arr.get(idx) {
                    Some(v) => v,
                    None => return Ok(Value::int64(0)),
                };
            } else {
                return Ok(Value::int64(0));
            }
        }

        match current.as_i64() {
            Some(n) => Ok(Value::int64(n)),
            None => Ok(Value::int64(0)),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_extract_float(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONExtractFloat requires at least 2 arguments",
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let json_str = match json_val.as_str() {
            Some(s) => s,
            None => return Ok(Value::float64(0.0)),
        };

        let mut path_parts: Vec<String> = Vec::new();
        for arg in &args[1..] {
            let part = Self::evaluate_expr(arg, batch, row_idx)?;
            if let Some(s) = part.as_str() {
                path_parts.push(s.to_string());
            } else if let Some(i) = part.as_i64() {
                path_parts.push(i.to_string());
            }
        }

        let parsed: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| Error::invalid_query(format!("Invalid JSON: {}", e)))?;

        let mut current = &parsed;
        for part in &path_parts {
            if let Some(obj) = current.as_object() {
                current = match obj.get(part) {
                    Some(v) => v,
                    None => return Ok(Value::float64(0.0)),
                };
            } else if let Some(arr) = current.as_array() {
                let idx: usize = part.parse().unwrap_or(0);
                current = match arr.get(idx) {
                    Some(v) => v,
                    None => return Ok(Value::float64(0.0)),
                };
            } else {
                return Ok(Value::float64(0.0));
            }
        }

        match current.as_f64() {
            Some(n) => Ok(Value::float64(n)),
            None => Ok(Value::float64(0.0)),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_extract_bool(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONExtractBool requires at least 2 arguments",
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let json_str = match json_val.as_str() {
            Some(s) => s,
            None => return Ok(Value::int64(0)),
        };

        let mut path_parts: Vec<String> = Vec::new();
        for arg in &args[1..] {
            let part = Self::evaluate_expr(arg, batch, row_idx)?;
            if let Some(s) = part.as_str() {
                path_parts.push(s.to_string());
            } else if let Some(i) = part.as_i64() {
                path_parts.push(i.to_string());
            }
        }

        let parsed: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| Error::invalid_query(format!("Invalid JSON: {}", e)))?;

        let mut current = &parsed;
        for part in &path_parts {
            if let Some(obj) = current.as_object() {
                current = match obj.get(part) {
                    Some(v) => v,
                    None => return Ok(Value::int64(0)),
                };
            } else if let Some(arr) = current.as_array() {
                let idx: usize = part.parse().unwrap_or(0);
                current = match arr.get(idx) {
                    Some(v) => v,
                    None => return Ok(Value::int64(0)),
                };
            } else {
                return Ok(Value::int64(0));
            }
        }

        match current.as_bool() {
            Some(b) => Ok(Value::int64(if b { 1 } else { 0 })),
            None => Ok(Value::int64(0)),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_extract_raw(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONExtractRaw requires at least 2 arguments",
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let json_str = match json_val.as_str() {
            Some(s) => s,
            None => return Ok(Value::string("".to_string())),
        };

        let mut path_parts: Vec<String> = Vec::new();
        for arg in &args[1..] {
            let part = Self::evaluate_expr(arg, batch, row_idx)?;
            if let Some(s) = part.as_str() {
                path_parts.push(s.to_string());
            } else if let Some(i) = part.as_i64() {
                path_parts.push(i.to_string());
            }
        }

        let parsed: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| Error::invalid_query(format!("Invalid JSON: {}", e)))?;

        let mut current = &parsed;
        for part in &path_parts {
            if let Some(obj) = current.as_object() {
                current = match obj.get(part) {
                    Some(v) => v,
                    None => return Ok(Value::string("".to_string())),
                };
            } else if let Some(arr) = current.as_array() {
                let idx: usize = part.parse().unwrap_or(0);
                current = match arr.get(idx) {
                    Some(v) => v,
                    None => return Ok(Value::string("".to_string())),
                };
            } else {
                return Ok(Value::string("".to_string()));
            }
        }

        Ok(Value::string(current.to_string()))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_extract_array_raw(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONExtractArrayRaw requires at least 2 arguments",
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let json_str = match json_val.as_str() {
            Some(s) => s,
            None => return Ok(Value::array(vec![])),
        };

        let mut path_parts: Vec<String> = Vec::new();
        for arg in &args[1..] {
            let part = Self::evaluate_expr(arg, batch, row_idx)?;
            if let Some(s) = part.as_str() {
                path_parts.push(s.to_string());
            } else if let Some(i) = part.as_i64() {
                path_parts.push(i.to_string());
            }
        }

        let parsed: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| Error::invalid_query(format!("Invalid JSON: {}", e)))?;

        let mut current = &parsed;
        for part in &path_parts {
            if let Some(obj) = current.as_object() {
                current = match obj.get(part) {
                    Some(v) => v,
                    None => return Ok(Value::array(vec![])),
                };
            } else if let Some(arr) = current.as_array() {
                let idx: usize = part.parse().unwrap_or(0);
                current = match arr.get(idx) {
                    Some(v) => v,
                    None => return Ok(Value::array(vec![])),
                };
            } else {
                return Ok(Value::array(vec![]));
            }
        }

        match current.as_array() {
            Some(arr) => {
                let values: Vec<Value> = arr.iter().map(|v| Value::string(v.to_string())).collect();
                Ok(Value::array(values))
            }
            None => Ok(Value::array(vec![])),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_extract_keys_and_values(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "JSONExtractKeysAndValues requires at least 1 argument",
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let json_str = match json_val.as_str() {
            Some(s) => s,
            None => return Ok(Value::array(vec![])),
        };

        let parsed: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| Error::invalid_query(format!("Invalid JSON: {}", e)))?;

        match parsed.as_object() {
            Some(obj) => {
                let tuples: Vec<Value> = obj
                    .iter()
                    .map(|(k, v)| {
                        let mut map = IndexMap::new();
                        map.insert("1".to_string(), Value::string(k.to_string()));
                        map.insert("2".to_string(), Value::string(v.to_string()));
                        Value::struct_val(map)
                    })
                    .collect();
                Ok(Value::array(tuples))
            }
            None => Ok(Value::array(vec![])),
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_has(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONHas requires at least 2 arguments",
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let json_str = match json_val.as_str() {
            Some(s) => s,
            None => return Ok(Value::int64(0)),
        };

        let mut path_parts: Vec<String> = Vec::new();
        for arg in &args[1..] {
            let part = Self::evaluate_expr(arg, batch, row_idx)?;
            if let Some(s) = part.as_str() {
                path_parts.push(s.to_string());
            } else if let Some(i) = part.as_i64() {
                path_parts.push(i.to_string());
            }
        }

        let parsed: serde_json::Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(_) => return Ok(Value::int64(0)),
        };

        let mut current = &parsed;
        for part in &path_parts {
            if let Some(obj) = current.as_object() {
                current = match obj.get(part) {
                    Some(v) => v,
                    None => return Ok(Value::int64(0)),
                };
            } else if let Some(arr) = current.as_array() {
                let idx: usize = match part.parse() {
                    Ok(i) => i,
                    Err(_) => return Ok(Value::int64(0)),
                };
                current = match arr.get(idx) {
                    Some(v) => v,
                    None => return Ok(Value::int64(0)),
                };
            } else {
                return Ok(Value::int64(0));
            }
        }

        Ok(Value::int64(1))
    }
}
