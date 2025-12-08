use std::collections::HashMap;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_ir::FunctionName;
use yachtsql_ir::expr::{Expr, LiteralValue};

use crate::Table;
use crate::query_executor::evaluator::physical_plan::ProjectionWithExprExec;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_map_function(
        name: &FunctionName,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        match name {
            FunctionName::Map => Self::evaluate_map_constructor(args, batch, row_idx),
            FunctionName::MapFromArrays => Self::evaluate_map_from_arrays(args, batch, row_idx),
            FunctionName::MapKeys => Self::evaluate_map_keys(args, batch, row_idx),
            FunctionName::MapValues => Self::evaluate_map_values(args, batch, row_idx),
            FunctionName::MapContains => Self::evaluate_map_contains(args, batch, row_idx),
            FunctionName::MapAdd => Self::evaluate_map_add(args, batch, row_idx),
            FunctionName::MapSubtract => Self::evaluate_map_subtract(args, batch, row_idx),
            FunctionName::MapUpdate => Self::evaluate_map_update(args, batch, row_idx),
            FunctionName::MapConcat => Self::evaluate_map_concat(args, batch, row_idx),
            FunctionName::MapPopulateSeries => {
                Self::evaluate_map_populate_series(args, batch, row_idx)
            }
            FunctionName::MapFilter => Self::evaluate_map_filter(args, batch, row_idx, dialect),
            FunctionName::MapApply => Self::evaluate_map_apply(args, batch, row_idx, dialect),
            FunctionName::MapExists => Self::evaluate_map_exists(args, batch, row_idx, dialect),
            FunctionName::MapAll => Self::evaluate_map_all(args, batch, row_idx, dialect),
            FunctionName::MapSort => Self::evaluate_map_sort(args, batch, row_idx, false),
            FunctionName::MapReverseSort => Self::evaluate_map_sort(args, batch, row_idx, true),
            FunctionName::MapPartialSort => {
                Self::evaluate_map_partial_sort(args, batch, row_idx, dialect)
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unsupported map function: {:?}",
                name
            ))),
        }
    }

    fn evaluate_map_constructor(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() % 2 != 0 {
            return Err(Error::invalid_query(
                "map() requires an even number of arguments (key-value pairs)",
            ));
        }

        let mut entries = Vec::with_capacity(args.len() / 2);
        for chunk in args.chunks(2) {
            let key = Self::evaluate_expr(&chunk[0], batch, row_idx)?;
            let value = Self::evaluate_expr(&chunk[1], batch, row_idx)?;
            entries.push((key, value));
        }

        Ok(Value::map(entries))
    }

    fn evaluate_map_from_arrays(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("mapFromArrays", args, 2)?;

        let keys_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let values_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if keys_val.is_null() || values_val.is_null() {
            return Ok(Value::null());
        }

        let keys = keys_val.as_array().ok_or_else(|| {
            Error::type_mismatch("ARRAY", keys_val.data_type().to_string().as_str())
        })?;
        let values = values_val.as_array().ok_or_else(|| {
            Error::type_mismatch("ARRAY", values_val.data_type().to_string().as_str())
        })?;

        if keys.len() != values.len() {
            return Err(Error::invalid_query(
                "mapFromArrays requires arrays of equal length",
            ));
        }

        let entries: Vec<(Value, Value)> = keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(Value::map(entries))
    }

    fn evaluate_map_keys(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("mapKeys", args, 1)?;

        let map_val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if map_val.is_null() {
            return Ok(Value::null());
        }

        let entries = map_val
            .as_map()
            .ok_or_else(|| Error::type_mismatch("MAP", map_val.data_type().to_string().as_str()))?;

        let keys: Vec<Value> = entries.iter().map(|(k, _)| k.clone()).collect();
        Ok(Value::array(keys))
    }

    fn evaluate_map_values(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("mapValues", args, 1)?;

        let map_val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if map_val.is_null() {
            return Ok(Value::null());
        }

        let entries = map_val
            .as_map()
            .ok_or_else(|| Error::type_mismatch("MAP", map_val.data_type().to_string().as_str()))?;

        let values: Vec<Value> = entries.iter().map(|(_, v)| v.clone()).collect();
        Ok(Value::array(values))
    }

    fn evaluate_map_contains(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("mapContains", args, 2)?;

        let map_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let key = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if map_val.is_null() {
            return Ok(Value::null());
        }

        let entries = map_val
            .as_map()
            .ok_or_else(|| Error::type_mismatch("MAP", map_val.data_type().to_string().as_str()))?;

        let contains = entries.iter().any(|(k, _)| k == &key);
        Ok(Value::bool_val(contains))
    }

    fn evaluate_map_add(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query("mapAdd requires at least 2 arguments"));
        }

        let mut result_map: HashMap<String, i64> = HashMap::new();

        for arg in args {
            let map_val = Self::evaluate_expr(arg, batch, row_idx)?;
            if map_val.is_null() {
                continue;
            }

            let entries = map_val.as_map().ok_or_else(|| {
                Error::type_mismatch("MAP", map_val.data_type().to_string().as_str())
            })?;

            for (k, v) in entries {
                let key_str = Self::value_to_map_key(k);
                let val_int = v.as_i64().unwrap_or(0);
                *result_map.entry(key_str).or_insert(0) += val_int;
            }
        }

        let entries: Vec<(Value, Value)> = result_map
            .into_iter()
            .map(|(k, v)| (Value::string(k), Value::int64(v)))
            .collect();

        Ok(Value::map(entries))
    }

    fn evaluate_map_subtract(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("mapSubtract", args, 2)?;

        let map1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let map2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if map1_val.is_null() || map2_val.is_null() {
            return Ok(Value::null());
        }

        let entries1 = map1_val.as_map().ok_or_else(|| {
            Error::type_mismatch("MAP", map1_val.data_type().to_string().as_str())
        })?;
        let entries2 = map2_val.as_map().ok_or_else(|| {
            Error::type_mismatch("MAP", map2_val.data_type().to_string().as_str())
        })?;

        let mut result_map: HashMap<String, i64> = HashMap::new();

        for (k, v) in entries1 {
            let key_str = Self::value_to_map_key(k);
            let val_int = v.as_i64().unwrap_or(0);
            *result_map.entry(key_str).or_insert(0) += val_int;
        }

        for (k, v) in entries2 {
            let key_str = Self::value_to_map_key(k);
            let val_int = v.as_i64().unwrap_or(0);
            *result_map.entry(key_str).or_insert(0) -= val_int;
        }

        let entries: Vec<(Value, Value)> = result_map
            .into_iter()
            .map(|(k, v)| (Value::string(k), Value::int64(v)))
            .collect();

        Ok(Value::map(entries))
    }

    fn evaluate_map_update(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("mapUpdate", args, 2)?;

        let map1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let map2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if map1_val.is_null() {
            return Ok(map2_val);
        }
        if map2_val.is_null() {
            return Ok(map1_val);
        }

        let entries1 = map1_val.as_map().ok_or_else(|| {
            Error::type_mismatch("MAP", map1_val.data_type().to_string().as_str())
        })?;
        let entries2 = map2_val.as_map().ok_or_else(|| {
            Error::type_mismatch("MAP", map2_val.data_type().to_string().as_str())
        })?;

        let mut result: Vec<(Value, Value)> = Vec::new();
        let mut seen_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        for (k, v) in entries2 {
            let key_str = Self::value_to_map_key(k);
            seen_keys.insert(key_str);
            result.push((k.clone(), v.clone()));
        }

        for (k, v) in entries1 {
            let key_str = Self::value_to_map_key(k);
            if !seen_keys.contains(&key_str) {
                result.push((k.clone(), v.clone()));
            }
        }

        Ok(Value::map(result))
    }

    fn evaluate_map_concat(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::map(vec![]));
        }

        let mut result: Vec<(Value, Value)> = Vec::new();
        let mut seen_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        for arg in args.iter().rev() {
            let map_val = Self::evaluate_expr(arg, batch, row_idx)?;
            if map_val.is_null() {
                continue;
            }

            let entries = map_val.as_map().ok_or_else(|| {
                Error::type_mismatch("MAP", map_val.data_type().to_string().as_str())
            })?;

            for (k, v) in entries.iter().rev() {
                let key_str = Self::value_to_map_key(k);
                if !seen_keys.contains(&key_str) {
                    seen_keys.insert(key_str);
                    result.push((k.clone(), v.clone()));
                }
            }
        }

        result.reverse();
        Ok(Value::map(result))
    }

    fn evaluate_map_populate_series(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::invalid_query(
                "mapPopulateSeries requires 1 or 2 arguments",
            ));
        }

        let map_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if map_val.is_null() {
            return Ok(Value::null());
        }

        let entries = map_val
            .as_map()
            .ok_or_else(|| Error::type_mismatch("MAP", map_val.data_type().to_string().as_str()))?;

        if entries.is_empty() {
            return Ok(Value::map(vec![]));
        }

        let mut int_entries: Vec<(i64, Value)> = Vec::new();
        for (k, v) in entries {
            let key_int = k
                .as_i64()
                .ok_or_else(|| Error::type_mismatch("INT64", k.data_type().to_string().as_str()))?;
            int_entries.push((key_int, v.clone()));
        }

        int_entries.sort_by_key(|(k, _)| *k);

        let min_key = int_entries.first().map(|(k, _)| *k).unwrap_or(0);
        let max_key = if args.len() == 2 {
            let max_arg = Self::evaluate_expr(&args[1], batch, row_idx)?;
            max_arg.as_i64().ok_or_else(|| {
                Error::type_mismatch("INT64", max_arg.data_type().to_string().as_str())
            })?
        } else {
            int_entries.last().map(|(k, _)| *k).unwrap_or(0)
        };

        let key_to_value: HashMap<i64, Value> = int_entries.into_iter().collect();

        let mut result: Vec<(Value, Value)> = Vec::new();
        for key in min_key..=max_key {
            let value = key_to_value
                .get(&key)
                .cloned()
                .unwrap_or_else(|| Value::int64(0));
            result.push((Value::int64(key), value));
        }

        Ok(Value::map(result))
    }

    fn evaluate_map_filter(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        Self::validate_arg_count("mapFilter", args, 2)?;

        let (params, body) = match &args[0] {
            Expr::Lambda { params, body } => (params.clone(), body.as_ref().clone()),
            _ => {
                return Err(Error::invalid_query(
                    "mapFilter requires a lambda as the first argument",
                ));
            }
        };

        let map_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if map_val.is_null() {
            return Ok(Value::null());
        }

        let entries = map_val
            .as_map()
            .ok_or_else(|| Error::type_mismatch("MAP", map_val.data_type().to_string().as_str()))?;

        let mut result: Vec<(Value, Value)> = Vec::new();

        for (k, v) in entries {
            let bindings = Self::create_map_lambda_bindings(&params, k, v);
            let predicate =
                Self::evaluate_map_lambda_body(&body, batch, row_idx, dialect, &bindings)?;
            if predicate.as_bool() == Some(true) {
                result.push((k.clone(), v.clone()));
            }
        }

        Ok(Value::map(result))
    }

    fn evaluate_map_apply(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        Self::validate_arg_count("mapApply", args, 2)?;

        let (params, body) = match &args[0] {
            Expr::Lambda { params, body } => (params.clone(), body.as_ref().clone()),
            _ => {
                return Err(Error::invalid_query(
                    "mapApply requires a lambda as the first argument",
                ));
            }
        };

        let map_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if map_val.is_null() {
            return Ok(Value::null());
        }

        let entries = map_val
            .as_map()
            .ok_or_else(|| Error::type_mismatch("MAP", map_val.data_type().to_string().as_str()))?;

        let mut result: Vec<(Value, Value)> = Vec::new();

        for (k, v) in entries {
            let bindings = Self::create_map_lambda_bindings(&params, k, v);
            let transformed =
                Self::evaluate_map_lambda_body(&body, batch, row_idx, dialect, &bindings)?;

            if let Some(struct_fields) = transformed.as_struct() {
                if struct_fields.len() == 2 {
                    let vals: Vec<_> = struct_fields.values().cloned().collect();
                    result.push((vals[0].clone(), vals[1].clone()));
                } else {
                    return Err(Error::invalid_query(
                        "mapApply lambda must return a tuple of (key, value)",
                    ));
                }
            } else if let Some(tuple) = transformed.as_array() {
                if tuple.len() == 2 {
                    result.push((tuple[0].clone(), tuple[1].clone()));
                } else {
                    return Err(Error::invalid_query(
                        "mapApply lambda must return a tuple of (key, value)",
                    ));
                }
            } else {
                return Err(Error::invalid_query(
                    "mapApply lambda must return a tuple of (key, value)",
                ));
            }
        }

        Ok(Value::map(result))
    }

    fn evaluate_map_exists(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        Self::validate_arg_count("mapExists", args, 2)?;

        let (params, body) = match &args[0] {
            Expr::Lambda { params, body } => (params.clone(), body.as_ref().clone()),
            _ => {
                return Err(Error::invalid_query(
                    "mapExists requires a lambda as the first argument",
                ));
            }
        };

        let map_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if map_val.is_null() {
            return Ok(Value::null());
        }

        let entries = map_val
            .as_map()
            .ok_or_else(|| Error::type_mismatch("MAP", map_val.data_type().to_string().as_str()))?;

        for (k, v) in entries {
            let bindings = Self::create_map_lambda_bindings(&params, k, v);
            let predicate =
                Self::evaluate_map_lambda_body(&body, batch, row_idx, dialect, &bindings)?;
            if predicate.as_bool() == Some(true) {
                return Ok(Value::bool_val(true));
            }
        }

        Ok(Value::bool_val(false))
    }

    fn evaluate_map_all(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        Self::validate_arg_count("mapAll", args, 2)?;

        let (params, body) = match &args[0] {
            Expr::Lambda { params, body } => (params.clone(), body.as_ref().clone()),
            _ => {
                return Err(Error::invalid_query(
                    "mapAll requires a lambda as the first argument",
                ));
            }
        };

        let map_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if map_val.is_null() {
            return Ok(Value::null());
        }

        let entries = map_val
            .as_map()
            .ok_or_else(|| Error::type_mismatch("MAP", map_val.data_type().to_string().as_str()))?;

        for (k, v) in entries {
            let bindings = Self::create_map_lambda_bindings(&params, k, v);
            let predicate =
                Self::evaluate_map_lambda_body(&body, batch, row_idx, dialect, &bindings)?;
            if predicate.as_bool() != Some(true) {
                return Ok(Value::bool_val(false));
            }
        }

        Ok(Value::bool_val(true))
    }

    fn evaluate_map_sort(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        reverse: bool,
    ) -> Result<Value> {
        Self::validate_arg_count(if reverse { "mapReverseSort" } else { "mapSort" }, args, 1)?;

        let map_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if map_val.is_null() {
            return Ok(Value::null());
        }

        let entries = map_val
            .as_map()
            .ok_or_else(|| Error::type_mismatch("MAP", map_val.data_type().to_string().as_str()))?;

        let mut sorted: Vec<(Value, Value)> = entries.to_vec();
        sorted.sort_by(|(k1, _), (k2, _)| Self::compare_map_values(k1, k2));

        if reverse {
            sorted.reverse();
        }

        Ok(Value::map(sorted))
    }

    fn evaluate_map_partial_sort(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        Self::validate_arg_count("mapPartialSort", args, 3)?;

        let (params, body) = match &args[0] {
            Expr::Lambda { params, body } => (params.clone(), body.as_ref().clone()),
            _ => {
                return Err(Error::invalid_query(
                    "mapPartialSort requires a lambda as the first argument",
                ));
            }
        };

        let limit_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let limit = limit_val.as_i64().ok_or_else(|| {
            Error::type_mismatch("INT64", limit_val.data_type().to_string().as_str())
        })? as usize;

        let map_val = Self::evaluate_expr(&args[2], batch, row_idx)?;
        if map_val.is_null() {
            return Ok(Value::null());
        }

        let entries = map_val
            .as_map()
            .ok_or_else(|| Error::type_mismatch("MAP", map_val.data_type().to_string().as_str()))?;

        let mut keyed_entries: Vec<(Value, Value, Value)> = Vec::new();
        for (k, v) in entries {
            let bindings = Self::create_map_lambda_bindings(&params, k, v);
            let sort_key =
                Self::evaluate_map_lambda_body(&body, batch, row_idx, dialect, &bindings)?;
            keyed_entries.push((sort_key, k.clone(), v.clone()));
        }

        keyed_entries.sort_by(|(sk1, _, _), (sk2, _, _)| Self::compare_map_values(sk1, sk2));

        let result: Vec<(Value, Value)> = keyed_entries
            .into_iter()
            .take(limit)
            .map(|(_, k, v)| (k, v))
            .collect();

        Ok(Value::map(result))
    }

    fn compare_map_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        if a.is_null() && b.is_null() {
            return std::cmp::Ordering::Equal;
        }
        if a.is_null() {
            return std::cmp::Ordering::Greater;
        }
        if b.is_null() {
            return std::cmp::Ordering::Less;
        }

        if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
            return x.cmp(&y);
        }
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal);
        }
        if let Some(x) = a.as_i64() {
            if let Some(y) = b.as_f64() {
                return (x as f64)
                    .partial_cmp(&y)
                    .unwrap_or(std::cmp::Ordering::Equal);
            }
        }
        if let Some(x) = a.as_f64() {
            if let Some(y) = b.as_i64() {
                return x
                    .partial_cmp(&(y as f64))
                    .unwrap_or(std::cmp::Ordering::Equal);
            }
        }
        if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
            return x.cmp(y);
        }
        if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
            return x.cmp(&y);
        }
        if let (Some(x), Some(y)) = (a.as_date(), b.as_date()) {
            return x.cmp(&y);
        }
        if let (Some(x), Some(y)) = (a.as_timestamp(), b.as_timestamp()) {
            return x.cmp(&y);
        }
        std::cmp::Ordering::Equal
    }

    fn value_to_map_key(val: &Value) -> String {
        if let Some(s) = val.as_str() {
            s.to_string()
        } else if let Some(i) = val.as_i64() {
            i.to_string()
        } else if let Some(f) = val.as_f64() {
            f.to_string()
        } else {
            format!("{}", val)
        }
    }

    fn create_map_lambda_bindings(
        params: &[String],
        key: &Value,
        value: &Value,
    ) -> HashMap<String, Value> {
        let mut bindings = HashMap::new();
        if !params.is_empty() {
            bindings.insert(params[0].clone(), key.clone());
        }
        if params.len() >= 2 {
            bindings.insert(params[1].clone(), value.clone());
        }
        bindings
    }

    fn evaluate_map_lambda_body(
        body: &Expr,
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
        bindings: &HashMap<String, Value>,
    ) -> Result<Value> {
        let resolved_body = Self::resolve_map_lambda_columns(body, bindings);
        Self::evaluate_expr_internal(&resolved_body, batch, row_idx, dialect)
    }

    fn resolve_map_lambda_columns(expr: &Expr, bindings: &HashMap<String, Value>) -> Expr {
        match expr {
            Expr::Column { name, table: None } => {
                if let Some(val) = bindings.get(name) {
                    Expr::Literal(LiteralValue::from_value(val))
                } else {
                    expr.clone()
                }
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::resolve_map_lambda_columns(left, bindings)),
                op: *op,
                right: Box::new(Self::resolve_map_lambda_columns(right, bindings)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(Self::resolve_map_lambda_columns(inner, bindings)),
            },
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| Self::resolve_map_lambda_columns(a, bindings))
                    .collect(),
            },
            Expr::Tuple(exprs) => Expr::Tuple(
                exprs
                    .iter()
                    .map(|e| Self::resolve_map_lambda_columns(e, bindings))
                    .collect(),
            ),
            _ => expr.clone(),
        }
    }
}
