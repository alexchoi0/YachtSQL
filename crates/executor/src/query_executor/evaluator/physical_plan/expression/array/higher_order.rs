use std::collections::HashMap;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(crate) fn evaluate_higher_order_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        match name {
            "ARRAYMAP" => Self::evaluate_array_map(args, batch, row_idx, dialect),
            "ARRAYFILTER" => Self::evaluate_array_filter(args, batch, row_idx, dialect),
            "ARRAYEXISTS" => Self::evaluate_array_exists(args, batch, row_idx, dialect),
            "ARRAYALL" => Self::evaluate_array_all(args, batch, row_idx, dialect),
            "ARRAYFIRST" => Self::evaluate_array_first(args, batch, row_idx, dialect),
            "ARRAYLAST" => Self::evaluate_array_last(args, batch, row_idx, dialect),
            "ARRAYFIRSTINDEX" => Self::evaluate_array_first_index(args, batch, row_idx, dialect),
            "ARRAYLASTINDEX" => Self::evaluate_array_last_index(args, batch, row_idx, dialect),
            "ARRAYCOUNT" => Self::evaluate_array_count(args, batch, row_idx, dialect),
            "ARRAYSUM" => Self::evaluate_array_sum_lambda(args, batch, row_idx, dialect),
            "ARRAYAVG" => Self::evaluate_array_avg_lambda(args, batch, row_idx, dialect),
            "ARRAYMIN" => Self::evaluate_array_min_lambda(args, batch, row_idx, dialect),
            "ARRAYMAX" => Self::evaluate_array_max_lambda(args, batch, row_idx, dialect),
            "ARRAYSORT" => Self::evaluate_array_sort_lambda(args, batch, row_idx, dialect),
            "ARRAYREVERSESORT" => {
                Self::evaluate_array_reverse_sort_lambda(args, batch, row_idx, dialect)
            }
            "ARRAYFOLD" => Self::evaluate_array_fold(args, batch, row_idx, dialect),
            "ARRAYREDUCE" => Self::evaluate_array_reduce(args, batch, row_idx, dialect),
            "ARRAYREDUCEINRANGES" => {
                Self::evaluate_array_reduce_in_ranges(args, batch, row_idx, dialect)
            }
            "ARRAYCUMSUM" => Self::evaluate_array_cum_sum(args, batch, row_idx, dialect),
            "ARRAYCUMSUMNONNEGATIVE" => {
                Self::evaluate_array_cum_sum_non_negative(args, batch, row_idx, dialect)
            }
            "ARRAYDIFFERENCE" => Self::evaluate_array_difference(args, batch, row_idx, dialect),
            "ARRAYSPLIT" => Self::evaluate_array_split(args, batch, row_idx, dialect),
            "ARRAYREVERSESPLIT" => {
                Self::evaluate_array_reverse_split(args, batch, row_idx, dialect)
            }
            "ARRAYCOMPACT" => Self::evaluate_array_compact(args, batch, row_idx, dialect),
            "ARRAYZIP" => Self::evaluate_array_zip(args, batch, row_idx, dialect),
            "ARRAYAUC" => Self::evaluate_array_auc(args, batch, row_idx, dialect),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown higher-order array function: {}",
                name
            ))),
        }
    }

    fn resolve_lambda_body_columns(body: &Expr, lambda_bindings: &HashMap<String, Value>) -> Expr {
        match body {
            Expr::Column { name, table: None } => {
                if let Some(val) = lambda_bindings.get(name) {
                    Expr::Literal(yachtsql_ir::expr::LiteralValue::from_value(val))
                } else {
                    body.clone()
                }
            }
            Expr::Column {
                name,
                table: Some(tbl),
            } => {
                if let Some(val) = lambda_bindings.get(name) {
                    Expr::Literal(yachtsql_ir::expr::LiteralValue::from_value(val))
                } else if let Some(val) = lambda_bindings.get(tbl) {
                    Expr::Literal(yachtsql_ir::expr::LiteralValue::from_value(val))
                } else {
                    body.clone()
                }
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::resolve_lambda_body_columns(left, lambda_bindings)),
                op: op.clone(),
                right: Box::new(Self::resolve_lambda_body_columns(right, lambda_bindings)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op: op.clone(),
                expr: Box::new(Self::resolve_lambda_body_columns(expr, lambda_bindings)),
            },
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| Self::resolve_lambda_body_columns(a, lambda_bindings))
                    .collect(),
            },
            Expr::Lambda { params, body } => Expr::Lambda {
                params: params.clone(),
                body: Box::new(Self::resolve_lambda_body_columns(body, lambda_bindings)),
            },
            _ => body.clone(),
        }
    }

    fn evaluate_lambda_with_bindings(
        body: &Expr,
        params: &[String],
        values: &[Value],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
        lambda_bindings: &HashMap<String, Value>,
    ) -> Result<Value> {
        match body {
            Expr::Column { name, table: None } => {
                if let Some(val) = lambda_bindings.get(name) {
                    return Ok(val.clone());
                }
                Self::evaluate_expr_internal(body, batch, row_idx, dialect)
            }
            Expr::Column {
                name,
                table: Some(tbl),
            } => {
                if let Some(val) = lambda_bindings.get(name) {
                    return Ok(val.clone());
                }
                if let Some(val) = lambda_bindings.get(tbl) {
                    return Ok(val.clone());
                }
                Self::evaluate_expr_internal(body, batch, row_idx, dialect)
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_lambda_with_bindings(
                    left,
                    params,
                    values,
                    batch,
                    row_idx,
                    dialect,
                    lambda_bindings,
                )?;
                let right_val = Self::evaluate_lambda_with_bindings(
                    right,
                    params,
                    values,
                    batch,
                    row_idx,
                    dialect,
                    lambda_bindings,
                )?;
                Self::evaluate_binary_op(&left_val, op, &right_val)
            }
            Expr::UnaryOp { op, expr } => {
                let operand = Self::evaluate_lambda_with_bindings(
                    expr,
                    params,
                    values,
                    batch,
                    row_idx,
                    dialect,
                    lambda_bindings,
                )?;
                Self::evaluate_unary_op(op, &operand)
            }
            Expr::Function { name, args } => {
                let func_name = name.as_str().to_uppercase();
                if matches!(
                    func_name.as_str(),
                    "ARRAYMAP"
                        | "ARRAYFILTER"
                        | "ARRAYEXISTS"
                        | "ARRAYALL"
                        | "ARRAYFIRST"
                        | "ARRAYLAST"
                        | "ARRAYFIRSTINDEX"
                        | "ARRAYLASTINDEX"
                        | "ARRAYCOUNT"
                ) && !args.is_empty()
                    && matches!(&args[0], Expr::Lambda { .. })
                {
                    let resolved_args: Vec<Expr> = args
                        .iter()
                        .enumerate()
                        .map(|(i, arg)| {
                            if i == 0 {
                                match arg {
                                    Expr::Lambda {
                                        params: lp,
                                        body: lb,
                                    } => {
                                        let resolved_body =
                                            Self::resolve_lambda_body_columns(lb, lambda_bindings);
                                        Expr::Lambda {
                                            params: lp.clone(),
                                            body: Box::new(resolved_body),
                                        }
                                    }
                                    _ => arg.clone(),
                                }
                            } else {
                                match Self::evaluate_lambda_with_bindings(
                                    arg,
                                    params,
                                    values,
                                    batch,
                                    row_idx,
                                    dialect,
                                    lambda_bindings,
                                ) {
                                    Ok(val) => Expr::Literal(
                                        yachtsql_ir::expr::LiteralValue::from_value(&val),
                                    ),
                                    Err(_) => arg.clone(),
                                }
                            }
                        })
                        .collect();

                    Self::evaluate_higher_order_function(
                        &func_name,
                        &resolved_args,
                        batch,
                        row_idx,
                        dialect,
                    )
                } else {
                    let evaluated_args: Vec<Value> = args
                        .iter()
                        .map(|arg| {
                            Self::evaluate_lambda_with_bindings(
                                arg,
                                params,
                                values,
                                batch,
                                row_idx,
                                dialect,
                                lambda_bindings,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Self::evaluate_function_with_values(name.as_str(), &evaluated_args, dialect)
                }
            }
            Expr::Tuple(exprs) => {
                let tuple_vals: Vec<Value> = exprs
                    .iter()
                    .map(|e| {
                        Self::evaluate_lambda_with_bindings(
                            e,
                            params,
                            values,
                            batch,
                            row_idx,
                            dialect,
                            lambda_bindings,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let fields: indexmap::IndexMap<String, Value> = tuple_vals
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| (format!("_{}", i + 1), v))
                    .collect();
                Ok(Value::struct_val(fields))
            }
            Expr::Lambda {
                params: inner_params,
                body: inner_body,
            } => {
                let array_arg = if let Some(next_arg) = values.get(params.len()) {
                    next_arg
                } else {
                    return Err(Error::invalid_query(
                        "Nested lambda requires array argument".to_string(),
                    ));
                };

                let Some(inner_arr) = array_arg.as_array() else {
                    return Ok(Value::null());
                };

                let mut result = Vec::with_capacity(inner_arr.len());
                for elem in inner_arr {
                    let mut inner_bindings = lambda_bindings.clone();
                    if let Some(param_name) = inner_params.first() {
                        inner_bindings.insert(param_name.clone(), elem.clone());
                    }
                    let val = Self::evaluate_lambda_with_bindings(
                        inner_body,
                        inner_params,
                        std::slice::from_ref(elem),
                        batch,
                        row_idx,
                        dialect,
                        &inner_bindings,
                    )?;
                    result.push(val);
                }
                Ok(Value::array(result))
            }
            Expr::Literal(lit) => Ok(lit.to_value()),
            Expr::Cast { expr, data_type } => {
                let val = Self::evaluate_lambda_with_bindings(
                    expr,
                    params,
                    values,
                    batch,
                    row_idx,
                    dialect,
                    lambda_bindings,
                )?;
                Self::cast_value(val, data_type)
            }
            _ => Self::evaluate_expr_internal(body, batch, row_idx, dialect),
        }
    }

    fn evaluate_function_with_values(
        name: &str,
        args: &[Value],
        _dialect: crate::DialectType,
    ) -> Result<Value> {
        match name.to_uppercase().as_str() {
            "UPPER" => {
                let s = args.first().and_then(|v| v.as_str()).unwrap_or_default();
                Ok(Value::string(s.to_uppercase()))
            }
            "LOWER" => {
                let s = args.first().and_then(|v| v.as_str()).unwrap_or_default();
                Ok(Value::string(s.to_lowercase()))
            }
            "LENGTH" | "LEN" => {
                let s = args.first().and_then(|v| v.as_str()).unwrap_or_default();
                Ok(Value::int64(s.len() as i64))
            }
            "ABS" => {
                let val = args
                    .first()
                    .ok_or_else(|| Error::invalid_query("ABS requires one argument".to_string()))?;
                match val.as_i64() {
                    Some(i) => Ok(Value::int64(i.abs())),
                    None => match val.as_f64() {
                        Some(f) => Ok(Value::float64(f.abs())),
                        None => Ok(Value::null()),
                    },
                }
            }
            "ARRAYMAP" => Err(Error::unsupported_feature(
                "Nested arrayMap in lambda".to_string(),
            )),
            _ => Err(Error::unsupported_feature(format!(
                "Function {} not supported in lambda context",
                name
            ))),
        }
    }

    fn evaluate_array_map(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayMap requires at least 2 arguments: lambda and array".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayMap must be a lambda expression".to_string(),
            ));
        };

        let arrays: Vec<Vec<Value>> = args[1..]
            .iter()
            .map(|arg| {
                let val = Self::evaluate_expr_internal(arg, batch, row_idx, dialect)?;
                val.as_array().cloned().ok_or_else(|| {
                    Error::invalid_query("arrayMap expects array arguments".to_string())
                })
            })
            .collect::<Result<Vec<_>>>()?;

        if arrays.is_empty() {
            return Ok(Value::array(vec![]));
        }

        let len = arrays[0].len();
        for arr in &arrays[1..] {
            if arr.len() != len {
                return Err(Error::invalid_query(
                    "All arrays passed to arrayMap must have the same length".to_string(),
                ));
            }
        }

        let mut result = Vec::with_capacity(len);
        for i in 0..len {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            for (param_idx, param_name) in params.iter().enumerate() {
                if param_idx < arrays.len() {
                    bindings.insert(param_name.clone(), arrays[param_idx][i].clone());
                }
            }

            let values: Vec<Value> = arrays.iter().map(|arr| arr[i].clone()).collect();
            let val = Self::evaluate_lambda_with_bindings(
                body, params, &values, batch, row_idx, dialect, &bindings,
            )?;
            result.push(val);
        }

        Ok(Value::array(result))
    }

    fn evaluate_array_filter(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayFilter requires 2 arguments: lambda and array".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayFilter must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        let mut result = Vec::new();
        for elem in array {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            let keep = val
                .as_bool()
                .or_else(|| val.as_i64().map(|i| i != 0))
                .unwrap_or(false);
            if keep {
                result.push(elem.clone());
            }
        }

        Ok(Value::array(result))
    }

    fn evaluate_array_exists(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayExists requires 2 arguments: lambda and array".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayExists must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        for elem in array {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            let matches = val
                .as_bool()
                .or_else(|| val.as_i64().map(|i| i != 0))
                .unwrap_or(false);
            if matches {
                return Ok(Value::int64(1));
            }
        }

        Ok(Value::int64(0))
    }

    fn evaluate_array_all(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayAll requires 2 arguments: lambda and array".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayAll must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        for elem in array {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            let matches = val
                .as_bool()
                .or_else(|| val.as_i64().map(|i| i != 0))
                .unwrap_or(false);
            if !matches {
                return Ok(Value::int64(0));
            }
        }

        Ok(Value::int64(1))
    }

    fn evaluate_array_first(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayFirst requires 2 arguments: lambda and array".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayFirst must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        for elem in array {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            let matches = val
                .as_bool()
                .or_else(|| val.as_i64().map(|i| i != 0))
                .unwrap_or(false);
            if matches {
                return Ok(elem.clone());
            }
        }

        Ok(Value::null())
    }

    fn evaluate_array_last(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayLast requires 2 arguments: lambda and array".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayLast must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        for elem in array.iter().rev() {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            let matches = val
                .as_bool()
                .or_else(|| val.as_i64().map(|i| i != 0))
                .unwrap_or(false);
            if matches {
                return Ok(elem.clone());
            }
        }

        Ok(Value::null())
    }

    fn evaluate_array_first_index(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayFirstIndex requires 2 arguments: lambda and array".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayFirstIndex must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        for (idx, elem) in array.iter().enumerate() {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            let matches = val
                .as_bool()
                .or_else(|| val.as_i64().map(|i| i != 0))
                .unwrap_or(false);
            if matches {
                return Ok(Value::int64((idx + 1) as i64));
            }
        }

        Ok(Value::int64(0))
    }

    fn evaluate_array_last_index(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayLastIndex requires 2 arguments: lambda and array".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayLastIndex must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        for (idx, elem) in array.iter().enumerate().rev() {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            let matches = val
                .as_bool()
                .or_else(|| val.as_i64().map(|i| i != 0))
                .unwrap_or(false);
            if matches {
                return Ok(Value::int64((idx + 1) as i64));
            }
        }

        Ok(Value::int64(0))
    }

    fn evaluate_array_count(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayCount requires 2 arguments: lambda and array".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayCount must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        let mut count = 0i64;
        for elem in array {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            let matches = val
                .as_bool()
                .or_else(|| val.as_i64().map(|i| i != 0))
                .unwrap_or(false);
            if matches {
                count += 1;
            }
        }

        Ok(Value::int64(count))
    }

    fn evaluate_array_sum_lambda(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arraySum with lambda requires 2 arguments".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arraySum must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        let mut sum_int: i64 = 0;
        let mut sum_float: f64 = 0.0;
        let mut use_float = false;

        for elem in array {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            if let Some(i) = val.as_i64() {
                if use_float {
                    sum_float += i as f64;
                } else {
                    sum_int += i;
                }
            } else if let Some(f) = val.as_f64() {
                if !use_float {
                    sum_float = sum_int as f64;
                    use_float = true;
                }
                sum_float += f;
            }
        }

        if use_float {
            Ok(Value::float64(sum_float))
        } else {
            Ok(Value::int64(sum_int))
        }
    }

    fn evaluate_array_avg_lambda(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayAvg with lambda requires 2 arguments".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayAvg must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        if array.is_empty() {
            return Ok(Value::null());
        }

        let mut sum: f64 = 0.0;
        for elem in array {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            if let Some(i) = val.as_i64() {
                sum += i as f64;
            } else if let Some(f) = val.as_f64() {
                sum += f;
            }
        }

        Ok(Value::float64(sum / array.len() as f64))
    }

    fn evaluate_array_min_lambda(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayMin with lambda requires 2 arguments".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayMin must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        if array.is_empty() {
            return Ok(Value::null());
        }

        let mut min_val: Option<Value> = None;
        for elem in array {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            match &min_val {
                None => min_val = Some(val),
                Some(current_min) => {
                    if Self::compare_values(&val, current_min) == std::cmp::Ordering::Less {
                        min_val = Some(val);
                    }
                }
            }
        }

        Ok(min_val.unwrap_or(Value::null()))
    }

    fn evaluate_array_max_lambda(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayMax with lambda requires 2 arguments".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayMax must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        if array.is_empty() {
            return Ok(Value::null());
        }

        let mut max_val: Option<Value> = None;
        for elem in array {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), elem.clone());
            }

            let val = Self::evaluate_lambda_with_bindings(
                body,
                params,
                std::slice::from_ref(elem),
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            match &max_val {
                None => max_val = Some(val),
                Some(current_max) => {
                    if Self::compare_values(&val, current_max) == std::cmp::Ordering::Greater {
                        max_val = Some(val);
                    }
                }
            }
        }

        Ok(max_val.unwrap_or(Value::null()))
    }

    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a.as_i64(), b.as_i64()) {
            (Some(ai), Some(bi)) => ai.cmp(&bi),
            _ => match (a.as_f64(), b.as_f64()) {
                (Some(af), Some(bf)) => af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal),
                _ => std::cmp::Ordering::Equal,
            },
        }
    }

    fn evaluate_array_sort_lambda(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arraySort with lambda requires 2 arguments".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arraySort must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        let mut indexed: Vec<(Value, Value)> = array
            .iter()
            .map(|elem| {
                let mut bindings: HashMap<String, Value> = HashMap::new();
                if let Some(param_name) = params.first() {
                    bindings.insert(param_name.clone(), elem.clone());
                }
                let key = Self::evaluate_lambda_with_bindings(
                    body,
                    params,
                    std::slice::from_ref(elem),
                    batch,
                    row_idx,
                    dialect,
                    &bindings,
                )
                .unwrap_or(Value::null());
                (elem.clone(), key)
            })
            .collect();

        indexed.sort_by(|a, b| Self::compare_values(&a.1, &b.1));

        Ok(Value::array(indexed.into_iter().map(|(v, _)| v).collect()))
    }

    fn evaluate_array_reverse_sort_lambda(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayReverseSort with lambda requires 2 arguments".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayReverseSort must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        let mut indexed: Vec<(Value, Value)> = array
            .iter()
            .map(|elem| {
                let mut bindings: HashMap<String, Value> = HashMap::new();
                if let Some(param_name) = params.first() {
                    bindings.insert(param_name.clone(), elem.clone());
                }
                let key = Self::evaluate_lambda_with_bindings(
                    body,
                    params,
                    std::slice::from_ref(elem),
                    batch,
                    row_idx,
                    dialect,
                    &bindings,
                )
                .unwrap_or(Value::null());
                (elem.clone(), key)
            })
            .collect();

        indexed.sort_by(|a, b| Self::compare_values(&b.1, &a.1));

        Ok(Value::array(indexed.into_iter().map(|(v, _)| v).collect()))
    }

    fn evaluate_array_fold(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "arrayFold requires 3 arguments: lambda, array, and initial value".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arrayFold must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        let mut acc = Self::evaluate_expr_internal(&args[2], batch, row_idx, dialect)?;

        for elem in array {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if params.len() >= 2 {
                bindings.insert(params[0].clone(), acc.clone());
                bindings.insert(params[1].clone(), elem.clone());
            } else if let Some(param_name) = params.first() {
                bindings.insert(param_name.clone(), acc.clone());
            }

            acc = Self::evaluate_lambda_with_bindings(
                body,
                params,
                &[acc.clone(), elem.clone()],
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;
        }

        Ok(acc)
    }

    fn evaluate_array_reduce(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayReduce requires 2 arguments: aggregate name and array".to_string(),
            ));
        }

        let agg_name_val = Self::evaluate_expr_internal(&args[0], batch, row_idx, dialect)?;
        let Some(agg_name) = agg_name_val.as_str() else {
            return Err(Error::invalid_query(
                "First argument to arrayReduce must be an aggregate function name".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        if array.is_empty() {
            return Ok(Value::null());
        }

        match agg_name.to_lowercase().as_str() {
            "sum" => {
                let mut sum_int: i64 = 0;
                let mut sum_float: f64 = 0.0;
                let mut use_float = false;
                for val in array {
                    if let Some(i) = val.as_i64() {
                        if use_float {
                            sum_float += i as f64;
                        } else {
                            sum_int += i;
                        }
                    } else if let Some(f) = val.as_f64() {
                        if !use_float {
                            sum_float = sum_int as f64;
                            use_float = true;
                        }
                        sum_float += f;
                    }
                }
                if use_float {
                    Ok(Value::float64(sum_float))
                } else {
                    Ok(Value::int64(sum_int))
                }
            }
            "max" => {
                let mut max_val: Option<Value> = None;
                for val in array {
                    match &max_val {
                        None => max_val = Some(val.clone()),
                        Some(current) => {
                            if Self::compare_values(&val, current) == std::cmp::Ordering::Greater {
                                max_val = Some(val.clone());
                            }
                        }
                    }
                }
                Ok(max_val.unwrap_or(Value::null()))
            }
            "min" => {
                let mut min_val: Option<Value> = None;
                for val in array {
                    match &min_val {
                        None => min_val = Some(val.clone()),
                        Some(current) => {
                            if Self::compare_values(&val, current) == std::cmp::Ordering::Less {
                                min_val = Some(val.clone());
                            }
                        }
                    }
                }
                Ok(min_val.unwrap_or(Value::null()))
            }
            "avg" => {
                let mut sum: f64 = 0.0;
                for val in array.iter() {
                    if let Some(i) = val.as_i64() {
                        sum += i as f64;
                    } else if let Some(f) = val.as_f64() {
                        sum += f;
                    }
                }
                Ok(Value::float64(sum / array.len() as f64))
            }
            "count" => Ok(Value::int64(array.len() as i64)),
            _ => Err(Error::unsupported_feature(format!(
                "Aggregate function '{}' not supported in arrayReduce",
                agg_name
            ))),
        }
    }

    fn evaluate_array_reduce_in_ranges(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "arrayReduceInRanges requires 3 arguments: aggregate name, ranges, and array"
                    .to_string(),
            ));
        }

        let agg_name_val = Self::evaluate_expr_internal(&args[0], batch, row_idx, dialect)?;
        let Some(agg_name) = agg_name_val.as_str() else {
            return Err(Error::invalid_query(
                "First argument must be an aggregate function name".to_string(),
            ));
        };

        let ranges_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(ranges) = ranges_val.as_array() else {
            return Ok(Value::null());
        };

        let array_val = Self::evaluate_expr_internal(&args[2], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        let mut result = Vec::new();
        for range in ranges {
            let Some(range_struct) = range.as_struct() else {
                continue;
            };
            let start = range_struct.get("_1").and_then(|v| v.as_i64()).unwrap_or(1) as usize;
            let length = range_struct.get("_2").and_then(|v| v.as_i64()).unwrap_or(0) as usize;

            let slice: Vec<Value> = array
                .iter()
                .skip(start.saturating_sub(1))
                .take(length)
                .cloned()
                .collect();

            let reduced = Self::reduce_array(&agg_name, &slice)?;
            result.push(reduced);
        }

        Ok(Value::array(result))
    }

    fn reduce_array(agg_name: &str, array: &[Value]) -> Result<Value> {
        if array.is_empty() {
            return Ok(Value::null());
        }

        match agg_name.to_lowercase().as_str() {
            "sum" => {
                let mut sum: i64 = 0;
                for val in array {
                    if let Some(i) = val.as_i64() {
                        sum += i;
                    }
                }
                Ok(Value::int64(sum))
            }
            "max" => {
                let mut max_val: Option<Value> = None;
                for val in array {
                    match &max_val {
                        None => max_val = Some(val.clone()),
                        Some(current) => {
                            if Self::compare_values(val, current) == std::cmp::Ordering::Greater {
                                max_val = Some(val.clone());
                            }
                        }
                    }
                }
                Ok(max_val.unwrap_or(Value::null()))
            }
            "min" => {
                let mut min_val: Option<Value> = None;
                for val in array {
                    match &min_val {
                        None => min_val = Some(val.clone()),
                        Some(current) => {
                            if Self::compare_values(val, current) == std::cmp::Ordering::Less {
                                min_val = Some(val.clone());
                            }
                        }
                    }
                }
                Ok(min_val.unwrap_or(Value::null()))
            }
            _ => Err(Error::unsupported_feature(format!(
                "Aggregate '{}' not supported",
                agg_name
            ))),
        }
    }

    fn evaluate_array_cum_sum(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "arrayCumSum requires 1 argument".to_string(),
            ));
        }

        let array_val = Self::evaluate_expr_internal(&args[0], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        let mut sum: i64 = 0;
        let mut result = Vec::with_capacity(array.len());
        for val in array {
            if let Some(i) = val.as_i64() {
                sum += i;
            }
            result.push(Value::int64(sum));
        }

        Ok(Value::array(result))
    }

    fn evaluate_array_cum_sum_non_negative(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "arrayCumSumNonNegative requires 1 argument".to_string(),
            ));
        }

        let array_val = Self::evaluate_expr_internal(&args[0], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        let mut sum: i64 = 0;
        let mut result = Vec::with_capacity(array.len());
        for val in array {
            if let Some(i) = val.as_i64() {
                sum += i;
                if sum < 0 {
                    sum = 0;
                }
            }
            result.push(Value::int64(sum));
        }

        Ok(Value::array(result))
    }

    fn evaluate_array_difference(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "arrayDifference requires 1 argument".to_string(),
            ));
        }

        let array_val = Self::evaluate_expr_internal(&args[0], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        if array.is_empty() {
            return Ok(Value::array(vec![]));
        }

        let mut result = Vec::with_capacity(array.len());
        result.push(Value::int64(0));

        for i in 1..array.len() {
            let prev = array[i - 1].as_i64().unwrap_or(0);
            let curr = array[i].as_i64().unwrap_or(0);
            result.push(Value::int64(curr - prev));
        }

        Ok(Value::array(result))
    }

    fn evaluate_array_split(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "arraySplit requires 3 arguments: lambda, array, and mask array".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument to arraySplit must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        let mask_val = Self::evaluate_expr_internal(&args[2], batch, row_idx, dialect)?;
        let Some(mask) = mask_val.as_array() else {
            return Ok(Value::null());
        };

        if array.len() != mask.len() {
            return Err(Error::invalid_query(
                "Array and mask must have the same length".to_string(),
            ));
        }

        let mut result: Vec<Value> = Vec::new();
        let mut current_group: Vec<Value> = Vec::new();

        for (i, (elem, mask_elem)) in array.iter().zip(mask.iter()).enumerate() {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if params.len() >= 2 {
                bindings.insert(params[0].clone(), elem.clone());
                bindings.insert(params[1].clone(), mask_elem.clone());
            }

            let should_split = Self::evaluate_lambda_with_bindings(
                body,
                params,
                &[elem.clone(), mask_elem.clone()],
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            let split = should_split
                .as_bool()
                .or_else(|| should_split.as_i64().map(|i| i != 0))
                .unwrap_or(false);

            if split && i > 0 {
                result.push(Value::array(std::mem::take(&mut current_group)));
            }
            current_group.push(elem.clone());
        }

        if !current_group.is_empty() {
            result.push(Value::array(current_group));
        }

        Ok(Value::array(result))
    }

    fn evaluate_array_reverse_split(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "arrayReverseSplit requires 3 arguments".to_string(),
            ));
        }

        let Expr::Lambda { params, body } = &args[0] else {
            return Err(Error::invalid_query(
                "First argument must be a lambda expression".to_string(),
            ));
        };

        let array_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        let mask_val = Self::evaluate_expr_internal(&args[2], batch, row_idx, dialect)?;
        let Some(mask) = mask_val.as_array() else {
            return Ok(Value::null());
        };

        if array.len() != mask.len() {
            return Err(Error::invalid_query(
                "Array and mask must have the same length".to_string(),
            ));
        }

        let mut result: Vec<Value> = Vec::new();
        let mut current_group: Vec<Value> = Vec::new();

        for (i, (elem, mask_elem)) in array.iter().zip(mask.iter()).enumerate().rev() {
            let mut bindings: HashMap<String, Value> = HashMap::new();
            if params.len() >= 2 {
                bindings.insert(params[0].clone(), elem.clone());
                bindings.insert(params[1].clone(), mask_elem.clone());
            }

            let should_split = Self::evaluate_lambda_with_bindings(
                body,
                params,
                &[elem.clone(), mask_elem.clone()],
                batch,
                row_idx,
                dialect,
                &bindings,
            )?;

            let split = should_split
                .as_bool()
                .or_else(|| should_split.as_i64().map(|i| i != 0))
                .unwrap_or(false);

            current_group.insert(0, elem.clone());
            if split && i > 0 {
                result.insert(0, Value::array(std::mem::take(&mut current_group)));
            }
        }

        if !current_group.is_empty() {
            result.insert(0, Value::array(current_group));
        }

        Ok(Value::array(result))
    }

    fn evaluate_array_compact(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "arrayCompact requires 1 argument".to_string(),
            ));
        }

        let array_val = Self::evaluate_expr_internal(&args[0], batch, row_idx, dialect)?;
        let Some(array) = array_val.as_array() else {
            return Ok(Value::null());
        };

        if array.is_empty() {
            return Ok(Value::array(vec![]));
        }

        let mut result = Vec::new();
        let mut prev: Option<&Value> = None;

        for val in array.iter() {
            match prev {
                None => {
                    result.push(val.clone());
                    prev = Some(val);
                }
                Some(p) => {
                    if val != p {
                        result.push(val.clone());
                        prev = Some(val);
                    }
                }
            }
        }

        Ok(Value::array(result))
    }

    fn evaluate_array_zip(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "arrayZip requires at least 1 argument".to_string(),
            ));
        }

        let arrays: Vec<Vec<Value>> = args
            .iter()
            .map(|arg| {
                let val = Self::evaluate_expr_internal(arg, batch, row_idx, dialect)?;
                val.as_array().cloned().ok_or_else(|| {
                    Error::invalid_query("arrayZip expects array arguments".to_string())
                })
            })
            .collect::<Result<Vec<_>>>()?;

        if arrays.is_empty() {
            return Ok(Value::array(vec![]));
        }

        let len = arrays[0].len();
        for arr in &arrays[1..] {
            if arr.len() != len {
                return Err(Error::invalid_query(
                    "All arrays passed to arrayZip must have the same length".to_string(),
                ));
            }
        }

        let mut result = Vec::with_capacity(len);
        for i in 0..len {
            let tuple: indexmap::IndexMap<String, Value> = arrays
                .iter()
                .enumerate()
                .map(|(j, arr)| (format!("_{}", j + 1), arr[i].clone()))
                .collect();
            result.push(Value::struct_val(tuple));
        }

        Ok(Value::array(result))
    }

    fn evaluate_array_auc(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        dialect: crate::DialectType,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "arrayAUC requires 2 arguments: scores and labels".to_string(),
            ));
        }

        let scores_val = Self::evaluate_expr_internal(&args[0], batch, row_idx, dialect)?;
        let Some(scores) = scores_val.as_array() else {
            return Ok(Value::null());
        };

        let labels_val = Self::evaluate_expr_internal(&args[1], batch, row_idx, dialect)?;
        let Some(labels) = labels_val.as_array() else {
            return Ok(Value::null());
        };

        if scores.len() != labels.len() {
            return Err(Error::invalid_query(
                "Scores and labels must have the same length".to_string(),
            ));
        }

        let mut pairs: Vec<(f64, i64)> = scores
            .iter()
            .zip(labels.iter())
            .filter_map(|(s, l)| {
                let score = s.as_f64().or_else(|| s.as_i64().map(|i| i as f64))?;
                let label = l.as_i64()?;
                Some((score, label))
            })
            .collect();

        pairs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        let mut tp = 0.0;
        let mut fp = 0.0;
        let total_pos = pairs.iter().filter(|(_, l)| *l == 1).count() as f64;
        let total_neg = pairs.iter().filter(|(_, l)| *l == 0).count() as f64;

        if total_pos == 0.0 || total_neg == 0.0 {
            return Ok(Value::float64(0.5));
        }

        let mut auc = 0.0;
        let mut prev_fp = 0.0;
        let mut prev_tp = 0.0;

        for (_, label) in &pairs {
            if *label == 1 {
                tp += 1.0;
            } else {
                fp += 1.0;
                auc += (tp - prev_tp) * (fp + prev_fp) / 2.0;
                prev_tp = tp;
                prev_fp = fp;
            }
        }

        auc /= total_pos * total_neg;

        Ok(Value::float64(auc))
    }
}
