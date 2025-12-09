use indexmap::IndexMap;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_ir::FunctionName;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_tuple_function(
        name: &FunctionName,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            FunctionName::Tuple => Self::eval_tuple(args, batch, row_idx),
            FunctionName::TupleElement => Self::eval_tuple_element(args, batch, row_idx),
            FunctionName::Untuple => Self::eval_untuple(args, batch, row_idx),
            FunctionName::TupleHammingDistance => {
                Self::eval_tuple_hamming_distance(args, batch, row_idx)
            }
            FunctionName::TuplePlus => Self::eval_tuple_plus(args, batch, row_idx),
            FunctionName::TupleMinus => Self::eval_tuple_minus(args, batch, row_idx),
            FunctionName::TupleMultiply => Self::eval_tuple_multiply(args, batch, row_idx),
            FunctionName::TupleDivide => Self::eval_tuple_divide(args, batch, row_idx),
            FunctionName::TupleNegate => Self::eval_tuple_negate(args, batch, row_idx),
            FunctionName::TupleMultiplyByNumber => {
                Self::eval_tuple_multiply_by_number(args, batch, row_idx)
            }
            FunctionName::TupleDivideByNumber => {
                Self::eval_tuple_divide_by_number(args, batch, row_idx)
            }
            FunctionName::TupleConcat => Self::eval_tuple_concat(args, batch, row_idx),
            FunctionName::TupleIntDiv => Self::eval_tuple_int_div(args, batch, row_idx),
            FunctionName::TupleIntDivOrZero => {
                Self::eval_tuple_int_div_or_zero(args, batch, row_idx)
            }
            FunctionName::TupleModulo => Self::eval_tuple_modulo(args, batch, row_idx),
            FunctionName::TupleModuloByNumber => {
                Self::eval_tuple_modulo_by_number(args, batch, row_idx)
            }
            FunctionName::TupleToNameValuePairs => {
                Self::eval_tuple_to_name_value_pairs(args, batch, row_idx)
            }
            FunctionName::TupleNames => Self::eval_tuple_names(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown tuple function: {}",
                name.as_str()
            ))),
        }
    }

    fn eval_tuple(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        let mut result_map = IndexMap::new();
        for (i, arg) in args.iter().enumerate() {
            let value = Self::evaluate_expr(arg, batch, row_idx)?;
            let field_name = (i + 1).to_string();
            result_map.insert(field_name, value);
        }
        Ok(Value::struct_val(result_map))
    }

    fn eval_tuple_element(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "tupleElement requires 2 arguments".to_string(),
            ));
        }
        let tuple_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let index_or_name = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if tuple_val.is_null() {
            return Ok(Value::null());
        }

        let struct_map = tuple_val.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE/STRUCT".to_string(),
            actual: tuple_val.data_type().to_string(),
        })?;

        if let Some(idx) = index_or_name.as_i64() {
            let idx = idx as usize;
            if idx == 0 || idx > struct_map.len() {
                return Err(Error::invalid_query(format!(
                    "Tuple index {} out of bounds (tuple has {} elements)",
                    idx,
                    struct_map.len()
                )));
            }
            Ok(struct_map
                .get_index(idx - 1)
                .map(|(_, v)| v.clone())
                .unwrap_or(Value::null()))
        } else if let Some(name) = index_or_name.as_str() {
            if let Some(value) = struct_map.get(name) {
                Ok(value.clone())
            } else if let Some((_, value)) = struct_map
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
            {
                Ok(value.clone())
            } else {
                Err(Error::invalid_query(format!(
                    "Tuple does not have field '{}'",
                    name
                )))
            }
        } else {
            Err(Error::InvalidQuery(
                "tupleElement second argument must be an integer index or string field name"
                    .to_string(),
            ))
        }
    }

    fn eval_untuple(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "untuple requires 1 argument".to_string(),
            ));
        }
        Self::evaluate_expr(&args[0], batch, row_idx)
    }

    fn eval_tuple_hamming_distance(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "tupleHammingDistance requires 2 arguments".to_string(),
            ));
        }
        let tuple1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let tuple2 = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let struct1 = tuple1.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE".to_string(),
            actual: tuple1.data_type().to_string(),
        })?;
        let struct2 = tuple2.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE".to_string(),
            actual: tuple2.data_type().to_string(),
        })?;

        if struct1.len() != struct2.len() {
            return Err(Error::invalid_query(
                "Tuples must have the same number of elements for hamming distance",
            ));
        }

        let mut distance = 0i64;
        for ((_, v1), (_, v2)) in struct1.iter().zip(struct2.iter()) {
            if v1 != v2 {
                distance += 1;
            }
        }
        Ok(Value::int64(distance))
    }

    fn eval_tuple_plus(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::tuple_binary_op(args, batch, row_idx, |a, b| a + b)
    }

    fn eval_tuple_minus(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::tuple_binary_op(args, batch, row_idx, |a, b| a - b)
    }

    fn eval_tuple_multiply(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::tuple_binary_op(args, batch, row_idx, |a, b| a * b)
    }

    fn eval_tuple_divide(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::tuple_binary_op_float(args, batch, row_idx, |a, b| a / b)
    }

    fn eval_tuple_negate(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "tupleNegate requires 1 argument".to_string(),
            ));
        }
        let tuple_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let struct_map = tuple_val.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE".to_string(),
            actual: tuple_val.data_type().to_string(),
        })?;

        let mut result = IndexMap::new();
        for (i, (_, v)) in struct_map.iter().enumerate() {
            let negated = if let Some(n) = v.as_i64() {
                Value::int64(-n)
            } else if let Some(f) = v.as_f64() {
                Value::float64(-f)
            } else {
                v.clone()
            };
            result.insert((i + 1).to_string(), negated);
        }
        Ok(Value::struct_val(result))
    }

    fn eval_tuple_multiply_by_number(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::tuple_scalar_op(args, batch, row_idx, |a, b| a * b)
    }

    fn eval_tuple_divide_by_number(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::tuple_scalar_op_float(args, batch, row_idx, |a, b| a / b)
    }

    fn eval_tuple_concat(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        let mut result = IndexMap::new();
        let mut field_idx = 1;

        for arg in args {
            let tuple_val = Self::evaluate_expr(arg, batch, row_idx)?;
            let struct_map = tuple_val.as_struct().ok_or_else(|| Error::TypeMismatch {
                expected: "TUPLE".to_string(),
                actual: tuple_val.data_type().to_string(),
            })?;

            for (_, v) in struct_map.iter() {
                result.insert(field_idx.to_string(), v.clone());
                field_idx += 1;
            }
        }
        Ok(Value::struct_val(result))
    }

    fn eval_tuple_int_div(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::tuple_binary_op(args, batch, row_idx, |a, b| if b != 0 { a / b } else { 0 })
    }

    fn eval_tuple_int_div_or_zero(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::tuple_binary_op(args, batch, row_idx, |a, b| if b != 0 { a / b } else { 0 })
    }

    fn eval_tuple_modulo(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::tuple_binary_op(args, batch, row_idx, |a, b| if b != 0 { a % b } else { 0 })
    }

    fn eval_tuple_modulo_by_number(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::tuple_scalar_op(args, batch, row_idx, |a, b| if b != 0 { a % b } else { 0 })
    }

    fn eval_tuple_to_name_value_pairs(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "tupleToNameValuePairs requires 1 argument".to_string(),
            ));
        }
        let tuple_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let struct_map = tuple_val.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE".to_string(),
            actual: tuple_val.data_type().to_string(),
        })?;

        let pairs: Vec<Value> = struct_map
            .iter()
            .map(|(k, v)| {
                let mut pair = IndexMap::new();
                pair.insert("1".to_string(), Value::string(k.clone()));
                pair.insert("2".to_string(), v.clone());
                Value::struct_val(pair)
            })
            .collect();

        Ok(Value::array(pairs))
    }

    fn eval_tuple_names(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "tupleNames requires 1 argument".to_string(),
            ));
        }
        let tuple_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let struct_map = tuple_val.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE".to_string(),
            actual: tuple_val.data_type().to_string(),
        })?;

        let names: Vec<Value> = struct_map
            .keys()
            .map(|k| Value::string(k.clone()))
            .collect();

        Ok(Value::array(names))
    }

    fn tuple_binary_op<F>(args: &[Expr], batch: &Table, row_idx: usize, op: F) -> Result<Value>
    where
        F: Fn(i64, i64) -> i64,
    {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "Tuple binary operation requires 2 arguments".to_string(),
            ));
        }
        let tuple1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let tuple2 = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let struct1 = tuple1.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE".to_string(),
            actual: tuple1.data_type().to_string(),
        })?;
        let struct2 = tuple2.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE".to_string(),
            actual: tuple2.data_type().to_string(),
        })?;

        if struct1.len() != struct2.len() {
            return Err(Error::invalid_query(
                "Tuples must have the same number of elements",
            ));
        }

        let mut result = IndexMap::new();
        for (i, ((_, v1), (_, v2))) in struct1.iter().zip(struct2.iter()).enumerate() {
            let n1 = v1.as_i64().unwrap_or(0);
            let n2 = v2.as_i64().unwrap_or(0);
            result.insert((i + 1).to_string(), Value::int64(op(n1, n2)));
        }
        Ok(Value::struct_val(result))
    }

    fn tuple_binary_op_float<F>(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        op: F,
    ) -> Result<Value>
    where
        F: Fn(f64, f64) -> f64,
    {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "Tuple binary operation requires 2 arguments".to_string(),
            ));
        }
        let tuple1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let tuple2 = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let struct1 = tuple1.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE".to_string(),
            actual: tuple1.data_type().to_string(),
        })?;
        let struct2 = tuple2.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE".to_string(),
            actual: tuple2.data_type().to_string(),
        })?;

        if struct1.len() != struct2.len() {
            return Err(Error::invalid_query(
                "Tuples must have the same number of elements",
            ));
        }

        let mut result = IndexMap::new();
        for (i, ((_, v1), (_, v2))) in struct1.iter().zip(struct2.iter()).enumerate() {
            let n1 = v1
                .as_f64()
                .or_else(|| v1.as_i64().map(|n| n as f64))
                .unwrap_or(0.0);
            let n2 = v2
                .as_f64()
                .or_else(|| v2.as_i64().map(|n| n as f64))
                .unwrap_or(0.0);
            result.insert((i + 1).to_string(), Value::float64(op(n1, n2)));
        }
        Ok(Value::struct_val(result))
    }

    fn tuple_scalar_op<F>(args: &[Expr], batch: &Table, row_idx: usize, op: F) -> Result<Value>
    where
        F: Fn(i64, i64) -> i64,
    {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "Tuple scalar operation requires 2 arguments".to_string(),
            ));
        }
        let tuple_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let scalar = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let struct_map = tuple_val.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE".to_string(),
            actual: tuple_val.data_type().to_string(),
        })?;
        let scalar_n = scalar.as_i64().unwrap_or(1);

        let mut result = IndexMap::new();
        for (i, (_, v)) in struct_map.iter().enumerate() {
            let n = v.as_i64().unwrap_or(0);
            result.insert((i + 1).to_string(), Value::int64(op(n, scalar_n)));
        }
        Ok(Value::struct_val(result))
    }

    fn tuple_scalar_op_float<F>(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        op: F,
    ) -> Result<Value>
    where
        F: Fn(f64, f64) -> f64,
    {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "Tuple scalar operation requires 2 arguments".to_string(),
            ));
        }
        let tuple_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let scalar = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let struct_map = tuple_val.as_struct().ok_or_else(|| Error::TypeMismatch {
            expected: "TUPLE".to_string(),
            actual: tuple_val.data_type().to_string(),
        })?;
        let scalar_n = scalar
            .as_f64()
            .or_else(|| scalar.as_i64().map(|n| n as f64))
            .unwrap_or(1.0);

        let mut result = IndexMap::new();
        for (i, (_, v)) in struct_map.iter().enumerate() {
            let n = v
                .as_f64()
                .or_else(|| v.as_i64().map(|x| x as f64))
                .unwrap_or(0.0);
            result.insert((i + 1).to_string(), Value::float64(op(n, scalar_n)));
        }
        Ok(Value::struct_val(result))
    }
}
